# ============================================================
# glue/libraries/transformations.py - Reusable transformation functions
# ============================================================

import logging
from typing import Dict, List, Any, Optional
from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import *
import re

logger = logging.getLogger(__name__)

class DataTransformations:
    """Reusable data transformation functions"""
    
    @staticmethod
    def standardize_column_names(df: DataFrame) -> DataFrame:
        """Standardize column names to snake_case"""
        for col in df.columns:
            new_col = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1_\2', col)
            new_col = re.sub(r'([a-z\d])([A-Z])', r'\1_\2', new_col)
            new_col = new_col.lower().replace(' ', '_').replace('-', '_')
            new_col = re.sub(r'[^a-zA-Z0-9_]', '', new_col)
            new_col = re.sub(r'_+', '_', new_col)
            df = df.withColumnRenamed(col, new_col)
        return df
    
    @staticmethod
    def add_audit_columns(df: DataFrame, source_system: str = "UKHSA") -> DataFrame:
        """Add audit columns to dataframe"""
        return df.withColumn('etl_timestamp', F.current_timestamp()) \
                 .withColumn('etl_date', F.current_date()) \
                 .withColumn('source_system', F.lit(source_system)) \
                 .withColumn('data_version', F.lit('1.0'))
    
    @staticmethod
    def handle_null_values(df: DataFrame, strategy: str = 'fill') -> DataFrame:
        """Handle null values based on strategy"""
        if strategy == 'drop':
            return df.dropna()
        elif strategy == 'fill':
            # Fill numeric columns with 0
            numeric_cols = [f.name for f in df.schema.fields 
                          if isinstance(f.dataType, (IntegerType, LongType, FloatType, DoubleType))]
            
            # Fill string columns with 'Unknown'
            string_cols = [f.name for f in df.schema.fields 
                         if isinstance(f.dataType, StringType)]
            
            for col in numeric_cols:
                df = df.fillna({col: 0})
            
            for col in string_cols:
                df = df.fillna({col: 'Unknown'})
        
        return df
    
    @staticmethod
    def normalize_dates(df: DataFrame, date_columns: List[str]) -> DataFrame:
        """Normalize date columns to standard format"""
        for col in date_columns:
            if col in df.columns:
                # Try multiple date formats
                df = df.withColumn(
                    col,
                    F.coalesce(
                        F.to_date(F.col(col), 'yyyy-MM-dd'),
                        F.to_date(F.col(col), 'dd/MM/yyyy'),
                        F.to_date(F.col(col), 'MM/dd/yyyy'),
                        F.to_date(F.col(col), 'dd-MM-yyyy')
                    )
                )
        return df
    
    @staticmethod
    def calculate_derived_metrics(df: DataFrame) -> DataFrame:
        """Calculate derived metrics"""
        
        # Calculate case fatality rate
        if 'death_count' in df.columns and 'case_count' in df.columns:
            df = df.withColumn(
                'case_fatality_rate',
                F.when(F.col('case_count') > 0, 
                       (F.col('death_count') / F.col('case_count')) * 100)
                .otherwise(0)
            )
        
        # Calculate 7-day rolling average
        if 'report_date' in df.columns and 'case_count' in df.columns:
            from pyspark.sql.window import Window
            
            window_spec = Window.partitionBy('region').orderBy('report_date').rowsBetween(-6, 0)
            df = df.withColumn('seven_day_avg', F.avg('case_count').over(window_spec))
        
        # Add age group categories
        if 'age' in df.columns:
            df = df.withColumn(
                'age_group',
                F.when(F.col('age') < 18, '0-17')
                .when((F.col('age') >= 18) & (F.col('age') < 30), '18-29')
                .when((F.col('age') >= 30) & (F.col('age') < 50), '30-49')
                .when((F.col('age') >= 50) & (F.col('age') < 70), '50-69')
                .otherwise('70+')
            )
        
        return df
    
    @staticmethod
    def validate_uk_regions(df: DataFrame, region_column: str = 'region') -> DataFrame:
        """Validate and standardize UK regions"""
        valid_regions = [
            "London", "South East", "South West", "East of England",
            "East Midlands", "West Midlands", "Yorkshire and the Humber",
            "North East", "North West", "Wales", "Scotland", "Northern Ireland"
        ]
        
        if region_column in df.columns:
            # Create mapping for common variations
            region_mapping = {
                'london': 'London',
                'south_east': 'South East',
                'south_west': 'South West',
                'east_england': 'East of England',
                'east_midlands': 'East Midlands',
                'west_midlands': 'West Midlands',
                'yorkshire': 'Yorkshire and the Humber',
                'north_east': 'North East',
                'north_west': 'North West',
                'wales': 'Wales',
                'scotland': 'Scotland',
                'northern_ireland': 'Northern Ireland',
                'ni': 'Northern Ireland'
            }
            
            # Apply mapping
            for old, new in region_mapping.items():
                df = df.withColumn(
                    region_column,
                    F.when(F.lower(F.col(region_column)) == old, new)
                    .otherwise(F.col(region_column))
                )
            
            # Mark invalid regions
            df = df.withColumn(
                'valid_region',
                F.col(region_column).isin(valid_regions)
            )
        
        return dfgg = create_weekly_aggregations(source_df)
        
        logger.info("Creating monthly aggregations...")
        monthly_agg = create_monthly_aggregations(source_df)
        
        logger.info("Creating regional rankings...")
        rankings = create_regional_rankings(source_df)
        
        # Write aggregations to S3
        output_base = f"s3://{args['TARGET_BUCKET']}/aggregations"
        
        daily_agg.coalesce(1).write.mode('overwrite').parquet(f"{output_base}/daily")
        weekly_a