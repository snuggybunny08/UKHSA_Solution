
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
        
        return df = create_weekly_aggregations(source_df)

        
        logger.info("Creating monthly aggregations...")
        monthly_agg = create_monthly_aggregations(source_df)
        
        logger.info("Creating regional rankings...")
        rankings = create_regional_rankings(source_df)
        
        # Write aggregations to S3
        output_base = f"s3://{args['TARGET_BUCKET']}/aggregations"
        
        daily_agg.coalesce(1).write.mode('overwrite').parquet(f"{output_base}/daily")
        weekly_a
        
        
    @staticmethod
    def apply_data_quality_flags(df: DataFrame) -> DataFrame:
        """Apply data quality flags for monitoring"""
        
        # Flag records with missing critical fields
        critical_fields = ['report_date', 'region', 'case_count']
        df = df.withColumn(
            'has_missing_critical',
            F.when(
                F.col(critical_fields[0]).isNull() |
                F.col(critical_fields[1]).isNull() |
                F.col(critical_fields[2]).isNull(),
                True
            ).otherwise(False)
        )
        
        # Flag potential outliers using IQR method
        if 'case_count' in df.columns:
            quantiles = df.select(
                F.expr('percentile_approx(case_count, 0.25)').alias('q1'),
                F.expr('percentile_approx(case_count, 0.75)').alias('q3')
            ).collect()[0]
            
            if quantiles['q1'] is not None and quantiles['q3'] is not None:
                iqr = quantiles['q3'] - quantiles['q1']
                lower_bound = quantiles['q1'] - 1.5 * iqr
                upper_bound = quantiles['q3'] + 1.5 * iqr
                
                df = df.withColumn(
                    'is_outlier',
                    (F.col('case_count') < lower_bound) | 
                    (F.col('case_count') > upper_bound)
                )
            else:
                df = df.withColumn('is_outlier', F.lit(False))
        
        # Flag duplicate records
        df = df.withColumn(
            'is_duplicate',
            F.count('*').over(
                Window.partitionBy('report_date', 'region', 'disease_name')
            ) > 1
        )
        
        # Calculate overall quality score
        df = df.withColumn(
            'quality_score',
            F.when(F.col('has_missing_critical'), 0)
            .when(F.col('is_outlier'), 0.7)
            .when(F.col('is_duplicate'), 0.8)
            .when(~F.col('valid_region'), 0.9)
            .otherwise(1.0)
        )
        
        return df
    
    @staticmethod
    def anonymize_pii(df: DataFrame) -> DataFrame:
        """Anonymize potential PII fields"""
        
        # Hash any ID fields
        id_columns = [col for col in df.columns if 'id' in col.lower() and col != 'report_id']
        for col in id_columns:
            df = df.withColumn(
                col,
                F.sha2(F.col(col).cast('string'), 256)
            )
        
        # Remove or mask name fields
        name_columns = [col for col in df.columns if 'name' in col.lower() and col != 'disease_name']
        for col in name_columns:
            df = df.withColumn(col, F.lit('***REDACTED***'))
        
        # Generalize postcodes to first part only
        if 'postcode' in df.columns:
            df = df.withColumn(
                'postcode',
                F.regexp_extract(F.col('postcode'), r'^([A-Z]{1,2}\d{1,2})', 1)
            )
        
        return df
    
    @staticmethod
    def create_summary_statistics(df: DataFrame, group_by_cols: List[str]) -> DataFrame:
        """Create summary statistics for grouped data"""
        
        agg_expressions = []
        
        # Add count
        agg_expressions.append(F.count('*').alias('record_count'))
        
        # Add numeric column statistics
        numeric_cols = [f.name for f in df.schema.fields 
                       if isinstance(f.dataType, (IntegerType, LongType, FloatType, DoubleType))]
        
        for col in numeric_cols:
            if col not in group_by_cols:
                agg_expressions.extend([
                    F.sum(col).alias(f'{col}_sum'),
                    F.avg(col).alias(f'{col}_avg'),
                    F.min(col).alias(f'{col}_min'),
                    F.max(col).alias(f'{col}_max'),
                    F.stddev(col).alias(f'{col}_stddev')
                ])
        
        # Add date range if report_date exists
        if 'report_date' in df.columns and 'report_date' not in group_by_cols:
            agg_expressions.extend([
                F.min('report_date').alias('earliest_date'),
                F.max('report_date').alias('latest_date')
            ])
        
        return df.groupBy(*group_by_cols).agg(*agg_expressions)


def create_weekly_aggregations(df: DataFrame) -> DataFrame:
    """Create weekly aggregations from daily data"""
    
    # Add week and year columns
    df_with_week = df.withColumn('week', F.weekofyear('report_date')) \
                     .withColumn('year', F.year('report_date'))
    
    # Group by week, year, and region
    weekly_agg = df_with_week.groupBy('year', 'week', 'region').agg(
        F.sum('case_count').alias('weekly_cases'),
        F.sum('death_count').alias('weekly_deaths'),
        F.avg('case_count').alias('avg_daily_cases'),
        F.max('case_count').alias('max_daily_cases'),
        F.min('case_count').alias('min_daily_cases'),
        F.count('*').alias('days_reported')
    )
    
    # Add week-over-week change
    window_spec = Window.partitionBy('region').orderBy('year', 'week')
    weekly_agg = weekly_agg.withColumn(
        'wow_change',
        F.col('weekly_cases') - F.lag('weekly_cases', 1).over(window_spec)
    ).withColumn(
        'wow_change_pct',
        F.when(F.lag('weekly_cases', 1).over(window_spec) > 0,
               ((F.col('weekly_cases') - F.lag('weekly_cases', 1).over(window_spec)) / 
                F.lag('weekly_cases', 1).over(window_spec)) * 100)
        .otherwise(None)
    )
    
    return weekly_agg


def create_monthly_aggregations(df: DataFrame) -> DataFrame:
    """Create monthly aggregations from daily data"""
    
    # Add month and year columns
    df_with_month = df.withColumn('month', F.month('report_date')) \
                      .withColumn('year', F.year('report_date'))
    
    # Group by year, month, and region
    monthly_agg = df_with_month.groupBy('year', 'month', 'region').agg(
        F.sum('case_count').alias('monthly_cases'),
        F.sum('death_count').alias('monthly_deaths'),
        F.avg('case_count').alias('avg_daily_cases'),
        F.stddev('case_count').alias('stddev_daily_cases'),
        F.count('*').alias('days_reported'),
        F.countDistinct('disease_name').alias('unique_diseases')
    )
    
    # Add month-over-month change
    window_spec = Window.partitionBy('region').orderBy('year', 'month')
    monthly_agg = monthly_agg.withColumn(
        'mom_change',
        F.col('monthly_cases') - F.lag('monthly_cases', 1).over(window_spec)
    ).withColumn(
        'mom_change_pct',
        F.when(F.lag('monthly_cases', 1).over(window_spec) > 0,
               ((F.col('monthly_cases') - F.lag('monthly_cases', 1).over(window_spec)) / 
                F.lag('monthly_cases', 1).over(window_spec)) * 100)
        .otherwise(None)
    )
    
    # Add year-over-year comparison
    window_yoy = Window.partitionBy('region', 'month').orderBy('year')
    monthly_agg = monthly_agg.withColumn(
        'yoy_change',
        F.col('monthly_cases') - F.lag('monthly_cases', 1).over(window_yoy)
    )
    
    return monthly_agg


def create_regional_rankings(df: DataFrame) -> DataFrame:
    """Create regional rankings based on various metrics"""
    
    # Calculate total metrics by region
    regional_stats = df.groupBy('region').agg(
        F.sum('case_count').alias('total_cases'),
        F.sum('death_count').alias('total_deaths'),
        F.avg('case_count').alias('avg_cases_per_day'),
        F.count('*').alias('reports_submitted')
    )
    
    # Calculate case fatality rate
    regional_stats = regional_stats.withColumn(
        'case_fatality_rate',
        F.when(F.col('total_cases') > 0,
               (F.col('total_deaths') / F.col('total_cases')) * 100)
        .otherwise(0)
    )
    
    # Add rankings
    window_cases = Window.orderBy(F.desc('total_cases'))
    window_cfr = Window.orderBy(F.desc('case_fatality_rate'))
    window_reports = Window.orderBy(F.desc('reports_submitted'))
    
    regional_stats = regional_stats.withColumn('cases_rank', F.rank().over(window_cases)) \
                                   .withColumn('cfr_rank', F.rank().over(window_cfr)) \
                                   .withColumn('reporting_rank', F.rank().over(window_reports))
    
    return regional_stats


# Completion of the cut-off aggregation code
def main():
    """Main execution function for aggregations"""
    
    # Assuming args and source_df are already defined from the parent context
    logger.info("Creating weekly aggregations...")
    weekly_agg = create_weekly_aggregations(source_df)
    
    logger.info("Creating monthly aggregations...")
    monthly_agg = create_monthly_aggregations(source_df)
    
    logger.info("Creating regional rankings...")
    rankings = create_regional_rankings(source_df)
    
    # Write aggregations to S3
    output_base = f"s3://{args['TARGET_BUCKET']}/aggregations"
    
    # Write with proper partitioning and optimization
    weekly_agg.repartition('year', 'week') \
              .sortWithinPartitions('region') \
              .write.mode('overwrite') \
              .partitionBy('year', 'week') \
              .parquet(f"{output_base}/weekly")
    
    monthly_agg.repartition('year', 'month') \
               .sortWithinPartitions('region') \
               .write.mode('overwrite') \
               .partitionBy('year', 'month') \
               .parquet(f"{output_base}/monthly")
    
    rankings.coalesce(1) \
            .write.mode('overwrite') \
            .parquet(f"{output_base}/rankings")
    
    logger.info(f"Aggregations completed successfully")
    logger.info(f"Weekly: {weekly_agg.count()} records")
    logger.info(f"Monthly: {monthly_agg.count()} records")
    logger.info(f"Rankings: {rankings.count()} regions")
    
    return {
        'weekly_count': weekly_agg.count(),
        'monthly_count': monthly_agg.count(),
        'rankings_count': rankings.count()
    }