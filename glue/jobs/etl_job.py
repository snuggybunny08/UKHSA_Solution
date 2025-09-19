# etl_job.py - AWS Glue ETL Job for UKHSA Data Platform

import sys
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Get job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'SOURCE_BUCKET',
    'TARGET_BUCKET',
    'DATABASE_NAME',
    'TABLE_NAME',
    'DATASET_TYPE'
])

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ============================================================
# Configuration
# ============================================================

SOURCE_BUCKET = args['SOURCE_BUCKET']
TARGET_BUCKET = args['TARGET_BUCKET']
DATABASE_NAME = args['DATABASE_NAME']
TABLE_NAME = args['TABLE_NAME']
DATASET_TYPE = args.get('DATASET_TYPE', 'communicable_disease')

# Data quality thresholds
QUALITY_THRESHOLDS = {
    'null_percentage': 0.1,  # Maximum 10% null values
    'duplicate_percentage': 0.05,  # Maximum 5% duplicates
    'outlier_percentage': 0.02  # Maximum 2% outliers
}

# ============================================================
# Helper Functions
# ============================================================

def log_metrics(df, stage: str):
    """Log DataFrame metrics for monitoring"""
    record_count = df.count()
    column_count = len(df.columns)
    
    logger.info(f"""
    Stage: {stage}
    Record Count: {record_count}
    Column Count: {column_count}
    Schema: {df.schema.simpleString()}
    """)
    
    return {
        'stage': stage,
        'record_count': record_count,
        'column_count': column_count,
        'timestamp': datetime.utcnow().isoformat()
    }

def profile_data(df):
    """Generate data profiling statistics"""
    profiling_results = {}
    
    for col_name in df.columns:
        col_type = str(df.schema[col_name].dataType)
        
        # Basic statistics
        total_count = df.count()
        null_count = df.filter(F.col(col_name).isNull()).count()
        distinct_count = df.select(col_name).distinct().count()
        
        profiling_results[col_name] = {
            'data_type': col_type,
            'null_count': null_count,
            'null_percentage': (null_count / total_count) * 100 if total_count > 0 else 0,
            'distinct_count': distinct_count,
            'completeness': ((total_count - null_count) / total_count) * 100 if total_count > 0 else 0
        }
        
        # Additional statistics for numeric columns
        if col_type in ['IntegerType', 'LongType', 'FloatType', 'DoubleType']:
            stats = df.select(
                F.min(col_name).alias('min'),
                F.max(col_name).alias('max'),
                F.avg(col_name).alias('mean'),
                F.stddev(col_name).alias('stddev'),
                F.expr(f'percentile_approx({col_name}, 0.5)').alias('median')
            ).collect()[0]
            
            profiling_results[col_name].update({
                'min': stats['min'],
                'max': stats['max'],
                'mean': stats['mean'],
                'stddev': stats['stddev'],
                'median': stats['median']
            })
    
    return profiling_results

# ============================================================
# Data Transformation Functions
# ============================================================

def clean_column_names(df):
    """Standardize column names"""
    for col in df.columns:
        new_col = col.lower().replace(' ', '_').replace('-', '_')
        df = df.withColumnRenamed(col, new_col)
    return df

def apply_data_types(df, dataset_type: str):
    """Apply appropriate data types based on dataset type"""
    
    type_mappings = {
        'communicable_disease': {
            'report_date': 'date',
            'case_count': 'integer',
            'death_count': 'integer',
            'population': 'long',
            'incidence_rate': 'double'
        },
        'immunisation': {
            'vaccination_date': 'date',
            'dose_number': 'integer',
            'age': 'integer',
            'coverage_percentage': 'double'
        },
        'surveillance': {
            'surveillance_date': 'date',
            'sample_count': 'integer',
            'positive_count': 'integer',
            'positivity_rate': 'double'
        }
    }
    
    mappings = type_mappings.get(dataset_type, {})
    
    for col, dtype in mappings.items():
        if col in df.columns:
            if dtype == 'date':
                df = df.withColumn(col, F.to_date(F.col(col), 'yyyy-MM-dd'))
            elif dtype == 'integer':
                df = df.withColumn(col, F.col(col).cast(IntegerType()))
            elif dtype == 'long':
                df = df.withColumn(col, F.col(col).cast(LongType()))
            elif dtype == 'double':
                df = df.withColumn(col, F.col(col).cast(DoubleType()))
    
    return df

def handle_missing_values(df):
    """Handle missing values with appropriate strategies"""
    
    for col_name in df.columns:
        col_type = str(df.schema[col_name].dataType)
        
        if col_type in ['IntegerType', 'LongType', 'FloatType', 'DoubleType']:
            # For numeric columns, fill with median
            median_value = df.select(F.expr(f'percentile_approx({col_name}, 0.5)')).collect()[0][0]
            if median_value is not None:
                df = df.fillna({col_name: median_value})
        elif col_type == 'StringType':
            # For string columns, fill with 'Unknown'
            df = df.fillna({col_name: 'Unknown'})
        elif col_type == 'DateType':
            # For date columns, forward fill
            window_spec = Window.orderBy('report_date').rowsBetween(Window.unboundedPreceding, 0)
            df = df.withColumn(
                col_name,
                F.when(F.col(col_name).isNull(), 
                       F.last(F.col(col_name), ignorenulls=True).over(window_spec))
                .otherwise(F.col(col_name))
            )
    
    return df

def remove_duplicates(df):
    """Remove duplicate records"""
    initial_count = df.count()
    df_dedup = df.dropDuplicates()
    final_count = df_dedup.count()
    
    if initial_count != final_count:
        logger.info(f"Removed {initial_count - final_count} duplicate records")
    
    return df_dedup

def detect_and_handle_outliers(df):
    """Detect and handle outliers using IQR method"""
    
    numeric_cols = [f.name for f in df.schema.fields 
                    if isinstance(f.dataType, (IntegerType, LongType, FloatType, DoubleType))]
    
    for col in numeric_cols:
        # Calculate IQR
        quantiles = df.select(
            F.expr(f'percentile_approx({col}, 0.25)').alias('q1'),
            F.expr(f'percentile_approx({col}, 0.75)').alias('q3')
        ).collect()[0]
        
        q1, q3 = quantiles['q1'], quantiles['q3']
        
        if q1 is not None and q3 is not None:
            iqr = q3 - q1
            lower_bound = q1 - 1.5 * iqr
            upper_bound = q3 + 1.5 * iqr
            
            # Cap outliers
            df = df.withColumn(
                col,
                F.when(F.col(col) < lower_bound, lower_bound)
                .when(F.col(col) > upper_bound, upper_bound)
                .otherwise(F.col(col))
            )
    
    return df

def add_metadata_columns(df):
    """Add metadata columns for tracking"""
    
    df = df.withColumn('etl_timestamp', F.current_timestamp()) \
           .withColumn('etl_date', F.current_date()) \
           .withColumn('etl_job_name', F.lit(args['JOB_NAME'])) \
           .withColumn('source_system', F.lit('UKHSA_DA_Platform')) \
           .withColumn('data_version', F.lit('1.0'))
    
    return df

def apply_business_rules(df, dataset_type: str):
    """Apply dataset-specific business rules"""
    
    if dataset_type == 'communicable_disease':
        # Ensure case counts are non-negative
        df = df.withColumn('case_count', 
                          F.when(F.col('case_count') < 0, 0)
                          .otherwise(F.col('case_count')))
        
        # Calculate 7-day rolling average
        window_spec = Window.partitionBy('disease_name').orderBy('report_date').rowsBetween(-6, 0)
        df = df.withColumn('seven_day_avg', F.avg('case_count').over(window_spec))
        
        # Calculate incidence rate if not present
        if 'incidence_rate' not in df.columns and 'population' in df.columns:
            df = df.withColumn('incidence_rate', 
                             (F.col('case_count') / F.col('population')) * 100000)
    
    elif dataset_type == 'immunisation':
        # Ensure dose numbers are valid
        df = df.filter((F.col('dose_number') >= 1) & (F.col('dose_number') <= 5))
        
        # Calculate coverage percentage by age group
        window_spec = Window.partitionBy('age_group', 'vaccine_type')
        df = df.withColumn('cumulative_doses', F.sum('dose_count').over(window_spec))
    
    elif dataset_type == 'surveillance':
        # Calculate positivity rate
        df = df.withColumn('positivity_rate',
                          F.when(F.col('sample_count') > 0,
                                (F.col('positive_count') / F.col('sample_count')) * 100)
                          .otherwise(0))
    
    return df

def create_aggregations(df, dataset_type: str):
    """Create useful aggregations for reporting"""
    
    aggregations = {}
    
    if dataset_type == 'communicable_disease':
        # Weekly aggregation
        weekly_agg = df.groupBy(
            F.weekofyear('report_date').alias('week'),
            F.year('report_date').alias('year'),
            'disease_name'
        ).agg(
            F.sum('case_count').alias('total_cases'),
            F.sum('death_count').alias('total_deaths'),
            F.avg('incidence_rate').alias('avg_incidence_rate')
        )
        aggregations['weekly'] = weekly_agg
        
        # Regional aggregation
        if 'region' in df.columns:
            regional_agg = df.groupBy('region', 'disease_name').agg(
                F.sum('case_count').alias('total_cases'),
                F.avg('incidence_rate').alias('avg_incidence_rate')
            )
            aggregations['regional'] = regional_agg
    
    return aggregations

# ============================================================
# Quality Checks
# ============================================================

def perform_quality_checks(df) -> Dict:
    """Perform comprehensive data quality checks"""
    
    quality_results = {
        'passed': True,
        'checks': {}
    }
    
    total_records = df.count()
    
    # Check for null values
    for col in df.columns:
        null_count = df.filter(F.col(col).isNull()).count()
        null_percentage = (null_count / total_records) * 100 if total_records > 0 else 0
        
        quality_results['checks'][f'{col}_null_check'] = {
            'null_count': null_count,
            'null_percentage': null_percentage,
            'passed': null_percentage <= QUALITY_THRESHOLDS['null_percentage'] * 100
        }
        
        if null_percentage > QUALITY_THRESHOLDS['null_percentage'] * 100:
            quality_results['passed'] = False
    
    # Check for duplicates
    duplicate_count = total_records - df.dropDuplicates().count()
    duplicate_percentage = (duplicate_count / total_records) * 100 if total_records > 0 else 0
    
    quality_results['checks']['duplicate_check'] = {
        'duplicate_count': duplicate_count,
        'duplicate_percentage': duplicate_percentage,
        'passed': duplicate_percentage <= QUALITY_THRESHOLDS['duplicate_percentage'] * 100
    }
    
    if duplicate_percentage > QUALITY_THRESHOLDS['duplicate_percentage'] * 100:
        quality_results['passed'] = False
    
    return quality_results

# ============================================================
# Main ETL Process
# ============================================================

def main():
    """Main ETL process"""
    
    try:
        # 1. Read source data
        logger.info(f"Reading data from s3://{SOURCE_BUCKET}/validated/")
        
        input_df = spark.read \
            .option("mergeSchema", "true") \
            .parquet(f"s3://{SOURCE_BUCKET}/validated/{DATASET_TYPE}/*.parquet")
        
        metrics = {'stages': []}
        metrics['stages'].append(log_metrics(input_df, "RAW_DATA"))
        
        # 2. Data Cleaning
        logger.info("Starting data cleaning...")
        df = clean_column_names(input_df)
        df = apply_data_types(df, DATASET_TYPE)
        df = handle_missing_values(df)
        df = remove_duplicates(df)
        df = detect_and_handle_outliers(df)
        
        metrics['stages'].append(log_metrics(df, "CLEANED_DATA"))
        
        # 3. Apply Business Rules
        logger.info("Applying business rules...")
        df = apply_business_rules(df, DATASET_TYPE)
        df = add_metadata_columns(df)
        
        metrics['stages'].append(log_metrics(df, "TRANSFORMED_DATA"))
        
        # 4. Data Profiling
        logger.info("Profiling data...")
        profiling_results = profile_data(df)
        metrics['profiling'] = profiling_results
        
        # 5. Quality Checks
        logger.info("Performing quality checks...")
        quality_results = perform_quality_checks(df)
        metrics['quality'] = quality_results
        
        if not quality_results['passed']:
            logger.warning("Data quality checks failed, but continuing with flagged data")
        
        # 6. Create Aggregations
        logger.info("Creating aggregations...")
        aggregations = create_aggregations(df, DATASET_TYPE)
        
        # 7. Write to Target
        logger.info(f"Writing data to s3://{TARGET_BUCKET}/processed/")
        
        # Write main dataset
        output_path = f"s3://{TARGET_BUCKET}/processed/{DATASET_TYPE}/year={datetime.now().year}/month={datetime.now().month:02d}"
        
        df.coalesce(10) \
          .write \
          .mode("overwrite") \
          .option("compression", "snappy") \
          .partitionBy("etl_date") \
          .parquet(output_path)
        
        logger.info(f"Main dataset written to {output_path}")
        
        # Write aggregations
        for agg_name, agg_df in aggregations.items():
            agg_path = f"s3://{TARGET_BUCKET}/aggregations/{DATASET_TYPE}/{agg_name}"
            agg_df.coalesce(1) \
                  .write \
                  .mode("overwrite") \
                  .parquet(agg_path)
            logger.info(f"Aggregation '{agg_name}' written to {agg_path}")
        
        # 8. Update Glue Catalog
        logger.info("Updating Glue Catalog...")
        
        # Convert to DynamicFrame for Glue Catalog
        dynamic_frame = DynamicFrame.fromDF(df, glueContext, "dynamic_frame")
        
        # Write to Glue Catalog
        glueContext.write_dynamic_frame.from_catalog(
            frame=dynamic_frame,
            database=DATABASE_NAME,
            table_name=TABLE_NAME,
            transformation_ctx="write_to_catalog"
        )
        
        # 9. Write metrics to S3
        metrics_path = f"s3://{TARGET_BUCKET}/metrics/{DATASET_TYPE}/{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        import json
        metrics_json = json.dumps(metrics, indent=2)
        
        # Use boto3 to write metrics
        import boto3
        s3 = boto3.client('s3')
        s3.put_object(
            Bucket=TARGET_BUCKET.replace('s3://', ''),
            Key=metrics_path.replace(f's3://{TARGET_BUCKET}/', ''),
            Body=metrics_json,
            ContentType='application/json'
        )
        
        logger.info(f"Metrics written to {metrics_path}")
        
        # 10. Success notification
        logger.info("ETL process completed successfully")
        
        # Commit job
        job.commit()
        
        return {
            'status': 'SUCCESS',
            'records_processed': df.count(),
            'output_location': output_path,
            'metrics': metrics
        }
        
    except Exception as e:
        logger.error(f"ETL process failed: {str(e)}")
        
        # Log error details
        import traceback
        error_details = {
            'error': str(e),
            'traceback': traceback.format_exc(),
            'timestamp': datetime.utcnow().isoformat()
        }
        
        # Write error log to S3
        error_path = f"s3://{TARGET_BUCKET}/errors/{DATASET_TYPE}/{datetime.now().strftime('%Y%m%d_%H%M%S')}_error.json"
        
        import json
        import boto3
        s3 = boto3.client('s3')
        s3.put_object(
            Bucket=TARGET_BUCKET.replace('s3://', ''),
            Key=error_path.replace(f's3://{TARGET_BUCKET}/', ''),
            Body=json.dumps(error_details, indent=2),
            ContentType='application/json'
        )
        
        raise e

# ============================================================
# Script Execution
# ============================================================

if __name__ == "__main__":
    result = main()
    logger.info(f"ETL Result: {result}")