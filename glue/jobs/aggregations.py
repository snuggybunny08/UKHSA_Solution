
import sys
import logging
from datetime import datetime, timedelta
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'SOURCE_DATABASE',
    'SOURCE_TABLE',
    'TARGET_BUCKET'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

def create_daily_aggregations(df):
    """Create daily aggregations"""
    
    daily_agg = df.groupBy(
        F.date_format('report_date', 'yyyy-MM-dd').alias('date'),
        'region',
        'disease_name'
    ).agg(
        F.sum('case_count').alias('total_cases'),
        F.sum('death_count').alias('total_deaths'),
        F.sum('hospitalization_count').alias('total_hospitalizations'),
        F.count('*').alias('record_count'),
        F.avg('case_count').alias('avg_cases'),
        F.stddev('case_count').alias('stddev_cases'),
        F.min('case_count').alias('min_cases'),
        F.max('case_count').alias('max_cases')
    ).withColumn('aggregation_level', F.lit('daily'))
    
    return daily_agg

def create_weekly_aggregations(df):
    """Create weekly aggregations"""
    
    weekly_agg = df.withColumn(
        'week_start',
        F.date_trunc('week', F.col('report_date'))
    ).groupBy(
        'week_start',
        'region',
        'disease_name'
    ).agg(
        F.sum('case_count').alias('total_cases'),
        F.sum('death_count').alias('total_deaths'),
        F.avg('case_count').alias('avg_daily_cases'),
        F.collect_list('report_date').alias('dates_included')
    ).withColumn('aggregation_level', F.lit('weekly'))
    
    # Calculate week-over-week change
    window_spec = Window.partitionBy('region', 'disease_name').orderBy('week_start')
    
    weekly_agg = weekly_agg.withColumn(
        'prev_week_cases',
        F.lag('total_cases').over(window_spec)
    ).withColumn(
        'week_over_week_change',
        F.when(F.col('prev_week_cases').isNotNull(),
               ((F.col('total_cases') - F.col('prev_week_cases')) / F.col('prev_week_cases')) * 100
        ).otherwise(None)
    )
    
    return weekly_agg

def create_monthly_aggregations(df):
    """Create monthly aggregations"""
    
    monthly_agg = df.groupBy(
        F.date_format('report_date', 'yyyy-MM').alias('month'),
        'region',
        'disease_name'
    ).agg(
        F.sum('case_count').alias('total_cases'),
        F.sum('death_count').alias('total_deaths'),
        F.avg('case_count').alias('avg_daily_cases'),
        F.min('report_date').alias('period_start'),
        F.max('report_date').alias('period_end')
    ).withColumn('aggregation_level', F.lit('monthly'))
    
    return monthly_agg

def create_regional_rankings(df):
    """Create regional rankings"""
    
    window_spec = Window.partitionBy('report_date').orderBy(F.desc('case_count'))
    
    rankings = df.groupBy('report_date', 'region').agg(
        F.sum('case_count').alias('total_cases')
    ).withColumn(
        'rank',
        F.rank().over(window_spec)
    ).withColumn(
        'percentile',
        F.percent_rank().over(window_spec)
    )
    
    return rankings

def main():
    """Main execution"""
    
    try:
        # Read source data
        logger.info(f"Reading data from {args['SOURCE_DATABASE']}.{args['SOURCE_TABLE']}")
        
        source_df = glueContext.create_dynamic_frame.from_catalog(
            database=args['SOURCE_DATABASE'],
            table_name=args['SOURCE_TABLE']
        ).toDF()
        
        # Create various aggregations
        logger.info("Creating daily aggregations...")
        daily_agg = create_daily_aggregations(source_df)
        
        logger.info("Creating weekly aggregations...")
        weekly_agg.coalesce(1).write.mode('overwrite').parquet(f"{output_base}/weekly")
        monthly_agg.coalesce(1).write.mode('overwrite').parquet(f"{output_base}/monthly")
        rankings.write.mode('overwrite').parquet(f"{output_base}/rankings")
        
        logger.info("Aggregations completed successfully")
        
        # Commit job
        job.commit()
        
    except Exception as e:
        logger.error(f"Aggregation job failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()

