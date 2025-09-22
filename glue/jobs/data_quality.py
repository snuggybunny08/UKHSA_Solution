import sys
import logging
from datetime import datetime
from typing import Dict, List, Any

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
import boto3
import json

# Initialize logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Get job parameters
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'DATABASE_NAME',
    'TABLE_NAME',
    'SNS_TOPIC_ARN'
])

# Initialize Spark and Glue contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# SNS client for notifications
sns_client = boto3.client('sns')

class DataQualityChecker:
    """Data quality validation class"""
    
    def __init__(self, df: DataFrame):
        self.df = df
        self.results = {
            'timestamp': datetime.utcnow().isoformat(),
            'checks': [],
            'summary': {
                'total_checks': 0,
                'passed': 0,
                'failed': 0,
                'warnings': 0
            }
        }
    
    def check_completeness(self, threshold: float = 0.95) -> Dict:
        """Check data completeness"""
        total_rows = self.df.count()
        results = {}
        
        for column in self.df.columns:
            non_null_count = self.df.filter(F.col(column).isNotNull()).count()
            completeness = non_null_count / total_rows if total_rows > 0 else 0
            
            results[column] = {
                'completeness': completeness,
                'passed': completeness >= threshold
            }
        
        check_result = {
            'check_name': 'completeness',
            'status': 'PASSED' if all(r['passed'] for r in results.values()) else 'FAILED',
            'details': results
        }
        
        self._add_result(check_result)
        return check_result
    
    def check_uniqueness(self, columns: List[str]) -> Dict:
        """Check for unique key violations"""
        total_rows = self.df.count()
        unique_rows = self.df.select(columns).distinct().count()
        
        check_result = {
            'check_name': 'uniqueness',
            'status': 'PASSED' if total_rows == unique_rows else 'FAILED',
            'details': {
                'total_rows': total_rows,
                'unique_rows': unique_rows,
                'duplicate_count': total_rows - unique_rows,
                'columns_checked': columns
            }
        }
        
        self._add_result(check_result)
        return check_result
    
    def check_validity(self, rules: Dict[str, Any]) -> Dict:
        """Check data validity against rules"""
        results = {}
        
        for column, rule in rules.items():
            if column not in self.df.columns:
                continue
            
            if 'min' in rule and 'max' in rule:
                invalid_count = self.df.filter(
                    (F.col(column) < rule['min']) | (F.col(column) > rule['max'])
                ).count()
                
                results[column] = {
                    'invalid_count': invalid_count,
                    'passed': invalid_count == 0
                }
        
        check_result = {
            'check_name': 'validity',
            'status': 'PASSED' if all(r.get('passed', True) for r in results.values()) else 'FAILED',
            'details': results
        }
        
        self._add_result(check_result)
        return check_result
    
    def check_consistency(self) -> Dict:
        """Check data consistency"""
        # Example: Check date consistency
        date_columns = [col for col in self.df.columns if 'date' in col.lower()]
        results = {}
        
        for col in date_columns:
            try:
                # Check if dates are in valid range
                min_date = self.df.agg(F.min(col)).collect()[0][0]
                max_date = self.df.agg(F.max(col)).collect()[0][0]
                
                results[col] = {
                    'min_date': str(min_date),
                    'max_date': str(max_date),
                    'passed': min_date is not None and max_date is not None
                }
            except:
                results[col] = {'passed': False, 'error': 'Date parsing failed'}
        
        check_result = {
            'check_name': 'consistency',
            'status': 'PASSED' if all(r.get('passed', True) for r in results.values()) else 'WARNING',
            'details': results
        }
        
        self._add_result(check_result)
        return check_result
    
    def check_outliers(self, numeric_columns: List[str], threshold: float = 3.0) -> Dict:
        """Check for outliers using z-score method"""
        results = {}
        
        for column in numeric_columns:
            if column not in self.df.columns:
                continue
            
            stats = self.df.select(
                F.mean(column).alias('mean'),
                F.stddev(column).alias('stddev')
            ).collect()[0]
            
            if stats['stddev'] and stats['stddev'] > 0:
                outlier_count = self.df.filter(
                    F.abs((F.col(column) - stats['mean']) / stats['stddev']) > threshold
                ).count()
                
                results[column] = {
                    'outlier_count': outlier_count,
                    'outlier_percentage': (outlier_count / self.df.count()) * 100,
                    'passed': outlier_count < (self.df.count() * 0.01)  # Less than 1%
                }
        
        check_result = {
            'check_name': 'outliers',
            'status': 'WARNING' if any(not r.get('passed', True) for r in results.values()) else 'PASSED',
            'details': results
        }
        
        self._add_result(check_result)
        return check_result
    
    def _add_result(self, check_result: Dict):
        """Add check result and update summary"""
        self.results['checks'].append(check_result)
        self.results['summary']['total_checks'] += 1
        
        status = check_result['status']
        if status == 'PASSED':
            self.results['summary']['passed'] += 1
        elif status == 'FAILED':
            self.results['summary']['failed'] += 1
        else:
            self.results['summary']['warnings'] += 1
    
    def get_results(self) -> Dict:
        """Get all quality check results"""
        return self.results
    
    def send_notification(self, topic_arn: str):
        """Send quality check results via SNS"""
        if self.results['summary']['failed'] > 0:
            subject = f"⚠️ Data Quality Check Failed - {args['TABLE_NAME']}"
        elif self.results['summary']['warnings'] > 0:
            subject = f"⚡ Data Quality Check Warnings - {args['TABLE_NAME']}"
        else:
            subject = f"✅ Data Quality Check Passed - {args['TABLE_NAME']}"
        
        sns_client.publish(
            TopicArn=topic_arn,
            Subject=subject,
            Message=json.dumps(self.results, indent=2)
        )

def main():
    """Main execution function"""
    
    try:
        # Read data from Glue catalog
        logger.info(f"Reading data from {args['DATABASE_NAME']}.{args['TABLE_NAME']}")
        
        df = glueContext.create_dynamic_frame.from_catalog(
            database=args['DATABASE_NAME'],
            table_name=args['TABLE_NAME']
        ).toDF()
        
        # Initialize quality checker
        checker = DataQualityChecker(df)
        
        # Run quality checks
        logger.info("Running completeness check...")
        checker.check_completeness(threshold=0.95)
        
        logger.info("Running uniqueness check...")
        checker.check_uniqueness(['report_date', 'region'])
        
        logger.info("Running validity check...")
        checker.check_validity({
            'case_count': {'min': 0, 'max': 1000000},
            'death_count': {'min': 0, 'max': 10000}
        })
        
        logger.info("Running consistency check...")
        checker.check_consistency()
        
        logger.info("Running outlier detection...")
        numeric_cols = [col for col in df.columns if df.schema[col].dataType.simpleString() in ['int', 'bigint', 'double']]
        checker.check_outliers(numeric_cols)
        
        # Get results
        results = checker.get_results()
        logger.info(f"Quality check summary: {results['summary']}")
        
        # Send notification
        if args.get('SNS_TOPIC_ARN'):
            checker.send_notification(args['SNS_TOPIC_ARN'])
        
        # Write results to S3
        output_path = f"s3://ukhsa-quality-reports-{args.get('ENVIRONMENT', 'dev')}/reports/{datetime.utcnow().strftime('%Y/%m/%d')}/{args['TABLE_NAME']}_quality_{datetime.utcnow().strftime('%H%M%S')}.json"
        
        spark.sparkContext.parallelize([json.dumps(results)]).saveAsTextFile(output_path)
        
        logger.info(f"Quality report saved to {output_path}")
        
        # Commit job
        job.commit()
        
    except Exception as e:
        logger.error(f"Data quality check failed: {str(e)}")
        raise

if __name__ == "__main__":
    main()

