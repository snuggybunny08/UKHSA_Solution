
# ============================================================
# lambdas/shared/utils.py - Shared utility functions
# ============================================================

import json
import logging
import boto3
from datetime import datetime
from typing import Dict, Any, Optional, List
import hashlib

logger = logging.getLogger(__name__)

class S3Helper:
    """Helper class for S3 operations"""
    
    def __init__(self):
        self.s3_client = boto3.client('s3')
    
    def read_json_from_s3(self, bucket: str, key: str) -> Dict[str, Any]:
        """Read JSON file from S3"""
        try:
            response = self.s3_client.get_object(Bucket=bucket, Key=key)
            content = response['Body'].read().decode('utf-8')
            return json.loads(content)
        except Exception as e:
            logger.error(f"Error reading JSON from S3: {str(e)}")
            raise
    
    def write_json_to_s3(self, bucket: str, key: str, data: Dict[str, Any]) -> bool:
        """Write JSON data to S3"""
        try:
            self.s3_client.put_object(
                Bucket=bucket,
                Key=key,
                Body=json.dumps(data, indent=2),
                ContentType='application/json'
            )
            return True
        except Exception as e:
            logger.error(f"Error writing JSON to S3: {str(e)}")
            return False
    
    def move_object(self, source_bucket: str, source_key: str, 
                   dest_bucket: str, dest_key: str) -> bool:
        """Move object between S3 locations"""
        try:
            # Copy object
            copy_source = {'Bucket': source_bucket, 'Key': source_key}
            self.s3_client.copy_object(
                CopySource=copy_source,
                Bucket=dest_bucket,
                Key=dest_key
            )
            
            # Delete original
            self.s3_client.delete_object(Bucket=source_bucket, Key=source_key)
            return True
        except Exception as e:
            logger.error(f"Error moving S3 object: {str(e)}")
            return False

class MetricsPublisher:
    """Publish custom CloudWatch metrics"""
    
    def __init__(self, namespace: str = "UKHSA/DataPlatform"):
        self.cloudwatch = boto3.client('cloudwatch')
        self.namespace = namespace
    
    def publish_metric(self, metric_name: str, value: float, 
                      unit: str = 'Count', dimensions: Optional[List[Dict]] = None):
        """Publish metric to CloudWatch"""
        try:
            metric_data = {
                'MetricName': metric_name,
                'Value': value,
                'Unit': unit,
                'Timestamp': datetime.utcnow()
            }
            
            if dimensions:
                metric_data['Dimensions'] = dimensions
            
            self.cloudwatch.put_metric_data(
                Namespace=self.namespace,
                MetricData=[metric_data]
            )
            logger.debug(f"Published metric: {metric_name}={value}")
        except Exception as e:
            logger.error(f"Error publishing metric: {str(e)}")

def calculate_file_hash(content: bytes) -> str:
    """Calculate SHA256 hash of file content"""
    return hashlib.sha256(content).hexdigest()

def format_file_size(size_bytes: int) -> str:
    """Format file size in human-readable format"""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} PB"

def is_valid_date_format(date_str: str, format: str = "%Y-%m-%d") -> bool:
    """Check if string is valid date format"""
    try:
        datetime.strptime(date_str, format)
        return True
    except ValueError:
        return False

def sanitize_filename(filename: str) -> str:
    """Sanitize filename for S3 key"""
    import re
    # Remove special characters except dash, underscore, and dot
    sanitized = re.sub(r'[^a-zA-Z0-9\-_.]', '_', filename)
    # Remove multiple consecutive underscores
    sanitized = re.sub(r'_+', '_', sanitized)
    return sanitized
