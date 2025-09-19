# lambdas/file_processor/index.py - File Processor Lambda Function

import json
import logging
import boto3
import os
from datetime import datetime
from typing import Dict, Any, Optional
import mimetypes
from urllib.parse import unquote_plus

# Configure logging
logger = logging.getLogger()
logger.setLevel(os.environ.get('LOG_LEVEL', 'INFO'))

# AWS Service Clients
s3_client = boto3.client('s3')
sns_client = boto3.client('sns')
lambda_client = boto3.client('lambda')
macie_client = boto3.client('macie2')

# Environment Variables
VALIDATED_BUCKET = os.environ.get('VALIDATED_BUCKET')
LOGS_BUCKET = os.environ.get('LOGS_BUCKET')
MACIE_JOB_ID = os.environ.get('MACIE_JOB_ID')
SNS_TOPIC_ARN = os.environ.get('SNS_TOPIC_ARN')
VALIDATOR_FUNCTION_NAME = os.environ.get('VALIDATOR_FUNCTION_NAME', 'ukhsa-data-validator')

class FileProcessor:
    """Main file processor class"""
    
    def __init__(self):
        self.supported_formats = {'.csv', '.xls', '.xlsx', '.xml', '.json'}
        self.max_file_size = 1024 * 1024 * 100  # 100MB
        
    def process_s3_event(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Process S3 event trigger"""
        results = []
        
        for record in event.get('Records', []):
            try:
                # Extract S3 event details
                s3_event = record['s3']
                bucket = s3_event['bucket']['name']
                key = unquote_plus(s3_event['object']['key'])
                size = s3_event['object']['size']
                event_time = record['eventTime']
                
                logger.info(f"Processing file: s3://{bucket}/{key}")
                
                # Validate file
                validation_result = self.validate_file(bucket, key, size)
                
                if validation_result['valid']:
                    # Trigger Macie scan if configured
                    if MACIE_JOB_ID:
                        self.trigger_macie_scan(bucket, key)
                    
                    # Invoke validator Lambda
                    validator_response = self.invoke_validator(bucket, key, size, event_time)
                    
                    results.append({
                        'status': 'success',
                        'bucket': bucket,
                        'key': key,
                        'validator_response': validator_response
                    })
                else:
                    # Handle validation failure
                    self.handle_validation_failure(bucket, key, validation_result)
                    results.append({
                        'status': 'failed',
                        'bucket': bucket,
                        'key': key,
                        'reason': validation_result['reason']
                    })
                    
            except Exception as e:
                logger.error(f"Error processing record: {str(e)}")
                results.append({
                    'status': 'error',
                    'error': str(e)
                })
        
        return {
            'processed': len(results),
            'results': results
        }
    
    def validate_file(self, bucket: str, key: str, size: int) -> Dict[str, Any]:
        """Validate file before processing"""
        
        # Check file extension
        file_ext = os.path.splitext(key)[1].lower()
        if file_ext not in self.supported_formats:
            return {
                'valid': False,
                'reason': f'Unsupported file format: {file_ext}'
            }
        
        # Check file size
        if size > self.max_file_size:
            return {
                'valid': False,
                'reason': f'File size {size} exceeds maximum {self.max_file_size}'
            }
        
        # Check if file exists and is accessible
        try:
            s3_client.head_object(Bucket=bucket, Key=key)
        except Exception as e:
            return {
                'valid': False,
                'reason': f'File not accessible: {str(e)}'
            }
        
        # Check for required metadata
        try:
            response = s3_client.get_object_tagging(Bucket=bucket, Key=key)
            tags = {tag['Key']: tag['Value'] for tag in response.get('TagSet', [])}
            
            required_tags = {'dataset_type', 'source_organization'}
            missing_tags = required_tags - set(tags.keys())
            
            if missing_tags:
                logger.warning(f"Missing tags for {key}: {missing_tags}")
                # Add default tags if missing
                self.add_default_tags(bucket, key, missing_tags)
                
        except Exception as e:
            logger.warning(f"Could not retrieve tags: {str(e)}")
        
        return {'valid': True}
    
    def add_default_tags(self, bucket: str, key: str, missing_tags: set):
        """Add default tags to S3 object"""
        default_tags = {
            'dataset_type': 'communicable_disease',
            'source_organization': 'unknown',
            'upload_date': datetime.utcnow().isoformat(),
            'processed': 'false'
        }
        
        try:
            existing_response = s3_client.get_object_tagging(Bucket=bucket, Key=key)
            existing_tags = {tag['Key']: tag['Value'] for tag in existing_response.get('TagSet', [])}
            
            # Add missing tags with defaults
            for tag in missing_tags:
                if tag in default_tags:
                    existing_tags[tag] = default_tags[tag]
            
            # Update tags
            new_tag_set = [{'Key': k, 'Value': v} for k, v in existing_tags.items()]
            s3_client.put_object_tagging(
                Bucket=bucket,
                Key=key,
                Tagging={'TagSet': new_tag_set}
            )
            
            logger.info(f"Added default tags to {key}")
            
        except Exception as e:
            logger.error(f"Failed to add tags: {str(e)}")
    
    def trigger_macie_scan(self, bucket: str, key: str):
        """Trigger Macie PII scan"""
        try:
            # Create a custom Macie job for this specific file
            response = macie_client.create_classification_job(
                name=f"scan-{bucket}-{key.replace('/', '-')}-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}",
                jobType='ONE_TIME',
                s3JobDefinition={
                    'bucketDefinitions': [{
                        'accountId': boto3.client('sts').get_caller_identity()['Account'],
                        'buckets': [bucket],
                        'criteria': {
                            'includes': {
                                'and': [{
                                    'simpleCriterion': {
                                        'key': 'OBJECT_KEY',
                                        'values': [key],
                                        'comparator': 'EQUALS'
                                    }
                                }]
                            }
                        }
                    }]
                }
            )
            
            logger.info(f"Triggered Macie scan job: {response['jobId']}")
            return response['jobId']
            
        except Exception as e:
            logger.error(f"Failed to trigger Macie scan: {str(e)}")
            return None
    
    def invoke_validator(self, bucket: str, key: str, size: int, event_time: str) -> Dict[str, Any]:
        """Invoke the data validator Lambda function"""
        
        payload = {
            'Records': [{
                's3': {
                    'bucket': {'name': bucket},
                    'object': {
                        'key': key,
                        'size': size
                    }
                },
                'eventTime': event_time,
                'eventName': 's3:ObjectCreated:Put'
            }]
        }
        
        try:
            response = lambda_client.invoke(
                FunctionName=VALIDATOR_FUNCTION_NAME,
                InvocationType='Event',  # Asynchronous invocation
                Payload=json.dumps(payload)
            )
            
            logger.info(f"Invoked validator Lambda for {key}")
            return {
                'status_code': response['StatusCode'],
                'request_id': response['ResponseMetadata']['RequestId']
            }
            
        except Exception as e:
            logger.error(f"Failed to invoke validator: {str(e)}")
            raise
    
    def handle_validation_failure(self, bucket: str, key: str, validation_result: Dict[str, Any]):
        """Handle files that fail validation"""
        
        # Move file to quarantine folder
        quarantine_key = f"quarantine/{datetime.utcnow().strftime('%Y/%m/%d')}/{key}"
        
        try:
            # Copy to quarantine
            s3_client.copy_object(
                Bucket=bucket,
                CopySource={'Bucket': bucket, 'Key': key},
                Key=quarantine_key
            )
            
            # Delete original
            s3_client.delete_object(Bucket=bucket, Key=key)
            
            logger.info(f"Moved {key} to quarantine: {quarantine_key}")
            
        except Exception as e:
            logger.error(f"Failed to quarantine file: {str(e)}")
        
        # Send notification
        self.send_notification(
            subject='File Validation Failed',
            message={
                'bucket': bucket,
                'key': key,
                'reason': validation_result['reason'],
                'quarantine_location': f"s3://{bucket}/{quarantine_key}",
                'timestamp': datetime.utcnow().isoformat()
            }
        )
    
    def send_notification(self, subject: str, message: Dict[str, Any]):
        """Send SNS notification"""
        
        if not SNS_TOPIC_ARN:
            logger.warning("SNS topic not configured, skipping notification")
            return
        
        try:
            sns_client.publish(
                TopicArn=SNS_TOPIC_ARN,
                Subject=subject,
                Message=json.dumps(message, indent=2),
                MessageAttributes={
                    'notification_type': {
                        'DataType': 'String',
                        'StringValue': 'file_processing'
                    }
                }
            )
            
            logger.info(f"Sent notification: {subject}")
            
        except Exception as e:
            logger.error(f"Failed to send notification: {str(e)}")
    
    def log_processing_event(self, bucket: str, key: str, status: str, details: Dict[str, Any]):
        """Log processing event to S3"""
        
        log_entry = {
            'timestamp': datetime.utcnow().isoformat(),
            'bucket': bucket,
            'key': key,
            'status': status,
            'details': details
        }
        
        log_key = f"file-processing-logs/{datetime.utcnow().strftime('%Y/%m/%d')}/{datetime.utcnow().strftime('%H%M%S')}-{key.replace('/', '-')}.json"
        
        try:
            s3_client.put_object(
                Bucket=LOGS_BUCKET,
                Key=log_key,
                Body=json.dumps(log_entry, indent=2),
                ContentType='application/json'
            )
            
            logger.debug(f"Logged processing event to {log_key}")
            
        except Exception as e:
            logger.error(f"Failed to log event: {str(e)}")

def handler(event, context):
    """Lambda handler function"""
    
    logger.info(f"Received event: {json.dumps(event)}")
    
    processor = FileProcessor()
    
    try:
        result = processor.process_s3_event(event)
        
        # Log the processing result
        processor.log_processing_event(
            bucket=event['Records'][0]['s3']['bucket']['name'] if event.get('Records') else 'unknown',
            key=event['Records'][0]['s3']['object']['key'] if event.get('Records') else 'unknown',
            status='completed',
            details=result
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps(result)
        }
        
    except Exception as e:
        logger.error(f"Processing failed: {str(e)}")
        
        # Send failure notification
        processor.send_notification(
            subject='File Processing Failed',
            message={
                'error': str(e),
                'event': event,
                'timestamp': datetime.utcnow().isoformat()
            }
        )
        
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'message': 'File processing failed'
            })
        }