# validator.py - Main validation Lambda function for UKHSA Data Platform

import json
import logging
import boto3
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Any
from dataclasses import dataclass, field
from enum import Enum
import re
from io import BytesIO
import traceback

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# AWS Service Clients
s3_client = boto3.client('s3')
sns_client = boto3.client('sns')
dynamodb = boto3.resource('dynamodb')
macie_client = boto3.client('macie2')

# Environment variables
import os
VALIDATED_BUCKET = os.environ.get('VALIDATED_BUCKET')
LOGS_BUCKET = os.environ.get('LOGS_BUCKET')
VALIDATION_RULES_TABLE = os.environ.get('VALIDATION_RULES_TABLE')
SNS_TOPIC_ARN = os.environ.get('NOTIFICATION_TOPIC')

# ============================================================
# Data Models
# ============================================================

class ValidationStatus(Enum):
    """Validation status enumeration"""
    PASSED = "PASSED"
    FAILED = "FAILED"
    WARNING = "WARNING"
    ERROR = "ERROR"

class DatasetType(Enum):
    """Dataset types enumeration"""
    COMMUNICABLE_DISEASE = "COMMUNICABLE_DISEASE"
    IMMUNISATION = "IMMUNISATION"
    SURVEILLANCE = "SURVEILLANCE"
    LABORATORY = "LABORATORY"
    OUTBREAK = "OUTBREAK"

@dataclass
class ValidationResult:
    """Validation result data class"""
    status: ValidationStatus
    message: str
    details: Dict[str, Any] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict:
        return {
            'status': self.status.value,
            'message': self.message,
            'details': self.details,
            'errors': self.errors,
            'warnings': self.warnings,
            'timestamp': datetime.utcnow().isoformat()
        }

@dataclass
class FileMetadata:
    """File metadata data class"""
    bucket: str
    key: str
    size: int
    file_type: str
    dataset_type: Optional[DatasetType] = None
    upload_timestamp: Optional[str] = None
    user_id: Optional[str] = None
    organisation: Optional[str] = None

# ============================================================
# Validation Rules Engine
# ============================================================

class ValidationRulesEngine:
    """Manages and applies validation rules from DynamoDB"""
    
    def __init__(self, table_name: str):
        self.table = dynamodb.Table(table_name)
        self.rules_cache = {}
    
    def load_rules(self, dataset_type: str) -> List[Dict]:
        """Load validation rules from DynamoDB"""
        if dataset_type in self.rules_cache:
            return self.rules_cache[dataset_type]
        
        try:
            response = self.table.query(
                KeyConditionExpression='dataset_type = :type',
                ExpressionAttributeValues={':type': dataset_type}
            )
            rules = response.get('Items', [])
            self.rules_cache[dataset_type] = rules
            logger.info(f"Loaded {len(rules)} rules for {dataset_type}")
            return rules
        except Exception as e:
            logger.error(f"Error loading rules: {str(e)}")
            return []
    
    def apply_rule(self, rule: Dict, df: pd.DataFrame) -> ValidationResult:
        """Apply a single validation rule to the dataframe"""
        rule_type = rule.get('rule_type')
        
        if rule_type == 'schema':
            return self._validate_schema(rule, df)
        elif rule_type == 'data_type':
            return self._validate_data_types(rule, df)
        elif rule_type == 'business_logic':
            return self._validate_business_logic(rule, df)
        elif rule_type == 'data_quality':
            return self._validate_data_quality(rule, df)
        else:
            return ValidationResult(
                ValidationStatus.WARNING,
                f"Unknown rule type: {rule_type}"
            )
    
    def _validate_schema(self, rule: Dict, df: pd.DataFrame) -> ValidationResult:
        """Validate dataframe schema"""
        required_columns = rule.get('required_columns', [])
        missing_columns = set(required_columns) - set(df.columns)
        
        if missing_columns:
            return ValidationResult(
                ValidationStatus.FAILED,
                "Schema validation failed",
                errors=[f"Missing columns: {', '.join(missing_columns)}"]
            )
        
        return ValidationResult(ValidationStatus.PASSED, "Schema validation passed")
    
    def _validate_data_types(self, rule: Dict, df: pd.DataFrame) -> ValidationResult:
        """Validate column data types"""
        type_mappings = rule.get('column_types', {})
        errors = []
        
        for column, expected_type in type_mappings.items():
            if column in df.columns:
                actual_type = str(df[column].dtype)
                if not self._check_type_compatibility(actual_type, expected_type):
                    errors.append(f"Column '{column}' has type '{actual_type}', expected '{expected_type}'")
        
        if errors:
            return ValidationResult(
                ValidationStatus.FAILED,
                "Data type validation failed",
                errors=errors
            )
        
        return ValidationResult(ValidationStatus.PASSED, "Data type validation passed")
    
    def _validate_business_logic(self, rule: Dict, df: pd.DataFrame) -> ValidationResult:
        """Apply business logic validation rules"""
        errors = []
        warnings = []
        
        # Example business rules
        if 'date_range' in rule:
            date_col = rule['date_range'].get('column')
            min_date = pd.to_datetime(rule['date_range'].get('min'))
            max_date = pd.to_datetime(rule['date_range'].get('max'))
            
            if date_col in df.columns:
                df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
                invalid_dates = df[(df[date_col] < min_date) | (df[date_col] > max_date)]
                
                if not invalid_dates.empty:
                    errors.append(f"Found {len(invalid_dates)} records with dates outside valid range")
        
        # Validate value ranges
        if 'value_ranges' in rule:
            for column, constraints in rule['value_ranges'].items():
                if column in df.columns:
                    min_val = constraints.get('min')
                    max_val = constraints.get('max')
                    
                    if min_val is not None:
                        invalid = df[df[column] < min_val]
                        if not invalid.empty:
                            warnings.append(f"Column '{column}' has {len(invalid)} values below minimum {min_val}")
                    
                    if max_val is not None:
                        invalid = df[df[column] > max_val]
                        if not invalid.empty:
                            warnings.append(f"Column '{column}' has {len(invalid)} values above maximum {max_val}")
        
        status = ValidationStatus.FAILED if errors else (
            ValidationStatus.WARNING if warnings else ValidationStatus.PASSED
        )
        
        return ValidationResult(
            status,
            "Business logic validation completed",
            errors=errors,
            warnings=warnings
        )
    
    def _validate_data_quality(self, rule: Dict, df: pd.DataFrame) -> ValidationResult:
        """Validate data quality metrics"""
        warnings = []
        details = {}
        
        # Check for null values
        null_threshold = rule.get('null_threshold', 0.1)  # 10% default
        for column in df.columns:
            null_ratio = df[column].isna().sum() / len(df)
            if null_ratio > null_threshold:
                warnings.append(f"Column '{column}' has {null_ratio:.2%} null values (threshold: {null_threshold:.0%})")
            details[f"{column}_null_ratio"] = null_ratio
        
        # Check for duplicates
        if rule.get('check_duplicates', False):
            duplicate_count = df.duplicated().sum()
            if duplicate_count > 0:
                warnings.append(f"Found {duplicate_count} duplicate rows")
            details['duplicate_count'] = int(duplicate_count)
        
        # Check for outliers using IQR method
        if 'outlier_columns' in rule:
            for column in rule['outlier_columns']:
                if column in df.columns and pd.api.types.is_numeric_dtype(df[column]):
                    Q1 = df[column].quantile(0.25)
                    Q3 = df[column].quantile(0.75)
                    IQR = Q3 - Q1
                    outliers = df[(df[column] < Q1 - 1.5 * IQR) | (df[column] > Q3 + 1.5 * IQR)]
                    
                    if not outliers.empty:
                        warnings.append(f"Column '{column}' has {len(outliers)} potential outliers")
                    details[f"{column}_outliers"] = len(outliers)
        
        return ValidationResult(
            ValidationStatus.WARNING if warnings else ValidationStatus.PASSED,
            "Data quality validation completed",
            details=details,
            warnings=warnings
        )
    
    def _check_type_compatibility(self, actual: str, expected: str) -> bool:
        """Check if actual type is compatible with expected type"""
        type_mappings = {
            'string': ['object', 'str'],
            'integer': ['int64', 'int32', 'int16', 'int8'],
            'float': ['float64', 'float32', 'float16'],
            'datetime': ['datetime64[ns]', 'datetime64'],
            'boolean': ['bool']
        }
        
        expected_types = type_mappings.get(expected, [expected])
        return any(t in actual.lower() for t in expected_types)

# ============================================================
# PII Detection
# ============================================================

class PIIDetector:
    """Detects potential PII in datasets"""
    
    # Common PII patterns (UK-specific)
    PII_PATTERNS = {
        'nhs_number': r'\b[0-9]{3}[\s-]?[0-9]{3}[\s-]?[0-9]{4}\b',
        'ni_number': r'\b[A-Z]{2}[0-9]{6}[A-Z]\b',
        'phone': r'\b(?:0[1-9]\d{8,9}|07\d{9}|\+44\s?7\d{3}\s?\d{6})\b',
        'email': r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b',
        'postcode': r'\b[A-Z]{1,2}[0-9]{1,2}[A-Z]?\s?[0-9][A-Z]{2}\b',
        'dob': r'\b(?:0[1-9]|[12][0-9]|3[01])[-/](?:0[1-9]|1[012])[-/](?:19|20)\d\d\b',
        'credit_card': r'\b(?:\d{4}[\s-]?){3}\d{4}\b'
    }
    
    @classmethod
    def scan_dataframe(cls, df: pd.DataFrame) -> Dict[str, List[str]]:
        """Scan dataframe for potential PII"""
        pii_findings = {}
        
        for column in df.columns:
            column_findings = []
            
            # Check column name for PII indicators
            if cls._is_pii_column_name(column):
                column_findings.append(f"Column name suggests PII: {column}")
            
            # Check data content for PII patterns
            if df[column].dtype == 'object':
                sample = df[column].dropna().astype(str).head(100)
                for pii_type, pattern in cls.PII_PATTERNS.items():
                    if sample.str.contains(pattern, regex=True, na=False).any():
                        column_findings.append(f"Potential {pii_type} detected")
            
            if column_findings:
                pii_findings[column] = column_findings
        
        return pii_findings
    
    @staticmethod
    def _is_pii_column_name(column_name: str) -> bool:
        """Check if column name suggests PII"""
        pii_indicators = [
            'name', 'email', 'phone', 'address', 'postcode', 'dob',
            'birth', 'ssn', 'ni_number', 'nhs', 'passport', 'license',
            'patient', 'contact', 'mobile', 'telephone'
        ]
        column_lower = column_name.lower()
        return any(indicator in column_lower for indicator in pii_indicators)
    
    @classmethod
    def redact_pii(cls, df: pd.DataFrame, pii_findings: Dict) -> pd.DataFrame:
        """Redact identified PII from dataframe"""
        df_copy = df.copy()
        
        for column in pii_findings.keys():
            if column in df_copy.columns:
                # Replace PII with redacted placeholder
                df_copy[column] = df_copy[column].apply(
                    lambda x: '***REDACTED***' if pd.notna(x) else x
                )
        
        return df_copy

# ============================================================
# File Processors
# ============================================================

class FileProcessor:
    """Process different file formats"""
    
    @staticmethod
    def process_csv(content: bytes, encoding: str = 'utf-8') -> pd.DataFrame:
        """Process CSV file"""
        try:
            df = pd.read_csv(BytesIO(content), encoding=encoding)
            return df
        except UnicodeDecodeError:
            # Try with different encoding
            df = pd.read_csv(BytesIO(content), encoding='latin-1')
            return df
    
    @staticmethod
    def process_excel(content: bytes) -> pd.DataFrame:
        """Process Excel file"""
        df = pd.read_excel(BytesIO(content), engine='openpyxl')
        return df
    
    @staticmethod
    def process_xml(content: bytes) -> pd.DataFrame:
        """Process XML file"""
        # Simple XML to DataFrame conversion
        # In production, use more sophisticated XML parsing
        import xml.etree.ElementTree as ET
        
        root = ET.fromstring(content.decode('utf-8'))
        data = []
        
        for child in root:
            record = {}
            for subchild in child:
                record[subchild.tag] = subchild.text
            data.append(record)
        
        return pd.DataFrame(data)

# ============================================================
# Main Handler
# ============================================================

class DataValidator:
    """Main data validation orchestrator"""
    
    def __init__(self):
        self.rules_engine = ValidationRulesEngine(VALIDATION_RULES_TABLE)
        self.pii_detector = PIIDetector()
        
    def validate_file(self, file_metadata: FileMetadata) -> ValidationResult:
        """Main validation method"""
        logger.info(f"Starting validation for {file_metadata.key}")
        
        try:
            # Download file from S3
            response = s3_client.get_object(
                Bucket=file_metadata.bucket,
                Key=file_metadata.key
            )
            content = response['Body'].read()
            
            # Process file based on type
            df = self._process_file(content, file_metadata.file_type)
            
            # Run PII detection
            pii_findings = self.pii_detector.scan_dataframe(df)
            if pii_findings:
                self._handle_pii_detection(file_metadata, pii_findings)
                return ValidationResult(
                    ValidationStatus.FAILED,
                    "PII detected in file",
                    details={'pii_findings': pii_findings}
                )
            
            # Load and apply validation rules
            rules = self.rules_engine.load_rules(
                file_metadata.dataset_type or 'default'
            )
            
            all_results = []
            for rule in rules:
                result = self.rules_engine.apply_rule(rule, df)
                all_results.append(result)
            
            # Aggregate results
            final_result = self._aggregate_results(all_results)
            
            # If validation passed, save to validated bucket
            if final_result.status == ValidationStatus.PASSED:
                self._save_validated_data(df, file_metadata)
            
            # Log validation results
            self._log_validation_results(file_metadata, final_result)
            
            return final_result
            
        except Exception as e:
            logger.error(f"Validation error: {str(e)}\n{traceback.format_exc()}")
            return ValidationResult(
                ValidationStatus.ERROR,
                f"Validation failed with error: {str(e)}"
            )
    
    def _process_file(self, content: bytes, file_type: str) -> pd.DataFrame:
        """Process file content based on file type"""
        file_type_lower = file_type.lower()
        
        if file_type_lower in ['csv', '.csv']:
            return FileProcessor.process_csv(content)
        elif file_type_lower in ['xls', 'xlsx', '.xls', '.xlsx']:
            return FileProcessor.process_excel(content)
        elif file_type_lower in ['xml', '.xml']:
            return FileProcessor.process_xml(content)
        else:
            raise ValueError(f"Unsupported file type: {file_type}")
    
    def _aggregate_results(self, results: List[ValidationResult]) -> ValidationResult:
        """Aggregate multiple validation results"""
        if not results:
            return ValidationResult(ValidationStatus.PASSED, "No validation rules applied")
        
        # Determine overall status
        has_error = any(r.status == ValidationStatus.ERROR for r in results)
        has_failure = any(r.status == ValidationStatus.FAILED for r in results)
        has_warning = any(r.status == ValidationStatus.WARNING for r in results)
        
        if has_error:
            status = ValidationStatus.ERROR
            message = "Validation encountered errors"
        elif has_failure:
            status = ValidationStatus.FAILED
            message = "Validation failed"
        elif has_warning:
            status = ValidationStatus.WARNING
            message = "Validation passed with warnings"
        else:
            status = ValidationStatus.PASSED
            message = "All validations passed"
        
        # Combine all errors and warnings
        all_errors = []
        all_warnings = []
        all_details = {}
        
        for r in results:
            all_errors.extend(r.errors)
            all_warnings.extend(r.warnings)
            all_details.update(r.details)
        
        return ValidationResult(
            status=status,
            message=message,
            errors=all_errors,
            warnings=all_warnings,
            details=all_details
        )
    
    def _save_validated_data(self, df: pd.DataFrame, metadata: FileMetadata):
        """Save validated data to S3"""
        # Generate output path
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        output_key = f"validated/{metadata.dataset_type}/{timestamp}_{metadata.key.split('/')[-1]}"
        
        # Convert to Parquet for better performance
        parquet_buffer = BytesIO()
        df.to_parquet(parquet_buffer, engine='pyarrow', compression='snappy')
        
        # Upload to validated bucket
        s3_client.put_object(
            Bucket=VALIDATED_BUCKET,
            Key=output_key.replace('.csv', '.parquet').replace('.xlsx', '.parquet'),
            Body=parquet_buffer.getvalue(),
            Metadata={
                'original_file': metadata.key,
                'validation_timestamp': timestamp,
                'record_count': str(len(df))
            }
        )
        
        logger.info(f"Saved validated data to {VALIDATED_BUCKET}/{output_key}")
    
    def _handle_pii_detection(self, metadata: FileMetadata, pii_findings: Dict):
        """Handle PII detection by notifying and removing file"""
        # Send notification
        message = {
            'file': f"{metadata.bucket}/{metadata.key}",
            'pii_findings': pii_findings,
            'action': 'File removed due to PII detection',
            'timestamp': datetime.utcnow().isoformat()
        }
        
        sns_client.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject='PII Detected in Uploaded File',
            Message=json.dumps(message, indent=2)
        )
        
        # Remove file from landing bucket
        s3_client.delete_object(
            Bucket=metadata.bucket,
            Key=metadata.key
        )
        
        logger.warning(f"File {metadata.key} removed due to PII detection")
    
    def _log_validation_results(self, metadata: FileMetadata, result: ValidationResult):
        """Log validation results to S3"""
        log_entry = {
            'file_metadata': {
                'bucket': metadata.bucket,
                'key': metadata.key,
                'size': metadata.size,
                'file_type': metadata.file_type,
                'dataset_type': metadata.dataset_type
            },
            'validation_result': result.to_dict(),
            'timestamp': datetime.utcnow().isoformat()
        }
        
        # Save log to S3
        timestamp = datetime.utcnow().strftime('%Y%m%d_%H%M%S')
        log_key = f"logs/{datetime.utcnow().strftime('%Y/%m/%d')}/{timestamp}_{metadata.key.split('/')[-1]}.json"
        
        s3_client.put_object(
            Bucket=LOGS_BUCKET,
            Key=log_key,
            Body=json.dumps(log_entry, indent=2),
            ContentType='application/json'
        )
        
        logger.info(f"Validation results logged to {LOGS_BUCKET}/{log_key}")

# ============================================================
# Lambda Handler
# ============================================================

def handler(event, context):
    """Lambda handler function"""
    logger.info(f"Received event: {json.dumps(event)}")
    
    try:
        # Parse S3 event
        for record in event['Records']:
            bucket = record['s3']['bucket']['name']
            key = record['s3']['object']['key']
            size = record['s3']['object']['size']
            
            # Determine file type and dataset type
            file_extension = key.split('.')[-1].lower()
            dataset_type = _determine_dataset_type(key)
            
            # Create file metadata
            metadata = FileMetadata(
                bucket=bucket,
                key=key,
                size=size,
                file_type=file_extension,
                dataset_type=dataset_type,
                upload_timestamp=record['eventTime'],
                user_id=record.get('userIdentity', {}).get('principalId')
            )
            
            # Validate file
            validator = DataValidator()
            result = validator.validate_file(metadata)
            
            # Send notification if validation failed
            if result.status in [ValidationStatus.FAILED, ValidationStatus.ERROR]:
                _send_failure_notification(metadata, result)
            
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': 'Validation completed',
                    'result': result.to_dict()
                })
            }
            
    except Exception as e:
        logger.error(f"Handler error: {str(e)}\n{traceback.format_exc()}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Internal server error',
                'error': str(e)
            })
        }

def _determine_dataset_type(key: str) -> str:
    """Determine dataset type from file path"""
    key_lower = key.lower()
    
    if 'immunisation' in key_lower:
        return DatasetType.IMMUNISATION.value
    elif 'surveillance' in key_lower:
        return DatasetType.SURVEILLANCE.value
    elif 'laboratory' in key_lower or 'lab' in key_lower:
        return DatasetType.LABORATORY.value
    elif 'outbreak' in key_lower:
        return DatasetType.OUTBREAK.value
    else:
        return DatasetType.COMMUNICABLE_DISEASE.value

def _send_failure_notification(metadata: FileMetadata, result: ValidationResult):
    """Send validation failure notification"""
    message = {
        'file': f"{metadata.bucket}/{metadata.key}",
        'status': result.status.value,
        'message': result.message,
        'errors': result.errors,
        'warnings': result.warnings,
        'timestamp': datetime.utcnow().isoformat()
    }
    
    sns_client.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=f'Validation Failed: {metadata.key}',
        Message=json.dumps(message, indent=2)
    )
    
    logger.info(f"Failure notification sent for {metadata.key}")