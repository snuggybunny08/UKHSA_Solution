
import pytest
import json
import pandas as pd
import numpy as np
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
import boto3
from moto import mock_s3, mock_dynamodb, mock_sns

# Import the validator module (adjust path as needed)
import sys
import os
sys.path.insert(0, os.path.abspath('../../lambdas/validation'))
from validator import (
    DataValidator, 
    ValidationStatus, 
    ValidationResult,
    FileMetadata,
    PIIDetector,
    ValidationRulesEngine,
    DatasetType
)

# ============================================================
# Fixtures
# ============================================================

@pytest.fixture
def sample_dataframe():
    """Create a sample dataframe for testing"""
    return pd.DataFrame({
        'report_date': pd.date_range('2025-01-01', periods=10),
        'case_count': np.random.randint(0, 100, 10),
        'death_count': np.random.randint(0, 10, 10),
        'region': ['London', 'Manchester', 'Birmingham'] * 3 + ['Leeds'],
        'disease_name': 'COVID-19'
    })

@pytest.fixture
def sample_pii_dataframe():
    """Create a dataframe with PII for testing"""
    return pd.DataFrame({
        'patient_name': ['John Doe', 'Jane Smith', 'Bob Johnson'],
        'nhs_number': ['123 456 7890', '987 654 3210', '456 789 0123'],
        'email': ['john@example.com', 'jane@example.com', 'bob@example.com'],
        'case_count': [1, 2, 3]
    })

@pytest.fixture
def file_metadata():
    """Create sample file metadata"""
    return FileMetadata(
        bucket='test-bucket',
        key='test/file.csv',
        size=1024,
        file_type='csv',
        dataset_type=DatasetType.COMMUNICABLE_DISEASE,
        upload_timestamp='2025-01-01T00:00:00Z',
        user_id='test-user',
        organisation='UKHSA'
    )

@pytest.fixture
def validation_rules():
    """Create sample validation rules"""
    return [
        {
            'rule_id': 'schema_001',
            'rule_type': 'schema',
            'required_columns': ['report_date', 'case_count', 'region']
        },
        {
            'rule_id': 'data_type_001',
            'rule_type': 'data_type',
            'column_types': {
                'case_count': 'integer',
                'report_date': 'datetime'
            }
        },
        {
            'rule_id': 'business_001',
            'rule_type': 'business_logic',
            'value_ranges': {
                'case_count': {'min': 0, 'max': 10000}
            }
        }
    ]

@pytest.fixture
def mock_aws_services():
    """Mock AWS services"""
    with mock_s3(), mock_dynamodb(), mock_sns():
        # Create S3 buckets
        s3 = boto3.client('s3', region_name='eu-west-2')
        s3.create_bucket(
            Bucket='test-bucket',
            CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'}
        )
        s3.create_bucket(
            Bucket='validated-bucket',
            CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'}
        )
        
        # Create DynamoDB table
        dynamodb = boto3.resource('dynamodb', region_name='eu-west-2')
        table = dynamodb.create_table(
            TableName='validation-rules',
            KeySchema=[
                {'AttributeName': 'dataset_type', 'KeyType': 'HASH'},
                {'AttributeName': 'rule_id', 'KeyType': 'RANGE'}
            ],
            AttributeDefinitions=[
                {'AttributeName': 'dataset_type', 'AttributeType': 'S'},
                {'AttributeName': 'rule_id', 'AttributeType': 'S'}
            ],
            BillingMode='PAY_PER_REQUEST'
        )
        
        # Create SNS topic
        sns = boto3.client('sns', region_name='eu-west-2')
        sns.create_topic(Name='validation-alerts')
        
        yield {
            's3': s3,
            'dynamodb': dynamodb,
            'sns': sns,
            'table': table
        }

# ============================================================
# Unit Tests - ValidationResult
# ============================================================

class TestValidationResult:
    """Test ValidationResult class"""
    
    def test_validation_result_creation(self):
        """Test creating a ValidationResult"""
        result = ValidationResult(
            status=ValidationStatus.PASSED,
            message="Test passed",
            details={'test': 'value'},
            errors=['error1'],
            warnings=['warning1']
        )
        
        assert result.status == ValidationStatus.PASSED
        assert result.message == "Test passed"
        assert result.details == {'test': 'value'}
        assert result.errors == ['error1']
        assert result.warnings == ['warning1']
    
    def test_validation_result_to_dict(self):
        """Test converting ValidationResult to dictionary"""
        result = ValidationResult(
            status=ValidationStatus.FAILED,
            message="Test failed"
        )
        
        result_dict = result.to_dict()
        
        assert result_dict['status'] == 'FAILED'
        assert result_dict['message'] == "Test failed"
        assert 'timestamp' in result_dict

# ============================================================
# Unit Tests - PIIDetector
# ============================================================

class TestPIIDetector:
    """Test PII detection functionality"""
    
    def test_detect_nhs_numbers(self, sample_pii_dataframe):
        """Test NHS number detection"""
        findings = PIIDetector.scan_dataframe(sample_pii_dataframe)
        
        assert 'nhs_number' in findings
        assert any('nhs_number' in finding for finding in findings['nhs_number'])
    
    def test_detect_email_addresses(self, sample_pii_dataframe):
        """Test email detection"""
        findings = PIIDetector.scan_dataframe(sample_pii_dataframe)
        
        assert 'email' in findings
        assert any('email' in finding for finding in findings['email'])
    
    def test_detect_pii_column_names(self, sample_pii_dataframe):
        """Test PII detection based on column names"""
        findings = PIIDetector.scan_dataframe(sample_pii_dataframe)
        
        assert 'patient_name' in findings
        assert any('Column name suggests PII' in finding for finding in findings['patient_name'])
    
    def test_no_pii_detected(self, sample_dataframe):
        """Test when no PII is detected"""
        findings = PIIDetector.scan_dataframe(sample_dataframe)
        
        assert len(findings) == 0
    
    def test_redact_pii(self, sample_pii_dataframe):
        """Test PII redaction"""
        findings = PIIDetector.scan_dataframe(sample_pii_dataframe)
        redacted_df = PIIDetector.redact_pii(sample_pii_dataframe, findings)
        
        # Check that PII columns are redacted
        for col in findings.keys():
            if col in redacted_df.columns:
                assert all(redacted_df[col] == '***REDACTED***')

# ============================================================
# Unit Tests - ValidationRulesEngine
# ============================================================

class TestValidationRulesEngine:
    """Test validation rules engine"""
    
    def test_validate_schema_pass(self, sample_dataframe):
        """Test schema validation - passing"""
        engine = ValidationRulesEngine('test-table')
        rule = {
            'rule_type': 'schema',
            'required_columns': ['report_date', 'case_count']
        }
        
        result = engine.apply_rule(rule, sample_dataframe)
        
        assert result.status == ValidationStatus.PASSED
    
    def test_validate_schema_fail(self, sample_dataframe):
        """Test schema validation - failing"""
        engine = ValidationRulesEngine('test-table')
        rule = {
            'rule_type': 'schema',
            'required_columns': ['missing_column']
        }
        
        result = engine.apply_rule(rule, sample_dataframe)
        
        assert result.status == ValidationStatus.FAILED
        assert 'Missing columns' in result.errors[0]
    
    def test_validate_data_types(self, sample_dataframe):
        """Test data type validation"""
        engine = ValidationRulesEngine('test-table')
        rule = {
            'rule_type': 'data_type',
            'column_types': {
                'case_count': 'integer'
            }
        }
        
        result = engine.apply_rule(rule, sample_dataframe)
        
        # This should pass as case_count is integer
        assert result.status == ValidationStatus.PASSED
    
    def test_validate_business_logic(self, sample_dataframe):
        """Test business logic validation"""
        engine = ValidationRulesEngine('test-table')
        rule = {
            'rule_type': 'business_logic',
            'value_ranges': {
                'case_count': {'min': 0, 'max': 1000}
            }
        }
        
        result = engine.apply_rule(rule, sample_dataframe)
        
        assert result.status in [ValidationStatus.PASSED, ValidationStatus.WARNING]
        assert 'duplicate_count' in result.details

# ============================================================
# Unit Tests - DataValidator
# ============================================================

class TestDataValidator:
    """Test main DataValidator class"""
    
    @patch('validator.s3_client')
    @patch('validator.ValidationRulesEngine')
    def test_validate_file_success(self, mock_rules_engine, mock_s3, 
                                   file_metadata, sample_dataframe):
        """Test successful file validation"""
        # Mock S3 response
        mock_s3.get_object.return_value = {
            'Body': Mock(read=Mock(return_value=sample_dataframe.to_csv().encode()))
        }
        
        # Mock validation rules
        mock_engine = Mock()
        mock_engine.load_rules.return_value = []
        mock_rules_engine.return_value = mock_engine
        
        # Create validator and validate
        validator = DataValidator()
        result = validator.validate_file(file_metadata)
        
        assert result.status == ValidationStatus.PASSED
    
    @patch('validator.s3_client')
    def test_validate_file_with_pii(self, mock_s3, file_metadata, sample_pii_dataframe):
        """Test file validation with PII detection"""
        # Mock S3 response with PII data
        mock_s3.get_object.return_value = {
            'Body': Mock(read=Mock(return_value=sample_pii_dataframe.to_csv().encode()))
        }
        
        validator = DataValidator()
        result = validator.validate_file(file_metadata)
        
        assert result.status == ValidationStatus.FAILED
        assert 'PII detected' in result.message
    
    @patch('validator.s3_client')
    def test_validate_file_error_handling(self, mock_s3, file_metadata):
        """Test error handling during validation"""
        # Mock S3 to raise an exception
        mock_s3.get_object.side_effect = Exception("S3 error")
        
        validator = DataValidator()
        result = validator.validate_file(file_metadata)
        
        assert result.status == ValidationStatus.ERROR
        assert "S3 error" in result.message

# ============================================================
# Integration Tests
# ============================================================

@pytest.mark.integration
class TestIntegration:
    """Integration tests with mocked AWS services"""
    
    def test_end_to_end_validation_flow(self, mock_aws_services, 
                                       sample_dataframe, file_metadata):
        """Test complete validation flow"""
        # Upload test file to S3
        s3 = mock_aws_services['s3']
        s3.put_object(
            Bucket='test-bucket',
            Key='test/file.csv',
            Body=sample_dataframe.to_csv().encode()
        )
        
        # Add validation rules to DynamoDB
        table = mock_aws_services['table']
        table.put_item(Item={
            'dataset_type': 'COMMUNICABLE_DISEASE',
            'rule_id': 'test_rule',
            'rule_type': 'schema',
            'required_columns': ['report_date', 'case_count']
        })
        
        # Run validation
        with patch.dict(os.environ, {
            'VALIDATED_BUCKET': 'validated-bucket',
            'LOGS_BUCKET': 'logs-bucket',
            'VALIDATION_RULES_TABLE': 'validation-rules',
            'NOTIFICATION_TOPIC': 'arn:aws:sns:eu-west-2:123456789012:validation-alerts'
        }):
            validator = DataValidator()
            result = validator.validate_file(file_metadata)
            
            # Verify results
            assert result is not None
            assert result.status in [ValidationStatus.PASSED, ValidationStatus.FAILED, 
                                    ValidationStatus.WARNING]

# ============================================================
# Performance Tests
# ============================================================

@pytest.mark.performance
class TestPerformance:
    """Performance tests for data validation"""
    
    def test_large_dataset_validation(self):
        """Test validation performance with large dataset"""
        # Create large dataframe (100k rows)
        large_df = pd.DataFrame({
            'report_date': pd.date_range('2020-01-01', periods=100000, freq='H'),
            'case_count': np.random.randint(0, 100, 100000),
            'region': np.random.choice(['London', 'Manchester', 'Birmingham'], 100000)
        })
        
        start_time = datetime.now()
        
        # Run PII detection
        findings = PIIDetector.scan_dataframe(large_df)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        # Should complete within 5 seconds
        assert duration < 5
        assert findings is not None
    
    def test_validation_rules_performance(self):
        """Test performance of validation rules"""
        df = pd.DataFrame({
            'col_' + str(i): np.random.randn(10000) 
            for i in range(50)  # 50 columns
        })
        
        engine = ValidationRulesEngine('test-table')
        rule = {
            'rule_type': 'data_quality',
            'null_threshold': 0.1,
            'check_duplicates': True,
            'outlier_columns': ['col_0', 'col_1', 'col_2']
        }
        
        start_time = datetime.now()
        result = engine.apply_rule(rule, df)
        end_time = datetime.now()
        
        duration = (end_time - start_time).total_seconds()
        
        # Should complete within 2 seconds
        assert duration < 2
        assert result.status in [ValidationStatus.PASSED, ValidationStatus.WARNING]

# ============================================================
# Smoke Tests
# ============================================================

@pytest.mark.smoke
class TestSmoke:
    """Smoke tests for deployed environment"""
    
    @pytest.fixture
    def env_config(self, request):
        """Get environment configuration"""
        env = request.config.getoption("--env", default="dev")
        return {
            'dev': {
                'landing_bucket': 'ukhsa-landing-dev',
                'validated_bucket': 'ukhsa-validated-dev',
                'function_name': 'ukhsa-data-validator-dev'
            },
            'staging': {
                'landing_bucket': 'ukhsa-landing-staging',
                'validated_bucket': 'ukhsa-validated-staging',
                'function_name': 'ukhsa-data-validator-staging'
            },
            'prod': {
                'landing_bucket': 'ukhsa-landing-prod',
                'validated_bucket': 'ukhsa-validated-prod',
                'function_name': 'ukhsa-data-validator-prod'
            }
        }.get(env, {})
    
    @pytest.mark.skipif(not os.environ.get('AWS_ACCESS_KEY_ID'), 
                       reason="AWS credentials not available")
    def test_s3_buckets_exist(self, env_config):
        """Test that S3 buckets exist"""
        s3 = boto3.client('s3')
        
        # Check landing bucket
        response = s3.head_bucket(Bucket=env_config['landing_bucket'])
        assert response['ResponseMetadata']['HTTPStatusCode'] == 200
        
        # Check validated bucket
        response = s3.head_bucket(Bucket=env_config['validated_bucket'])
        assert response['ResponseMetadata']['HTTPStatusCode'] == 200
    
    @pytest.mark.skipif(not os.environ.get('AWS_ACCESS_KEY_ID'),
                       reason="AWS credentials not available")
    def test_lambda_function_exists(self, env_config):
        """Test that Lambda function exists and is configured"""
        lambda_client = boto3.client('lambda')
        
        response = lambda_client.get_function(
            FunctionName=env_config['function_name']
        )
        
        assert response['Configuration']['FunctionName'] == env_config['function_name']
        assert response['Configuration']['Runtime'] == 'python3.11'
        assert response['Configuration']['Timeout'] >= 300
        assert response['Configuration']['MemorySize'] >= 1024

# ============================================================
# Test Configuration
# ============================================================

def pytest_addoption(parser):
    """Add custom command line options"""
    parser.addoption(
        "--env",
        action="store",
        default="dev",
        help="Environment to run tests against: dev, staging, or prod"
    )
    parser.addoption(
        "--integration",
        action="store_true",
        default=False,
        help="Run integration tests"
    )
    parser.addoption(
        "--performance",
        action="store_true",
        default=False,
        help="Run performance tests"
    )
    parser.addoption(
        "--smoke",
        action="store_true",
        default=False,
        help="Run smoke tests"
    )

def pytest_configure(config):
    """Configure pytest"""
    config.addinivalue_line(
        "markers", "integration: mark test as integration test"
    )
    config.addinivalue_line(
        "markers", "performance: mark test as performance test"
    )
    config.addinivalue_line(
        "markers", "smoke: mark test as smoke test"
    )

def pytest_collection_modifyitems(config, items):
    """Modify test collection based on markers"""
    if not config.getoption("--integration"):
        skip_integration = pytest.mark.skip(reason="need --integration option to run")
        for item in items:
            if "integration" in item.keywords:
                item.add_marker(skip_integration)
    
    if not config.getoption("--performance"):
        skip_performance = pytest.mark.skip(reason="need --performance option to run")
        for item in items:
            if "performance" in item.keywords:
                item.add_marker(skip_performance)
    
    if not config.getoption("--smoke"):
        skip_smoke = pytest.mark.skip(reason="need --smoke option to run")
        for item in items:
            if "smoke" in item.keywords:
                item.add_marker(skip_smoke)

# ============================================================
# Test Utilities
# ============================================================

class TestUtilities:
    """Utility functions for testing"""
    
    @staticmethod
    def create_test_file(bucket: str, key: str, content: bytes):
        """Helper to create test file in S3"""
        s3 = boto3.client('s3')
        s3.put_object(Bucket=bucket, Key=key, Body=content)
    
    @staticmethod
    def cleanup_test_files(bucket: str, prefix: str):
        """Helper to cleanup test files from S3"""
        s3 = boto3.client('s3')
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        
        if 'Contents' in response:
            for obj in response['Contents']:
                s3.delete_object(Bucket=bucket, Key=obj['Key'])
    
    @staticmethod
    def invoke_lambda_function(function_name: str, payload: dict):
        """Helper to invoke Lambda function"""
        lambda_client = boto3.client('lambda')
        
        response = lambda_client.invoke(
            FunctionName=function_name,
            InvocationType='RequestResponse',
            Payload=json.dumps(payload)
        )
        
        return json.loads(response['Payload'].read())
    
    @staticmethod
    def wait_for_s3_object(bucket: str, key: str, timeout: int = 30):
        """Wait for S3 object to be available"""
        s3 = boto3.client('s3')
        import time
        
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                s3.head_object(Bucket=bucket, Key=key)
                return True
            except:
                time.sleep(1)
        
        return False.WARNING]
    
    def test_validate_data_quality(self, sample_dataframe):
        """Test data quality validation"""
        engine = ValidationRulesEngine('test-table')
        rule = {
            'rule_type': 'data_quality',
            'null_threshold': 0.1,
            'check_duplicates': True
        }
        
        result = engine.apply_rule(rule, sample_dataframe)
        
        assert result.status in [ValidationStatus.PASSED, ValidationStatus