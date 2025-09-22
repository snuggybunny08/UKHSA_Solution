
import pytest
import os
import sys
import boto3
from moto import mock_s3, mock_lambda, mock_dynamodb, mock_sns
from datetime import datetime
import pandas as pd
import json

# Add project directories to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../lambdas')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../glue')))

# Test configuration
TEST_CONFIG = {
    'aws_region': 'eu-west-2',
    'test_bucket': 'test-bucket',
    'environment': os.environ.get('TEST_ENV', 'test')
}

@pytest.fixture(scope='session')
def aws_credentials():
    """Mocked AWS Credentials for moto"""
    os.environ['AWS_ACCESS_KEY_ID'] = 'testing'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'testing'
    os.environ['AWS_SECURITY_TOKEN'] = 'testing'
    os.environ['AWS_SESSION_TOKEN'] = 'testing'
    os.environ['AWS_DEFAULT_REGION'] = TEST_CONFIG['aws_region']

@pytest.fixture
def s3_client(aws_credentials):
    """Create mocked S3 client"""
    with mock_s3():
        client = boto3.client('s3', region_name=TEST_CONFIG['aws_region'])
        # Create test bucket
        client.create_bucket(
            Bucket=TEST_CONFIG['test_bucket'],
            CreateBucketConfiguration={'LocationConstraint': TEST_CONFIG['aws_region']}
        )
        yield client

@pytest.fixture
def dynamodb_resource(aws_credentials):
    """Create mocked DynamoDB resource"""
    with mock_dynamodb():
        resource = boto3.resource('dynamodb', region_name=TEST_CONFIG['aws_region'])
        # Create validation rules table
        table = resource.create_table(
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
        yield resource

@pytest.fixture
def sample_csv_data():
    """Generate sample CSV data"""
    data = {
        'report_date': pd.date_range('2025-01-01', periods=10),
        'region': ['London', 'Wales', 'Scotland'] * 3 + ['Northern Ireland'],
        'disease_name': 'COVID-19',
        'case_count': [100, 150, 200, 120, 180, 220, 190, 160, 140, 170],
        'death_count': [5, 8, 10, 6, 9, 11, 9, 8, 7, 8],
        'hospitalization_count': [20, 30, 40, 24, 36, 44, 38, 32, 28, 34]
    }
    return pd.DataFrame(data)

@pytest.fixture
def sample_json_data():
    """Generate sample JSON data"""
    return {
        "records": [
            {
                "report_date": "2025-01-01",
                "region": "London",
                "disease_name": "COVID-19",
                "case_count": 100,
                "death_count": 5
            },
            {
                "report_date": "2025-01-02",
                "region": "Wales",
                "disease_name": "COVID-19",
                "case_count": 150,
                "death_count": 8
            }
        ]
    }

@pytest.fixture
def sample_xml_data():
    """Generate sample XML data"""
    return """<?xml version="1.0" encoding="UTF-8"?>
    <surveillance>
        <record>
            <report_date>2025-01-01</report_date>
            <region>London</region>
            <disease_name>COVID-19</disease_name>
            <case_count>100</case_count>
            <death_count>5</death_count>
        </record>
        <record>
            <report_date>2025-01-02</report_date>
            <region>Wales</region>
            <disease_name>COVID-19</disease_name>
            <case_count>150</case_count>
            <death_count>8</death_count>
        </record>
    </surveillance>"""

@pytest.fixture
def validation_rules():
    """Sample validation rules"""
    return [
        {
            'dataset_type': 'communicable_disease',
            'rule_id': 'schema_001',
            'rule_type': 'schema',
            'required_columns': ['report_date', 'case_count', 'region']
        },
        {
            'dataset_type': 'communicable_disease',
            'rule_id': 'range_001',
            'rule_type': 'value_ranges',
            'ranges': {
                'case_count': {'min': 0, 'max': 1000000},
                'death_count': {'min': 0, 'max': 10000}
            }
        }
    ]

@pytest.fixture
def mock_environment():
    """Set up mock environment variables"""
    env_vars = {
        'VALIDATED_BUCKET': 'test-validated',
        'LOGS_BUCKET': 'test-logs',
        'VALIDATION_RULES_TABLE': 'validation-rules',
        'SNS_TOPIC_ARN': 'arn:aws:sns:eu-west-2:123456789012:test-topic',
        'ENVIRONMENT': 'test',
        'LOG_LEVEL': 'DEBUG'
    }
    
    for key, value in env_vars.items():
        os.environ[key] = value
    
    yield
    
    # Cleanup
    for key in env_vars:
        if key in os.environ:
            del os.environ[key]

# Markers for different test types
def pytest_configure(config):
    config.addinivalue_line("markers", "unit: Unit tests")
    config.addinivalue_line("markers", "integration: Integration tests")
    config.addinivalue_line("markers", "smoke: Smoke tests")
    config.addinivalue_line("markers", "performance: Performance tests")
    config.addinivalue_line("markers", "slow: Slow running tests")

