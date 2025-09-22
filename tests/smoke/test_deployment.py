
import pytest
import boto3
import os

@pytest.mark.smoke
class TestDeploymentSmoke:
    """Smoke tests for deployed environment"""
    
    @pytest.fixture
    def environment(self):
        """Get environment from command line"""
        return os.environ.get('ENVIRONMENT', 'dev')
    
    def test_s3_buckets_exist(self, environment):
        """Test that required S3 buckets exist"""
        s3 = boto3.client('s3')
        
        required_buckets = [
            f'ukhsa-landing-{environment}',
            f'ukhsa-validated-{environment}',
            f'ukhsa-logs-{environment}'
        ]
        
        response = s3.list_buckets()
        bucket_names = [b['Name'] for b in response['Buckets']]
        
        for bucket in required_buckets:
            assert bucket in bucket_names, f"Bucket {bucket} not found"
    
    def test_lambda_functions_exist(self, environment):
        """Test that Lambda functions are deployed"""
        lambda_client = boto3.client('lambda')
        
        required_functions = [
            f'ukhsa-file-processor-{environment}',
            f'ukhsa-data-validator-{environment}'
        ]
        
        for function_name in required_functions:
            try:
                response = lambda_client.get_function(FunctionName=function_name)
                assert response['Configuration']['State'] == 'Active'
            except lambda_client.exceptions.ResourceNotFoundException:
                pytest.fail(f"Lambda function {function_name} not found")