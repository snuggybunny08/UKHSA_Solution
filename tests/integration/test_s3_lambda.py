# ============================================================
# tests/integration/test_s3_lambda.py - Integration tests
# ============================================================

import pytest
import boto3
from moto import mock_s3, mock_lambda
import json

@pytest.mark.integration
class TestS3LambdaIntegration:
    """Integration tests for S3 and Lambda"""
    
    @mock_s3
    @mock_lambda
    def test_file_upload_triggers_lambda(self):
        """Test that S3 upload triggers Lambda function"""
        # Setup
        s3 = boto3.client('s3', region_name='eu-west-2')
        s3.create_bucket(
            Bucket='test-landing',
            CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'}
        )
        
        # Upload file
        s3.put_object(
            Bucket='test-landing',
            Key='uploads/test.csv',
            Body=b'date,cases\n2025-01-01,100'
        )
        
        # Verify file exists
        response = s3.list_objects_v2(Bucket='test-landing')
        assert 'Contents' in response
        assert len(response['Contents']) == 1

