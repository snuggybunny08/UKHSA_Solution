
# ============================================================
# tests/unit/test_processor.py - File processor unit tests
# ============================================================

import pytest
import json
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
import sys
import os

# Add lambda directory to path
sys.path.insert(0, os.path.abspath('../../lambdas/file_processor'))
from index import FileProcessor, handler

class TestFileProcessor:
    """Unit tests for FileProcessor class"""
    
    @pytest.fixture
    def processor(self):
        """Create FileProcessor instance"""
        return FileProcessor()
    
    @pytest.fixture
    def s3_event(self):
        """Create sample S3 event"""
        return {
            'Records': [{
                's3': {
                    'bucket': {'name': 'test-bucket'},
                    'object': {
                        'key': 'test/data.csv',
                        'size': 1024
                    }
                },
                'eventTime': '2025-01-01T00:00:00Z',
                'eventName': 's3:ObjectCreated:Put'
            }]
        }
    
    def test_validate_file_valid(self, processor):
        """Test file validation with valid file"""
        with patch('boto3.client') as mock_client:
            mock_s3 = Mock()
            mock_client.return_value = mock_s3
            mock_s3.head_object.return_value = {'ContentLength': 1024}
            mock_s3.get_object_tagging.return_value = {
                'TagSet': [
                    {'Key': 'dataset_type', 'Value': 'test'},
                    {'Key': 'source_organization', 'Value': 'UKHSA'}
                ]
            }
            
            result = processor.validate_file('bucket', 'file.csv', 1024)
            assert result['valid'] is True
    
    def test_validate_file_unsupported_format(self, processor):
        """Test validation with unsupported file format"""
        result = processor.validate_file('bucket', 'file.pdf', 1024)
        assert result['valid'] is False
        assert 'Unsupported file format' in result['reason']
    
    def test_validate_file_too_large(self, processor):
        """Test validation with oversized file"""
        result = processor.validate_file('bucket', 'file.csv', 200*1024*1024)
        assert result['valid'] is False
        assert 'exceeds maximum' in result['reason']
    
    @patch('boto3.client')
    def test_process_s3_event_success(self, mock_boto, processor, s3_event):
        """Test successful S3 event processing"""
        mock_s3 = Mock()
        mock_lambda = Mock()
        mock_boto.side_effect = lambda service: mock_s3 if service == 's3' else mock_lambda
        
        mock_s3.head_object.return_value = {'ContentLength': 1024}
        mock_s3.get_object_tagging.return_value = {'TagSet': []}
        mock_lambda.invoke.return_value = {
            'StatusCode': 202,
            'ResponseMetadata': {'RequestId': 'test-123'}
        }
        
        with patch.object(processor, 'trigger_macie_scan'):
            result = processor.process_s3_event(s3_event)
            
            assert result['processed'] == 1
            assert result['results'][0]['status'] == 'success'

