import io
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

TEST_DATA_DIR = Path(__file__).parent / 'data'

@pytest.fixture
def get_test_csv():
    """Fixture that returns a function to read a CSV file from the test data directory."""
    def _read_csv(filename):
        path = TEST_DATA_DIR / filename
        if not path.exists():
            raise FileNotFoundError(f"Test data file not found: {path}")
        with open(path) as f:
            return f.read()
    return _read_csv

@pytest.fixture
def mock_s3():
    """Fixture to handle the entire S3 client lifecycle and mocking."""
    with patch('core.tasks.s3_processing.boto3.client') as mock_client:
        mock_instance = MagicMock()
        mock_client.return_value = mock_instance
        yield mock_instance

@pytest.fixture
def set_s3_content():
    """Fixture that returns a function to set mocked S3 content."""
    def _set_content(bucket, key, content):
        mock_instance = MagicMock()
        mock_instance.get_object.return_value = {
            'Body': io.BytesIO(content.encode('utf-8'))
        }
        return mock_instance
    return _set_content
