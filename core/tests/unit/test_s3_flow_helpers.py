from unittest.mock import MagicMock

from core.tests.utils import ensure_bucket


def test_ensure_bucket_creates_if_missing():
    """Test that create_bucket is called if head_bucket fails."""
    s3 = MagicMock()
    # Simulate head_bucket failing (bucket doesn't exist)
    s3.head_bucket.side_effect = Exception("Not Found")

    ensure_bucket(s3, "test-bucket")

    s3.create_bucket.assert_called_once_with(Bucket="test-bucket")


def test_ensure_bucket_does_nothing_if_exists():
    """Test that create_bucket is NOT called if head_bucket succeeds."""
    s3 = MagicMock()

    ensure_bucket(s3, "test-bucket")

    # create_bucket should not be called
    s3.create_bucket.assert_not_called()
