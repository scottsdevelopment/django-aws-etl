"""
Integration tests for the s3_processing_task Celery task.
"""

from unittest.mock import patch

import pytest

from core.models import Artifact
from core.tasks import process_s3_file


@pytest.mark.django_db
def test_audit_s3_processing_success(set_s3_content):
    """Test successful ingestion of an AuditRecord file from S3 into RawData."""
    # Setup
    csv_content = "provider_npi,billing_amount,service_date,status\n1234567890,100.00,2023-01-01,submitted"
    mock_instance = set_s3_content("bucket", "audit/test.csv", csv_content)

    with (
        patch("core.tasks.s3_processing.boto3.client", return_value=mock_instance),
        patch("core.tasks.s3_processing.process_artifact_task") as mock_process_task,
    ):
        # Execute
        result = process_s3_file("bucket", "audit/test.csv")

        # Assert
        assert result["success"] == 1
        assert "artifact_id" in result

        # Verify Artifact created
        assert Artifact.objects.filter(file="audit/test.csv").exists()

        # Verify next task triggered
        mock_process_task.delay.assert_called_once_with(result["artifact_id"])

        # NOTE: We no longer check AuditRecord here as that is done async in step 2.
        # See test_raw_ingestion.py for step 2 verification.


@pytest.mark.django_db
def test_audit_s3_processing_failure(set_s3_content):
    """Test failure scenarios (empty/bad file) logs result."""
    # Using empty CSV which fails raw ingestion (no headers)
    csv_content = ""
    mock_instance = set_s3_content("bucket", "audit/test.csv", csv_content)

    with (
        patch("core.tasks.s3_processing.boto3.client", return_value=mock_instance),
        patch("core.tasks.s3_processing.process_artifact_task") as mock_process_task,
    ):
        # Execute
        result = process_s3_file("bucket", "audit/test.csv")

    # Assert
    assert result["success"] == 0
    assert result["failed"] == 1
    # raw ingestion marks artifact as FAILED if empty headers
    assert Artifact.objects.filter(status="FAILED").exists()
    mock_process_task.delay.assert_not_called()


def test_process_s3_file_unknown_prefix():
    """
    Verifies that an unknown object key prefix logs an error.
    """
    result = process_s3_file("bucket", "unknown/file.csv")
    assert result["success"] == 0
    assert "No content type mapping found" in result["error"]


def test_process_s3_file_exception_retry(mock_s3, monkeypatch):
    """
    Verifies that an exception during S3 processing triggers a Celery retry.
    """
    mock_s3.get_object.side_effect = Exception("S3 Connection Failure")

    # We need to mock 'self.retry' because we're calling the task function directly
    # and it expects to be bound to a Celery task instance.
    with patch("core.tasks.s3_processing.process_s3_file.retry") as mock_retry:
        mock_retry.side_effect = Exception("Retry Triggered")
        with pytest.raises(Exception, match="Retry Triggered"):
            process_s3_file("bucket", "audit/test.csv")
        mock_retry.assert_called_once()
