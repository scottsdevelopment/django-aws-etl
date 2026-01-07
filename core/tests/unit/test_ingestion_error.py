"""
Unit tests for the IngestionError model.
"""
import pytest

from core.models.ingestion_error import IngestionError


@pytest.mark.django_db
def test_ingestion_error_creation():
    """Test creating an IngestionError record."""
    error = IngestionError.objects.create(
        raw_data={"foo": "bar"},
        error_reason="Test Error"
    )
    assert error.raw_data == {"foo": "bar"}
    assert error.error_reason == "Test Error"

@pytest.mark.django_db
def test_ingestion_error_str():
    """Test the string representation of IngestionError."""
    error = IngestionError.objects.create(
        raw_data={"test": "data"},
        error_reason="Test Reason"
    )
    assert "Error at" in str(error)
    assert "Test Reason" in str(error)
