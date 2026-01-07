"""
Integration tests for the core ingestion service.
"""
import csv
import io
from unittest.mock import MagicMock

import pytest

from core.models import AuditRecord, IngestionError, PharmacyClaim
from core.services.ingestion import ingest_csv_data
from core.strategies import get_strategy


@pytest.mark.django_db
def test_ingest_audit_data_success():
    """Test successful ingestion of valid Audit Record CSV data."""
    data = "provider_npi,billing_amount,service_date,status\n1234567890,100.50,2023-01-01,submitted"
    file_obj = io.StringIO(data)
    strategy = get_strategy('audit')
    
    success, failure = ingest_csv_data(file_obj, strategy)
    
    assert success == 1
    assert failure == 0
    assert AuditRecord.objects.filter(provider_npi="1234567890").exists()

@pytest.mark.django_db
def test_ingest_pharmacy_data_success():
    """Test successful ingestion of valid Pharmacy Claim CSV data."""
    data = (
        "claim_id,ncpdp_id,bin_number,service_date,total_amount_paid,transaction_code\n"
        "CLM123,NCP123,BIN123,2025-01-01,150.00,TXN01"
    )
    file_obj = io.StringIO(data)
    strategy = get_strategy('pharmacy')
    
    success, failure = ingest_csv_data(file_obj, strategy)
    
    assert success == 1
    assert failure == 0
    assert PharmacyClaim.objects.filter(claim_id="CLM123").exists()

@pytest.mark.django_db
def test_ingest_invalid_data_logs_error():
    """Test that invalid data triggers validation errors and logs to IngestionError."""
    data = "provider_npi,billing_amount,service_date,status\n123,100.50,2023-01-01,submitted"
    file_obj = io.StringIO(data)
    strategy = get_strategy('audit')
    
    success, failure = ingest_csv_data(file_obj, strategy)
    
    assert success == 0
    assert failure == 1
    assert IngestionError.objects.count() == 1
    assert "Validation Failed" in IngestionError.objects.first().error_reason

@pytest.mark.django_db
def test_ingest_csv_data_empty_file():
    """Test that an empty file returns zero success and failure counts."""
    file_handle = io.StringIO("")
    strategy = get_strategy('audit')
    
    success, failed = ingest_csv_data(file_handle, strategy)
    assert success == 0
    assert failed == 0

@pytest.mark.django_db
def test_ingest_csv_data_fatal_read_error(monkeypatch):
    """Test handling of fatal CSV read errors at the reader level."""
    strategy = get_strategy('audit')
    file_handle = io.StringIO("broken")
    
    def mock_reader(*args, **kwargs):
        raise csv.Error("Fatal CSV Error")
    
    monkeypatch.setattr(csv, "DictReader", mock_reader)
    
    success, failed = ingest_csv_data(file_handle, strategy)
    assert success == 0
    assert failed == 0

@pytest.mark.django_db
def test_ingest_csv_data_generic_exception(monkeypatch):
    """Test that generic database or runtime exceptions are caught and logged."""
    strategy = get_strategy('audit')
    file_handle = io.StringIO("provider_npi,billing_amount,service_date,status\n1234567890,100.00,2025-01-01,active")
    
    def mock_update_or_create(*args, **kwargs):
        raise Exception("Database Error")
    
    monkeypatch.setattr(strategy.model_class.objects, "update_or_create", mock_update_or_create)
    
    success, failed = ingest_csv_data(file_handle, strategy)
    assert success == 0
    assert failed == 1
    assert IngestionError.objects.filter(error_reason__contains="Runtime Error: Database Error").exists()

@pytest.mark.django_db
def test_ingest_csv_data_schema_generic_error(monkeypatch):
    """Test that generic schema validation errors are caught and logged."""
    strategy = get_strategy('audit')
    file_handle = io.StringIO("provider_npi,billing_amount,service_date,status\n1234567890,100.00,2025-01-01,active")
    
    def mock_validate(*args, **kwargs):
        raise Exception("Generic Schema Error")
    
    monkeypatch.setattr(strategy.schema_class, "model_validate", mock_validate)
    
    success, failed = ingest_csv_data(file_handle, strategy)
    assert success == 0
    assert failed == 1
    assert IngestionError.objects.filter(error_reason__contains="Schema Error: Generic Schema Error").exists()

@pytest.mark.django_db
def test_ingest_csv_data_empty_headers(monkeypatch):
    """Test that empty headers log an error and return 0,0."""
    file_handle = io.StringIO("") # Empty file
    strategy = get_strategy('audit')
    
    mock_reader = MagicMock()
    mock_reader.fieldnames = None
    
    monkeypatch.setattr(csv, "DictReader", lambda *args, **kwargs: mock_reader)
    
    success, failed = ingest_csv_data(file_handle, strategy)
    assert success == 0
    assert failed == 0


@pytest.mark.django_db
def test_ingest_csv_data_schema_runtime_error(monkeypatch):
    """Test that runtime errors during schema validation are caught and logged."""
    strategy = get_strategy('audit')
    file_handle = io.StringIO("provider_npi,billing_amount,service_date,status\n1234567890,100.00,2025-01-01,active")
    
    def mock_validate(*args, **kwargs):
        raise RuntimeError("Something went wrong during schema validation")
    
    monkeypatch.setattr(strategy.schema_class, "model_validate", mock_validate)
    
    success, failed = ingest_csv_data(file_handle, strategy)
    assert success == 0
    assert failed == 1
    assert IngestionError.objects.filter(error_reason__contains="Schema Error: Something went wrong").exists()

@pytest.mark.django_db
def test_ingest_csv_data_unexpected_error(monkeypatch):
    """Test that unexpected exceptions during reading are caught."""
    strategy = get_strategy('audit')
    file_handle = io.StringIO("some,data")
    
    def mock_reader(*args, **kwargs):
        raise ValueError("Unexpected Boom")
    
    monkeypatch.setattr(csv, "DictReader", mock_reader)
    
    success, failed = ingest_csv_data(file_handle, strategy)
    assert success == 0
    assert failed == 0
