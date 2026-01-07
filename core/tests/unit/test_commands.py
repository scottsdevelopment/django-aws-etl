"""
Unit tests for the 'ingest_csv_file' management command.
"""
from io import StringIO

import pytest
from django.core.management import call_command

from core.models import AuditRecord


@pytest.mark.django_db
def test_ingest_csv_file_command_success(tmp_path):
    """Test successful execution of the ingestion command."""
    csv_file = tmp_path / "test.csv"
    csv_file.write_text("provider_npi,billing_amount,service_date,status\n1234567890,100.00,2025-01-01,active")
    
    out = StringIO()
    call_command('ingest_csv_file', str(csv_file), '--type', 'audit', stdout=out)
    
    output = out.getvalue()
    assert "Ingestion complete" in output, f"Command failed to report completion. Output: {output}"
    assert "Success: 1" in output
    assert AuditRecord.objects.count() == 1


@pytest.mark.django_db
def test_ingest_csv_file_command_file_not_found():
    """Test command behavior when the file is missing."""
    out = StringIO()
    call_command('ingest_csv_file', 'non_existent.csv', '--type', 'audit', stdout=out)
    assert "File not found" in out.getvalue()


@pytest.mark.django_db
def test_ingest_csv_file_command_unknown_type(tmp_path):
    """Test rejection of unknown ingestion types."""
    csv_file = tmp_path / "test.csv"
    csv_file.touch()
    
    out = StringIO()
    call_command('ingest_csv_file', str(csv_file), '--type', 'unknown', stdout=out)
    assert "Unknown data type" in out.getvalue()


@pytest.mark.django_db
def test_ingest_csv_file_command_fatal_error(tmp_path, monkeypatch):
    """Test handling of fatal errors during processing."""
    csv_file = tmp_path / "test.csv"
    csv_file.write_text("dummy")
    
    def mock_ingest(*args, **kwargs):
        raise Exception("Mocked Fatal Error")
    
    monkeypatch.setattr("core.management.commands.ingest_csv_file.ingest_csv_data", mock_ingest)
    
    out = StringIO()
    call_command('ingest_csv_file', str(csv_file), '--type', 'audit', stdout=out)
    assert "Fatal error: Mocked Fatal Error" in out.getvalue()


@pytest.mark.django_db
def test_ingest_csv_file_command_generic_exception(tmp_path, monkeypatch):
    """Test handling of generic unexpected exceptions."""
    csv_file = tmp_path / "test.csv"
    csv_file.write_text("dummy")
    
    def mock_ingest(*args, **kwargs):
        raise Exception("Generic Unexpected Error")
    
    monkeypatch.setattr("core.management.commands.ingest_csv_file.ingest_csv_data", mock_ingest)
    
    out = StringIO()
    call_command('ingest_csv_file', str(csv_file), '--type', 'audit', stdout=out)
    assert "Fatal error: Generic Unexpected Error" in out.getvalue()


@pytest.mark.django_db
def test_ingest_csv_file_command_os_error(tmp_path, monkeypatch):
    """Test handling of OSError (e.g., permissions) when opening the file."""
    csv_file = tmp_path / "test.csv"
    csv_file.touch()
    
    # Mock open to raise OSError
    # We strip the encoding arg from mock signature if strict, but simple side effect is enough
    def mock_open(*args, **kwargs):
        raise OSError("Permission Denied")
    
    # We need to patch builtins.open. 
    # Since the command uses `with open(...)`, this is safer.
    monkeypatch.setattr("builtins.open", mock_open)
    
    out = StringIO()
    call_command('ingest_csv_file', str(csv_file), '--type', 'audit', stdout=out)
    assert "Error opening/reading file: Permission Denied" in out.getvalue()
