"""
Unit tests for the 'ingest_csv_file' management command.
"""

from io import StringIO
from unittest.mock import MagicMock

import pytest
from django.core.management import call_command

from core.models import Artifact


@pytest.mark.django_db
def test_ingest_csv_file_command_success(tmp_path, monkeypatch):
    """Test successful execution of the ingestion command."""
    csv_file = tmp_path / "test.csv"
    csv_file.write_text("provider_npi,billing_amount,service_date,status\n1234567890,100.00,2025-01-01,active")

    mock_artifact = MagicMock(spec=Artifact)
    mock_artifact.id = 1
    mock_artifact.status = "COMPLETED"

    # Mock ingest_file_to_raw
    # Note: mocking where it is used (in the command module)
    monkeypatch.setattr(
        "core.management.commands.ingest_csv_file.ingest_file_to_raw", lambda *args, **kwargs: mock_artifact
    )
    # Mock process_artifact returning (success, failed)
    monkeypatch.setattr("core.management.commands.ingest_csv_file.process_artifact", lambda *args, **kwargs: (1, 0))

    out = StringIO()
    call_command("ingest_csv_file", str(csv_file), "--type", "audit", stdout=out)

    output = out.getvalue()
    assert "Ingestion complete" in output
    assert "Success: 1" in output


@pytest.mark.django_db
def test_ingest_csv_file_command_file_not_found():
    """Test command behavior when the file is missing."""
    out = StringIO()
    call_command("ingest_csv_file", "non_existent.csv", "--type", "audit", stdout=out)
    assert "File not found" in out.getvalue()


@pytest.mark.django_db
def test_ingest_csv_file_command_unknown_type(tmp_path):
    """Test rejection of unknown ingestion types."""
    csv_file = tmp_path / "test.csv"
    csv_file.touch()

    out = StringIO()
    call_command("ingest_csv_file", str(csv_file), "--type", "unknown", stdout=out)
    assert "Unknown data type" in out.getvalue()


@pytest.mark.django_db
def test_ingest_csv_file_command_fatal_error(tmp_path, monkeypatch):
    """Test handling of fatal errors during processing."""
    csv_file = tmp_path / "test.csv"
    csv_file.write_text("dummy")

    def mock_ingest(*args, **kwargs):
        raise Exception("Mocked Fatal Error")

    monkeypatch.setattr("core.management.commands.ingest_csv_file.ingest_file_to_raw", mock_ingest)

    out = StringIO()
    call_command("ingest_csv_file", str(csv_file), "--type", "audit", stdout=out)
    assert "Fatal error: Mocked Fatal Error" in out.getvalue()


@pytest.mark.django_db
def test_ingest_csv_file_command_raw_failure(tmp_path, monkeypatch):
    """Test handling when raw ingestion reports failure (e.g. bad CSV)."""
    csv_file = tmp_path / "test.csv"
    csv_file.write_text("dummy")

    mock_artifact = MagicMock(spec=Artifact)
    mock_artifact.status = "FAILED"

    monkeypatch.setattr(
        "core.management.commands.ingest_csv_file.ingest_file_to_raw", lambda *args, **kwargs: mock_artifact
    )

    out = StringIO()
    call_command("ingest_csv_file", str(csv_file), "--type", "audit", stdout=out)
    assert "Raw ingestion failed" in out.getvalue()


@pytest.mark.django_db
def test_ingest_csv_file_command_os_error(tmp_path, monkeypatch):
    """Test handling of OSError (e.g., permissions) when opening the file."""
    csv_file = tmp_path / "test.csv"
    csv_file.touch()

    # Mock open to raise OSError
    def mock_open(*args, **kwargs):
        raise OSError("Permission Denied")

    monkeypatch.setattr("builtins.open", mock_open)

    out = StringIO()
    call_command("ingest_csv_file", str(csv_file), "--type", "audit", stdout=out)
    assert "Error opening/reading file: Permission Denied" in out.getvalue()
