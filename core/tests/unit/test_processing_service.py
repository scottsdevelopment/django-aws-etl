from unittest.mock import MagicMock, patch

import pytest

from core.models import Artifact, PharmacyClaim, RawData
from core.services.processing_service import process_artifact


@pytest.mark.django_db
def test_process_artifact_success():
    """Test processing of valid RawData into domain model."""
    artifact = Artifact.objects.create(file="drop-zone/test.csv", content_type="pharmacy", status="COMPLETED")
    RawData.objects.create(
        artifact=artifact,
        row_index=1,
        data={
            "claim_id": "C999",
            "ncpdp_id": "NCPDP99",
            "bin_number": "BIN99",
            "service_date": "2023-12-31",
            "total_amount_paid": "99.99",
            "transaction_code": "T99",
        },
        status="PENDING",
    )

    process_artifact(artifact.id)

    assert PharmacyClaim.objects.filter(claim_id="C999").exists()
    row = RawData.objects.get(artifact=artifact, row_index=1)
    assert row.status == "PROCESSED"


@pytest.mark.django_db
def test_process_artifact_validation_failure():
    """Test that invalid data is marked as FAILED with error message."""
    artifact = Artifact.objects.create(file="bad.csv", content_type="pharmacy", status="COMPLETED")
    RawData.objects.create(
        artifact=artifact,
        row_index=1,
        data={
            "claim_id": "CBad",
            # Missing required fields
        },
        status="PENDING",
    )

    process_artifact(artifact.id)

    assert not PharmacyClaim.objects.filter(claim_id="CBad").exists()
    row = RawData.objects.get(artifact=artifact, row_index=1)
    assert row.status == "FAILED"
    assert "Validation Failed" in row.error_message


@pytest.mark.django_db
def test_process_artifact_idempotency():
    """Test that processing is idempotent (update_or_create)."""
    artifact = Artifact.objects.create(file="idempotent.csv", content_type="pharmacy", status="COMPLETED")
    data = {
        "claim_id": "CIdempotent",
        "ncpdp_id": "NCPDP1",
        "bin_number": "BIN1",
        "service_date": "2023-01-01",
        "total_amount_paid": "100.00",
        "transaction_code": "T1",
    }

    RawData.objects.create(artifact=artifact, row_index=1, data=data, status="PENDING")
    process_artifact(artifact.id)
    assert PharmacyClaim.objects.filter(claim_id="CIdempotent").count() == 1

    RawData.objects.create(artifact=artifact, row_index=2, data=data, status="PENDING")
    process_artifact(artifact.id)
    assert PharmacyClaim.objects.filter(claim_id="CIdempotent").count() == 1


@pytest.mark.django_db
def test_process_artifact_not_found():
    """Test graceful handling when artifact ID does not exist."""
    result = process_artifact(999999)
    assert result is None


@pytest.mark.django_db
def test_process_artifact_no_strategy():
    """Test handling of unknown content_type."""
    artifact = Artifact.objects.create(file="unknown.csv", content_type="unknown_type", status="COMPLETED")
    result = process_artifact(artifact.id)
    assert result is None


@pytest.mark.django_db
def test_process_artifact_runtime_exception():
    """Test generic runtime exception during row processing."""
    artifact = Artifact.objects.create(file="crash.csv", content_type="pharmacy", status="COMPLETED")
    RawData.objects.create(artifact=artifact, row_index=1, data={"some": "data"}, status="PENDING")

    # Patch StrategyFactory inside processing_service
    mock_strategy = MagicMock()
    mock_strategy.model_class.__name__ = "MockModel"
    mock_strategy.unique_fields = []
    mock_strategy.schema_class.model_validate.side_effect = Exception("Runtime Boom")

    with patch("core.services.processing_service.StrategyFactory") as mock_factory:
        mock_factory.get_strategy.return_value = mock_strategy
        success, failed = process_artifact(artifact.id)

    assert success == 0
    assert failed == 1

    row = RawData.objects.get(artifact=artifact, row_index=1)
    assert row.status == "FAILED"
    assert "Runtime Boom" in row.error_message
