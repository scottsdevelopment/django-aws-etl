"""
End-to-End tests for the full S3 ingestion flow.
Verifies the integration between S3, Celery, and the Database.
"""

import logging
from datetime import date
from decimal import Decimal
from pathlib import Path

import boto3
import pytest
from django.conf import settings as django_settings

from core.models import Artifact, AuditRecord, LabResult, PharmacyClaim, RawData
from core.strategies import audit_record, lab_result, pharmacy_claim  # noqa: F401 (Ensure registration)
from core.tasks.s3_processing import process_s3_file
from core.tests.utils import ensure_bucket, wait_for_artifact

logger = logging.getLogger(__name__)

# Test Configuration
BUCKET_NAME = "healthcare-ingestion-drop-zone"
BASE_DIR = Path(__file__).resolve().parents[1]
TEST_DATA_DIR = BASE_DIR / "data"

TEST_CASES = [
    {
        "id": "audit_flow",
        "key": "audit/e2e_test.csv",
        "filename": "audit_record_valid.csv",
        "model": AuditRecord,
        "first_record_checks": {
            "provider_npi": "1234567890",
            "billing_amount": Decimal("123.45"),
            "service_date": date(2023, 1, 15),
            "status": "processed",
        },
    },
    {
        "id": "pharmacy_flow",
        "key": "pharmacy/e2e_test.csv",
        "filename": "pharmacy_claim_valid.csv",
        "model": PharmacyClaim,
        "first_record_checks": {
            "claim_id": "CLM001",
            "ncpdp_id": "NCP001",
            "bin_number": "BIN001",
            "service_date": date(2025, 1, 1),
            "total_amount_paid": Decimal("150.00"),
            "transaction_code": "TXN001",
        },
    },
    {
        "id": "lab_result_flow",
        "key": "labs/e2e_test.csv",
        "filename": "lab_result_valid.csv",
        "model": LabResult,
        "first_record_checks": {
            "patient_id": "P001",
            "test_code": "L001",
            "result_value": Decimal("95.50"),
            "result_unit": "MG/DL",
            "test_name": "Glucose"
        },
    },
]


@pytest.fixture
def s3_client():
    """Provides a configured S3 client for checking LocalStack."""
    return boto3.client(
        "s3",
        endpoint_url=django_settings.AWS_ENDPOINT_URL,
        aws_access_key_id=django_settings.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=django_settings.AWS_SECRET_ACCESS_KEY,
        region_name=django_settings.AWS_DEFAULT_REGION,
    )


@pytest.mark.django_db(transaction=True)  # Pragmatic/Sticky: Commits allowed, seen by worker
@pytest.mark.parametrize("case", TEST_CASES, ids=lambda x: x["id"])
def test_ingestion_flow(s3_client, case):
    """
    Pragmatic Async E2E Test:
    - Uploads file to S3.
    - Dispatches task to Celery (SQS).
    - Polls DB for Artifact completion (Visual Trail).
    - Verifies "Sticky" Data (Persistence).
    """
    ensure_bucket(s3_client, BUCKET_NAME)

    file_path = TEST_DATA_DIR / case["filename"]
    if not file_path.exists():
        pytest.fail(f"Test data file not found: {file_path}")

    # 1. Upload to S3
    with open(file_path, "rb") as f:
        s3_client.upload_fileobj(f, BUCKET_NAME, case["key"])

    # 2. Dispatch Task (Async)
    # This sends message to localstack SQS, picked up by 'celery' container
    process_s3_file.delay(bucket_name=BUCKET_NAME, object_key=case["key"])

    # 3. Wait for Artifact (Polling)
    try:
        artifact = wait_for_artifact(case["key"], timeout=20)
    except TimeoutError:
        pytest.fail(f"Artifact processing timed out for key {case['key']}")

    assert artifact.status == Artifact.COMPLETED, f"Artifact failed. Status: {artifact.status}"
    
    # 4. Verify RawData Processing
    pending_raw = RawData.objects.filter(artifact=artifact, status=RawData.PENDING).count()
    processed_raw = RawData.objects.filter(artifact=artifact, status=RawData.PROCESSED).count()
    
    assert pending_raw == 0, "Found pending RawData rows"
    assert processed_raw > 0, "No RawData rows processed"

    # 5. Verify Domain Model Persistence (Idempotent Check)
    # Robust against duplicates from previous runs
    exists = case["model"].objects.filter(**case["first_record_checks"]).exists()
    assert exists, f"Expected domain record not found for {case['first_record_checks']}"


def test_ingestion_flow_missing_data(s3_client):
    """
    Test that the E2E test fails gracefully if the data file is missing.
    """
    case = {
        "id": "missing_file",
        "key": "audit/missing.csv",
        "filename": "non_existent_file_ABC123.csv",
        "model": AuditRecord, 
    }

    with pytest.raises(BaseException, match="Test data file not found"):
        test_ingestion_flow(s3_client, case)
