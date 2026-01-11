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
from django.conf import settings
from django.test import override_settings

from core.models import Artifact, AuditRecord, LabResult, PharmacyClaim
from core.tasks.s3_processing import process_s3_file
from core.tests.utils import ensure_bucket

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
        "sort_field": "service_date",
        "expected_count": 2,
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
        "sort_field": "service_date",
        "expected_count": 2,
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
        "sort_field": "performed_at",
        "expected_count": 3,
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
        endpoint_url=settings.AWS_ENDPOINT_URL,
        aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
        region_name=settings.AWS_DEFAULT_REGION,
    )


@pytest.mark.django_db
@pytest.mark.parametrize("case", TEST_CASES, ids=lambda x: x["id"])
def test_ingestion_flow(s3_client, case):
    """
    Synchronous Integration Test:
    Validates that CSVs uploaded to S3 are correctly processed into the Database
    with 100% field fidelity. Uses CELERY_TASK_ALWAYS_EAGER to run in-process.
    """

    ensure_bucket(s3_client, BUCKET_NAME)

    file_path = TEST_DATA_DIR / case["filename"]
    if not file_path.exists():
        pytest.fail(f"Test data file not found: {file_path}")

    # 1. Upload to S3 (so it exists for the task to download)
    with open(file_path, "rb") as f:
        s3_client.upload_fileobj(f, BUCKET_NAME, case["key"])

    try:
        # 2. Run Task Synchronously (Eager Mode)
        # We manually trigger the task, effectively simulating the S3->SQS->Worker handoff
        # but configured to run immediately in this thread/transaction.
        with override_settings(CELERY_TASK_ALWAYS_EAGER=True, CELERY_TASK_EAGER_PROPAGATES=True):
            process_s3_file.delay(bucket_name=BUCKET_NAME, object_key=case["key"])

        # 3. Verify Results immediately (no waiting/retries needed)
        
        # Check count
        actual_count = case["model"].objects.count()
        if actual_count != case["expected_count"]:
            # Debug info
            artifacts = Artifact.objects.filter(file=case["key"])
            debug_info = f"Artifacts: {list(artifacts.values('id', 'status', 'created_at'))}"
            pytest.fail(
                f"Count mismatch. Expected {case['expected_count']}, got {actual_count}. {debug_info}"
            )

        # Check data fidelity
        record = case["model"].objects.order_by(case["sort_field"]).first()
        for field, expected_value in case["first_record_checks"].items():
            actual_value = getattr(record, field)
            assert actual_value == expected_value, (
                f"Field mismatch for '{field}': Expected {expected_value}, got {actual_value}"
            )

    finally:
        # Cleanup S3 only (DB is handled by transaction rollback)
        s3_client.delete_object(Bucket=BUCKET_NAME, Key=case["key"])


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
