"""
End-to-End tests for the full S3 ingestion flow.
Verifies the integration between S3, Celery, and the Database.
"""
import logging
import time
from datetime import date
from decimal import Decimal
from pathlib import Path

import boto3
import pytest
from django.conf import settings

from core.models import AuditRecord, PharmacyClaim
from core.tests.utils import ensure_bucket

logger = logging.getLogger(__name__)

# Test Configuration
BUCKET_NAME = 'healthcare-ingestion-drop-zone'
BASE_DIR = Path(__file__).resolve().parents[1]
TEST_DATA_DIR = BASE_DIR / 'data'

TEST_CASES = [
    {
        "id": "audit_flow",
        "key": "audit/e2e_test.csv",
        "filename": "audit_record_valid.csv",
        "model": AuditRecord,
        "expected_count": 2,
        "first_record_checks": {
            "provider_npi": "1234567890",
            "billing_amount": Decimal("123.45"),
            "service_date": date(2023, 1, 15),
            "status": "processed"
        }
    },
    {
        "id": "pharmacy_flow",
        "key": "pharmacy/e2e_test.csv",
        "filename": "pharmacy_claim_valid.csv",
        "model": PharmacyClaim,
        "expected_count": 2,
        "first_record_checks": {
            "claim_id": "CLM001",
            "ncpdp_id": "NCP001",
            "bin_number": "BIN001",
            "service_date": date(2025, 1, 1),
            "total_amount_paid": Decimal("150.00"),
            "transaction_code": "TXN001"
        }
    }
]

@pytest.fixture
def s3_client():
    """Provides a configured S3 client for checking LocalStack."""
    return boto3.client(
        's3',
        endpoint_url=settings.AWS_ENDPOINT_URL,
        aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
        region_name=settings.AWS_DEFAULT_REGION
    )

@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("case", TEST_CASES, ids=lambda x: x["id"])
def test_ingestion_flow(s3_client, case):
    """
    Data-Driven E2E Test:
    Validates that CSVs uploaded to S3 are correctly processed into the Database 
    with 100% field fidelity.
    """
    
    ensure_bucket(s3_client, BUCKET_NAME)
    
    file_path = TEST_DATA_DIR / case["filename"]
    if not file_path.exists():
         pytest.fail(f"Test data file not found: {file_path}") 
         
    with open(file_path, 'rb') as f:
        s3_client.upload_fileobj(f, BUCKET_NAME, case["key"])
    
    try:
        # We wait for the records to appear in the database
        max_retries = 30
        records_found = False
        
        for _ in range(max_retries):
            count = case["model"].objects.count()
            if count >= case["expected_count"]:
                records_found = True
                break
            time.sleep(1)
            
        assert records_found, (
            f"Timeout waiting for {case['expected_count']} records to appear in {case['model'].__name__}. "
            f"Found {case['model'].objects.count()}"
        )
        
        assert case["model"].objects.count() == case["expected_count"]
        
        record = case["model"].objects.order_by('service_date').first()
        for field, expected_value in case["first_record_checks"].items():
            actual_value = getattr(record, field)
            assert actual_value == expected_value, \
                f"Field mismatch for '{field}': Expected {expected_value}, got {actual_value}"
                
    finally:
        s3_client.delete_object(Bucket=BUCKET_NAME, Key=case["key"])
        # Clear database for next test case run
        case["model"].objects.all().delete()

def test_ingestion_flow_missing_data(s3_client):
    """
    Test that the E2E test fails gracefully if the data file is missing.
    This covers the defensive check in test_ingestion_flow.
    """
    case = {
        "id": "missing_file",
        "key": "audit/missing.csv",
        "filename": "non_existent_file_ABC123.csv",
        "model": AuditRecord, # minimal fields to satisfy function sig if it proceeded
    }
    
    # pytest.fail raises a Failed exception (inherits from BaseException)
    with pytest.raises(BaseException, match="Test data file not found"):
        test_ingestion_flow(s3_client, case)
