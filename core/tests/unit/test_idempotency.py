import pytest

from core.models import Artifact, PharmacyClaim, RawData
from core.services.processing_service import process_artifact


@pytest.mark.django_db
def test_repro_whitespace_duplication():
    """Test if whitespace in claim_id causes duplicates (it should, if not stripped)."""
    artifact = Artifact.objects.create(file="whitespace.csv", content_type="pharmacy", status="COMPLETED")
    
    # Row 1: "C_WS_1"
    RawData.objects.create(
        artifact=artifact, 
        row_index=1, 
        data={
            "claim_id": "C_WS_1",
            "ncpdp_id": "N1",
            "bin_number": "B1",
            "service_date": "2023-01-01",
            "total_amount_paid": "10.00",
            "transaction_code": "T1"
        },
        status="PENDING"
    )

    process_artifact(artifact.id)
    assert PharmacyClaim.objects.count() == 1
    
    # Row 2: "C_WS_1 " (Trailing space)
    RawData.objects.create(
        artifact=artifact, 
        row_index=2, 
        data={
            "claim_id": "C_WS_1 ",
            "ncpdp_id": "N1",
            "bin_number": "B1",
            "service_date": "2023-01-01",
            "total_amount_paid": "10.00",
            "transaction_code": "T1"
        },
        status="PENDING"
    )
    
    process_artifact(artifact.id)
    
    # If stripping is NOT implemented, we expect 2 records (which is the "bug" or "behavior" 
    # we want to prevent if we want natural key robustness)
    # If the user considers "C_WS_1 " and "C_WS_1" to be the same claim, then we currently fail this check 
    # (we have 2 records).
    count = PharmacyClaim.objects.count()
    print(f"\nWhitespace Test Count: {count}")
    # assert count == 1 # We expect this to fail currently

@pytest.mark.django_db
def test_repro_same_batch_duplicates():
    """Test if same batch duplicates cause crash or duplicates."""
    artifact = Artifact.objects.create(file="batch_dup.csv", content_type="pharmacy", status="COMPLETED")
    
    # Two rows with SAME claim_id in SAME batch (pending)
    data = {
        "claim_id": "C_BATCH_1",
        "ncpdp_id": "N1",
        "bin_number": "B1",
        "service_date": "2023-01-01",
        "total_amount_paid": "10.00",
        "transaction_code": "T1"
    }
    
    RawData.objects.create(artifact=artifact, row_index=1, data=data, status="PENDING")
    RawData.objects.create(artifact=artifact, row_index=2, data=data, status="PENDING")
    
    # processing_service processes them in one batch.
    # Postgres bulk_create with update_conflicts on duplicate keys in same batch raises error?
    try:
        process_artifact(artifact.id)
        print("\nBatch processing succeeded")
    except Exception as e:
        print(f"\nBatch processing failed with: {e}")

    count = PharmacyClaim.objects.filter(claim_id="C_BATCH_1").count()
    print(f"Batch Test Count: {count}")

@pytest.mark.django_db
def test_idempotency_on_restart_simulate_crash():
    """
    Simulate a scenario where the task crashed PREVIOUSLY:
    1. Domain model (PharmacyClaim) was created.
    2. But RawData was NOT updated to PROCESSED (still PENDING).
    
    We expect:
    - process_artifact runs again.
    - It detects the existing record (via unique_fields).
    - It performs an UPSERT (updates data if changed, or no-op).
    - It finally updates RawData to PROCESSED.
    - Result: No duplicates, correct status.
    """
    # 1. Setup Artifact
    artifact = Artifact.objects.create(file="crash_test.csv", content_type="pharmacy", status="COMPLETED")
    
    # 2. Create the PENDING raw row
    data = {
        "claim_id": "C_CRASH_1",
        "ncpdp_id": "NCPDP_OLD", 
        "bin_number": "BIN_1",
        "service_date": "2024-01-01",
        "total_amount_paid": "100.00",
        "transaction_code": "T1"
    }
    RawData.objects.create(artifact=artifact, row_index=1, data=data, status="PENDING")

    # 3. SIMULATE THE CRASH STATE:
    # Use the VALID strategy logic to pre-seed the DB, but "forget" to update RawData
    # If we run process_artifact now, it will try to insert C_CRASH_1 again.
    PharmacyClaim.objects.create(
        claim_id="C_CRASH_1",
        ncpdp_id="NCPDP_OLD",
        bin_number="BIN_1",
        service_date="2024-01-01",
        total_amount_paid="100.00",
        transaction_code="T1"
    )
    
    assert PharmacyClaim.objects.count() == 1
    assert RawData.objects.filter(status="PENDING").count() == 1

    # 4. START THE TASK (The Restart)
    success, failed = process_artifact(artifact.id)

    # 5. VERIFY
    assert success == 1
    assert failed == 0
    
    # Check NO duplicates
    assert PharmacyClaim.objects.count() == 1
    
    # Check Idempotency/Upsert (Data should match what was in RawData)
    claim = PharmacyClaim.objects.get(claim_id="C_CRASH_1")
    assert claim.ncpdp_id == "NCPDP_OLD"
    
    # Check RawData is now PROCESSED
    assert RawData.objects.filter(status="PROCESSED").count() == 1
