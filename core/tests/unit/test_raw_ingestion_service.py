from unittest.mock import patch

import pytest
from django.core.files.base import ContentFile

from core.models import Artifact, RawData
from core.services.raw_ingestion_service import ingest_file_to_raw

EXPECTED_RAW_COUNT = 2

@pytest.mark.django_db
def test_ingest_file_to_raw_success():
    """Test successful ingestion of a CSV into RawData."""
    csv_content = (
        "claim_id,ncpdp_id,bin_number,service_date,total_amount_paid,transaction_code\n"
        "C123,NCPDP1,BIN1,2023-01-01,100.00,T1\n"
        "C124,NCPDP1,BIN1,2023-01-02,200.00,T1"
    )
    file_obj = ContentFile(csv_content.encode('utf-8'), name="test.csv")
    
    # Pass s3_key as second argument
    artifact = ingest_file_to_raw(file_obj, "drop-zone/test.csv", "pharmacy")
    
    assert artifact.status == "COMPLETED"
    assert artifact.file == "drop-zone/test.csv"
    # This file uses name= in assertions. I need to replace those.
    assert Artifact.objects.filter(file="drop-zone/test.csv").count() == 1
    assert RawData.objects.filter(artifact=artifact).count() == EXPECTED_RAW_COUNT
    
    first_row = RawData.objects.get(artifact=artifact, row_index=1)
    assert first_row.data['claim_id'] == 'C123'
    assert first_row.status == "PENDING"
    assert first_row.artifact == artifact

@pytest.mark.django_db
def test_ingest_file_to_raw_exception():
    """Test that generic exceptions during ingestion mark artifact as FAILED."""
    file_obj = ContentFile(b"some data", name="crash.csv")
    
    with patch('core.services.raw_ingestion_service.csv.DictReader', side_effect=Exception("Boom")):
        artifact = ingest_file_to_raw(file_obj, "crash.csv", "pharmacy")
    
    assert artifact.status == "FAILED"

BATCH_TEST_SIZE = 2
BATCH_EXPECTED_CALLS = 2
BATCH_FIRST_CHUNK = 2
BATCH_SECOND_CHUNK = 1
TOTAL_BATCH_ITEMS = 3

@pytest.mark.django_db
def test_ingest_file_to_raw_batching():
    """Test that ingestion respects batch size limits."""
    csv_content = (
        "claim_id,ncpdp_id,bin_number,service_date,total_amount_paid,transaction_code\n"
        "C1,N,B,2023-01-01,10.00,T\n"
        "C2,N,B,2023-01-01,10.00,T\n"
        "C3,N,B,2023-01-01,10.00,T"
    )
    file_obj = ContentFile(csv_content.encode('utf-8'), name="batch.csv")
    
    # Patch the BATCH_SIZE constant in the module
    with patch('core.services.raw_ingestion_service.BATCH_SIZE', BATCH_TEST_SIZE), \
         patch('core.models.RawData.objects.bulk_create', wraps=RawData.objects.bulk_create) as mock_bulk:
            ingest_file_to_raw(file_obj, "batch.csv", "pharmacy")
            
            assert mock_bulk.call_count == BATCH_EXPECTED_CALLS
            assert len(mock_bulk.call_args_list[0][0][0]) == BATCH_FIRST_CHUNK
            assert len(mock_bulk.call_args_list[1][0][0]) == BATCH_SECOND_CHUNK

    assert RawData.objects.filter(artifact__file="batch.csv").count() == TOTAL_BATCH_ITEMS

@pytest.mark.django_db
def test_ingest_file_to_raw_no_seek():
    """Test ingestion when file_obj lacks seek method (e.g. string/bytes)."""
    # Pass bytes directly, which has no .seek()
    csv_content = b"key,value\nval1,val2"
    artifact = ingest_file_to_raw(csv_content, "direct_bytes.csv", "test")
    
    assert artifact.status == "COMPLETED"
    assert RawData.objects.filter(artifact=artifact).count() == 1
