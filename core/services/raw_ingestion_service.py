import csv
import logging
from typing import TextIO

from core.models import Artifact, RawData

# Use a constant for batch size
BATCH_SIZE = 1000

logger = logging.getLogger(__name__)


def ingest_file_to_raw(file_obj: TextIO, file_name: str, content_type: str) -> Artifact:
    """
    Step 1: Ingests a CSV file into RawData models grouped by an Artifact.
    Table structure is preserved 1:1 in the 'data' JSONField.
    """
    # Note: We now store s3_key instead of file. 
    # file_obj is passed in just for reading parsing, not saving to the model.
    artifact = Artifact.objects.create(
        # We use the 'file' field to store the S3 Key/URI
        file=file_name,
        content_type=content_type,
        status="PROCESSING",
    )

    try:
        # Reset pointer just in case
        if hasattr(file_obj, 'seek'):
            file_obj.seek(0)
            content = file_obj.read()
        else:
            content = file_obj
        
        decoded_file = (
            content.decode("utf-8-sig").splitlines()
            if isinstance(content, bytes)
            else content.splitlines()
        )
            
        reader = csv.DictReader(decoded_file)

        if not reader.fieldnames:
            logger.error(f"CSV {file_name} is empty or missing headers")
            artifact.status = "FAILED"
            artifact.save()
            return artifact

        raw_rows = []
        for i, row in enumerate(reader, start=1):
            # Clean keys/values
            cleaned_row = {
                k.strip(): v.strip() for k, v in row.items() if k and k.strip()
            }
            raw_rows.append(
                RawData(
                    artifact=artifact,
                    row_index=i,
                    data=cleaned_row,
                    status="PENDING",
                )
            )

            # Batch create
            if len(raw_rows) >= BATCH_SIZE:
                RawData.objects.bulk_create(raw_rows)
                raw_rows = []

        if raw_rows:
            RawData.objects.bulk_create(raw_rows)

        artifact.status = "COMPLETED"
        artifact.save()
        logger.info(f"Successfully ingested artifact {artifact.id} with {i} rows")

    except Exception as e:
        logger.error(f"Failed to ingest artifact {s3_key}: {str(e)}")
        artifact.status = "FAILED"
        artifact.save()

    return artifact
