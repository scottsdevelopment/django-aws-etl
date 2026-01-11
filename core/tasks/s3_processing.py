import logging
from typing import Any

import boto3
from celery import shared_task
from django.conf import settings
from django.core.files.base import ContentFile

from core.models import Artifact
from core.services.raw_ingestion_service import ingest_file_to_raw
from core.strategies.factory import StrategyFactory
from core.tasks.artifact_processing import process_artifact_task

logger = logging.getLogger(__name__)


@shared_task(name="process_s3_file", bind=True, max_retries=3)
def process_s3_file(self: Any, bucket_name: str, object_key: str) -> dict[str, Any]:
    """
    Step 1: Downloads a CSV file from S3 and ingests it into RawData.
    On success, triggers process_artifact_task.
    """
    try:
        content_type = StrategyFactory.get_content_type(object_key)
    except ValueError as e:
        logger.error(f"Skipping processing: {str(e)}")
        return {"success": 0, "failed": 0, "error": str(e)}

    logger.info(f"Processing file from S3: bucket={bucket_name}, key={object_key}, content_type={content_type}")

    s3_client = boto3.client(
        "s3",
        endpoint_url=settings.AWS_ENDPOINT_URL,
        aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
        region_name=settings.AWS_DEFAULT_REGION,
    )

    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        file_content = response["Body"].read()  # bytes

        # Create a Django ContentFile
        file_obj = ContentFile(file_content, name=object_key)

        # 1. Ingest to Raw
        artifact = ingest_file_to_raw(file_obj, object_key, content_type)

        if artifact.status == Artifact.FAILED:
            return {"success": 0, "failed": 1, "error": "Raw ingestion failed"}

        # 2. Trigger Artifact Processing
        process_artifact_task.delay(artifact.id)

        return {"success": 1, "failed": 0, "artifact_id": artifact.id}

    except Exception as e:
        logger.error(f"Error processing file {object_key}: {str(e)}")
        # Retry logic
        raise self.retry(exc=e, countdown=60) from e
