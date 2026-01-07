import io
import logging
from typing import Any

import boto3
from celery import shared_task
from django.conf import settings

from core.services.ingestion import ingest_csv_data
from core.strategies.factory import StrategyFactory

logger = logging.getLogger(__name__)


@shared_task(name='process_s3_file', bind=True, max_retries=3)
def process_s3_file(self: Any, bucket_name: str, object_key: str) -> dict[str, Any]:
    """
    Downloads a CSV file from S3 and processes it.
    Uses StrategyFactory to determine ingestion type based on file path.
    """
    try:
        strategy = StrategyFactory.get_strategy_by_key(object_key)
    except ValueError as e:
        logger.error(f"Skipping processing: {str(e)}")
        return {"success": 0, "failed": 0, "error": str(e)}
    
    logger.info(
        f"Processing file from S3: bucket={bucket_name}, key={object_key}, "
        f"strategy={strategy.__class__.__name__}"
    )
    
    s3_client = boto3.client(
        's3',
        endpoint_url=settings.AWS_ENDPOINT_URL.replace('localhost', 'localstack'),
        aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
        aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
        region_name=settings.AWS_DEFAULT_REGION
    )

    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        file_content = response['Body'].read().decode('utf-8')
        
        # Create a file-like object
        file_obj = io.StringIO(file_content)
        
        # Ingest
        success, failed = ingest_csv_data(file_obj, strategy)
        
        logger.info(f"Finished processing {object_key}. Success: {success}, Failed: {failed}")
        return {"success": success, "failed": failed}

    except Exception as e:
        logger.error(f"Error processing file {object_key}: {str(e)}")
        # Retry logic
        raise self.retry(exc=e, countdown=60) from e
