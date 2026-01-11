import logging
from typing import Any

from celery import shared_task

from core.services.processing_service import process_artifact

logger = logging.getLogger(__name__)


@shared_task(name="process_artifact_task", bind=True, max_retries=3)
def process_artifact_task(self: Any, artifact_id: int) -> dict[str, Any]:
    """
    Step 2: Processes an ingested Artifact into domain models.
    """

    logger.info(f"Starting processing task for artifact {artifact_id}")

    try:
        success_count, failed_count = process_artifact(artifact_id)

        return {"success": success_count, "failed": failed_count}

    except Exception as e:
        logger.error(f"Error in process_artifact_task {artifact_id}: {str(e)}")
        raise self.retry(exc=e, countdown=60) from e
