import logging
from itertools import batched

from pydantic import ValidationError as PydanticValidationError

from core.models import Artifact, RawData
from core.strategies.factory import StrategyFactory

logger = logging.getLogger(__name__)

# Batch processing configuration
BATCH_SIZE = 1000

def process_artifact(artifact_id: int) -> tuple[int, int]:
    """
    Step 2: Forward job. Processes RawData from an Artifact using the appropriate Strategy.
    """
    try:
        artifact = Artifact.objects.get(id=artifact_id)
    except Artifact.DoesNotExist:
        logger.error(f"Artifact {artifact_id} does not exist")
        return None

    logger.info(f"Starting processing for artifact {artifact.id} ({artifact.content_type})")

    try:
        # Use factory to get strategy (router + registry lookup)
        strategy = StrategyFactory.get_strategy(artifact.content_type)
        if not strategy:
            raise ValueError(f"Strategy not found for content_type: {artifact.content_type}")
    except ValueError as e:
        logger.error(str(e))
        return None

    # Process pending rows
    pending_rows = RawData.objects.filter(artifact=artifact, status=RawData.PENDING)

    success_count = 0
    failure_count = 0
    
    # Static calculation of update_fields
    update_fields = (
        list(set(strategy.schema_class.model_fields.keys()) - set(strategy.unique_fields))
        if strategy.unique_fields
        else []
    )

    for batch in batched(pending_rows.iterator(), BATCH_SIZE):
        instances, success_rows, failed_rows = _prepare_batch(strategy, batch)
        
        s_count, f_count = _flush_batch(
            strategy, 
            instances, 
            success_rows, 
            failed_rows, 
            update_fields
        )
        success_count += s_count
        failure_count += f_count

    logger.info(f"Artifact {artifact.id} processed: {success_count} success, {failure_count} failures")
    return success_count, failure_count


def _prepare_batch(strategy, batch):
    """
    Processes a batch of raw rows into model instances.
    Returns: (instances, success_rows, failed_rows)
    """
    instances = []
    success_rows = []
    failed_rows = []

    for raw_row in batch:
        try:
            # 1. Validation: Pydantic validates types and coerces raw strings into python objects
            schema_data = strategy.schema_class.model_validate(raw_row.data)
            
            # 2. Transformation: Strategy converts Pydantic model to dict, handling domain logic (e.g. unit conversion)
            django_data = strategy.transform(schema_data)
            
            instances.append(strategy.model_class(**django_data))
            success_rows.append(raw_row)
            
        except (PydanticValidationError, Exception) as e:
            msg = f"Validation Failed: {e}" if isinstance(e, PydanticValidationError) else str(e)
            
            # Explicitly log error for observability (since bulk_update bypasses signals)
            logger.error(
                f"Row processing failed (Artifact: {strategy.model_class.__name__}): {msg}", 
                extra={"data": raw_row.data}
            )

            raw_row.status = RawData.FAILED
            raw_row.error_message = msg
            
            failed_rows.append(raw_row)
            
    return instances, success_rows, failed_rows


def _flush_batch(strategy, instances, success_rows, failed_rows, update_fields):
    """
    Helper to execute bulk operations.
    """
    # 1. Bulk Upsert Domain Models
    if instances:
        if strategy.unique_fields:
             strategy.model_class.objects.bulk_create(
                 instances, 
                 update_conflicts=True,
                 unique_fields=strategy.unique_fields,
                 update_fields=update_fields
             )
        else:
            strategy.model_class.objects.bulk_create(instances)

    # 2. Bulk Update RawData Status (Success)
    if success_rows:
        for row in success_rows:
            row.status = RawData.PROCESSED
        RawData.objects.bulk_update(success_rows, fields=["status"])

    # 3. Bulk Update RawData Status (Failed)
    if failed_rows:
        RawData.objects.bulk_update(failed_rows, fields=["status", "error_message"])

    return len(success_rows), len(failed_rows)
