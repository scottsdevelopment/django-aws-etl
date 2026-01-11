import logging

from pydantic import ValidationError as PydanticValidationError

from core.models import Artifact, RawData
from core.strategies.base import IngestionStrategy
from core.strategies.factory import StrategyFactory

logger = logging.getLogger(__name__)


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

    for raw_row in pending_rows.iterator():
        if _process_single_raw_row(raw_row, strategy):
            success_count += 1
        else:
            failure_count += 1

    logger.info(f"Artifact {artifact.id} processed: {success_count} success, {failure_count} failures")
    return success_count, failure_count


def _process_single_raw_row(raw_row: RawData, strategy: IngestionStrategy) -> bool:
    """
    Processes a single RawData row.
    """
    row_data = raw_row.data

    try:
        # 1. Validation Layer
        try:
            schema_data = strategy.schema_class.model_validate(row_data)
        except PydanticValidationError as e:
            msg = f"Validation Failed: {e}"
            raw_row.status = RawData.FAILED
            raw_row.error_message = msg
            raw_row.save()
            return False

        # 2. Transformation
        django_data = strategy.transform(schema_data)

        # 3. Idempotent Loading
        lookup_kwargs = {k: django_data[k] for k in strategy.unique_fields}
        defaults = {k: v for k, v in django_data.items() if k not in strategy.unique_fields}

        strategy.model_class.objects.update_or_create(defaults=defaults, **lookup_kwargs)

        raw_row.status = RawData.PROCESSED
        raw_row.save()
        return True

    except Exception as e:
        raw_row.status = RawData.FAILED
        raw_row.error_message = str(e)
        raw_row.save()
        return False
