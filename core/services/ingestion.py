import csv
import logging
from typing import Any, TextIO

from pydantic import ValidationError as PydanticValidationError

from core.models import IngestionError
from core.strategies import IngestionStrategy

logger = logging.getLogger(__name__)


def ingest_csv_data(file_obj: TextIO, strategy: IngestionStrategy) -> tuple[int, int]:
    """
    Ingests CSV data from a file-like object using the provided strategy.
    Returns a tuple of (success_count, failure_count).
    """
    success_count = 0
    failure_count = 0
    
    try:
        reader = csv.DictReader(file_obj)
        
        # Handle basic CSV errors (empty, malformed) or empty file
        if not reader.fieldnames:
             logger.error("CSV file is empty or missing headers")
             return 0, 0 

        for row in reader:
            if _process_single_row(row, strategy):
                success_count += 1
            else:
                failure_count += 1

    except csv.Error as csv_err:
        logger.error(f"Fatal error reading CSV: {str(csv_err)}")
    except Exception as fatal_error:
        logger.error(f"Unexpected error reading CSV: {str(fatal_error)}")

    return success_count, failure_count

def _process_single_row(row: dict[str, str], strategy: IngestionStrategy) -> bool:
    """
    Processes a single CSV row: Validation -> Transformation -> Load.
    Returns True if successful, False otherwise.
    """
    # Strip whitespace from keys and values
    row = {k.strip(): v.strip() for k, v in row.items() if k and v}
    
    try:
        # 1. Validation Layer (Pydantic via Strategy)
        try:
            schema_data = strategy.schema_class.model_validate(row)
        except PydanticValidationError as e:
            _log_error(row, f"Validation Failed: {e.errors()}")
            return False
        except Exception as e:
            _log_error(row, f"Schema Error: {str(e)}")
            return False

        # 2. Transformation (Optional)
        django_data = strategy.transform(schema_data)
        
        # 3. Idempotent Loading (Django via Strategy)
        lookup_kwargs = {k: django_data[k] for k in strategy.unique_fields}
        defaults = {k: v for k, v in django_data.items() if k not in strategy.unique_fields}
        
        strategy.model_class.objects.update_or_create(
            defaults=defaults,
            **lookup_kwargs
        )
        return True

    except Exception as e:
         # Catch-all for database errors or other runtime issues
         _log_error(row, f"Runtime Error: {str(e)}")
         return False



def _log_error(row_data: dict[str, Any], reason: str):
    IngestionError.objects.create(
        raw_data=row_data,
        error_reason=reason
    )
