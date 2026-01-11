from typing import Any

from django.db import models
from pydantic import BaseModel


class IngestionStrategy:
    """
    Abstract base class defining the contract for data ingestion.
    """

    model_class: type[models.Model]
    schema_class: type[BaseModel]
    unique_fields: list[str]

    def transform(self, schema_instance: BaseModel) -> dict[str, Any]:
        """
        Transforms validated Pydantic model into a dictionary for Django.
        Default implementation assumes field names match.
        """
        return schema_instance.model_dump()
