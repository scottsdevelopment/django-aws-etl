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

    @classmethod
    def can_handle(cls, object_key: str) -> bool:
        """
        Logic to determine if this strategy should run.
        """
        return False

    def transform(self, schema_instance: BaseModel) -> dict[str, Any]:
        """
        Transforms validated Pydantic model into a dictionary for Django.
        Default implementation assumes field names match.
        """
        return schema_instance.model_dump()


STRATEGY_REGISTRY: dict[str, type[IngestionStrategy]] = {}


def register_strategy(name: str):
    """
    Decorator to register an IngestionStrategy with a unique name.
    """

    def decorator(cls: type[IngestionStrategy]):
        STRATEGY_REGISTRY[name] = cls
        return cls

    return decorator


def get_strategy(type_name: str) -> IngestionStrategy | None:
    """
    Function to retrieve a strategy instance from the registry.
    """
    strategy_cls = STRATEGY_REGISTRY.get(type_name)
    if strategy_cls:
        return strategy_cls()
    return None
