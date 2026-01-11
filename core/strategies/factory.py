import logging

from core.strategies.base import STRATEGY_REGISTRY, get_strategy

logger = logging.getLogger(__name__)


class StrategyFactory:
    """
    Determines the appropriate IngestionStrategy based on object key.
    Uses 'Dynamic Discovery' via the Registry.
    """

    @classmethod
    def get_content_type(cls, object_key: str) -> str:
        """
        Returns the content_type (strategy name) for a given object key.
        Iterates through registered strategies to find one that can handle the key.
        """
        for name, strategy_cls in STRATEGY_REGISTRY.items():
            if strategy_cls.can_handle(object_key):
                return name

        raise ValueError(f"No content type mapping found for key: {object_key}")

    @classmethod
    def get_strategy_by_key(cls, object_key: str):
        """
        Determines and returns the appropriate strategy instance for a given key.
        """
        content_type = cls.get_content_type(object_key)
        return get_strategy(content_type)

    @classmethod
    def get_strategy(cls, type_name: str):
        """
        Directly retrieves a strategy instance by name.
        """
        return get_strategy(type_name)
