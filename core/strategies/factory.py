import logging

logger = logging.getLogger(__name__)


class StrategyFactory:
    """
    Determines the appropriate IngestionStrategy based on S3 object key prefixes.
    Implements 'Option A: Prefix/Suffix Routing'.
    """

    PREFIX_MAP = {
        "audit/": "audit",
        "pharmacy/": "pharmacy",
    }

    @classmethod
    def get_content_type(cls, object_key: str) -> str:
        """
        Returns the content_type (strategy name) for a given object key.
        """
        for prefix, strategy_name in cls.PREFIX_MAP.items():
            if object_key.startswith(prefix):
                return strategy_name
        raise ValueError(f"No content type mapping found for key: {object_key}")

    @classmethod
    def get_strategy_by_key(cls, object_key: str):
        """
        Determines and returns the appropriate strategy instance for a given S3 key.
        """
        from core.strategies import get_strategy  # noqa: PLC0415

        content_type = cls.get_content_type(object_key)
        return get_strategy(content_type)
