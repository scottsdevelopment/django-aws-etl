import logging

from core.strategies import IngestionStrategy, get_strategy

logger = logging.getLogger(__name__)


class StrategyFactory:
    """
    Determines the appropriate IngestionStrategy based on S3 object key prefixes.
    Implements 'Option A: Prefix/Suffix Routing'.
    """
    
    PREFIX_MAP = {
        'audit/': 'audit',
        'pharmacy/': 'pharmacy',
    }

    @classmethod
    def get_strategy_by_key(cls, object_key: str) -> IngestionStrategy:
        """
        Parses the object key to find a matching prefix and returns the corresponding strategy.
        Raises ValueError if no matching prefix is found.
        """
        for prefix, strategy_name in cls.PREFIX_MAP.items():
            if object_key.startswith(prefix):
                logger.info(f"Routing '{object_key}' to strategy '{strategy_name}' via prefix '{prefix}'")
                return get_strategy(strategy_name)
        
        error_msg = (
            f"No ingestion strategy found for key: {object_key}. "
            f"Supported prefixes: {list(cls.PREFIX_MAP.keys())}"
        )
        logger.error(error_msg)
        raise ValueError(error_msg)
