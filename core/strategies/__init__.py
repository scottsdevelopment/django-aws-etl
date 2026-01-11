import importlib
import pkgutil

from .base import STRATEGY_REGISTRY, IngestionStrategy, get_strategy
from .factory import StrategyFactory


def discover_strategies():
    """
    Auto-discover all modules in this package to trigger registration.
    """
    for _, name, _ in pkgutil.iter_modules(__path__):
        if name not in ("base", "factory"):
            importlib.import_module(f".{name}", __package__)


# Trigger discovery on import
discover_strategies()

__all__ = ["IngestionStrategy", "get_strategy", "StrategyFactory", "STRATEGY_REGISTRY"]
