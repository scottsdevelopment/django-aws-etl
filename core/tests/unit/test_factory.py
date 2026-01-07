import pytest

from core.strategies import AuditRecordStrategy, PharmacyClaimStrategy
from core.strategies.factory import StrategyFactory


@pytest.mark.parametrize("key, expected_strategy_type", [
    ("audit/2023_data.csv", AuditRecordStrategy),
    ("pharmacy/claims_001.csv", PharmacyClaimStrategy),
    ("audit/subfolder/test.csv", AuditRecordStrategy),
])
def test_strategy_routing(key, expected_strategy_type):
    """Test that GetStrategy returns the correct Strategy class for valid prefixes."""
    strategy = StrategyFactory.get_strategy_by_key(key)
    assert isinstance(strategy, expected_strategy_type), f"Expected {expected_strategy_type}, got {type(strategy)}"

def test_unknown_prefix_routing():
    """Test that unknown prefixes raise ValueError."""
    with pytest.raises(ValueError) as excinfo:
        StrategyFactory.get_strategy_by_key("unknown/data.csv")
    assert "No ingestion strategy found" in str(excinfo.value)

def test_root_file_routing():
    """Test that root level files (no valid prefix) raise ValueError."""
    with pytest.raises(ValueError):
        StrategyFactory.get_strategy_by_key("data.csv")
