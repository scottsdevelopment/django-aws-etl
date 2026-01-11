import pytest

from core.strategies.base import IngestionStrategy
from core.strategies.factory import StrategyFactory
from core.strategies.pharmacy_claim import PharmacyClaimStrategy


@pytest.mark.parametrize(
    "key, expected_type",
    [
        ("audit/2023_data.csv", "audit"),
        ("pharmacy/claims_001.csv", "pharmacy"),
        ("audit/subfolder/test.csv", "audit"),
    ],
)
def test_strategy_routing(key, expected_type):
    """Test that get_content_type returns the correct strategy name for valid prefixes."""
    content_type = StrategyFactory.get_content_type(key)
    assert content_type == expected_type


def test_unknown_prefix_routing():
    """Test that unknown prefixes raise ValueError."""
    with pytest.raises(ValueError) as excinfo:
        StrategyFactory.get_content_type("unknown/data.csv")
    assert "No content type mapping found" in str(excinfo.value)


def test_root_file_routing():
    """Test that root level files (no valid prefix) raise ValueError."""
    with pytest.raises(ValueError):
        StrategyFactory.get_content_type("data.csv")


def test_get_strategy_by_key():
    """Test full resolution from key to strategy instance."""

    strategy = StrategyFactory.get_strategy_by_key("pharmacy/claim.csv")
    assert isinstance(strategy, PharmacyClaimStrategy)


def test_base_strategy_defaults():
    """Test base IngestionStrategy defaults."""
    assert IngestionStrategy.can_handle("any/key") is False
