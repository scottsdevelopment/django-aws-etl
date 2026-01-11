"""
Unit tests for the PharmacyClaim model and schema.
"""

from datetime import date
from decimal import Decimal

import pytest
from pydantic import ValidationError

from core.models import PharmacyClaim
from core.schemas.pharmacy_claim import PharmacyClaimSchema


@pytest.mark.django_db
def test_pharmacy_claim_str():
    """Test string representation of PharmacyClaim."""
    record = PharmacyClaim.objects.create(
        claim_id="CLM001",
        ncpdp_id="NCP001",
        bin_number="BIN001",
        service_date=date(2025, 1, 1),
        total_amount_paid=Decimal("150.00"),
        transaction_code="TXN01",
    )
    assert str(record) == "Claim CLM001 (2025-01-01)"


def test_pharmacy_claim_valid():
    """Test valid schema validation."""
    data = {
        "claim_id": "CLM123",
        "ncpdp_id": "NCP123",
        "bin_number": "BIN123",
        "service_date": "2025-01-01",
        "total_amount_paid": "100.50",
        "transaction_code": "TXN01",
    }
    schema = PharmacyClaimSchema(**data)
    assert schema.claim_id == "CLM123"
    assert schema.total_amount_paid == Decimal("100.50")
    assert schema.service_date == date(2025, 1, 1)


def test_pharmacy_claim_negative_amount_fails():
    """Test rejection of negative amounts."""
    data = {
        "claim_id": "CLM123",
        "ncpdp_id": "NCP123",
        "bin_number": "BIN123",
        "service_date": "2025-01-01",
        "total_amount_paid": "-50.00",
        "transaction_code": "TXN01",
    }
    with pytest.raises(ValidationError):
        PharmacyClaimSchema(**data)


def test_pharmacy_claim_missing_field_fails():
    """Test rejection when required fields are missing."""
    data = {
        "claim_id": "CLM123",
        "bin_number": "BIN123",
        "service_date": "2025-01-01",
        "total_amount_paid": "100.50",
        "transaction_code": "TXN01",
    }
    with pytest.raises(ValidationError):
        PharmacyClaimSchema(**data)
