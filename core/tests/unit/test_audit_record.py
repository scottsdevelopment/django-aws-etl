"""
Unit tests for the AuditRecord model and schema.
"""
from datetime import date
from decimal import Decimal

import pytest
from pydantic import ValidationError

from core.models import AuditRecord
from core.schemas.audit_record import AuditRecordSchema


@pytest.mark.django_db
def test_audit_record_str():
    """Test the string representation of the AuditRecord model."""
    record = AuditRecord.objects.create(
        provider_npi="1234567890",
        billing_amount=Decimal("100.00"),
        service_date=date(2025, 1, 1),
        status="active"
    )
    assert str(record) == "1234567890 - 2025-01-01 - active"


def test_valid_audit_record_schema():
    """Test that valid data passes schema validation."""
    data = {
        "provider_npi": "1234567890",
        "billing_amount": "100.50",
        "service_date": "2023-01-01",
        "status": "submitted"
    }
    schema = AuditRecordSchema(**data)
    assert schema.provider_npi == "1234567890"
    assert schema.billing_amount == Decimal("100.50")
    assert schema.service_date == date(2023, 1, 1)


def test_invalid_npi_length():
    """Test that NPI length validation works."""
    data = {
        "provider_npi": "123", 
        "billing_amount": "100.50",
        "service_date": "2023-01-01",
        "status": "submitted"
    }
    with pytest.raises(ValidationError) as excinfo:
        AuditRecordSchema(**data)
    assert "Provider NPI must be exactly 10 digits" in str(excinfo.value)


def test_negative_billing_amount():
    """Test that negative billing amounts are rejected."""
    data = {
        "provider_npi": "1234567890",
        "billing_amount": "-100.50",
        "service_date": "2023-01-01",
        "status": "submitted"
    }
    with pytest.raises(ValidationError) as excinfo:
        AuditRecordSchema(**data)
    assert "Billing amount must be positive" in str(excinfo.value)


def test_invalid_date_format():
    """Test that invalid date formats are rejected."""
    data = {
        "provider_npi": "1234567890",
        "billing_amount": "100.50",
        "service_date": "not-a-date",
        "status": "submitted"
    }
    with pytest.raises(ValidationError):
        AuditRecordSchema(**data)
