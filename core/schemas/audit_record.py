import re
from datetime import date
from decimal import Decimal

from pydantic import BaseModel, field_validator


class AuditRecordSchema(BaseModel):
    """
    Schema for Audit Records (Compliance Data).
    """

    provider_npi: str
    billing_amount: Decimal
    service_date: date
    status: str

    @field_validator("billing_amount")
    @classmethod
    def validate_positive_amount(cls, billing_amount: Decimal) -> Decimal:
        if billing_amount <= 0:
            raise ValueError("Billing amount must be positive")
        return billing_amount

    @field_validator("provider_npi")
    @classmethod
    def validate_npi_format(cls, npi_value: str) -> str:
        # Standard validation: 10 digits
        if not re.match(r"^\d{10}$", npi_value):
            raise ValueError("Provider NPI must be exactly 10 digits")
        return npi_value
