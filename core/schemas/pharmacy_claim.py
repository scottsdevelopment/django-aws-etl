from datetime import date
from decimal import Decimal

from pydantic import BaseModel, field_validator


class PharmacyClaimSchema(BaseModel):
    """
    Schema for Pharmacy Claims (Financial Transactions).
    """
    claim_id: str
    ncpdp_id: str
    bin_number: str
    service_date: date
    total_amount_paid: Decimal
    transaction_code: str

    @field_validator('total_amount_paid')
    @classmethod
    def validate_positive_amount(cls, amount: Decimal) -> Decimal:
        if amount <= 0:
            raise ValueError('Total amount paid must be positive')
        return amount
