from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel, Field, field_validator


class LabResultSchema(BaseModel):
    """
    Pydantic schema for validating LabResult data.
    """

    patient_id: str = Field(..., min_length=1, description="External patient identifier")
    test_code: str = Field(..., min_length=1, description="LOINC or internal test code")
    test_name: str = Field(..., min_length=1, description="Human-readable test name")
    result_value: Decimal = Field(..., description="Numeric result value")
    result_unit: str = Field(..., min_length=1, description="Unit of measurement")
    reference_range: str | None = Field(None, description="Reference range string")
    performed_at: datetime = Field(..., description="Timestamp of the test")

    @field_validator("result_value")
    @classmethod
    def validate_result_value(cls, v):
        # Example validation: assume lab results shouldn't be excessively large negative numbers
        # effectively handling potential data entry errors or specific domain logic
        if v < Decimal("-1000"):
            raise ValueError("Result value implies potential error (<-1000)")
        return v
