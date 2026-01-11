
import pytest
from datetime import datetime, date, timezone
from decimal import Decimal
from pydantic import ValidationError

from core.models.lab_result import LabResult
from core.schemas.lab_result import LabResultSchema
from core.strategies.lab_result import LabResultStrategy

@pytest.mark.django_db
def test_lab_result_str():
    """Test __str__ method of LabResult model."""
    result = LabResult.objects.create(
        patient_id="P1",
        test_code="T1",
        test_name="Test",
        result_value=Decimal("10.0"),
        result_unit="u",
        performed_at=datetime.now(timezone.utc)
    )
    # Expected: "P1 - Test: 10.0 u" (checking substring to be safe with Decimal formatting)
    assert "P1 - Test: 10.0" in str(result)

def test_schema_validator_error():
    """Test result_value validator raises error for values < -1000."""
    data = {
        "patient_id": "P1",
        "test_code": "T1",
        "test_name": "Test",
        "result_value": Decimal("-1001.0"),
        "result_unit": "u",
        "performed_at": datetime.now(timezone.utc)
    }
    with pytest.raises(ValidationError) as exc:
        LabResultSchema(**data)
    assert "Result value implies potential error" in str(exc.value)

def test_strategy_transform_low_flag():
    """Test logic for [LOW] flagging."""
    strategy = LabResultStrategy()
    data = {
        "patient_id": "P1",
        "test_code": "T1",
        "test_name": "Glucose",
        "result_value": Decimal("50.0"),
        "result_unit": "mg/dL",
        "reference_range": "70-100",
        "performed_at": datetime.now(timezone.utc)
    }
    schema = LabResultSchema(**data)
    result = strategy.transform(schema)
    assert result["test_name"] == "Glucose [LOW]"

def test_strategy_transform_exception_handling():
    """Test exception handling in reference range parsing."""
    strategy = LabResultStrategy()
    data = {
        "patient_id": "P1",
        "test_code": "T1",
        "test_name": "Glucose",
        "result_value": Decimal("50.0"),
        "result_unit": "mg/dL",
        "reference_range": "invalid-range-format", # This will fail float conversion or split
        "performed_at": datetime.now(timezone.utc)
    }
    schema = LabResultSchema(**data)
    # Should not raise exception, but catch it and return data as-is
    result = strategy.transform(schema)
    assert result["test_name"] == "Glucose"
