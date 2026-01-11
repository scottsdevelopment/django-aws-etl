from datetime import datetime, timezone
from decimal import Decimal

import pytest

from core.models.lab_result import LabResult
from core.schemas.lab_result import LabResultSchema
from core.strategies.lab_result import LabResultStrategy


@pytest.mark.django_db
class TestLabResultStrategy:
    def test_can_handle(self):
        assert LabResultStrategy.can_handle("labs/lab_results_2024.csv") is True
        assert LabResultStrategy.can_handle("folder/labs/data.csv") is False
        assert LabResultStrategy.can_handle("folder/billing_data.csv") is False

    def test_transform_unit_conversion(self):
        # Glucose (L001) in mmol/L should convert to mg/dL
        strategy = LabResultStrategy()
        data = {
            "patient_id": "P123",
            "test_code": "L001",
            "test_name": "Glucose",
            "result_value": Decimal("5.0"), # approx 90 mg/dL
            "result_unit": "mmol/L",
            "reference_range": "70-100",
            "performed_at": datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        }
        schema_instance = LabResultSchema(**data)
        result = strategy.transform(schema_instance)
        
        # 5.0 * 18.0182 = 90.091
        expected = Decimal("90.091")
        assert result["result_unit"] == "MG/DL"
        assert abs(result["result_value"] - expected) < Decimal("0.001")

    def test_transform_flagging_high(self):
        strategy = LabResultStrategy()
        data = {
            "patient_id": "P123",
            "test_code": "L001",
            "test_name": "Glucose",
            "result_value": Decimal("150.0"), 
            "result_unit": "mg/dL",
            "reference_range": "70-100",
            "performed_at": datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        }
        schema_instance = LabResultSchema(**data)
        result = strategy.transform(schema_instance)
        
        assert result["test_name"] == "Glucose [HIGH]"

    def test_integration_save(self):
        # Test full save via strategy (simulating base class behavior)
        strategy = LabResultStrategy()
        data = {
            "patient_id": "PSAVE",
            "test_code": "TSAVE",
            "test_name": "Save Test",
            "result_value": Decimal("50.0"),
            "result_unit": "units",
            "performed_at": datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
        }
        schema = LabResultSchema(**data)
        transformed = strategy.transform(schema)
        
        record = LabResult.objects.create(**transformed)
        assert record.pk is not None
        assert record.result_unit == "UNITS"
