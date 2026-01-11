from decimal import Decimal
from typing import Any

from core.models.lab_result import LabResult
from core.schemas.lab_result import LabResultSchema
from core.strategies.base import IngestionStrategy, register_strategy


@register_strategy("lab_result")
class LabResultStrategy(IngestionStrategy):
    """
    Strategy for ingesting LabResult data.
    Handles files containing 'lab_result' in the key.
    """

    model_class = LabResult
    schema_class = LabResultSchema
    unique_fields = ["patient_id", "test_code", "performed_at"]

    @classmethod
    def can_handle(cls, object_key: str) -> bool:
        return object_key.startswith("labs/")

    def transform(self, schema_instance: LabResultSchema) -> dict[str, Any]:
        """
        Transform the schema instance into a dictionary for the model.
        
        Transformations:
        1. Normalize units: Convert 'mmol/L' to 'mg/dL' for Glucose ("L001").
        2. Reference Range Flagging: Append "[HIGH]" or "[LOW]" to test_name 
           if result is outside parsed reference range.
        3. Standardize unit case.
        """
        data = schema_instance.model_dump()
        
        # 1. Unit Conversion (Glucose L001: mmol/L -> mg/dL)
        # Factor: 1 mmol/L = 18.0182 mg/dL
        if data["test_code"] == "L001" and data["result_unit"].lower() == "mmol/l":
            data["result_value"] = data["result_value"] * Decimal("18.0182")
            data["result_unit"] = "mg/dL"

        # Standardize unit case (after potential conversion)
        if data.get("result_unit"):
            data["result_unit"] = data["result_unit"].upper()

        # 2. Reference Range Flagging
        # Simple parser for ranges involved in format "min-max" or "<max" or ">min"
        # Since this is a demo, we will handle "min-max" (e.g. "70-100")
        ref_range = data.get("reference_range", "")
        if ref_range and "-" in ref_range:
            try:
                low_str, high_str = ref_range.split("-")
                low = Decimal(low_str.strip())
                high = Decimal(high_str.strip())
                value = data["result_value"]

                if value < low:
                    data["test_name"] = f"{data['test_name']} [LOW]"
                elif value > high:
                    data["test_name"] = f"{data['test_name']} [HIGH]"
            except Exception:
                 # If parsing fails, skip flagging logic
                pass

        return data
