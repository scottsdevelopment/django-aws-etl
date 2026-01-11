from core.models.pharmacy_claim import PharmacyClaim
from core.schemas.pharmacy_claim import PharmacyClaimSchema

from .base import IngestionStrategy, register_strategy


@register_strategy("pharmacy")
class PharmacyClaimStrategy(IngestionStrategy):
    """
    Strategy for ingesting PharmacyClaim data.
    """

    model_class = PharmacyClaim
    schema_class = PharmacyClaimSchema
    unique_fields = ["claim_id"]

    @classmethod
    def can_handle(cls, object_key: str) -> bool:
        return object_key.startswith("pharmacy/")
