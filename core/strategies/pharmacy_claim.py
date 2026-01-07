from core.models.pharmacy_claim import PharmacyClaim
from core.schemas.pharmacy_claim import PharmacyClaimSchema

from .base import IngestionStrategy


class PharmacyClaimStrategy(IngestionStrategy):
    """
    Strategy for ingesting PharmacyClaim data.
    """
    model_class = PharmacyClaim
    schema_class = PharmacyClaimSchema
    unique_fields = ['claim_id']
