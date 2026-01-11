from core.models import AuditRecord
from core.schemas.audit_record import AuditRecordSchema

from .base import IngestionStrategy, register_strategy


@register_strategy("audit")
class AuditRecordStrategy(IngestionStrategy):
    """
    Strategy for ingesting AuditRecord data.
    """

    model_class = AuditRecord
    schema_class = AuditRecordSchema
    unique_fields = ["provider_npi", "service_date", "billing_amount"]

    @classmethod
    def can_handle(cls, object_key: str) -> bool:
        return object_key.startswith("audit/")
