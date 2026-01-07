from .audit_record import AuditRecordStrategy
from .base import IngestionStrategy
from .pharmacy_claim import PharmacyClaimStrategy

STRATEGY_REGISTRY = {
    'audit': AuditRecordStrategy(),
    'pharmacy': PharmacyClaimStrategy()
}

def get_strategy(type_name: str) -> IngestionStrategy:
    return STRATEGY_REGISTRY.get(type_name)

__all__ = ['IngestionStrategy', 'get_strategy', 'AuditRecordStrategy', 'PharmacyClaimStrategy']
