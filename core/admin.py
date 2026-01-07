from django.contrib import admin

from core.models import AuditRecord, IngestionError, PharmacyClaim


@admin.register(AuditRecord)
class AuditRecordAdmin(admin.ModelAdmin):
    list_display = ('provider_npi', 'service_date', 'billing_amount', 'status', 'created_at')
    search_fields = ('provider_npi', 'status')
    list_filter = ('status', 'service_date')
    ordering = ('-created_at',)


@admin.register(PharmacyClaim)
class PharmacyClaimAdmin(admin.ModelAdmin):
    list_display = ('claim_id', 'ncpdp_id', 'service_date', 'total_amount_paid', 'transaction_code', 'created_at')
    search_fields = ('claim_id', 'ncpdp_id')
    list_filter = ('service_date',)
    ordering = ('-created_at',)


@admin.register(IngestionError)
class IngestionErrorAdmin(admin.ModelAdmin):
    list_display = ('error_reason', 'created_at')
    list_filter = ('created_at',)
    ordering = ('-created_at',)
