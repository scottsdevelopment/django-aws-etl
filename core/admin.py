from django.contrib import admin

from core.models import Artifact, AuditRecord, PharmacyClaim, RawData


@admin.register(AuditRecord)
class AuditRecordAdmin(admin.ModelAdmin):
    list_display = ("provider_npi", "service_date", "billing_amount", "status")

@admin.register(PharmacyClaim)
class PharmacyClaimAdmin(admin.ModelAdmin):
    list_display = ("claim_id", "service_date", "total_amount_paid")

@admin.register(Artifact)
class ArtifactAdmin(admin.ModelAdmin):
    list_display = ("file", "content_type", "status", "created_at")
    list_filter = ("status", "content_type", "created_at")

@admin.register(RawData)
class RawDataAdmin(admin.ModelAdmin):
    list_display = ("artifact", "row_index", "status")
    list_filter = ("status", "artifact")
