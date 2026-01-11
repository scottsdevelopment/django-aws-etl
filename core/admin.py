from django.contrib import admin

from core.models import Artifact, AuditRecord, LabResult, PharmacyClaim, RawData


@admin.register(AuditRecord)
class AuditRecordAdmin(admin.ModelAdmin):
    list_display = ("provider_npi", "service_date", "billing_amount", "status")


@admin.register(PharmacyClaim)
class PharmacyClaimAdmin(admin.ModelAdmin):
    list_display = ("claim_id", "service_date", "total_amount_paid")


@admin.register(LabResult)
class LabResultAdmin(admin.ModelAdmin):
    list_display = ("patient_id", "test_code", "test_name", "result_value", "result_unit", "performed_at")


@admin.register(Artifact)
class ArtifactAdmin(admin.ModelAdmin):
    list_display = ("file", "content_type", "status", "created_at")
    list_filter = ("status", "content_type", "created_at")


@admin.register(RawData)
class RawDataAdmin(admin.ModelAdmin):
    list_display = ("artifact", "row_index", "status")
    list_filter = ("status", "artifact")
