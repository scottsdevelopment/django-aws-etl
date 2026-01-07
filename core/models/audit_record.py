from django.db import models


class AuditRecord(models.Model):
    provider_npi = models.CharField(max_length=15)
    billing_amount = models.DecimalField(max_digits=10, decimal_places=2)
    service_date = models.DateField()
    status = models.CharField(max_length=50)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=['provider_npi', 'service_date', 'billing_amount'],
                name='unique_audit_record'
            )
        ]
        app_label = 'core'

    def __str__(self):
        return f"{self.provider_npi} - {self.service_date} - {self.status}"
