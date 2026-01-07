from django.db import models


class PharmacyClaim(models.Model):
    claim_id = models.CharField(max_length=50, unique=True)
    ncpdp_id = models.CharField(max_length=20)
    bin_number = models.CharField(max_length=20)
    service_date = models.DateField()
    total_amount_paid = models.DecimalField(max_digits=10, decimal_places=2)
    transaction_code = models.CharField(max_length=10)
    
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        app_label = 'core'

    def __str__(self):
        return f"Claim {self.claim_id} ({self.service_date})"
