from django.db import models


class LabResult(models.Model):
    """
    Model representing a clinical laboratory test result.
    Example of a 'transformative' data set that might require unit conversion or normalization.
    """

    patient_id = models.CharField(max_length=50, help_text="External patient identifier")
    test_code = models.CharField(max_length=50, help_text="LOINC or internal test code")
    test_name = models.CharField(max_length=255, help_text="Human-readable test name")
    result_value = models.DecimalField(
        max_digits=10, decimal_places=2, help_text="Numeric result value"
    )
    result_unit = models.CharField(max_length=20, help_text="Unit of measurement (e.g., mg/dL)")
    reference_range = models.CharField(
        max_length=50, blank=True, null=True, help_text="Reference range (e.g., '70-100')"
    )
    performed_at = models.DateTimeField(help_text="When the test was performed")

    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        constraints = [
            models.UniqueConstraint(
                fields=["patient_id", "test_code", "performed_at"], name="unique_lab_result"
            )
        ]
        app_label = "core"
        ordering = ["-performed_at"]

    def __str__(self):
        return f"{self.patient_id} - {self.test_name}: {self.result_value} {self.result_unit}"
