from django.db import models

from core.models.raw_data import RawData


class Artifact(models.Model):
    """
    Represents an ingested file artifact.
    """

    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"

    STATUS_CHOICES = [
        (PENDING, "Pending"),
        (PROCESSING, "Processing"),
        (COMPLETED, "Completed"),
        (FAILED, "Failed"),
    ]

    file = models.CharField(max_length=1024, help_text="S3 URI or Key")
    content_type = models.CharField(max_length=50)
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default=PENDING)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        app_label = "core"

    @property
    def success_count(self) -> int:
        """
        Returns the number of successfully processed raw rows.
        """
        return self.raw_rows.filter(status=RawData.PROCESSED).count()

    @property
    def failure_count(self) -> int:
        """
        Returns the number of failed raw rows.
        """
        return self.raw_rows.filter(status=RawData.FAILED).count()

    def __str__(self):
        return f"{self.file} ({self.status})"

