from typing import TYPE_CHECKING

from django.db import models

if TYPE_CHECKING:
    from core.models.artifact import Artifact


class RawData(models.Model):
    """
    Represents a single raw row of data from an ingested artifact.
    """

    PENDING = "PENDING"
    PROCESSED = "PROCESSED"
    FAILED = "FAILED"

    STATUS_CHOICES = [
        (PENDING, "Pending"),
        (PROCESSED, "Processed"),
        (FAILED, "Failed"),
    ]

    artifact = models.ForeignKey("core.Artifact", on_delete=models.CASCADE, related_name="raw_rows")
    data = models.JSONField(help_text="The raw data row as a dictionary", null=True, blank=True)
    raw_content = models.TextField(null=True, blank=True, help_text="Fallback for malformed rows")
    row_index = models.IntegerField()
    status = models.CharField(max_length=20, choices=STATUS_CHOICES, default=PENDING)
    error_message = models.TextField(null=True, blank=True)

    class Meta:
        app_label = "core"
        indexes = [
            models.Index(fields=["artifact", "status"]),
        ]

    def __str__(self):
        return f"Row {self.row_index} for Artifact {self.artifact_id}"
