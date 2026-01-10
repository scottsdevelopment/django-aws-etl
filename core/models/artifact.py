from django.db import models


class Artifact(models.Model):
    """
    Represents an ingested file artifact.
    """

    STATUS_CHOICES = [
        ("PENDING", "Pending"),
        ("PROCESSING", "Processing"),
        ("COMPLETED", "Completed"),
        ("FAILED", "Failed"),
    ]

    file = models.CharField(max_length=1024, help_text="S3 URI or Key")
    content_type = models.CharField(max_length=50)
    status = models.CharField(
        max_length=20,
        choices=STATUS_CHOICES,
        default='PENDING'
    )
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        app_label = "core"

    def __str__(self):
        return f"{self.file} ({self.status})"
