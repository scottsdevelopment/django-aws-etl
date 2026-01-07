from django.db import models


class IngestionError(models.Model):
    raw_data = models.JSONField()
    error_reason = models.TextField()
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        app_label = 'core'

    def __str__(self):
        return f"Error at {self.created_at}: {self.error_reason[:50]}"
