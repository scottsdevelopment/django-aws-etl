import os

from celery import Celery

# Set the default Django settings module for the 'celery' program.
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "config.settings")

celery_app = Celery("config")

# Namespace='CELERY' means all celery-related configuration keys
# should have a `CELERY_` prefix.
celery_app.config_from_object("django.conf:settings", namespace="CELERY")

# autodiscover_tasks is safe after config_from_object
celery_app.autodiscover_tasks()

# Add bootstep to worker steps
# Use the string path to avoid early import of core.tasks.consumers
celery_app.steps["worker"].add("core.tasks.consumers.S3EventConsumer")
