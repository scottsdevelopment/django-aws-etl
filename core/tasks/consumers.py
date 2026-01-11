import json
import logging
import threading
import time
from typing import Any
from urllib.parse import unquote_plus

import boto3
from celery import bootsteps, current_app
from django.conf import settings

logger = logging.getLogger(__name__)


class S3EventConsumer(bootsteps.StartStopStep):
    """
    Celery Bootstep that consumes raw S3 notifications from a dedicated SQS queue.
    Bridges the gap between raw AWS events and Celery tasks.
    """

    requires = {"celery.worker.components:Pool"}

    def __init__(self, worker, **kwargs):
        self.sqs = None
        self.queue_url = None
        self.enabled = True
        self.thread = None

    def start(self, worker: Any):
        self.enabled = True
        # Lazy initialization
        endpoint_url = settings.AWS_ENDPOINT_URL
        self.sqs = boto3.client(
            "sqs",
            endpoint_url=endpoint_url,
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
            region_name=settings.AWS_DEFAULT_REGION,
        )
        self.queue_url = f"{endpoint_url}/000000000000/s3-event-queue"

        logger.info(f"S3EventConsumer starting for queue: {self.queue_url}")

        self.thread = threading.Thread(target=self.run, daemon=True)
        self.thread.start()

    def stop(self, worker: Any):
        self.enabled = False
        logger.info("S3EventConsumer stopping.")

    def run(self):
        """
        Continuously polls SQS for messages and dispatches tasks.
        """
        while self.enabled:
            try:
                response = self.sqs.receive_message(QueueUrl=self.queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=5)

                if "Messages" in response:
                    for msg in response["Messages"]:
                        self._process_message(msg)

            except Exception as e:
                logger.error(f"S3EventConsumer polling error: {e}")
                time.sleep(1)

    def _process_message(self, msg: dict[str, Any]):
        """
        Processes a single SQS message, extracts records, and deletes execution.
        """
        try:
            body = json.loads(msg["Body"])
            if "Records" in body:
                for record in body["Records"]:
                    self._dispatch_task(record)

            self.sqs.delete_message(QueueUrl=self.queue_url, ReceiptHandle=msg["ReceiptHandle"])
        except Exception as e:
            logger.error(f"S3EventConsumer message error: {e}")

    def _dispatch_task(self, record: dict[str, Any]):
        """
        Extracts S3 details and dispatches the Celery task.
        """
        bucket = record["s3"]["bucket"]["name"]
        key = unquote_plus(record["s3"]["object"]["key"])

        logger.info(f"S3EventConsumer: Dispatching task 'process_s3_file' for s3://{bucket}/{key}")

        current_app.send_task(
            "process_s3_file", kwargs={"bucket_name": bucket, "object_key": key}, queue="healthcare-ingestion-queue"
        )
