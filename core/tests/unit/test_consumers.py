"""
Unit tests for the S3EventConsumer Celery bootstep.
"""

import json
from unittest.mock import ANY, MagicMock, patch

import pytest

from core.tasks.consumers import S3EventConsumer


@pytest.fixture
def consumer():
    """Fixture providing an S3EventConsumer instance with a mock worker."""
    # Mocking the worker as it is not used in the __init__ or start method specifically
    # but required by StartStopStep signature
    worker = MagicMock()
    return S3EventConsumer(worker)


def test_consumer_init(consumer):
    """Test the initialization state of the consumer."""
    assert consumer.sqs is None
    assert consumer.queue_url is None
    assert consumer.enabled is True
    assert consumer.requires == {"celery.worker.components:Pool"}


@patch("core.tasks.consumers.boto3.client")
@patch("core.tasks.consumers.threading.Thread")
def test_consumer_start_stop(mock_thread, mock_boto, consumer):
    """Test start and stop lifecycle methods."""
    worker = MagicMock()
    worker = MagicMock()

    # Test Start
    consumer.start(worker)

    assert consumer.enabled is True
    mock_boto.assert_called_with(
        "sqs", endpoint_url=ANY, aws_access_key_id=ANY, aws_secret_access_key=ANY, region_name=ANY
    )
    mock_thread.return_value.start.assert_called_once()
    assert consumer.queue_url is not None

    # Test Stop
    consumer.stop(worker)
    assert consumer.enabled is False


@patch("core.tasks.consumers.current_app.send_task")
def test_consumer_run_loop(mock_send_task, consumer):
    # Setup mocks
    consumer.sqs = MagicMock()
    consumer.queue_url = "test-queue-url"
    consumer.enabled = True

    # Mock SQS Response
    s3_event = {"Records": [{"s3": {"bucket": {"name": "test-bucket"}, "object": {"key": "test/key.csv"}}}]}

    def side_effect(*args, **kwargs):
        consumer.enabled = False  # Stop loop after first call
        return {"Messages": [{"Body": json.dumps(s3_event), "ReceiptHandle": "handle-123"}]}

    consumer.sqs.receive_message.side_effect = side_effect

    consumer.run()

    # Verification
    mock_send_task.assert_called_once_with(
        "process_s3_file",
        kwargs={"bucket_name": "test-bucket", "object_key": "test/key.csv"},
        queue="healthcare-ingestion-queue",
    )
    consumer.sqs.delete_message.assert_called_once_with(QueueUrl="test-queue-url", ReceiptHandle="handle-123")


def test_consumer_run_loop_handles_exception(consumer):
    # Setup logger mock to verify error logging
    with patch("core.tasks.consumers.logger") as mock_logger:
        consumer.sqs = MagicMock()
        consumer.enabled = True

        # Simulate a crash in polling
        def side_effect(*args, **kwargs):
            consumer.enabled = False
            raise Exception("Polling Logic Crash")

        consumer.sqs.receive_message.side_effect = side_effect

        # We need to mock time.sleep so we don't actually wait
        with patch("time.sleep"):
            consumer.run()

        mock_logger.error.assert_called_with("S3EventConsumer polling error: Polling Logic Crash")


@patch("core.tasks.consumers.current_app.send_task")
def test_consumer_message_processing_exception(mock_send_task, consumer):
    """Test that exceptions during message processing are caught and logged."""
    with patch("core.tasks.consumers.logger") as mock_logger:
        consumer.sqs = MagicMock()
        consumer.queue_url = "test-queue-url"
        consumer.enabled = True

        # Mock SQS Response with valid structure but we will force a crash during processing
        s3_event = {"Records": [{"s3": {"bucket": {"name": "b"}, "object": {"key": "k"}}}]}

        def side_effect(*args, **kwargs):
            consumer.enabled = False
            return {"Messages": [{"Body": json.dumps(s3_event), "ReceiptHandle": "handle"}]}

        consumer.sqs.receive_message.side_effect = side_effect

        # Force send_task to raise exception
        mock_send_task.side_effect = Exception("Processing Crash")

        consumer.run()

        mock_logger.error.assert_called_with("S3EventConsumer message error: Processing Crash")
