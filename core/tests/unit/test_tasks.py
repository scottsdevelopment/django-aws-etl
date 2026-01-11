from unittest.mock import patch

import pytest

from core.tasks.artifact_processing import process_artifact_task


@patch("core.tasks.artifact_processing.getattr")
def test_process_artifact_task_retry(mock_getattr):
    """Test that the task retries on exception."""
    artifact_id = 123

    # Mock dependencies
    # Patch where it is imported/used: core.tasks.artifact_processing.process_artifact
    with (
        patch("core.tasks.artifact_processing.process_artifact", side_effect=ValueError("Processing Failed")),
        patch.object(process_artifact_task, "retry", side_effect=Exception("Retry Triggered")) as mock_retry,
        pytest.raises(Exception, match="Retry Triggered"),
    ):
        # .apply executes the task locally. throw=True ensuress exceptions bubble up.
        process_artifact_task.apply(args=[artifact_id], throw=True)

    mock_retry.assert_called()


def test_process_artifact_task_success():
    """Test successful task execution."""
    artifact_id = 123

    # Mock successful service call returning (success, failed)
    # Patch where it is imported/used
    with patch("core.tasks.artifact_processing.process_artifact", return_value=(5, 0)) as mock_service:
        # Use .apply() for synchronous execution
        result = process_artifact_task.apply(args=[artifact_id]).get()

        assert result == {"success": 5, "failed": 0}
        mock_service.assert_called_once_with(artifact_id)
