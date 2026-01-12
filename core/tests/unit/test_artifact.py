import pytest

from core.models import Artifact


@pytest.mark.django_db
def test_artifact_str():
    artifact = Artifact.objects.create(file="drop-zone/test.csv", status="PENDING", content_type="test")
    assert str(artifact) == "drop-zone/test.csv (PENDING)"
@pytest.mark.django_db
def test_artifact_counts():
    """Test virtual properties for success/failure counts."""
    from core.models import RawData
    artifact = Artifact.objects.create(file="counts.csv", status="COMPLETED", content_type="test")
    
    # Create 2 success, 1 failed, 1 pending
    RawData.objects.create(artifact=artifact, row_index=1, status=RawData.PROCESSED, data={})
    RawData.objects.create(artifact=artifact, row_index=2, status=RawData.PROCESSED, data={})
    RawData.objects.create(artifact=artifact, row_index=3, status=RawData.FAILED, error_message="oops")
    RawData.objects.create(artifact=artifact, row_index=4, status=RawData.PENDING, data={})
    
    assert artifact.success_count == 2
    assert artifact.failure_count == 1
