import pytest

from core.models import Artifact


@pytest.mark.django_db
def test_artifact_str():
    artifact = Artifact.objects.create(
        file="drop-zone/test.csv",
        status="PENDING", 
        content_type="test"
    )
    assert str(artifact) == "drop-zone/test.csv (PENDING)"
