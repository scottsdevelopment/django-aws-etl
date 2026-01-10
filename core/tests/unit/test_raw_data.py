import pytest

from core.models import Artifact, RawData


@pytest.mark.django_db
def test_raw_data_str():
    artifact = Artifact.objects.create(file="k", content_type="c")
    row = RawData.objects.create(artifact=artifact, row_index=5, status="PENDING")
    assert str(row) == f"Row 5 for Artifact {artifact.id}"
