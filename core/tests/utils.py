import time

from core.models import Artifact


def ensure_bucket(s3_client, bucket_name):
    """
    Ensures that an S3 bucket exists. Checks if it exists, and if not, creates it.
    Useful for LocalStack setups in tests.
    """
    try:
        s3_client.head_bucket(Bucket=bucket_name)
    except Exception:
        s3_client.create_bucket(Bucket=bucket_name)

def wait_for_artifact(file_key, timeout=10, interval=0.5):
    """
    Polls for an Artifact with the given file key to reach COMPLETED or FAILED status.
    Returns the artifact if found and finished, or raises TimeoutError.
    """
    start_time = time.time()
    while time.time() - start_time < timeout:
        # Get the latest artifact for this key
        artifact = Artifact.objects.filter(file=file_key).order_by("-created_at").first()
        
        if artifact and artifact.status in [Artifact.COMPLETED, Artifact.FAILED]:
            return artifact
        
        time.sleep(interval)
    
    raise TimeoutError(f"Timed out waiting for artifact {file_key} to process.")
