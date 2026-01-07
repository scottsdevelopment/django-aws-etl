def ensure_bucket(s3_client, bucket_name):
    """
    Ensures that an S3 bucket exists. Checks if it exists, and if not, creates it.
    Useful for LocalStack setups in tests.
    """
    try:
        s3_client.head_bucket(Bucket=bucket_name)
    except Exception:
        s3_client.create_bucket(Bucket=bucket_name)
