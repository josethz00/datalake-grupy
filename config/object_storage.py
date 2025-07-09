from config import env

S3_DELTA_PATH = f"s3://{env.MINIO_BUCKET}/test-table"

object_storage_options = {
    "AWS_ACCESS_KEY_ID": env.AWS_ACCESS_KEY_ID,
    "AWS_SECRET_ACCESS_KEY": env.AWS_SECRET_ACCESS_KEY,
    "AWS_REGION": env.AWS_REGION,
    "AWS_ALLOW_HTTP": "true",
    "endpoint_url": env.MINIO_HOST,
    "DELTA_LAKE_URL": f"s3://{env.MINIO_BUCKET}/"
}
