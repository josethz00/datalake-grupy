from config import env

object_storage_options = {
    "ACCESS_KEY_ID": env.AWS_ACCESS_KEY_ID,
    "SECRET_ACCESS_KEY": env.AWS_SECRET_ACCESS_KEY,
    "REGION": env.AWS_REGION,
    "ALLOW_HTTP": "true",
    "endpoint_url": env.MINIO_HOST,
    "DELTA_LAKE_URL": f"s3://{env.MINIO_BUCKET}/",
}
