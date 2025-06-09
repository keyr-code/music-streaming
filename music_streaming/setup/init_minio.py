"""
MinIO initialization module for setting up buckets for the music streaming application.
"""
from minio import Minio
from minio.error import S3Error
from ..config.minio_config import MINIO_CONFIG
from ..logger.logger import setup_logger

logger = setup_logger("init_minio")
logger.info("MinIO initialization starting...")

def main():
    """Initialize MinIO buckets."""
    try:
        # Connect to MinIO using config
        minio_client = Minio(
            MINIO_CONFIG['endpoint'],
            access_key=MINIO_CONFIG['access_key'],
            secret_key=MINIO_CONFIG['secret_key'],
            secure=MINIO_CONFIG['secure']
        )
        logger.info(f"Connected to MinIO at {MINIO_CONFIG['endpoint']}")
    except S3Error as err:
        logger.error(f"Error connecting to MinIO: {err}")
        return

    # Create the configured bucket
    bucket = MINIO_CONFIG['bucket']
    try:
        if not minio_client.bucket_exists(bucket):
            minio_client.make_bucket(bucket)
            logger.info(f"Bucket '{bucket}' created successfully")
        else:
            logger.info(f"Bucket '{bucket}' already exists")
    except S3Error as err:
        logger.error(f"Error creating bucket '{bucket}': {err}")

if __name__ == "__main__":
    main()