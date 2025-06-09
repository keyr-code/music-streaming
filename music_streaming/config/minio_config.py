"""
MinIO configuration settings for the music streaming application.
"""

# Default MinIO configuration
MINIO_CONFIG = {
    'endpoint': 'localhost:9002',
    'access_key': 'minioadmin',
    'secret_key': 'minioadmin',
    'secure': False,
    'bucket': 'music-streaming-data-live'
}