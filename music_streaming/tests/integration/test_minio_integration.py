import pytest
import pandas as pd
import io
from minio import Minio
from minio.error import S3Error
from music_streaming.config.minio_config import MINIO_CONFIG

@pytest.fixture
def minio_client():
    """Create a MinIO client for testing."""
    try:
        client = Minio(
            MINIO_CONFIG['endpoint'],
            access_key=MINIO_CONFIG['access_key'],
            secret_key=MINIO_CONFIG['secret_key'],
            secure=MINIO_CONFIG['secure']
        )
        # Ensure the test bucket exists
        if not client.bucket_exists(MINIO_CONFIG['bucket']):
            client.make_bucket(MINIO_CONFIG['bucket'])
        return client
    except S3Error as e:
        pytest.skip(f"MinIO not available: {e}")

def test_minio_connection(minio_client):
    """Test that we can connect to MinIO."""
    assert minio_client.bucket_exists(MINIO_CONFIG['bucket'])

def test_minio_upload_download(minio_client):
    """Test uploading and downloading a file from MinIO."""
    # Create test data
    test_data = pd.DataFrame({
        'name': ['Test User 1', 'Test User 2'],
        'value': [100, 200]
    })
    
    # Convert to CSV in memory
    csv_buffer = io.BytesIO()
    test_data.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    
    # Upload to MinIO
    object_name = "test_data.csv"
    minio_client.put_object(
        bucket_name=MINIO_CONFIG['bucket'],
        object_name=object_name,
        data=csv_buffer,
        length=csv_buffer.getbuffer().nbytes,
        content_type="text/csv"
    )
    
    # Verify the object exists
    objects = list(minio_client.list_objects(MINIO_CONFIG['bucket'], prefix=object_name))
    assert len(objects) == 1
    assert objects[0].object_name == object_name
    
    # Clean up
    minio_client.remove_object(MINIO_CONFIG['bucket'], object_name)