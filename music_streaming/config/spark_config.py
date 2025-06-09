"""
Spark configuration settings for the music streaming application.
"""

# Spark configuration
SPARK_CONFIG = {
    'app_name': 'Music Streaming Data Processing',
    'master': 'local[*]',
    'packages': 'org.apache.hadoop:hadoop-aws:3.3.1',
    's3a': {
        'endpoint': 'http://localhost:9000',
        'path_style_access': 'true',
        'impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem'
    }
}