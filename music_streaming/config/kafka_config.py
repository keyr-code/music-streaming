"""
Kafka configuration settings for the music streaming application.
"""

# Default Kafka configuration
DEFAULT_CONFIG = {
    'bootstrap.servers': 'localhost:9092',
    'topic': 'user-listening-events-live'
}

# Producer specific configuration
PRODUCER_CONFIG = {
    **DEFAULT_CONFIG,
    'batch.size': 16384,
    'linger.ms': 1,
    'acks': 'all'
}

# Consumer specific configuration
CONSUMER_CONFIG = {
    **DEFAULT_CONFIG,
    'group.id': 'music-streaming-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True
}