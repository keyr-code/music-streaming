"""
Kafka Consumer module for reading data from Kafka topics and storing in MinIO and PostgreSQL.
"""
import json
import time
import pandas as pd
import io
import psycopg2
from datetime import datetime
from confluent_kafka import Consumer, KafkaError
from minio import Minio
from minio.error import S3Error
from ..logger.logger import setup_logger
from ..config.kafka_config import CONSUMER_CONFIG
from ..config.minio_config import MINIO_CONFIG
from ..config.postgres_config import POSTGRES_CONFIG

# Initialize logger
logger = setup_logger("kafka_consumer")
logger.info("Kafka Consumer logger initialized.")

class KafkaDataConsumer:
    """Kafka Consumer to read data from a topic and store in MinIO and PostgreSQL."""
    
    def __init__(self, topic, group_id="music-streaming-group", broker=CONSUMER_CONFIG['bootstrap.servers']):
        """
        Initialize the Kafka consumer.
        
        Args:
            topic (str): Kafka topic to consume data from
            group_id (str): Consumer group ID
            broker (str): Kafka broker address
        """
        self.topic = topic
        self.group_id = group_id
        logger.info(f"Initializing Kafka consumer for topic: {topic}, group: {group_id}")
        
        # Initialize Kafka consumer
        try:
            self.consumer = Consumer({
                'bootstrap.servers': broker,
                'group.id': self.group_id,
                'auto.offset.reset': 'earliest',
                'enable.auto.commit': True,
                'auto.commit.interval.ms': 5000
            })
            self.consumer.subscribe([self.topic])
            logger.info(f"Successfully connected to Kafka broker at {broker}")
        except Exception as e:
            logger.error(f"Failed to connect to Kafka broker: {e}")
            raise
        
        # Initialize MinIO client
        try:
            self.minio_client = Minio(
                MINIO_CONFIG['endpoint'],
                access_key=MINIO_CONFIG['access_key'],
                secret_key=MINIO_CONFIG['secret_key'],
                secure=MINIO_CONFIG['secure']
            )
            logger.info(f"Successfully connected to MinIO at {MINIO_CONFIG['endpoint']}")
            
            # Ensure bucket exists
            if not self.minio_client.bucket_exists(MINIO_CONFIG['bucket']):
                self.minio_client.make_bucket(MINIO_CONFIG['bucket'])
                logger.info(f"Created bucket: {MINIO_CONFIG['bucket']}")
            else:
                logger.info(f"Bucket '{MINIO_CONFIG['bucket']}' already exists")
        except S3Error as e:
            logger.error(f"Failed to connect to MinIO: {e}")
            raise
            
        # Initialize PostgreSQL connection
        try:
            self.pg_conn = psycopg2.connect(
                host=POSTGRES_CONFIG['host'],
                port=POSTGRES_CONFIG['port'],
                database=POSTGRES_CONFIG['database'],
                user=POSTGRES_CONFIG['user'],
                password=POSTGRES_CONFIG['password']
            )
            logger.info(f"Successfully connected to PostgreSQL at {POSTGRES_CONFIG['host']}")
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise

    def upload_to_minio(self, data):
        """
        Upload data to MinIO.
        
        Args:
            data (pd.DataFrame): DataFrame to upload
            
        Returns:
            bool: True if upload was successful, False otherwise
        """
        # Generate a filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        object_name = f"user_listening_data/user_listening_data_{timestamp}.csv"
        
        try:
            # Convert DataFrame to CSV in memory
            csv_buffer = io.BytesIO()
            data.to_csv(csv_buffer, index=False)
            csv_buffer.seek(0)
            
            # Upload to MinIO
            self.minio_client.put_object(
                bucket_name=MINIO_CONFIG['bucket'],
                object_name=object_name,
                data=csv_buffer,
                length=csv_buffer.getbuffer().nbytes,
                content_type="text/csv"
            )
            # Verify the object exists after upload
            try:
                self.minio_client.stat_object(
                    bucket_name=MINIO_CONFIG['bucket'],
                    object_name=object_name
                )
                logger.info(f"Successfully uploaded and verified data in MinIO: {object_name}")
                return True
            except Exception as verify_err:
                logger.error(f"Upload appeared successful but verification failed: {verify_err}")
                return False
        
        except Exception as e:
            logger.error(f"Error uploading to MinIO: {e}")
            return False
            
    def insert_to_postgres(self, record):
        """
        Insert a record into PostgreSQL using the stored procedure.
        
        Args:
            record (dict): Record to insert
            
        Returns:
            bool: True if insert was successful, False otherwise
        """
        try:
            cursor = self.pg_conn.cursor()
            
            # Convert string durations to interval format
            song_duration = record['song_duration']
            listen_duration = record['listen_duration']
            listen_timestamp = record['listen_timestamp']
            
            # Call the stored procedure
            cursor.execute(
                "SELECT insert_streaming_data(%s, %s, %s, %s, %s, %s::interval, %s::timestamp, %s::interval, %s)",
                (
                    record['first_name'],
                    record['last_name'],
                    record['artist_name'],
                    record['song_title'],
                    record['genre'],
                    song_duration,
                    listen_timestamp,
                    listen_duration,
                    record['completed']
                )
            )
            
            self.pg_conn.commit()
            return True
        except Exception as e:
            self.pg_conn.rollback()
            logger.error(f"Error inserting into PostgreSQL: {e}")
            return False

    def consume_and_store(self, batch_size=50, timeout=600):
        """
        Consume messages from Kafka and store in MinIO and PostgreSQL.
        
        Args:
            batch_size (int): Number of messages to batch before uploading to MinIO
            timeout (int): Maximum seconds to wait for a batch to fill
            
        Returns:
            int: Number of records processed
        """
        messages = []
        records_processed = 0
        batch_count = 0
        
        logger.info(f"Starting to consume messages from topic {self.topic}")
        logger.info(f"Using batch size: {batch_size}")
        
        try:
            # Poll-based consumption
            while True:
                msg = self.consumer.poll(1.0)
                
                if msg is None:
                    continue
                    
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        logger.info(f"Reached end of partition {msg.partition()}")
                    else:
                        logger.error(f"Error: {msg.error()}")
                else:
                    # Process the message
                    record = json.loads(msg.value().decode('utf-8'))
                    
                    # Insert into PostgreSQL immediately
                    pg_success = self.insert_to_postgres(record)
                    if pg_success:
                        logger.debug(f"Successfully inserted record into PostgreSQL")
                    
                    # Add to batch for MinIO
                    messages.append(record)
                    records_processed += 1
                    
                    # When batch is full, upload to MinIO
                    if len(messages) >= batch_size:
                        batch_count += 1
                        logger.info(f"Processing batch #{batch_count} with {len(messages)} messages")
                        
                        # Convert messages to DataFrame and upload to MinIO
                        df = pd.DataFrame(messages)
                        upload_success = self.upload_to_minio(df)
                        
                        # Only reset batch if upload was successful
                        if upload_success:
                            # Reset batch
                            messages = []
                            
                            # Log progress
                            logger.info(f"Progress: {records_processed} records processed")
                        else:
                            logger.error(f"Failed to upload batch #{batch_count} to MinIO, will retry with next batch")
                
        except KeyboardInterrupt:
            logger.info("Consumption interrupted by user")
        except Exception as e:
            logger.error(f"Error consuming messages: {e}")
        finally:
            # Upload any remaining messages to MinIO
            if messages:
                logger.info(f"Processing final batch with {len(messages)} messages")
                df = pd.DataFrame(messages)
                self.upload_to_minio(df)
            
            logger.info(f"Completed processing {records_processed} records")
            return records_processed

    def close(self):
        """Close the Kafka consumer and PostgreSQL connections."""
        if hasattr(self, 'consumer'):
            self.consumer.close()
            logger.info("Kafka consumer connection closed")
            
        if hasattr(self, 'pg_conn'):
            self.pg_conn.close()
            logger.info("PostgreSQL connection closed")


if __name__ == "__main__":
    # Use the topic from the config
    kafka_topic = CONSUMER_CONFIG['topic']
    
    # Create and run the consumer
    try:
        consumer = KafkaDataConsumer(topic=kafka_topic)
        consumer.consume_and_store(batch_size=50)
    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user")
    except Exception as e:
        logger.error(f"Error in consumer: {e}")
    finally:
        if 'consumer' in locals():
            consumer.close()