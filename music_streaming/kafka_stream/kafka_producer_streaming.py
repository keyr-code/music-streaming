"""
Kafka Producer module for streaming user listening data to Kafka topics.
"""
import argparse
import json
import socket
import time
from confluent_kafka import Producer
from ..data_generators.streaming_listening_data import UserListeningStreamGenerator
from ..config.kafka_config import PRODUCER_CONFIG
from ..logger.logger import setup_logger

# Initialize logger
logger = setup_logger("kafka_producer")
logger.info("Kafka Producer logger initialized.")

def delivery_report(err, msg):
    """Callback for message delivery reports"""
    if err is not None:
        logger.error(f'Message delivery failed: {err}')
    else:
        logger.debug(f'Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}')

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Stream user listening data to Kafka')
    # Data format parameter
    parser.add_argument('-f', '--format', type=str, choices=['csv', 'json'], 
                        default='json', help='Format for the Kafka messages')
    parser.add_argument('-u', '--users', type=int, default=100, 
                        help='Number of unique users to generate')
    parser.add_argument('-s', '--songs', type=int, default=200, 
                        help='Number of unique songs to generate')
    parser.add_argument('-b', '--batch_size', type=int, default=10, 
                        help='Number of records to generate in each batch')
    parser.add_argument('-i', '--interval', type=float, default=1.0, 
                        help='Time interval between batches in seconds')
    # Kafka specific parameters
    parser.add_argument('--bootstrap-servers', default=PRODUCER_CONFIG['bootstrap.servers'],
                        help='Kafka bootstrap servers')
    parser.add_argument('-t', '--topic', default=PRODUCER_CONFIG['topic'],
                        help='Kafka topic to produce to')
    
    args = parser.parse_args()
    
    # Create data generator
    generator = UserListeningStreamGenerator()
    generator.initialize_data(n_users=args.users, n_songs=args.songs)
    
    # Set the format type for record generation
    generator.format_type = args.format
    
    # Kafka is the only output destination
    
    # Configure Kafka producer
    conf = {
        'bootstrap.servers': args.bootstrap_servers,
        'client.id': socket.gethostname()
    }
    
    producer = Producer(conf)
    
    logger.info(f"Starting Kafka producer, sending to topic {args.topic}...")
    logger.info(f"Batch size: {args.batch_size}, Interval: {args.interval} seconds")
    
    try:
        record_count = 0
        while True:
            # Generate a batch of records
            batch_records = []
            for _ in range(args.batch_size):
                record = generator.generate_listening_record()
                batch_records.append(record)
                record_count += 1
                
                # Format and send to Kafka based on format type
                if args.format == 'json':
                    data = json.dumps(record)
                elif args.format == 'csv':
                    # Convert single record to CSV format
                    if record_count == 1:  # Add header for first record
                        header = ','.join(record.keys())
                        data = header + '\n' + ','.join(str(v) for v in record.values())
                    else:
                        data = ','.join(str(v) for v in record.values())
                else:
                    # Default to JSON for other formats since Kafka works best with string data
                    data = json.dumps(record)
                
                producer.produce(args.topic, data.encode('utf-8'), callback=delivery_report)
            
            # Flush producer queue after each batch
            producer.flush()
            
            logger.info(f"Sent batch of {args.batch_size} records. Total: {record_count}")
            
            # Wait for the specified interval
            time.sleep(args.interval)
            
    except KeyboardInterrupt:
        logger.info(f"Streaming stopped. Total records sent: {record_count}")
        # Flush any remaining messages
        producer.flush()

if __name__ == "__main__":
    main()