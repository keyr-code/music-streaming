"""
Data generator module for creating simulated music streaming user listening data.
"""
import argparse
import json
import os
import random
import sys
import time
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from faker import Faker

from ..logger.logger import setup_logger

# Initialize logger
logger = setup_logger("streaming_data_generator")
logger.info("Streaming Data Generator logger initialized.")

class UserListeningStreamGenerator:
    def __init__(self):
        self.fake = Faker()
        self.genres = ['Pop', 'Rock', 'Hip Hop', 'R&B', 'Country', 
                      'Electronic', 'Jazz', 'Classical', 'Reggae', 'Blues']
        
        # Initialize users and songs
        self.users = []
        self.songs = []
        
        # Default format type
        self.format_type = 'json'
    
    def generate_duration(self):
        """Generate a random song duration"""
        seconds = random.randint(120, 300)
        return str(timedelta(seconds=seconds))[2:7]
    
    def generate_users(self, n_users):
        """Generate a list of users with first and last names"""
        users = []
        for _ in range(n_users):
            users.append({
                'first_name': self.fake.first_name(),
                'last_name': self.fake.last_name()
            })
        return users
    
    def generate_songs(self, n_songs):
        """Generate a list of songs"""
        songs = []
        for _ in range(n_songs):
            songs.append({
                'artist_name': self.fake.name(),
                'song_title': self.fake.sentence(nb_words=3).strip('.'),
                'genre': random.choice(self.genres),
                'duration': self.generate_duration()
            })
        return songs
    
    def initialize_data(self, n_users=100, n_songs=200):
        """Initialize users and songs data"""
        self.users = self.generate_users(n_users)
        self.songs = self.generate_songs(n_songs)
        logger.info(f"Initialized {len(self.users)} users and {len(self.songs)} songs")
    
    def generate_listening_record(self):
        """Generate a single listening record"""
        # Select a random user
        user = random.choice(self.users)
        
        # Select a random song
        song = random.choice(self.songs)
        
        # Generate a timestamp (recent - within last hour)
        timestamp = datetime.now() - timedelta(minutes=random.randint(0, 60))
        
        # Create the record
        record = {
            'first_name': user['first_name'],
            'last_name': user['last_name'],
            'artist_name': song['artist_name'],
            'song_title': song['song_title'],
            'genre': song['genre'],
            'song_duration': song['duration'],
            'listen_timestamp': timestamp.isoformat(),
            'listen_duration': str(timedelta(seconds=random.randint(30, int(song['duration'].split(':')[0])*60 + int(song['duration'].split(':')[1]))))[2:7],
            'completed': random.choice([True, False, False, False, True])  # 60% chance of completing the song
        }
            
        return record
    
    def stream_to_file(self, output_path, format_type='json', batch_size=10, interval=1.0):
        """Stream records to a file continuously"""
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        try:
            record_count = 0
            while True:
                # Generate a batch of records
                batch_records = []
                for _ in range(batch_size):
                    record = self.generate_listening_record()
                    batch_records.append(record)
                    record_count += 1
                
                # Save the batch
                if format_type == 'json':
                    # Append to JSON file
                    with open(output_path, 'a') as f:
                        for record in batch_records:
                            f.write(json.dumps(record) + '\n')
                elif format_type == 'csv':
                    # Convert to DataFrame and save as CSV
                    df = pd.DataFrame(batch_records)
                    
                    # For first batch, write with header, then append without header
                    if record_count <= batch_size and not os.path.exists(output_path):
                        df.to_csv(output_path, index=False)
                    else:
                        df.to_csv(output_path, mode='a', header=False, index=False)
                
                logger.info(f"Generated {batch_size} records. Total: {record_count}")
                
                # Wait for the specified interval
                time.sleep(interval)
                
        except KeyboardInterrupt:
            logger.info(f"Streaming stopped. Total records generated: {record_count}")
    
    def stream_to_stdout(self, interval=0.5):
        """Stream records to stdout continuously"""
        try:
            record_count = 0
            while True:
                # Generate a single record
                record = self.generate_listening_record()
                record_count += 1
                
                # Print the record as JSON
                print(json.dumps(record))  # Keep print here as it's intended for stdout output
                logger.debug(f"Generated record {record_count}")
                
                # Wait for the specified interval
                time.sleep(interval)
                
        except KeyboardInterrupt:
            logger.info(f"Streaming stopped. Total records generated: {record_count}")

def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Stream user listening data continuously')
    parser.add_argument('-m', '--mode', type=str, choices=['stream', 'save'], 
                        default='stream', help='Mode: stream to stdout or save to local file')
    parser.add_argument('-o', '--output', type=str, 
                        default='stdout',
                        help='Output destination: "stdout" or file path (when mode is "save")')
    parser.add_argument('-f', '--format', type=str, choices=['csv', 'json'], 
                        default='json', help='Output format for the data file')
    parser.add_argument('-u', '--users', type=int, default=100, 
                        help='Number of unique users to generate')
    parser.add_argument('-s', '--songs', type=int, default=200, 
                        help='Number of unique songs to generate')
    parser.add_argument('-b', '--batch_size', type=int, default=1, 
                        help='Number of records to generate in each batch')
    parser.add_argument('-i', '--interval', type=float, default=1.0, 
                        help='Time interval between batches in seconds')
    
    args = parser.parse_args()
    
    # Create generator
    generator = UserListeningStreamGenerator()
    
    # Initialize data
    generator.initialize_data(n_users=args.users, n_songs=args.songs)
    
    logger.info(f"Starting streaming with {args.batch_size} records every {args.interval} seconds...")
    
    # Stream data based on mode
    if args.mode == 'stream':
        generator.stream_to_stdout(interval=args.interval)
    else:  # save mode
        # If output is still set to stdout but mode is save, use a default filename
        if args.output.lower() == 'stdout':
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            args.output = f"listening_data_{timestamp}.{args.format}"
            logger.info(f"No output file specified. Saving to {args.output}")
        
        # Create output directory if it doesn't exist
        os.makedirs(os.path.dirname(args.output) if os.path.dirname(args.output) else '.', exist_ok=True)
        
        # Stream to file
        generator.stream_to_file(
            output_path=args.output,
            format_type=args.format,
            batch_size=args.batch_size,
            interval=args.interval
        )

if __name__ == "__main__":
    main()