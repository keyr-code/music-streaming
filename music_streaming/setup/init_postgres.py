"""
PostgreSQL initialization module for setting up database tables for the music streaming application.
"""
import psycopg2
from ..config.postgres_config import POSTGRES_CONFIG
from ..logger.logger import setup_logger

logger = setup_logger("init_postgres")
logger.info("PostgreSQL initialization starting...")

def main():
    """Initialize PostgreSQL database tables."""
    try:
        # Connect to PostgreSQL using config
        conn = psycopg2.connect(
            host=POSTGRES_CONFIG['host'],
            port=POSTGRES_CONFIG['port'],
            database=POSTGRES_CONFIG['database'],
            user=POSTGRES_CONFIG['user'],
            password=POSTGRES_CONFIG['password']
        )
        logger.info(f"Connected to PostgreSQL at {POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}")
        
        # Create a cursor
        cur = conn.cursor()
        
        # Execute the create table statements
        cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id SERIAL PRIMARY KEY,
            first_name VARCHAR(100) NOT NULL,
            last_name VARCHAR(100) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (first_name, last_name)
        );

        CREATE TABLE IF NOT EXISTS artists (
            artist_id SERIAL PRIMARY KEY,
            artist_name VARCHAR(200) NOT NULL UNIQUE
        );

        CREATE TABLE IF NOT EXISTS genres (
            genre_id SERIAL PRIMARY KEY,
            genre_name VARCHAR(50) NOT NULL UNIQUE
        );

        CREATE TABLE IF NOT EXISTS songs (
            song_id SERIAL PRIMARY KEY,
            artist_id INTEGER NOT NULL REFERENCES artists(artist_id),
            genre_id INTEGER NOT NULL REFERENCES genres(genre_id),
            song_title VARCHAR(200) NOT NULL,
            song_duration INTERVAL NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (artist_id, song_title)
        );

        CREATE TABLE IF NOT EXISTS listening_events (
            event_id SERIAL PRIMARY KEY,
            user_id INTEGER NOT NULL REFERENCES users(user_id),
            song_id INTEGER NOT NULL REFERENCES songs(song_id),
            listen_timestamp TIMESTAMP NOT NULL,
            listen_duration INTERVAL NOT NULL,
            completed BOOLEAN NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        
        # Create stored procedure for inserting streaming data
        cur.execute("""
        CREATE OR REPLACE FUNCTION insert_streaming_data(
            p_first_name VARCHAR(100),
            p_last_name VARCHAR(100),
            p_artist_name VARCHAR(200),
            p_song_title VARCHAR(200),
            p_genre VARCHAR(50),
            p_song_duration INTERVAL,
            p_listen_timestamp TIMESTAMP,
            p_listen_duration INTERVAL,
            p_completed BOOLEAN
        ) RETURNS VOID AS $$
        DECLARE
            v_user_id INTEGER;
            v_artist_id INTEGER;
            v_genre_id INTEGER;
            v_song_id INTEGER;
        BEGIN
            -- Insert or get user
            INSERT INTO users (first_name, last_name)
            VALUES (p_first_name, p_last_name)
            ON CONFLICT (first_name, last_name) DO NOTHING;
            
            SELECT user_id INTO v_user_id
            FROM users
            WHERE first_name = p_first_name AND last_name = p_last_name;
            
            -- Insert or get artist
            INSERT INTO artists (artist_name)
            VALUES (p_artist_name)
            ON CONFLICT (artist_name) DO NOTHING;
            
            SELECT artist_id INTO v_artist_id
            FROM artists
            WHERE artist_name = p_artist_name;
            
            -- Insert or get genre
            INSERT INTO genres (genre_name)
            VALUES (p_genre)
            ON CONFLICT (genre_name) DO NOTHING;
            
            SELECT genre_id INTO v_genre_id
            FROM genres
            WHERE genre_name = p_genre;
            
            -- Insert or get song
            INSERT INTO songs (artist_id, genre_id, song_title, song_duration)
            VALUES (v_artist_id, v_genre_id, p_song_title, p_song_duration)
            ON CONFLICT (artist_id, song_title) DO NOTHING;
            
            SELECT song_id INTO v_song_id
            FROM songs
            WHERE artist_id = v_artist_id AND song_title = p_song_title;
            
            -- Insert listening event
            INSERT INTO listening_events (user_id, song_id, listen_timestamp, listen_duration, completed)
            VALUES (v_user_id, v_song_id, p_listen_timestamp, p_listen_duration, p_completed);
        END;
        $$ LANGUAGE plpgsql;
        """)
        
        # Commit the transaction
        conn.commit()
        logger.info("Database tables and stored procedure created successfully")
        
    except psycopg2.Error as e:
        logger.error(f"Error initializing PostgreSQL: {e}")
    finally:
        # Close the connection
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()
        logger.info("PostgreSQL connection closed")

if __name__ == "__main__":
    main()