import pytest
from music_streaming.data_generators.streaming_listening_data import UserListeningStreamGenerator

def test_generate_users():
    generator = UserListeningStreamGenerator()
    users = generator.generate_users(10)
    assert len(users) == 10
    assert all('first_name' in user for user in users)
    assert all('last_name' in user for user in users)

def test_generate_songs():
    generator = UserListeningStreamGenerator()
    songs = generator.generate_songs(15)
    assert len(songs) == 15
    assert all('artist_name' in song for song in songs)
    assert all('song_title' in song for song in songs)
    assert all('genre' in song for song in songs)
    assert all('duration' in song for song in songs)

def test_generate_listening_record():
    generator = UserListeningStreamGenerator()
    generator.initialize_data(n_users=5, n_songs=5)
    record = generator.generate_listening_record()
    assert 'first_name' in record
    assert 'last_name' in record
    assert 'artist_name' in record
    assert 'song_title' in record
    assert 'genre' in record
    assert 'song_duration' in record
    assert 'listen_timestamp' in record
    assert 'listen_duration' in record
    assert 'completed' in record