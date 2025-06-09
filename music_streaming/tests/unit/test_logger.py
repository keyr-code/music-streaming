import pytest
import os
import logging
from pathlib import Path
from music_streaming.logger.logger import setup_logger

def test_logger_creation():
    logger = setup_logger("test_logger")
    assert logger.name == "test_logger"
    assert logger.level == logging.INFO
    assert len(logger.handlers) == 2  # Console handler and file handler

def test_logger_handlers():
    logger = setup_logger("test_handler_logger")
    
    # Check that we have both a StreamHandler and a RotatingFileHandler
    handlers = logger.handlers
    handler_types = [h.__class__.__name__ for h in handlers]
    
    assert "StreamHandler" in handler_types
    assert "RotatingFileHandler" in handler_types

def test_log_file_creation():
    # Get the log directory path - should be in music_streaming/logs
    project_root = Path(__file__).resolve().parent.parent.parent
    log_dir = project_root / "logs"
    
    # Create logger and write a log
    logger = setup_logger("test_file_logger")
    logger.info("Test log message")
    
    # Verify log directory exists
    assert os.path.exists(log_dir)
    
    # There should be at least one log file
    files = os.listdir(log_dir)
    assert len(files) > 0
    
    # At least one file should be a .log file
    log_files = [f for f in files if f.endswith('.log')]
    assert len(log_files) > 0