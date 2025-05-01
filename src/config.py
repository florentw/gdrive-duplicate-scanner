"""Configuration module for Google Drive API access."""

import logging
import sys
from pathlib import Path
from datetime import datetime

def setup_logger():
    """Set up and configure the drive_scanner logger."""
    # Configure logging format
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    formatter = logging.Formatter(log_format)

    # Create logger if it doesn't exist
    logger = logging.getLogger('drive_scanner')
    
    # Remove any existing handlers
    logger.handlers.clear()
    
    # Set level
    logger.setLevel(logging.INFO)
    
    # Generate timestamp for log file
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_filename = f'drive_scanner_{timestamp}.log'
    
    # Add file handler
    file_handler = logging.FileHandler(log_filename)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # Add console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # Prevent propagation to root logger
    logger.propagate = False
    
    return logger

# Set up the logger
logger = setup_logger()

# Set external library loggers to WARNING
for lib in ['googleapiclient', 'oauth2client', 'urllib3']:
    lib_logger = logging.getLogger(lib)
    lib_logger.setLevel(logging.WARNING)
    lib_logger.propagate = False

# Google Drive API scopes
SCOPES = ['https://www.googleapis.com/auth/drive']

# CSV headers
CSV_HEADERS = [
    'File Name',
    'Full Path',
    'Size (Bytes)',
    'Size (Human Readable)',
    'File ID',
    'MD5 Checksum',
    'Duplicate Group ID',
    'Parent Folder',
    'Parent Folder ID',
    'Duplicate File Name',
    'Duplicate File Path',
    'Duplicate File Size',
    'Duplicate File ID'
]

# Cache settings
CACHE_FILE = 'cache.json'
SAVE_INTERVAL_MINUTES = 1  # Save cache every minute

# API settings
BATCH_SIZE = 100  # Reduced from 900 to 100 to stay well under Google's limits
MAX_RETRIES = 3
RETRY_DELAY = 1  # seconds

# Fields needed for file operations
METADATA_FIELDS = 'id, name, parents, size, md5Checksum, mimeType, trashed' 