import os
import logging
from pathlib import Path

# API Configuration
SCOPES = ['https://www.googleapis.com/auth/drive']
CREDENTIALS_FILE = 'credentials.json'
TOKEN_FILE = 'token.json'

# Batch Processing
BATCH_SIZE = 100  # Maximum number of requests per batch
MAX_RETRIES = 3   # Maximum number of retries for failed requests

# Cache Configuration
CACHE_FILE = 'metadata_cache.json'
CACHE_SAVE_INTERVAL = 300  # Save cache every 5 minutes when modified

# CSV Export
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

# Logging Configuration
LOG_FILE = 'drive_scanner.log'
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
LOG_LEVEL = logging.INFO

# Create logger
logger = logging.getLogger('drive_scanner')

def setup_logging():
    """Configure logging for the application."""
    # Create logs directory if it doesn't exist
    log_dir = Path('logs')
    log_dir.mkdir(exist_ok=True)
    
    # Configure root logger
    logging.basicConfig(
        level=LOG_LEVEL,
        format=LOG_FORMAT,
        handlers=[
            logging.FileHandler(LOG_FILE),
            logging.StreamHandler()
        ]
    )
    
    # Set specific log levels for different modules
    logging.getLogger('googleapiclient').setLevel(logging.WARNING)
    logging.getLogger('google_auth_oauthlib').setLevel(logging.WARNING)
    logging.getLogger('google.auth').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    
    # Set our application's logger to INFO
    logger.setLevel(LOG_LEVEL)

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
CACHE_FILE = 'drive_metadata_cache.json'
SAVE_INTERVAL_MINUTES = 5  # Save every 5 minutes if modified

# API settings
BATCH_SIZE = 100  # Reduced from 900 to 100 to stay well under Google's limits
MAX_RETRIES = 3
RETRY_DELAY = 1  # seconds
METADATA_FIELDS = 'id, name, parents, size, md5Checksum, mimeType, trashed' 