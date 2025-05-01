import logging
import sys
from logging.handlers import RotatingFileHandler

# Configure logging
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
LOG_FILE = 'drive_scanner.log'
MAX_LOG_SIZE = 5 * 1024 * 1024  # 5MB
BACKUP_COUNT = 3  # Keep 3 backup files

# Create rotating file handler
file_handler = RotatingFileHandler(
    LOG_FILE,
    maxBytes=MAX_LOG_SIZE,
    backupCount=BACKUP_COUNT
)
file_handler.setLevel(logging.INFO)
file_handler.setFormatter(logging.Formatter(LOG_FORMAT))

# Create console handler
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter(LOG_FORMAT))

# Configure root logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(file_handler)
logger.addHandler(console_handler)

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