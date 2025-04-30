import logging
import sys

# Configure logging
LOG_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(filename='drive_scanner.log', level=logging.INFO, format=LOG_FORMAT)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter(LOG_FORMAT))
logging.getLogger().addHandler(console_handler)

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
CACHE_EXPIRY_HOURS = 24  # Cache expires after 24 hours
SAVE_INTERVAL_MINUTES = 5  # Save every 5 minutes if modified

# API settings
BATCH_SIZE = 100  # Reduced from 900 to 100 to stay well under Google's limits
MAX_RETRIES = 3
RETRY_DELAY = 1  # seconds
METADATA_FIELDS = 'id, name, parents, size, md5Checksum, mimeType, trashed' 