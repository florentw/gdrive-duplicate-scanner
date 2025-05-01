import logging
import sys
from pathlib import Path

# Configure root logger
logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('drive_scanner.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

# Create and configure the drive_scanner logger
logger = logging.getLogger('drive_scanner')
logger.setLevel(logging.INFO)
logger.propagate = False

# Set external library loggers to WARNING
for lib in ['googleapiclient', 'oauth2client', 'urllib3']:
    logging.getLogger(lib).setLevel(logging.WARNING)

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
METADATA_FIELDS = 'id, name, parents, size, md5Checksum, mimeType, trashed' 