from __future__ import print_function
import os.path
import logging
import argparse
import pickle
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
import sys
from collections import defaultdict
import csv
from datetime import datetime, timedelta
import json
import hashlib
from functools import lru_cache
from googleapiclient.http import BatchHttpRequest
from typing import List, Dict, Set, Optional, Any, Callable
from googleapiclient.discovery import Resource
import time

# Configure logging to write logs to a file and the console
log_format = '%(asctime)s - %(levelname)s - %(message)s'
logging.basicConfig(filename='drive_scanner.log', level=logging.INFO, format=log_format)
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.INFO)
console_handler.setFormatter(logging.Formatter(log_format))
logging.getLogger().addHandler(console_handler)

# If modifying these SCOPES, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/drive']

# Add CSV headers as a constant
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

# Add these constants near the top of the file with other constants
CACHE_FILE = 'drive_metadata_cache.json'
CACHE_EXPIRY_HOURS = 24  # Cache expires after 24 hours
SAVE_INTERVAL_MINUTES = 5  # Save every 5 minutes if modified
BATCH_SIZE = 100  # Reduced from 900 to 100 to stay well under Google's limits
MAX_RETRIES = 3
RETRY_DELAY = 1  # seconds
METADATA_FIELDS = 'id, name, parents, size, md5Checksum, mimeType, trashed'

def get_cache_key():
    """Generate a unique cache key based on the credentials file."""
    try:
        with open('credentials.json', 'rb') as f:
            content = f.read()
            # Use first 8 characters of hash to identify the account
            return hashlib.md5(content).hexdigest()[:8]
    except FileNotFoundError:
        return 'default'

class MetadataCache:
    """Centralized cache manager for file metadata."""
    
    def __init__(self, cache_file: str = CACHE_FILE):
        self._cache_file = cache_file
        self._temp_file = f"{cache_file}.tmp"
        self._cache = {}
        self._last_save = datetime.now()
        self._last_cleanup = datetime.now()
        self._modified = False
        self._load()

    def _cleanup_expired(self) -> None:
        """Remove expired entries from cache."""
        if datetime.now() - self._last_cleanup < timedelta(hours=CACHE_EXPIRY_HOURS):
            return

        expired_keys = []
        for key, value in self._cache.items():
            if isinstance(value, dict) and 'timestamp' in value:
                try:
                    timestamp = datetime.fromisoformat(value['timestamp'])
                    if datetime.now() - timestamp > timedelta(hours=CACHE_EXPIRY_HOURS):
                        expired_keys.append(key)
                except (ValueError, TypeError):
                    expired_keys.append(key)

        if expired_keys:
            for key in expired_keys:
                self._cache.pop(key, None)
            self._modified = True
            self._save(force=True)

        self._last_cleanup = datetime.now()

    def _save(self, force: bool = False) -> None:
        """Save cache to disk if needed."""
        if not (self._modified or force):
            return

        if not force and datetime.now() - self._last_save < timedelta(minutes=SAVE_INTERVAL_MINUTES):
            return

        try:
            data = {
                'timestamp': datetime.now().isoformat(),
                'cache_key': get_cache_key(),
                'files': self._cache
            }
            
            # Write to temporary file first
            with open(self._temp_file, 'w') as f:
                json.dump(data, f)
            
            # Atomic rename
            os.replace(self._temp_file, self._cache_file)
            
            self._last_save = datetime.now()
            self._modified = False
            cached_files = self._cache.get('all_files', [])
            logging.info(f"Saved cache with {len(cached_files)} files")

        except Exception as e:
            logging.error(f"Failed to save cache: {e}")
            # Clean up temp file if it exists
            try:
                os.remove(self._temp_file)
            except OSError:
                pass

    def _load(self) -> None:
        """Load cache from disk."""
        try:
            if os.path.exists(self._cache_file):
                with open(self._cache_file, 'r') as f:
                    data = json.load(f)
                    
                    # Skip if cache key doesn't match
                    if data.get('cache_key') != get_cache_key():
                        logging.info("Cache key mismatch, starting fresh")
                        self._cache = {}
                        self._last_save = None
                        self._save(force=True)  # Save empty cache
                        return
                    
                    self._cache = data.get('files', {})
                    self._last_save = datetime.fromisoformat(data.get('timestamp', datetime.now().isoformat()))
                    
                    # Check cache expiry
                    if self._last_save and datetime.now() - self._last_save > timedelta(hours=CACHE_EXPIRY_HOURS):
                        self._cache = {}  # Clear the cache
                        self._last_save = None
                        self._save(force=True)  # Save empty cache
        except Exception as e:
            logging.error(f"Failed to load cache: {e}")
            self._cache = {}
            self._last_save = None

    def get(self, key: str) -> Any:
        """Retrieve item from cache."""
        self._cleanup_expired()  # Check for expired entries
        return self._cache.get(key)

    def set(self, key: str, value: Any) -> None:
        """Store single item in cache."""
        if isinstance(value, dict):
            value['timestamp'] = datetime.now().isoformat()
        self._cache[key] = value
        self._modified = True
        self._save()

    def update(self, items: Dict[str, Any]) -> None:
        """Store multiple items in cache."""
        timestamp = datetime.now().isoformat()
        for key, value in items.items():
            if isinstance(value, dict):
                value['timestamp'] = timestamp
        self._cache.update(items)
        self._modified = True
        self._save()

    def remove(self, keys: List[str]) -> None:
        """Remove multiple items from cache."""
        for key in keys:
            self._cache.pop(key, None)
        self._modified = True
        self._save()

    def clear(self) -> None:
        """Clear all items from cache."""
        self._cache.clear()
        self._modified = True
        self._save(force=True)

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, *_):
        """Context manager exit - ensure cache is saved."""
        if self._modified:
            self._save(force=True)

def get_human_readable_size(size_bytes):
    """Convert size in bytes to human readable format."""
    try:
        size_bytes = int(size_bytes)  # Ensure size_bytes is an integer
        if not isinstance(size_bytes, (int, float)) or size_bytes < 0:
            return "Unknown size"
        
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.2f} PB"
    except (ValueError, TypeError):
        return "Unknown size"

def get_service():
    """Authorize and return Google Drive service."""
    creds = None
    token_file = 'token.json'
    
    # Check file permissions
    if os.path.exists(token_file):
        try:
            # Ensure token file has correct permissions
            os.chmod(token_file, 0o600)
            with open(token_file, 'rb') as token:
                creds = pickle.load(token)
        except (pickle.PickleError, IOError, PermissionError) as e:
            logging.error(f"Error loading credentials: {e}")
            creds = None

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
                # Save refreshed credentials
                with open(token_file, 'wb') as token:
                    pickle.dump(creds, token)
                os.chmod(token_file, 0o600)  # Set secure permissions
            except Exception as e:
                logging.error(f"Error refreshing credentials: {e}")
                creds = None
        
        if not creds:
            try:
                flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
                creds = flow.run_local_server(port=0)
                with open(token_file, 'wb') as token:
                    pickle.dump(creds, token)
                os.chmod(token_file, 0o600)  # Set secure permissions
            except Exception as e:
                logging.error(f"Error creating new credentials: {e}")
                raise SystemExit("Failed to authenticate with Google Drive")

    try:
        return build('drive', 'v3', credentials=creds)
    except Exception as e:
        logging.error(f"Error building Drive service: {e}")
        raise SystemExit("Failed to initialize Google Drive service")

class BatchHandler:
    """Generic handler for batching Google Drive API requests.
    
    This class manages batch operations for both metadata fetching and trash operations.
    It handles caching, retries, and maintains separate tracking for cached vs API results.
    """
    
    def __init__(self, service: Resource, cache: MetadataCache):
        """Initialize batch handler.
        
        Args:
            service: Google Drive API service instance
            cache: MetadataCache instance for storing/retrieving cached results
        """
        self.service = service
        self.cache = cache
        self.batch = service.new_batch_http_request()
        self.results = {}  # Store API call results
        self.count = 0  # Track number of requests in current batch
        self._updates = {}  # Track successful updates for cache
        self._removals = []  # Track files to remove from cache
        self._failed_requests = set()  # Track failed requests for retry
        self._cached_results = {}  # Store results from cache hits

    def add_metadata_request(self, file_id: str) -> None:
        """Add metadata fetch request to batch.
        
        Checks cache first, then adds to batch if not cached.
        Automatically executes batch if size limit is reached.
        """
        # Check cache first to avoid unnecessary API calls
        cached = self.cache.get(file_id)
        if cached:
            self._cached_results[file_id] = cached
            return  # Don't count cached files in batch size

        # Execute current batch if we've reached the size limit
        if self.count >= BATCH_SIZE:
            self.execute()

        def callback(request_id, response, exception):
            """Handle batch request response.
            
            Updates results and tracks failures for retry logic.
            """
            if exception:
                logging.warning(f"Failed to fetch metadata for {file_id} in batch: {exception}")
                self.results[file_id] = None
                self._failed_requests.add(file_id)
            else:
                self.results[file_id] = response
                self._updates[file_id] = response

        # Add request to batch
        self.batch.add(
            self.service.files().get(fileId=file_id, fields='*'),
            callback=callback
        )
        self.count += 1

    def add_trash_request(self, file_id: str) -> None:
        """Add trash move request to batch."""
        if self.count >= BATCH_SIZE:
            self.execute()

        def callback(request_id, response, exception):
            if exception:
                logging.error(f"Failed to trash file {file_id}: {exception}")
                self.results[file_id] = False
                self._failed_requests.add(file_id)
            else:
                self.results[file_id] = True
                self._removals.append(file_id)

        self.batch.add(
            self.service.files().update(fileId=file_id, body={'trashed': True}),
            callback=callback
        )
        self.count += 1

    def execute(self) -> None:
        """Execute batch requests if any pending.
        
        Handles successful updates, cache management, and error recovery.
        Resets batch state for next operation.
        """
        if not self.count:
            return
            
        try:
            self.batch.execute()
            # Update cache with successful results
            if self._updates:
                self.cache.update(self._updates)
            # Remove trashed files from cache
            if self._removals:
                self.cache.remove(self._removals)
        except Exception as e:
            logging.error(f"Batch execution failed: {e}")
            # Mark all pending requests as failed for retry
            for file_id in self._updates:
                if file_id not in self.results:
                    self.results[file_id] = None
                    self._failed_requests.add(file_id)
        finally:
            # Reset batch state for next operation
            self.batch = self.service.new_batch_http_request()
            self.count = 0
            self._updates = {}
            self._removals = []

    def get_results(self) -> Dict[str, Any]:
        """Get combined results from both cache and API calls.
        
        Returns:
            Dict mapping file IDs to their metadata, combining cached and API results.
        """
        results = self.results.copy()
        results.update(self._cached_results)
        return results

    def get_failed_requests(self) -> Set[str]:
        """Get set of file IDs that failed to process."""
        return self._failed_requests

class DriveAPI:
    """Simplified Google Drive API wrapper with caching and batching.
    
    Provides high-level operations for file management with automatic
    caching, batching, and retry logic.
    """
    
    def __init__(self, service: Resource, cache: Optional[MetadataCache] = None):
        self.service = service
        self.cache = cache or MetadataCache()  # Use provided cache or create new one
        self._file_cache = {}  # In-memory cache for file list

    def list_files(self, force_refresh: bool = False) -> List[Dict]:
        """List all files in Drive with caching."""
        cache_key = 'all_files'
        if not force_refresh:
            cached = self.cache.get(cache_key)
            if cached:
                logging.info(f"Using cached list of {len(cached)} files")
                return cached
        
        files = []
        page_token = None
        
        try:
            while True:
                response = self.service.files().list(
                    q="trashed = false",
                    pageSize=1000,
                    fields="nextPageToken, files(id, name, size, md5Checksum, trashed, parents)",
                    pageToken=page_token
                ).execute()
                
                batch = response.get('files', [])
                files.extend(batch)
                logging.info(f"Retrieved {len(files)} files")
                
                page_token = response.get('nextPageToken')
                if not page_token:
                    break
            
            self.cache.set(cache_key, files)
            logging.info(f"Cached {len(files)} files")
            return files
            
        except Exception as e:
            logging.error(f"Failed to fetch files: {e}")
            cached = self.cache.get(cache_key)
            if cached:
                logging.info(f"Using cached data as fallback ({len(cached)} files)")
                return cached
            raise

    def get_file_metadata(self, file_id: str) -> Optional[dict]:
        """Get file metadata with caching."""
        if not file_id:
            return None

        # Check cache first
        cached_metadata = self.cache.get(file_id)
        if cached_metadata is not None:
            return cached_metadata

        try:
            metadata = self.service.files().get(
                fileId=file_id,
                fields=METADATA_FIELDS
            ).execute()
            self.cache.set(file_id, metadata)
            return metadata
        except Exception as e:
            logging.error(f"Error getting file metadata for {file_id}: {e}")
            return None

    def get_files_metadata_batch(self, file_ids: list[str]) -> Dict[str, dict]:
        """Get metadata for multiple files in batches with retry logic.
        
        Implements a three-step process:
        1. Check cache for each file
        2. Batch fetch uncached files
        3. Retry failed requests individually
        
        Args:
            file_ids: List of file IDs to fetch metadata for
            
        Returns:
            Dict mapping file IDs to their metadata
        """
        results = {}
        files_to_fetch = []
        
        # Step 1: Check cache for each file
        for file_id in file_ids:
            cached_metadata = self.cache.get(file_id)
            if cached_metadata is not None:
                results[file_id] = cached_metadata
            else:
                files_to_fetch.append(file_id)
        
        if not files_to_fetch:
            return results
        
        # Step 2: Process remaining files in smaller batches
        for i in range(0, len(files_to_fetch), BATCH_SIZE):
            batch_ids = files_to_fetch[i:i + BATCH_SIZE]
            logging.info(f"Processing batch {i//BATCH_SIZE + 1} of {(len(files_to_fetch) + BATCH_SIZE - 1)//BATCH_SIZE}")
            
            # Try batch request first
            handler = BatchHandler(self.service, self.cache)
            for file_id in batch_ids:
                handler.add_metadata_request(file_id)
            
            try:
                # Execute the batch
                handler.execute()
                results.update(handler.get_results())
            except Exception as e:
                logging.error(f"Batch execution failed: {e}")
                # Mark all pending requests as failed
                for file_id in batch_ids:
                    if file_id not in results:
                        results[file_id] = None
            
            # Step 3: Retry failed requests individually with exponential backoff
            failed_requests = handler.get_failed_requests()
            for file_id in failed_requests:
                if file_id not in results or results[file_id] is None:
                    retries = 0
                    while retries < MAX_RETRIES:
                        try:
                            metadata = self.service.files().get(
                                fileId=file_id,
                                fields=METADATA_FIELDS
                            ).execute()
                            if metadata:  # Only update if we got valid metadata
                                results[file_id] = metadata
                                self.cache.set(file_id, metadata)
                                break
                        except Exception as retry_e:
                            retries += 1
                            if retries == MAX_RETRIES:
                                logging.error(f"Failed to fetch metadata for {file_id} after {MAX_RETRIES} retries: {retry_e}")
                                results[file_id] = None
                            else:
                                delay = RETRY_DELAY * (2 ** (retries - 1))
                                logging.warning(f"Retry {retries} for {file_id}, waiting {delay}s")
                                time.sleep(delay)
        
        return results

    def move_files_to_trash_batch(self, file_ids: list[str]) -> Dict[str, bool]:
        """Move multiple files to trash in batches."""
        results = {}
        
        # Process files in smaller batches
        for i in range(0, len(file_ids), BATCH_SIZE):
            batch_ids = file_ids[i:i + BATCH_SIZE]
            handler = BatchHandler(self.service, self.cache)
            
            for file_id in batch_ids:
                handler.add_trash_request(file_id)
            
            # Execute the batch
            handler.execute()
            results.update(handler.results)
            
            # Retry failed requests individually
            failed_requests = handler.get_failed_requests()
            for file_id in failed_requests:
                if file_id not in results:
                    retries = 0
                    while retries < MAX_RETRIES:
                        try:
                            self.service.files().update(
                                fileId=file_id,
                                body={'trashed': True}
                            ).execute()
                            results[file_id] = True
                            self.cache.remove([file_id])
                            break
                        except Exception as retry_e:
                            retries += 1
                            if retries == MAX_RETRIES:
                                logging.error(f"Failed to trash file {file_id} after {MAX_RETRIES} retries: {retry_e}")
                                results[file_id] = False
                            else:
                                delay = RETRY_DELAY * (2 ** (retries - 1))
                                logging.warning(f"Retry {retries} for {file_id}, waiting {delay}s")
                                time.sleep(delay)
        
        return results

class DuplicateGroup:
    """Represents a group of duplicate files with their metadata."""
    
    def __init__(self, files: List[Dict], metadata: Dict[str, dict]):
        self.files = files
        self.metadata = metadata
        self._total_size = None
        self._wasted_space = None

    @property
    def total_size(self) -> int:
        """Calculate total size of all files in the group."""
        if self._total_size is None:
            self._total_size = sum(int(self.metadata[f['id']].get('size', 0)) for f in self.files)
        return self._total_size

    @property
    def wasted_space(self) -> int:
        """Calculate wasted space (size of redundant copies)."""
        if self._wasted_space is None:
            self._wasted_space = int(self.files[0].get('size', 0)) * (len(self.files) - 1)
        return self._wasted_space

    def get_parent_folders(self) -> Set[str]:
        """Get set of parent folder IDs for all files in the group."""
        folders = set()
        for file in self.files:
            file_meta = self.metadata.get(file['id'])
            if file_meta:
                folders.update(file_meta.get('parents', []))
        return folders

    def print_info(self) -> None:
        """Print information about the duplicate group."""
        example_file = self.files[0]
        file_size = get_human_readable_size(int(example_file.get('size', 0)))
        print(f"\nFound duplicate group ({len(self.files)} files):")
        
        for file in self.files:
            file_meta = self.metadata.get(file['id'])
            if not file_meta:
                continue
                
            print(f"  - {file_meta['name']} (Size: {file_size})")
            print(f"    ID: {file_meta['id']}")
            print(f"    Parent: {', '.join(file_meta.get('parents', ['No parent']))}")

class DuplicateFolder:
    """Represents a folder containing duplicate files."""
    
    def __init__(self, folder_id: str, folder_meta: dict, duplicate_files: Set[str]):
        self.id = folder_id
        self.name = folder_meta.get('name', 'Unknown')
        self.duplicate_files = duplicate_files
        self._total_size = None
        self._is_duplicate_only = None

    @property
    def total_size(self) -> int:
        """Calculate total size of duplicate files in the folder."""
        if self._total_size is None:
            self._total_size = sum(int(meta.get('size', 0)) for meta in self.file_metadata.values())
        return self._total_size

    def update_metadata(self, file_metadata: Dict[str, dict]) -> None:
        """Update file metadata for size calculations."""
        self.file_metadata = file_metadata
        self._total_size = None  # Reset cached total size

    def check_if_duplicate_only(self, all_folder_files: Set[str]) -> bool:
        """Check if folder contains only files that are duplicated elsewhere.
        
        Args:
            all_folder_files: Set of file IDs in the folder
            
        Returns:
            True if every file in the folder has a duplicate in another location.
            False if any file in the folder has no duplicates elsewhere.
            
        Example:
            If file1 has a duplicate in another folder:
            - {file1} -> True (has duplicate elsewhere)
            - {file1, file2} -> True (if both have duplicates elsewhere)
            - {file3} -> False (no duplicates elsewhere)
        """
        # A folder is duplicate-only if every file in it has a duplicate elsewhere
        return all(
            file_id in self.duplicate_files
            for file_id in all_folder_files
        )

    def print_info(self) -> None:
        """Print folder information."""
        print(f"\n{self.name}:")
        print(f"  - {len(self.duplicate_files)} duplicate files")
        print(f"  - Total size: {get_human_readable_size(self.total_size)}")
        print(f"  - Folder ID: {self.id}")

class DuplicateScanner:
    """Main class for scanning and managing duplicate files."""
    
    def __init__(self, drive_api: DriveAPI):
        self.drive_api = drive_api
        self.duplicate_groups: List[DuplicateGroup] = []
        self.duplicate_folders: Dict[str, DuplicateFolder] = {}

    def scan(self, delete: bool = False, force_refresh: bool = False) -> List[DuplicateGroup]:
        """Scan for duplicate files and organize results."""
        # Get and validate files
        all_files = self.drive_api.list_files(force_refresh=force_refresh)
        valid_files = self._filter_valid_files(all_files)
        
        # Find duplicates
        self._find_duplicates(valid_files)
        
        # Process results
        if self.duplicate_groups:
            self._process_results(delete)
        
        return self.duplicate_groups

    def _filter_valid_files(self, files: List[Dict]) -> List[Dict]:
        """Filter files that have valid MD5 checksums and non-zero size."""
        return [
            f for f in files 
            if 'md5Checksum' in f and f.get('size', '0') != '0'
        ]

    def _find_duplicates(self, valid_files: List[Dict]) -> None:
        """Find duplicate files using size and MD5 grouping."""
        # Group by size first
        files_by_size = self._group_files_by_size(valid_files)
        
        # Find duplicates within each size group
        for size, size_files in files_by_size.items():
            if len(size_files) > 1:
                self._process_size_group(size_files)

    def _group_files_by_size(self, files: List[Dict]) -> Dict[str, List[Dict]]:
        """Group files by their size."""
        files_by_size = defaultdict(list)
        for file in files:
            size = file.get('size', '0')
            if size != '0':  # Skip zero-size files
                files_by_size[size].append(file)
        return files_by_size

    def _process_size_group(self, size_files: List[Dict]) -> None:
        """Process a group of files with the same size."""
        # Group by MD5
        files_by_md5 = self._group_files_by_md5(size_files)
        
        # Process each group of duplicates
        for md5, files in files_by_md5.items():
            if len(files) > 1:
                self._process_duplicate_group(files)

    def _group_files_by_md5(self, files: List[Dict]) -> Dict[str, List[Dict]]:
        """Group files by their MD5 checksum."""
        files_by_md5 = defaultdict(list)
        for file in files:
            if 'md5Checksum' in file:  # Skip files without MD5
                files_by_md5[file['md5Checksum']].append(file)
        return files_by_md5

    def _process_duplicate_group(self, files: List[Dict]) -> None:
        """Process a group of duplicate files."""
        # Get metadata for all files in this group
        file_ids = [f['id'] for f in files]
        metadata = self.drive_api.get_files_metadata_batch(file_ids)
        
        # Create duplicate group
        group = DuplicateGroup(files, metadata)
        self.duplicate_groups.append(group)
        
        # Print group info
        group.print_info()
        
        # Update folder tracking
        self._update_folder_tracking(group)

    def _update_folder_tracking(self, group: DuplicateGroup) -> None:
        """Update folder tracking with files from a duplicate group."""
        for file in group.files:
            file_meta = group.metadata.get(file['id'])
            if file_meta:
                for parent in file_meta.get('parents', []):
                    if parent not in self.duplicate_folders:
                        folder_meta = self.drive_api.get_file_metadata(parent)
                        if folder_meta:
                            self.duplicate_folders[parent] = DuplicateFolder(
                                parent, folder_meta, set()
                            )
                    if parent in self.duplicate_folders:
                        self.duplicate_folders[parent].duplicate_files.add(file['id'])

    def _process_results(self, delete: bool) -> None:
        """Process and display results."""
        # Calculate statistics
        total_duplicates = sum(len(group.files) for group in self.duplicate_groups)
        total_wasted = sum(group.wasted_space for group in self.duplicate_groups)
        
        # Print summary
        self._print_summary(total_duplicates, total_wasted)
        
        # Process folders
        if self.duplicate_folders:
            self._process_folders()
        
        # Handle deletion if requested
        if delete:
            self._handle_deletions()

    def _print_summary(self, total_duplicates: int, total_wasted: int) -> None:
        """Print summary of duplicate files found."""
        print("\nDuplicate Files Summary:")
        print(f"Total files scanned: {len(self.duplicate_groups)}")
        print(f"Duplicate groups found: {len(self.duplicate_groups)}")
        print(f"Total duplicate files: {total_duplicates}")
        print(f"Wasted space: {get_human_readable_size(total_wasted)}")
        print(f"Folders containing duplicates: {len(self.duplicate_folders)}")

    def _process_folders(self) -> None:
        """Process and display folder information."""
        # Get all folder metadata
        folder_ids = list(self.duplicate_folders.keys())
        folder_metadata = self.drive_api.get_files_metadata_batch(folder_ids)
        
        # Update folder metadata
        for folder_id, folder in self.duplicate_folders.items():
            if folder_id in folder_metadata:
                folder.update_metadata(folder_metadata)
        
        # Print folder summary
        print("\nFolders containing duplicates:")
        print("==============================")
        for folder in sorted(self.duplicate_folders.values(), 
                           key=lambda f: f.total_size, reverse=True):
            folder.print_info()
        
        # Find and print duplicate-only folders
        self._print_duplicate_only_folders()

    def _print_duplicate_only_folders(self) -> None:
        """Find and print folders that only contain duplicates."""
        # Get all files in these folders
        all_files = self.drive_api.list_files(force_refresh=True)
        folder_files = defaultdict(set)
        for file in all_files:
            for parent in file.get('parents', []):
                if parent in self.duplicate_folders:
                    folder_files[parent].add(file['id'])
        
        # Find duplicate-only folders
        duplicate_only = [
            folder for folder in self.duplicate_folders.values()
            if folder.check_if_duplicate_only(folder_files.get(folder.id, set()))
        ]
        
        if duplicate_only:
            print("\nFolders containing only duplicates:")
            print("================================")
            for folder in sorted(duplicate_only, key=lambda f: f.total_size, reverse=True):
                folder.print_info()

    def _handle_deletions(self) -> None:
        """Handle deletion of duplicate files."""
        for group in self.duplicate_groups:
            _handle_group_deletion(self.drive_api, group.files, group.metadata)

def handle_duplicate(drive_api: DriveAPI, file1: dict, file2: dict, duplicate_folders: Dict[str, Set[str]], delete: bool = False) -> None:
    """Handle a duplicate file pair with batched metadata fetching."""
    # Get metadata for both files in a single batch
    metadata = drive_api.get_files_metadata_batch([file1['id'], file2['id']])
    
    file1_meta = metadata.get(file1['id'])
    file2_meta = metadata.get(file2['id'])
    
    if not file1_meta or not file2_meta:
        logging.error(f"Could not fetch metadata for one or both files: {file1['id']}, {file2['id']}")
        return

    # Update duplicate folders tracking
    for parent in file1_meta.get('parents', []):
        duplicate_folders[parent].add(file1_meta['id'])
    for parent in file2_meta.get('parents', []):
        duplicate_folders[parent].add(file2_meta['id'])

    # Log the duplicate
    file1_size = get_human_readable_size(int(file1_meta.get('size', 0)))
    file2_size = get_human_readable_size(int(file2_meta.get('size', 0)))
    
    logging.info(f"Found duplicate files:")
    logging.info(f"  1: {file1_meta['name']} (Size: {file1_size})")
    logging.info(f"  2: {file2_meta['name']} (Size: {file2_size})")

    if delete:
        print(f"\nDuplicate files found:")
        print(f"1: {file1_meta['name']} (Size: {file1_size})")
        print(f"2: {file2_meta['name']} (Size: {file2_size})")
        
        while True:
            choice = input("\nWhich file would you like to delete? (1/2/s to skip): ").lower()
            if choice == 's':
                break
            elif choice in ('1', '2'):
                file_to_delete = file1_meta if choice == '1' else file2_meta
                result = drive_api.move_files_to_trash_batch([file_to_delete['id']])
                if result.get(file_to_delete['id']):
                    print(f"File moved to trash: {file_to_delete['name']}")
                else:
                    print(f"Error moving file to trash: {file_to_delete['name']}")
                break
            else:
                print("Invalid choice. Please enter 1, 2, or s.")

def generate_csv_filename():
    """Generate a CSV filename with timestamp."""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    return f'drive_duplicates_{timestamp}.csv'

def write_to_csv(duplicate_pairs, drive_api: DriveAPI):
    """Write duplicate file information to CSV with minimal API calls."""
    csv_filename = generate_csv_filename()
    duplicate_group_id = 1
    
    with open(csv_filename, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=CSV_HEADERS)
        writer.writeheader()
        
        for file1, file2 in duplicate_pairs:
            # Get complete metadata for both files
            file1_meta = drive_api.get_file_metadata(file1['id'])
            file2_meta = drive_api.get_file_metadata(file2['id'])
            
            if not file1_meta or not file2_meta:
                continue

            def prepare_file_row(file_meta, duplicate_meta):
                parent_id = file_meta.get('parents', [''])[0]
                return {
                    'File Name': file_meta['name'],
                    'Full Path': f"{parent_id}/{file_meta['name']}",
                    'Size (Bytes)': file_meta.get('size', '0'),
                    'Size (Human Readable)': get_human_readable_size(int(file_meta.get('size', 0))),
                    'File ID': file_meta['id'],
                    'MD5 Checksum': file_meta.get('md5Checksum', ''),
                    'Duplicate Group ID': duplicate_group_id,
                    'Parent Folder': parent_id,
                    'Parent Folder ID': parent_id,
                    'Duplicate File Name': duplicate_meta['name'],
                    'Duplicate File Path': f"{duplicate_meta.get('parents', [''])[0]}/{duplicate_meta['name']}",
                    'Duplicate File Size': get_human_readable_size(int(duplicate_meta.get('size', 0))),
                    'Duplicate File ID': duplicate_meta['id']
                }

            # Write both files to CSV
            writer.writerow(prepare_file_row(file1_meta, file2_meta))
            writer.writerow(prepare_file_row(file2_meta, file1_meta))
            duplicate_group_id += 1

    logging.info(f"CSV export completed: {csv_filename}")
    return csv_filename

def _filter_valid_files(files: List[Dict]) -> List[Dict]:
    """Filter files that have valid MD5 checksums and non-zero size."""
    return [
        f for f in files 
        if 'md5Checksum' in f and f.get('size', '0') != '0'
    ]

def _group_files_by_size(files: List[Dict]) -> Dict[str, List[Dict]]:
    """Group files by their size."""
    files_by_size = defaultdict(list)
    for file in files:
        size = file.get('size', '0')
        if size != '0':  # Skip zero-size files
            files_by_size[size].append(file)
    return files_by_size

def _group_files_by_md5(files: List[Dict]) -> Dict[str, List[Dict]]:
    """Group files by their MD5 checksum."""
    files_by_md5 = defaultdict(list)
    for file in files:
        if 'md5Checksum' in file:  # Skip files without MD5
            files_by_md5[file['md5Checksum']].append(file)
    return files_by_md5

def _print_duplicate_group(files: List[Dict], metadata: Dict[str, dict]) -> None:
    """Print information about a group of duplicate files."""
    example_file = files[0]
    file_size = get_human_readable_size(int(example_file.get('size', 0)))
    print(f"\nFound duplicate group ({len(files)} files):")
    
    for file in files:
        file_meta = metadata.get(file['id'])
        if not file_meta:
            continue
            
        print(f"  - {file_meta['name']} (Size: {file_size})")
        print(f"    ID: {file_meta['id']}")
        print(f"    Parent: {', '.join(file_meta.get('parents', ['No parent']))}")

def _print_duplicate_folders_summary(drive_api: DriveAPI, duplicate_folders: Dict[str, Set[str]]):
    """Print summary of folders containing duplicates."""
    print("\nFolders containing duplicates:")
    print("==============================")
    
    # Get all folder metadata in one batch
    folder_ids = list(duplicate_folders.keys())
    folder_metadata = drive_api.get_files_metadata_batch(folder_ids)
    
    # Calculate total size of duplicates in each folder
    folder_info = []
    for folder_id, files in duplicate_folders.items():
        folder_meta = folder_metadata.get(folder_id)
        if folder_meta:
            # Get metadata for all files in this folder
            file_metadata = drive_api.get_files_metadata_batch(list(files))
            total_size = sum(int(meta.get('size', 0)) for meta in file_metadata.values())
            
            folder_info.append({
                'name': folder_meta.get('name', 'Unknown'),
                'count': len(files),
                'id': folder_id,
                'total_size': total_size
            })
    
    # Sort by total size of duplicates (descending)
    folder_info.sort(key=lambda x: x['total_size'], reverse=True)
    
    # Print sorted summary
    for info in folder_info:
        print(f"\n{info['name']}:")
        print(f"  - {info['count']} duplicate files")
        print(f"  - Total size: {get_human_readable_size(info['total_size'])}")
        print(f"  - Folder ID: {info['id']}")

def _get_duplicate_only_folders(drive_api: DriveAPI, duplicate_folders: Dict[str, Set[str]]) -> List[Dict]:
    """Get folders that only contain duplicate files, ordered by decreasing size."""
    # Get all folder metadata in one batch
    folder_ids = list(duplicate_folders.keys())
    folder_metadata = drive_api.get_files_metadata_batch(folder_ids)
    
    # Get all files in these folders
    all_files = drive_api.list_files(force_refresh=True)
    folder_files = defaultdict(set)
    for file in all_files:
        for parent in file.get('parents', []):
            if parent in folder_ids:
                folder_files[parent].add(file['id'])
    
    # Find folders that only contain duplicates
    duplicate_only_folders = []
    for folder_id, duplicate_files in duplicate_folders.items():
        folder_meta = folder_metadata.get(folder_id)
        if not folder_meta:
            continue
            
        # Get all files in this folder
        all_folder_files = folder_files.get(folder_id, set())
        
        # If all files in the folder are duplicates, add it to the list
        if all_folder_files.issubset(duplicate_files):
            # Get metadata for all files in this folder
            file_metadata = drive_api.get_files_metadata_batch(list(duplicate_files))
            total_size = sum(int(meta.get('size', 0)) for meta in file_metadata.values())
            
            duplicate_only_folders.append({
                'name': folder_meta.get('name', 'Unknown'),
                'count': len(duplicate_files),
                'id': folder_id,
                'total_size': total_size
            })
    
    # Sort by total size of duplicates (descending)
    duplicate_only_folders.sort(key=lambda x: x['total_size'], reverse=True)
    return duplicate_only_folders

def find_duplicates(drive_api: DriveAPI, delete: bool = False, force_refresh: bool = False) -> List[List[Dict]]:
    """Find duplicate files in Google Drive with batched operations."""
    scanner = DuplicateScanner(drive_api)
    duplicate_groups = scanner.scan(delete, force_refresh)
    
    # Write duplicates to CSV if any were found
    if duplicate_groups:
        duplicate_pairs = []
        for group in duplicate_groups:
            # Create pairs of files from each group
            for i in range(len(group.files)):
                for j in range(i + 1, len(group.files)):
                    duplicate_pairs.append((group.files[i], group.files[j]))
        
        csv_file = write_to_csv(duplicate_pairs, drive_api)
        print(f"\nDuplicate pairs have been written to: {csv_file}")
    
    return [group.files for group in duplicate_groups]

def _handle_group_deletion(drive_api: DriveAPI, files: List[dict], metadata: Dict[str, dict]):
    """Handle deletion for a group of duplicate files."""
    print("\nDuplicate files found:")
    for i, file in enumerate(files, 1):
        file_meta = metadata.get(file['id'])
        if file_meta:
            print(f"{i}: {file_meta['name']}")
            print(f"   Location: {', '.join(file_meta.get('parents', ['No parent']))}")
    
    while True:
        choice = input("\nWhich file number would you like to keep? (1-{} or s to skip): ".format(len(files)))
        if choice.lower() == 's':
            break
        try:
            idx = int(choice) - 1
            if 0 <= idx < len(files):
                # Move all other files to trash
                files_to_trash = [f['id'] for i, f in enumerate(files) if i != idx]
                results = drive_api.move_files_to_trash_batch(files_to_trash)
                
                # Report results
                success = sum(1 for v in results.values() if v)
                print(f"\nMoved {success} of {len(files_to_trash)} files to trash")
                break
        except ValueError:
            pass
        print("Invalid choice. Please enter a valid number or 's' to skip.")

def main():
    parser = argparse.ArgumentParser(description="Find duplicate files in Google Drive")
    parser.add_argument('--delete', action='store_true', help='Move duplicate files to trash')
    parser.add_argument('--refresh-cache', action='store_true', help='Force refresh of cache')
    args = parser.parse_args()
    
    service = get_service()
    drive_api = DriveAPI(service)
    find_duplicates(drive_api, delete=args.delete, force_refresh=args.refresh_cache)

if __name__ == '__main__':
    main()
