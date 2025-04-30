import os
import json
import hashlib
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List
from config import CACHE_FILE, CACHE_EXPIRY_HOURS, SAVE_INTERVAL_MINUTES

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

    def get_all_files(self) -> List[Dict]:
        """Get all cached files."""
        self._cleanup_expired()
        return self._cache.get('all_files', [])

    def get_all_folders(self) -> List[Dict]:
        """Get all cached folders."""
        self._cleanup_expired()
        return self._cache.get('all_folders', [])

    def cache_files(self, files: List[Dict]) -> None:
        """Cache a list of files."""
        self._cache['all_files'] = files
        self._modified = True
        self._save()

    def cache_folders(self, folders: List[Dict]) -> None:
        """Cache a list of folders."""
        self._cache['all_folders'] = folders
        self._modified = True
        self._save()

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, *_):
        """Context manager exit - ensure cache is saved."""
        if self._modified:
            self._save(force=True) 