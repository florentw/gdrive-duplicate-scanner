import logging
from typing import List, Dict, Optional, Any
from googleapiclient.discovery import Resource
from cache import MetadataCache
from batch import BatchHandler
from config import BATCH_SIZE, METADATA_FIELDS
from tqdm import tqdm

class DriveAPI:
    """Wrapper for Google Drive API operations."""
    
    def __init__(self, service: Resource, cache: Optional[MetadataCache] = None):
        self.service = service
        self.cache = cache or MetadataCache()
        self.batch_handler = None

    def _get_batch_handler(self) -> BatchHandler:
        """Get a new batch handler instance."""
        if not self.batch_handler:
            self.batch_handler = BatchHandler(self.service, self.cache)
        return self.batch_handler

    def _get_total_file_count(self) -> int:
        """Get total number of files in Google Drive."""
        try:
            initial_response = self.service.files().list(
                q="trashed=false",
                spaces='drive',
                fields='nextPageToken, files(id)',
                pageSize=1
            ).execute()
            
            if not initial_response.get('files'):
                return 0
                
            # Get total count from the API
            count_response = self.service.files().list(
                q="trashed=false",
                spaces='drive',
                fields='nextPageToken, files(id)',
                pageSize=1000
            ).execute()
            return len(count_response.get('files', []))
            
        except Exception as e:
            logging.error(f"Error getting file count: {e}")
            return 0

    def _fetch_files_page(self, page_token: Optional[str] = None) -> tuple[List[Dict], Optional[str]]:
        """Fetch a single page of files from Google Drive."""
        try:
            response = self.service.files().list(
                q="trashed=false",
                spaces='drive',
                fields=f'nextPageToken, files({METADATA_FIELDS})',
                pageToken=page_token
            ).execute()
            
            return response.get('files', []), response.get('nextPageToken')
            
        except Exception as e:
            logging.error(f"Error listing files: {e}")
            return [], None

    def list_files(self, force_refresh: bool = False) -> List[Dict]:
        """List all non-trashed files in Google Drive."""
        if not force_refresh:
            cached_files = self.cache.get('all_files')
            if cached_files:
                return cached_files

        files = []
        page_token = None
        total_count = self._get_total_file_count()
        
        with tqdm(total=total_count, desc="Listing files", unit="file") as pbar:
            while True:
                new_files, page_token = self._fetch_files_page(page_token)
                files.extend(new_files)
                pbar.update(len(new_files))
                
                if not page_token:
                    break

        if files:
            self.cache.set('all_files', files)
            
        return files

    def get_file_metadata(self, file_id: str) -> Optional[dict]:
        """Get metadata for a single file."""
        cached_meta = self.cache.get(file_id)
        if cached_meta:
            return cached_meta

        try:
            file = self.service.files().get(
                fileId=file_id,
                fields=METADATA_FIELDS
            ).execute()
            
            self.cache.set(file_id, file)
            return file
            
        except Exception as e:
            logging.error(f"Error getting metadata for file {file_id}: {e}")
            return None

    def get_files_metadata_batch(self, file_ids: list[str]) -> Dict[str, dict]:
        """Get metadata for multiple files using batch requests."""
        results = {}
        remaining_ids = set(file_ids)
        
        # Check cache first
        for file_id in file_ids:
            cached_meta = self.cache.get(file_id)
            if cached_meta:
                results[file_id] = cached_meta
                remaining_ids.remove(file_id)
        
        if not remaining_ids:
            return results

        # Process remaining files in batches
        batch_handler = self._get_batch_handler()
        with tqdm(total=len(remaining_ids), desc="Fetching metadata", unit="file") as pbar:
            for i in range(0, len(remaining_ids), BATCH_SIZE):
                batch_ids = list(remaining_ids)[i:i + BATCH_SIZE]
                
                for file_id in batch_ids:
                    batch_handler.add_metadata_request(file_id)
                
                try:
                    batch_handler.execute()
                    batch_results = batch_handler.get_results()
                    results.update(batch_results)
                    pbar.update(len(batch_ids))
                    
                    # Handle failed requests
                    failed = batch_handler.get_failed_requests()
                    if failed:
                        logging.warning(f"Failed to get metadata for {len(failed)} files")
                        # Try to get failed files individually
                        for file_id in failed:
                            try:
                                single_result = self.get_file_metadata(file_id)
                                if single_result:
                                    results[file_id] = single_result
                            except Exception as e:
                                logging.error(f"Failed to get metadata for file {file_id}: {e}")
                                # Remove failed result if it was added
                                results.pop(file_id, None)
                except Exception as e:
                    logging.error(f"Batch execution failed: {e}")
                    # Try to get files individually after batch failure
                    for file_id in batch_ids:
                        try:
                            single_result = self.get_file_metadata(file_id)
                            if single_result:
                                results[file_id] = single_result
                        except Exception as inner_e:
                            logging.error(f"Failed to get metadata for file {file_id}: {inner_e}")
                            # Remove failed result if it was added
                            results.pop(file_id, None)

        return results

    def move_files_to_trash_batch(self, file_ids: list[str]) -> Dict[str, bool]:
        """Move multiple files to trash using batch requests."""
        results = {}
        remaining_ids = set(file_ids)
        
        # Process files in batches
        batch_handler = self._get_batch_handler()
        for i in range(0, len(remaining_ids), BATCH_SIZE):
            batch_ids = list(remaining_ids)[i:i + BATCH_SIZE]
            
            for file_id in batch_ids:
                batch_handler.add_trash_request(file_id)
            
            try:
                batch_handler.execute()
                batch_results = batch_handler.get_results()
                results.update(batch_results)
                
                # Handle failed requests
                failed = batch_handler.get_failed_requests()
                if failed:
                    logging.warning(f"Failed to trash {len(failed)} files")
                    for file_id in failed:
                        results[file_id] = False
            except Exception as e:
                logging.error(f"Batch execution failed: {e}")
                for file_id in batch_ids:
                    results[file_id] = False

        return results