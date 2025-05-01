import logging
from typing import List, Dict, Optional, Any
from googleapiclient.discovery import Resource
from cache import MetadataCache
from batch import BatchHandler
from config import BATCH_SIZE, METADATA_FIELDS, logger
from tqdm import tqdm

class DriveAPI:
    """Wrapper for Google Drive API operations."""
    
    def __init__(self, service: Resource, cache: Optional[MetadataCache] = None):
        self.service = service
        self.cache = cache or MetadataCache()
        self.batch_handler = None
        self.api_request_count = 0  # Add counter for API requests
        self._total_batches_processed = 0
        self._total_batch_requests = 0
        self._total_batch_successes = 0
        self._total_batch_failures = 0
        self._total_batch_retries = 0

    def _get_batch_handler(self) -> BatchHandler:
        """Get a new batch handler instance."""
        if not self.batch_handler:
            self.batch_handler = BatchHandler(self.service, self.cache, self._increment_request_count)
        return self.batch_handler

    def _increment_request_count(self) -> None:
        """Increment the API request counter."""
        self.api_request_count += 1

    def _update_batch_statistics(self, stats: Dict[str, int]) -> None:
        """Update batch operation statistics."""
        self._total_batch_requests += stats['total_requests']
        self._total_batch_successes += stats['successful_requests']
        self._total_batch_failures += stats['failed_requests']
        self._total_batch_retries += stats['retry_count']
        self._total_batches_processed += 1

    def get_batch_statistics(self) -> Dict[str, int]:
        """Get overall batch operation statistics."""
        return {
            'total_batches': self._total_batches_processed,
            'total_requests': self._total_batch_requests,
            'successful_requests': self._total_batch_successes,
            'failed_requests': self._total_batch_failures,
            'retry_count': self._total_batch_retries,
            'total_api_requests': self.api_request_count
        }

    def _fetch_files_page(self, page_token: Optional[str] = None) -> tuple[List[Dict], Optional[str]]:
        """Fetch a single page of files from Google Drive."""
        try:
            self._increment_request_count()  # Count API request
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
            cached_files = self.cache.get_all_files()
            if cached_files:
                return cached_files

        files = []
        page_token = None
        
        with tqdm(desc="Scanning Drive", unit=" files", unit_scale=True) as pbar:
            while True:
                new_files, page_token = self._fetch_files_page(page_token)
                files.extend(new_files)
                pbar.update(len(new_files))
                
                if not page_token:
                    break

        if files:
            self.cache.cache_files(files)
            
        return files

    def get_file_metadata(self, file_id: str) -> Optional[dict]:
        """Get metadata for a single file."""
        cached_meta = self.cache.get(file_id)
        if cached_meta:
            return cached_meta

        try:
            self._increment_request_count()  # Only increment for actual API calls
            file = self.service.files().get(
                fileId=file_id,
                fields=METADATA_FIELDS
            ).execute()
            
            self.cache.set(file_id, file)
            return file
            
        except Exception as e:
            logging.error(f"Error getting metadata for file {file_id}: {e}")
            return None

    def _process_batch_results(self, batch_handler: BatchHandler, batch_ids: list[str], results: Dict[str, dict]) -> None:
        """Process results from a batch request and handle any failures."""
        try:
            batch_handler.execute()
            batch_results = batch_handler.get_results()
            results.update(batch_results)
            
            # Handle failed requests
            failed = batch_handler.get_failed_requests()
            if failed:
                logging.warning(f"Failed to get metadata for {len(failed)} files")
                self._handle_failed_requests(failed, results)
                
        except Exception as e:
            logging.error(f"Batch execution failed: {e}")
            self._handle_failed_requests(batch_ids, results)

    def _handle_failed_requests(self, failed_ids: set[str], results: Dict[str, dict]) -> None:
        """Handle failed requests by trying to get metadata individually."""
        for file_id in failed_ids:
            try:
                single_result = self.get_file_metadata(file_id)
                if single_result:
                    results[file_id] = single_result
            except Exception as e:
                logging.error(f"Failed to get metadata for file {file_id}: {e}")
                # Remove failed result if it was added
                results.pop(file_id, None)

    def _get_cached_metadata(self, file_ids: list[str]) -> tuple[Dict[str, dict], set[str]]:
        """Get metadata from cache and return remaining uncached file IDs."""
        results = {}
        remaining_ids = set(file_ids)
        
        for file_id in file_ids:
            cached_meta = self.cache.get(file_id)
            if cached_meta:
                results[file_id] = cached_meta
                remaining_ids.remove(file_id)
                
        return results, remaining_ids

    def get_files_metadata_batch(self, file_ids: list[str]) -> Dict[str, dict]:
        """Get metadata for multiple files using batch requests."""
        # Check cache first
        results, remaining_ids = self._get_cached_metadata(file_ids)
        
        if not remaining_ids:
            return results

        # Process remaining files in batches
        batch_handler = self._get_batch_handler()
        total_files = len(remaining_ids)
        total_batches = (total_files + BATCH_SIZE - 1) // BATCH_SIZE
        avg_batch_size = total_files / total_batches if total_batches > 0 else 0
        
        logger.info(
            f"Processing {total_files} files in {total_batches} batches "
            f"(avg {avg_batch_size:.1f} files per batch, {self.api_request_count} API requests so far)"
        )
        
        current_batch_size = 0
        for file_id in remaining_ids:
            batch_handler.add_metadata_request(file_id)
            current_batch_size += 1
            
            # Only execute batch when we reach BATCH_SIZE or it's the last batch
            if current_batch_size >= BATCH_SIZE or file_id == list(remaining_ids)[-1]:
                # Process batch results
                self._process_batch_results(batch_handler, list(remaining_ids), results)
                
                # Update statistics
                self._update_batch_statistics(batch_handler.get_statistics())
                current_batch_size = 0

        # Log final batch statistics
        stats = self.get_batch_statistics()
        success_rate = (stats['successful_requests'] / stats['total_requests'] * 100) if stats['total_requests'] > 0 else 0
        logger.info(
            f"Batch operations completed: {stats['total_batches']} batches, "
            f"{stats['successful_requests']}/{stats['total_requests']} successful ({success_rate:.1f}%), "
            f"{stats['failed_requests']} failed, {stats['retry_count']} retries, "
            f"{stats['total_api_requests']} total API requests"
        )

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