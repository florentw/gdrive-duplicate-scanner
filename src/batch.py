"""Batch processing module for Google Drive API operations."""

import logging
import time
from typing import Dict, Set, Any, Callable
from googleapiclient.discovery import Resource
from googleapiclient.http import BatchHttpRequest
from cache import MetadataCache
from config import BATCH_SIZE, MAX_RETRIES, RETRY_DELAY, logger

class BatchHandler:
    """Handles batch requests to Google Drive API."""
    
    def __init__(self, service: Resource, cache: MetadataCache):
        self.service = service
        self.cache = cache
        self.batch = None
        self.results: Dict[str, Dict] = {}
        self._failed_requests: Set[str] = set()
        self._request_count = 0
        self._success_count = 0
        self._failure_count = 0
        self._retry_count = 0
        self._current_batch_size = 0
        self._init_batch()

    def _init_batch(self) -> None:
        """Initialize a new batch request."""
        self.batch = self.service.new_batch_http_request()
        self._current_batch_size = 0

    def add_metadata_request(self, file_id: str) -> None:
        """Add a metadata request to the batch."""
        if not self.batch or self._current_batch_size >= BATCH_SIZE:
            self._init_batch()

        self._request_count += 1
        self._current_batch_size += 1

        def callback(request_id, response, exception):
            if exception is not None:
                self._failed_requests.add(file_id)
                self._failure_count += 1
                logger.error(f"Error getting metadata for file {file_id}: {exception}")
            else:
                self._success_count += 1
                self.results[file_id] = response
                if response:
                    self.cache.set(file_id, response)

        self.batch.add(
            self.service.files().get(
                fileId=file_id,
                fields='id, name, parents, size, md5Checksum, mimeType, trashed'
            ),
            callback=callback
        )

    def add_trash_request(self, file_id: str) -> None:
        """Add a trash request to the batch."""
        if not self.batch or self._current_batch_size >= BATCH_SIZE:
            self._init_batch()

        self._request_count += 1
        self._current_batch_size += 1

        def callback(request_id, response, exception):
            if exception is not None:
                self._failed_requests.add(file_id)
                self._failure_count += 1
                logger.error(f"Error trashing file {file_id}: {exception}")
                self.results[file_id] = False
            else:
                self._success_count += 1
                self.results[file_id] = True
                self.cache.remove([file_id])

        self.batch.add(
            self.service.files().update(
                fileId=file_id,
                body={'trashed': True}
            ),
            callback=callback
        )

    def execute(self) -> None:
        """Execute the batch request with retries."""
        if not self.batch:
            return

        for attempt in range(MAX_RETRIES):
            try:
                logger.debug(f"Executing batch with {self._request_count} requests (Attempt {attempt + 1}/{MAX_RETRIES})")
                self.batch.execute()
                break
            except Exception as e:
                self._retry_count += 1
                if attempt < MAX_RETRIES - 1:
                    logger.warning(f"Batch execution failed, retrying in {RETRY_DELAY} seconds: {e}")
                    time.sleep(RETRY_DELAY)
                else:
                    logger.error(f"Batch execution failed after {MAX_RETRIES} attempts: {e}")
                    raise

        # Log batch statistics
        success_rate = (self._success_count / self._request_count * 100) if self._request_count > 0 else 0
        logger.info(
            f"Batch execution completed: {self._success_count}/{self._request_count} successful "
            f"({success_rate:.1f}%), {self._failure_count} failed, {self._retry_count} retries"
        )

        # Initialize new batch for next use
        self._init_batch()

    def get_results(self) -> Dict[str, Any]:
        """Get the results of the batch request."""
        return self.results

    def get_failed_requests(self) -> Set[str]:
        """Get the set of failed request IDs."""
        return self._failed_requests

    def get_statistics(self) -> Dict[str, int]:
        """Get batch operation statistics."""
        return {
            'total_requests': self._request_count,
            'successful_requests': self._success_count,
            'failed_requests': self._failure_count,
            'retry_count': self._retry_count
        } 