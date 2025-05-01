import logging
import time
from typing import Dict, Set, Any, Callable
from googleapiclient.discovery import Resource
from googleapiclient.http import BatchHttpRequest
from cache import MetadataCache
from config import BATCH_SIZE, MAX_RETRIES, RETRY_DELAY

class BatchHandler:
    """Handles batch requests to Google Drive API."""
    
    def __init__(self, service: Resource, cache: MetadataCache):
        self.service = service
        self.cache = cache
        self.batch = None
        self.results = {}
        self.failed_requests = set()
        self._init_batch()

    def _init_batch(self) -> None:
        """Initialize a new batch request."""
        self.batch = self.service.new_batch_http_request()
        # Don't reset results or failed_requests here

    def add_metadata_request(self, file_id: str) -> None:
        """Add a metadata request to the batch."""
        if not self.batch:
            self._init_batch()

        def callback(request_id, response, exception):
            if exception is not None:
                self.failed_requests.add(file_id)
                logging.error(f"Error getting metadata for file {file_id}: {exception}")
            else:
                self.results[file_id] = response
                # Cache the result
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
        if not self.batch:
            self._init_batch()

        def callback(request_id, response, exception):
            if exception is not None:
                self.failed_requests.add(file_id)
                logging.error(f"Error trashing file {file_id}: {exception}")
                self.results[file_id] = False
            else:
                self.results[file_id] = True
                # Remove from cache
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
                self.batch.execute()
                break
            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    logging.warning(f"Batch execution failed, retrying in {RETRY_DELAY} seconds: {e}")
                    time.sleep(RETRY_DELAY)
                else:
                    logging.error(f"Batch execution failed after {MAX_RETRIES} attempts: {e}")
                    raise

        # Initialize new batch for next use, but preserve results and failed_requests
        self._init_batch()

    def get_results(self) -> Dict[str, Any]:
        """Get the results of the batch request."""
        return self.results

    def get_failed_requests(self) -> Set[str]:
        """Get the set of failed request IDs."""
        return self.failed_requests 