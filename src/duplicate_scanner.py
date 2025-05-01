import os
import logging
from typing import Dict, List, Any
from googleapiclient.discovery import Resource
from tqdm import tqdm

from .auth import get_service
from .cache import Cache
from .config import logger, BATCH_SIZE

class DuplicateScanner:
    """A class to scan Google Drive for duplicate files."""
    
    def __init__(self):
        """Initialize the scanner with Google Drive service and cache."""
        self.drive_service = get_service()
        if not self.drive_service:
            raise RuntimeError("Failed to initialize Google Drive service")
            
        self.cache = Cache()
        self.logger = logger
        
    def scan_files(self) -> Dict[str, Any]:
        """Scan all files in Google Drive for duplicates.
        
        Returns:
            Dictionary containing:
            - files_by_size: Dict mapping file sizes to lists of file IDs
            - total_files: Total number of files scanned
            - total_size: Total size of all files in bytes
        """
        self.logger.info("Starting file scan...")
        
        # Get all files
        files = self._list_all_files()
        total_files = len(files)
        self.logger.info(f"Found {total_files} files to check for duplicates")
        
        # Group files by size
        files_by_size = {}
        total_size = 0
        
        for file in tqdm(files, desc="Scanning files", unit="files"):
            size = int(file.get('size', 0))
            if size > 0:  # Skip files with no size
                total_size += size
                files_by_size.setdefault(size, []).append(file['id'])
                
        self.logger.info(f"Found {len(files_by_size)} unique file sizes")
        
        return {
            'files_by_size': files_by_size,
            'total_files': total_files,
            'total_size': total_size
        }
        
    def _list_all_files(self) -> List[Dict[str, Any]]:
        """List all files in Google Drive.
        
        Returns:
            List of file metadata dictionaries
        """
        files = []
        page_token = None
        
        while True:
            try:
                # Get files in batches
                response = self.drive_service.files().list(
                    pageSize=BATCH_SIZE,
                    fields="nextPageToken, files(id, name, size, md5Checksum)",
                    pageToken=page_token
                ).execute()
                
                files.extend(response.get('files', []))
                page_token = response.get('nextPageToken')
                
                if not page_token:
                    break
                    
            except Exception as e:
                self.logger.error(f"Error listing files: {e}")
                break
                
        return files
        
    def find_duplicates(self, files_by_size: Dict[int, List[str]]) -> List[Dict[str, Any]]:
        """Find duplicate files based on size and MD5 checksum.
        
        Args:
            files_by_size: Dictionary mapping file sizes to lists of file IDs
            
        Returns:
            List of duplicate groups, where each group is a dictionary containing:
            - size: File size in bytes
            - files: List of file metadata dictionaries
        """
        duplicate_groups = []
        
        # Process each size group
        for size, file_ids in tqdm(files_by_size.items(), desc="Checking duplicates", unit="size"):
            if len(file_ids) < 2:
                continue  # Skip if only one file of this size
                
            # Get MD5 checksums for all files of this size
            checksums = {}
            for file_id in file_ids:
                try:
                    file = self.drive_service.files().get(
                        fileId=file_id,
                        fields="id, name, size, md5Checksum, parents"
                    ).execute()
                    
                    if 'md5Checksum' in file:
                        checksums.setdefault(file['md5Checksum'], []).append(file)
                except Exception as e:
                    self.logger.warning(f"Error getting file {file_id}: {e}")
                    
            # Find duplicates
            for checksum, files in checksums.items():
                if len(files) > 1:
                    duplicate_groups.append({
                        'size': size,
                        'files': files
                    })
                    
        return duplicate_groups 