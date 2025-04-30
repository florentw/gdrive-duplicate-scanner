import logging
from typing import List, Dict, Set
from collections import defaultdict
from drive_api import DriveAPI
from models import DuplicateGroup, DuplicateFolder
from utils import get_human_readable_size

class DuplicateScanner:
    """Scans Google Drive for duplicate files."""
    
    def __init__(self, drive_api: DriveAPI):
        self.drive_api = drive_api
        self.duplicate_groups = []
        self.duplicate_folders = {}
        self.folder_files = defaultdict(set)

    def scan(self, delete: bool = False, force_refresh: bool = False) -> List[DuplicateGroup]:
        """Scan for duplicates and optionally delete them."""
        files = self.drive_api.list_files(force_refresh)
        valid_files = self._filter_valid_files(files)
        
        self._find_duplicates(valid_files)
        self._process_folders()
        
        if delete:
            self._handle_deletions()
            
        return self.duplicate_groups

    def _filter_valid_files(self, files: List[Dict]) -> List[Dict]:
        """Filter out invalid files."""
        return [f for f in files if f.get('size') and f.get('md5Checksum')]

    def _find_duplicates(self, valid_files: List[Dict]) -> None:
        """Find duplicate files by size and MD5 hash."""
        # Group files by size first
        size_groups = self._group_files_by_size(valid_files)
        
        # Process each size group
        for size_files in size_groups.values():
            if len(size_files) > 1:  # Only process groups with multiple files
                self._process_size_group(size_files)

    def _group_files_by_size(self, files: List[Dict]) -> Dict[str, List[Dict]]:
        """Group files by their size."""
        size_groups = defaultdict(list)
        for file in files:
            size_groups[file['size']].append(file)
        return size_groups

    def _process_size_group(self, size_files: List[Dict]) -> None:
        """Process a group of files with the same size."""
        # Group by MD5 hash
        md5_groups = self._group_files_by_md5(size_files)
        
        # Process each MD5 group
        for files in md5_groups.values():
            if len(files) > 1:  # Only process groups with multiple files
                self._process_duplicate_group(files)

    def _group_files_by_md5(self, files: List[Dict]) -> Dict[str, List[Dict]]:
        """Group files by their MD5 hash."""
        md5_groups = defaultdict(list)
        for file in files:
            md5_groups[file['md5Checksum']].append(file)
        return md5_groups

    def _process_duplicate_group(self, files: List[Dict]) -> None:
        """Process a group of duplicate files."""
        # Get metadata for all files
        file_ids = [f['id'] for f in files]
        metadata = self.drive_api.get_files_metadata_batch(file_ids)
        
        # Create duplicate group
        group = DuplicateGroup(files, metadata)
        self.duplicate_groups.append(group)
        
        # Update folder tracking
        self._update_folder_tracking(group)

    def _update_folder_tracking(self, group: DuplicateGroup) -> None:
        """Update folder tracking with duplicate files."""
        for file in group.files:
            if 'parents' in file:
                for parent_id in file['parents']:
                    # Add to folder's duplicate files
                    if parent_id not in self.duplicate_folders:
                        self.duplicate_folders[parent_id] = set()
                    self.duplicate_folders[parent_id].add(file['id'])
                    
                    # Add to folder's total files
                    self.folder_files[parent_id].add(file['id'])

    def _process_folders(self) -> None:
        """Process folders containing duplicates."""
        # Get metadata for all folders
        folder_ids = list(self.duplicate_folders.keys())
        folder_metadata = self.drive_api.get_files_metadata_batch(folder_ids)
        
        # Create DuplicateFolder objects
        for folder_id, duplicate_files in self.duplicate_folders.items():
            folder_meta = folder_metadata.get(folder_id, {})
            folder = DuplicateFolder(folder_id, folder_meta, duplicate_files)
            folder.update_metadata(folder_metadata)
            self.duplicate_folders[folder_id] = folder

    def _handle_deletions(self) -> None:
        """Handle deletion of duplicate files."""
        for group in self.duplicate_groups:
            self._handle_group_deletion(group)

    def _handle_group_deletion(self, group: DuplicateGroup) -> None:
        """Handle deletion of files in a duplicate group."""
        if not group.files:
            return

        # Get user input for files with different names
        files_by_name = defaultdict(list)
        for file in group.files:
            files_by_name[file['name']].append(file)

        for name, files in files_by_name.items():
            if len(files) > 1:
                print(f"\nFound {len(files)} files named '{name}':")
                for i, file in enumerate(files, 1):
                    print(f"{i}. {file['name']} ({get_human_readable_size(file['size'])})")
                
                while True:
                    try:
                        choice = int(input("Which file would you like to keep? (enter number): "))
                        if 1 <= choice <= len(files):
                            break
                        print("Invalid choice. Please try again.")
                    except ValueError:
                        print("Please enter a number.")
                
                # Move all other files to trash
                files_to_trash = [f for i, f in enumerate(files, 1) if i != choice]
                file_ids = [f['id'] for f in files_to_trash]
                self.drive_api.move_files_to_trash_batch(file_ids)
            else:
                # If all files have the same name, keep the first one
                file_ids = [f['id'] for f in files[1:]]
                self.drive_api.move_files_to_trash_batch(file_ids) 