from typing import List, Dict, Set, Optional
from collections import defaultdict
from drive_api import DriveAPI
from models import DuplicateGroup, DuplicateFolder
from utils import get_human_readable_size
from cache import MetadataCache
from config import logger
from tqdm import tqdm

class BaseDuplicateScanner:
    """Base class for scanning Google Drive for duplicate files."""
    
    def __init__(self, drive_api: DriveAPI, cache: MetadataCache):
        self.drive_api = drive_api
        self.cache = cache
        self.duplicate_groups: List[DuplicateGroup] = []
        self.duplicate_files_in_folders: Dict[str, DuplicateFolder] = {}
        self.logger = logger

    def _filter_valid_files(self, files: List[Dict]) -> List[Dict]:
        """Filter out files that are not valid for duplicate detection."""
        return [
            file for file in files
            if file.get('size', '0') != '0'  # Skip empty files
            and not file.get('mimeType', '').startswith('application/vnd.google-apps.')  # Skip Google Workspace files
        ]

    def _group_files_by_size(self, files: List[Dict]) -> Dict[str, List[Dict]]:
        """Group files by their size.
        
        This is an optimization step that reduces the number of MD5 hash comparisons needed.
        Instead of comparing every file's MD5 with every other file (O(n²) comparisons),
        we first group files by size. Since files of different sizes cannot be duplicates,
        we only need to compare MD5 hashes within each size group.
        
        For example, if we have 10,000 files distributed across 1,000 different sizes:
        - Without size grouping: Up to 50 million MD5 comparisons (n * (n-1) / 2)
        - With size grouping: If files are evenly distributed, each size group has ~10 files,
          leading to only ~45,000 comparisons (1000 groups * (10 * 9 / 2) comparisons per group)
        
        Note: MD5 hashes are pre-computed by Google Drive, so we're optimizing the number of
        string comparisons, not hash computations.
        """
        size_groups: Dict[str, List[Dict]] = {}
        for file in files:
            size = file.get('size', '0')
            if size not in size_groups:
                size_groups[size] = []
            size_groups[size].append(file)
        return size_groups

    def _group_files_by_md5(self, files: List[Dict]) -> Dict[str, List[Dict]]:
        """Group files by their MD5 hash.
        
        This is called after size-based grouping to identify actual duplicates.
        Files with the same MD5 hash within a size group are duplicates.
        The MD5 hashes are pre-computed by Google Drive and provided in the file metadata.
        """
        md5_groups: Dict[str, List[Dict]] = {}
        for file in files:
            md5 = file.get('md5Checksum', '')
            if md5:
                if md5 not in md5_groups:
                    md5_groups[md5] = []
                md5_groups[md5].append(file)
        return md5_groups

    def _process_duplicate_group(self, files: List[Dict], metadata: Dict[str, dict]) -> None:
        """Process a group of duplicate files."""
        if len(files) > 1:
            group = DuplicateGroup(files, metadata)
            self.duplicate_groups.append(group)

    def _scan_for_duplicates(self, files: List[Dict]) -> None:
        """Common scanning logic for finding duplicate files.
        
        The duplicate detection process is optimized using a two-step approach:
        1. Group files by size (O(n) operation) - files of different sizes cannot be duplicates
        2. Within each size group, group files by MD5 hash to find actual duplicates
        
        This significantly reduces the number of comparisons needed compared to
        comparing every file with every other file.
        """
        # Filter valid files
        valid_files = self._filter_valid_files(files)
        self.logger.info(f"Found {len(valid_files)} valid files to check for duplicates")
        
        # Group by size first - optimization to reduce number of MD5 comparisons needed
        size_groups = self._group_files_by_size(valid_files)
        self.logger.info(f"Found {len(size_groups)} unique file sizes")
        
        # For each size group, check MD5 hashes
        with tqdm(total=len(size_groups), desc="Scanning for duplicates", unit="size group") as pbar:
            for size, files in size_groups.items():
                if len(files) > 1:  # Only check if there are multiple files of the same size
                    md5_groups = self._group_files_by_md5(files)
                    for md5, duplicate_files in md5_groups.items():
                        if len(duplicate_files) > 1:  # Only process if there are actual duplicates
                            # Create metadata dictionary for the group
                            metadata = {file['id']: file for file in duplicate_files}
                            self._process_duplicate_group(duplicate_files, metadata)
                pbar.update(1)

    def scan(self) -> None:
        """Scan for duplicate files."""
        raise NotImplementedError("Subclasses must implement scan()")

class DuplicateScanner(BaseDuplicateScanner):
    """Scanner for finding duplicate files in Google Drive."""
    
    def scan(self, force_refresh: bool = False) -> None:
        """Scan for duplicate files.
        
        Args:
            force_refresh: If True, clear the cache and fetch fresh data from the API.
        """
        self.logger.info("Starting duplicate file scan...")
        
        # Get all files from cache or API
        files = None
        if force_refresh:
            self.cache.clear()
            files = self.drive_api.list_files(force_refresh=True)
            self.cache.cache_files(files)
        else:
            files = self.cache.get_all_files()
            if not files:
                files = self.drive_api.list_files()
                self.cache.cache_files(files)
        
        # Use common scanning logic
        self._scan_for_duplicates(files)
        
        # Calculate and log summary statistics
        total_duplicates = sum(len(group.files) for group in self.duplicate_groups)
        total_wasted = sum(group.wasted_space for group in self.duplicate_groups)
        wasted_gb = total_wasted / (1024 * 1024 * 1024)
        
        print(f"\nFound {len(self.duplicate_groups)} duplicate groups")
        print(f"Total duplicate files: {total_duplicates}")
        print(f"Total wasted space: {wasted_gb:.2f} GB")

class DuplicateScannerWithFolders(BaseDuplicateScanner):
    """Scanner for finding duplicate files and analyzing folder structures."""
    
    def __init__(self, drive_api: DriveAPI, cache: MetadataCache):
        super().__init__(drive_api, cache)
        self.duplicate_only_folders: Dict[str, DuplicateFolder] = {}
    
    def scan(self) -> None:
        """Scan for duplicate files and analyze folder structures."""
        self.logger.info("Starting duplicate file scan with folder analysis...")
        
        # Get all files and folders from cache or API
        files = self.cache.get_all_files()
        folders = self.cache.get_all_folders()
        
        if not files or not folders:
            files, folders = self.drive_api.list_all_files_and_folders()
            self.cache.cache_files(files)
            self.cache.cache_folders(folders)
        
        # Use common scanning logic
        self._scan_for_duplicates(files)
        
        # Analyze folder structures
        self._analyze_folder_structures(folders)
        
        # Calculate and log summary statistics
        total_duplicates = sum(len(group.files) for group in self.duplicate_groups)
        total_wasted = sum(group.wasted_space for group in self.duplicate_groups)
        wasted_gb = total_wasted / (1024 * 1024 * 1024)
        
        print(f"\nFound {len(self.duplicate_groups)} duplicate groups")
        print(f"Total duplicate files: {total_duplicates}")
        print(f"Total wasted space: {wasted_gb:.2f} GB")
        print(f"Found {len(self.duplicate_files_in_folders)} folders with duplicate files")
        print(f"Found {len(self.duplicate_only_folders)} folders containing only duplicate files\n")

    def _analyze_folder_structures(self, folders: List[Dict]) -> None:
        """Analyze folder structures to identify folders containing duplicate files."""
        # Create a mapping of folder IDs to their files
        folder_files: Dict[str, Set[str]] = {}
        folder_total_files: Dict[str, Set[str]] = {}
        
        # First, collect all files in each folder
        for file in self.drive_api.list_files():
            if 'parents' in file:
                for parent_id in file['parents']:
                    if parent_id not in folder_total_files:
                        folder_total_files[parent_id] = set()
                    folder_total_files[parent_id].add(file['id'])
        
        # Then, collect duplicate files in each folder
        for group in self.duplicate_groups:
            for file in group.files:
                if 'parents' in file:
                    for parent_id in file['parents']:
                        if parent_id not in folder_files:
                            folder_files[parent_id] = set()
                        folder_files[parent_id].add(file['id'])
        
        # Analyze each folder
        with tqdm(total=len(folders), desc="Analyzing folders", unit="folder") as pbar:
            for folder in folders:
                folder_id = folder['id']
                if folder_id in folder_files:
                    duplicate_files = folder_files[folder_id]
                    if duplicate_files:
                        folder_obj = DuplicateFolder(
                            folder_id,
                            folder,
                            duplicate_files
                        )
                        folder_obj.total_files = folder_total_files.get(folder_id, set())
                        self.duplicate_files_in_folders[folder_id] = folder_obj
                        
                        # Check if folder contains only duplicates
                        if folder_obj.check_if_duplicate_only():
                            self.duplicate_only_folders[folder_id] = folder_obj
                pbar.update(1) 