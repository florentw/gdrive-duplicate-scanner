from typing import List, Dict, Set, Optional
from utils import get_human_readable_size
from src.config import logger


class BaseDuplicate:
    """Base class for duplicate file information."""

    def __init__(self, files: List[Dict], metadata: Dict[str, dict]):
        self.files = files
        self.metadata = metadata
        self._total_size = None
        self.logger = logger

    @property
    def total_size(self) -> int:
        """Calculate total size of all files in bytes."""
        if self._total_size is None:
            self._total_size = sum(int(f.get("size", 0)) for f in self.files)
        return self._total_size

    def print_info(self) -> None:
        """Print information about the duplicate files."""
        size_mb = self.total_size / (1024 * 1024)
        logger.info(
            f"Found {len(self.files)} duplicate files, total size: {size_mb:.2f} MB"
        )
        for file in self.files:
            logger.info(
                f"  - {file.get('name', 'Unknown')} ({file.get('id', 'No ID')})"
            )


class DuplicateGroup(BaseDuplicate):
    """Represents a group of duplicate files."""

    @property
    def wasted_space(self) -> int:
        """Calculate the wasted space (size * (number of duplicates - 1))."""
        return self.total_size - int(self.files[0].get("size", 0))

    def get_parent_folders(self) -> Set[str]:
        """Get all parent folder IDs for files in this group."""
        parent_ids = set()
        for file in self.files:
            file_meta = self.metadata.get(file["id"])
            if file_meta and "parents" in file_meta:
                parent_ids.update(file_meta["parents"])
        return parent_ids

    def print_info(self) -> None:
        """Print information about the duplicate group."""
        logger.info("\nDuplicate Group:")
        super().print_info()


class DuplicateFolder(BaseDuplicate):
    """Represents a folder containing duplicate files."""

    def __init__(self, folder_id: str, folder_info: Dict, duplicate_file_ids: Set[str]):
        super().__init__([], {})  # Initialize base class
        self.folder_id = folder_id
        self.folder_info = folder_info
        self.metadata = folder_info  # Set metadata to folder_info for compatibility
        self.duplicate_file_ids = duplicate_file_ids
        self._total_size = None
        self.files = []  # Will be populated later with actual files
        self.total_files = set()  # Initialize empty set for total files
        self.logger = logger

    @property
    def id(self) -> str:
        """Get the folder ID for backward compatibility."""
        return self.folder_id

    @property
    def duplicate_files(self) -> Set[str]:
        """Get duplicate files for backward compatibility."""
        return self.duplicate_file_ids

    @property
    def size(self) -> int:
        """Get the folder size from metadata."""
        return int(self.folder_info.get("size", 0))

    def update_metadata(self, metadata: Dict[str, dict]) -> None:
        """Update folder metadata."""
        if self.folder_id in metadata:
            self.folder_info = metadata[self.folder_id]
            self.metadata = self.folder_info  # Keep metadata in sync

    def check_if_duplicate_only(self) -> bool:
        """Check if the folder contains only duplicate files."""
        return len(self.duplicate_file_ids) == len(self.total_files)

    @property
    def total_size(self) -> int:
        """Calculate total size of all duplicate files in the folder."""
        if self._total_size is None:
            self._total_size = sum(
                int(f.get("size", 0))
                for f in self.files
                if f.get("id") in self.duplicate_file_ids
            )
        return self._total_size

    def print_info(self) -> None:
        """Print information about the folder and its duplicate files."""
        logger.info(
            f"\nFolder: {self.folder_info.get('name', 'Unknown')} ({self.folder_id})"
        )
        size_mb = self.total_size / (1024 * 1024)
        logger.info(
            f"Contains {len(self.duplicate_file_ids)} duplicate files, total size: {size_mb:.2f} MB"
        )
        for file in self.files:
            if file.get("id") in self.duplicate_file_ids:
                logger.info(
                    f"  - {file.get('name', 'Unknown')} ({file.get('id', 'No ID')})"
                )
