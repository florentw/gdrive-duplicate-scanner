from typing import List, Dict, Set, Optional
from utils import get_human_readable_size
from config import logger

class BaseDuplicate:
    """Base class for duplicate items."""
    
    def __init__(self, id: str, metadata: Dict):
        self.id = id
        self.metadata = metadata
        self.name = metadata.get('name', 'Unknown')
        self.size = int(metadata.get('size', 0))
        self.mime_type = metadata.get('mimeType', '')
        self.created_time = metadata.get('createdTime', '')
        self.modified_time = metadata.get('modifiedTime', '')
        self.owners = metadata.get('owners', [])
        self.parents = metadata.get('parents', [])

    @property
    def total_size(self) -> int:
        """Get the total size of the duplicate item."""
        return self.size

    def print_info(self) -> None:
        """Print information about the duplicate item."""
        logger.info(f"Name: {self.name}")
        logger.info(f"Size: {get_human_readable_size(self.size)}")
        logger.info(f"Type: {self.mime_type}")
        logger.info(f"Created: {self.created_time}")
        logger.info(f"Modified: {self.modified_time}")
        logger.info(f"Owners: {[owner.get('emailAddress', 'Unknown') for owner in self.owners]}")
        logger.info(f"Parents: {self.parents}")
        logger.info("---")

class DuplicateGroup(BaseDuplicate):
    """Represents a group of duplicate files."""
    
    def __init__(self, files: List[Dict], metadata: Dict[str, dict]):
        super().__init__(files[0]['id'], metadata.get(files[0]['id'], {}))
        self.files = files
        self.metadata = metadata

    @property
    def total_size(self) -> int:
        """Get the total size of all duplicate files."""
        return sum(int(file.get('size', 0)) for file in self.files)

    @property
    def wasted_space(self) -> int:
        """Calculate the wasted space (size * (number of duplicates - 1))."""
        return self.total_size - int(self.files[0].get('size', 0))

    def get_parent_folders(self) -> Set[str]:
        """Get the set of parent folder IDs for all files."""
        folders = set()
        for file in self.files:
            if 'parents' in file:
                folders.update(file['parents'])
        return folders

    def print_info(self) -> None:
        """Print information about the duplicate group."""
        logger.info(f"\nFound {len(self.files)} duplicate files:")
        logger.info(f"Total size: {get_human_readable_size(self.total_size)}")
        logger.info(f"Wasted space: {get_human_readable_size(self.wasted_space)}")
        for file in self.files:
            file_meta = self.metadata.get(file['id'], {})
            logger.info(f"\nFile: {file_meta.get('name', 'Unknown')}")
            logger.info(f"Size: {get_human_readable_size(int(file.get('size', 0)))}")
            logger.info(f"Type: {file_meta.get('mimeType', '')}")
            logger.info(f"Created: {file_meta.get('createdTime', '')}")
            logger.info(f"Modified: {file_meta.get('modifiedTime', '')}")
            logger.info(f"Owners: {[owner.get('emailAddress', 'Unknown') for owner in file_meta.get('owners', [])]}")
            logger.info(f"Parents: {file_meta.get('parents', [])}")
        logger.info("---")

class DuplicateFolder(BaseDuplicate):
    """Represents a folder containing duplicate files."""
    
    def __init__(self, folder_id: str, metadata: Dict, duplicate_files: Set[str]):
        super().__init__(folder_id, metadata)
        self.duplicate_files = duplicate_files
        self.total_files = set()
        self.parent_folders = set()

    def update_metadata(self, metadata: Dict[str, dict]) -> None:
        """Update folder metadata."""
        self.metadata = metadata.get(self.id, {})
        self.name = self.metadata.get('name', 'Unknown')
        self.size = int(self.metadata.get('size', 0))
        self.mime_type = self.metadata.get('mimeType', '')
        self.created_time = self.metadata.get('createdTime', '')
        self.modified_time = self.metadata.get('modifiedTime', '')
        self.owners = self.metadata.get('owners', [])
        self.parents = self.metadata.get('parents', [])

    def get_parent_folders(self, folders: Dict[str, dict]) -> None:
        """Get all parent folders recursively."""
        for parent_id in self.parents:
            if parent_id in folders:
                self.parent_folders.add(parent_id)
                parent = DuplicateFolder(parent_id, folders[parent_id], set())
                parent.get_parent_folders(folders)
                self.parent_folders.update(parent.parent_folders)

    def check_if_duplicate_only(self) -> bool:
        """Check if the folder contains only duplicate files."""
        return len(self.duplicate_files) == len(self.total_files)

    def print_info(self) -> None:
        """Print information about the duplicate folder."""
        logger.info(f"\nFolder: {self.name}")
        logger.info(f"ID: {self.id}")
        logger.info(f"Size: {get_human_readable_size(self.size)}")
        logger.info(f"Created: {self.created_time}")
        logger.info(f"Modified: {self.modified_time}")
        logger.info(f"Owners: {[owner.get('emailAddress', 'Unknown') for owner in self.owners]}")
        logger.info(f"Parents: {self.parents}")
        logger.info(f"Parent folders: {self.parent_folders}")
        logger.info(f"Duplicate files: {len(self.duplicate_files)}")
        logger.info(f"Total files: {len(self.total_files)}")
        logger.info(f"Contains only duplicates: {self.check_if_duplicate_only()}")
        logger.info("---") 