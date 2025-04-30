from typing import List, Dict, Set
from utils import get_human_readable_size

class DuplicateGroup:
    """Represents a group of duplicate files."""
    
    def __init__(self, files: List[Dict], metadata: Dict[str, dict]):
        self.files = files
        self.metadata = metadata

    @property
    def total_size(self) -> int:
        """Get the total size of all files in the group."""
        return sum(int(file.get('size', 0)) for file in self.files)

    @property
    def wasted_space(self) -> int:
        """Get the wasted space (total size minus size of one file)."""
        if not self.files:
            return 0
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
        print(f"\nDuplicate Group (Total Size: {get_human_readable_size(self.total_size)})")
        print(f"Wasted Space: {get_human_readable_size(self.wasted_space)}")
        for file in self.files:
            print(f"  - {file.get('name', 'Unknown')} ({get_human_readable_size(file.get('size', 0))})")

class DuplicateFolder:
    """Represents a folder containing duplicate files."""
    
    def __init__(self, folder_id: str, folder_meta: dict, duplicate_files: Set[str]):
        self.folder_id = folder_id
        self.folder_meta = folder_meta
        self.duplicate_files = duplicate_files
        self.total_size = 0
        self.update_metadata({})

    @property
    def total_size(self) -> int:
        """Get the total size of duplicate files in the folder."""
        return self._total_size

    @total_size.setter
    def total_size(self, value: int):
        self._total_size = value

    def update_metadata(self, file_metadata: Dict[str, dict]) -> None:
        """Update folder metadata with file sizes."""
        self.total_size = sum(
            int(file_metadata.get(file_id, {}).get('size', 0))
            for file_id in self.duplicate_files
        )

    def check_if_duplicate_only(self, all_folder_files: Set[str]) -> bool:
        """Check if the folder only contains duplicate files."""
        return self.duplicate_files == all_folder_files

    def print_info(self) -> None:
        """Print information about the duplicate folder."""
        print(f"\nFolder: {self.folder_meta.get('name', 'Unknown')}")
        print(f"Total Size of Duplicates: {get_human_readable_size(self.total_size)}")
        print(f"Number of Duplicate Files: {len(self.duplicate_files)}") 