import argparse
import logging
from auth import get_service
from drive_api import DriveAPI
from scanner import DuplicateScannerWithFolders
from export import write_to_csv
from cache import MetadataCache
from utils import get_human_readable_size

def main():
    """Main entry point for the duplicate file scanner."""
    parser = argparse.ArgumentParser(description='Scan Google Drive for duplicate files.')
    parser.add_argument('--delete', action='store_true', help='Move duplicate files to trash')
    parser.add_argument('--refresh-cache', action='store_true', help='Force refresh the cache')
    args = parser.parse_args()

    # Get Google Drive service
    service = get_service()
    if not service:
        logging.error("Failed to get Google Drive service")
        return

    # Initialize API, cache, and scanner
    drive_api = DriveAPI(service)
    cache = MetadataCache()
    scanner = DuplicateScannerWithFolders(drive_api, cache)

    # Scan for duplicates
    scanner.scan()  # Use the scan() method which handles caching internally

    # Print duplicate files summary
    total_duplicates = sum(len(group.files) - 1 for group in scanner.duplicate_groups)
    total_wasted = sum(group.wasted_space for group in scanner.duplicate_groups)
    
    print("\n=== Duplicate Files Summary ===")
    print(f"Found {len(scanner.duplicate_groups)} duplicate groups")
    print(f"Total duplicate files: {total_duplicates}")
    print(f"Total wasted space: {get_human_readable_size(total_wasted)}")

    # Print folder analysis
    print("\n=== Folder Analysis ===")
    print(f"Found {len(scanner.duplicate_files_in_folders)} folders containing duplicate files")
    print(f"Found {len(scanner.duplicate_only_folders)} folders containing only duplicate files")

    # Print details of duplicate groups
    if scanner.duplicate_groups:
        print("\n=== Duplicate Groups ===")
        for i, group in enumerate(scanner.duplicate_groups, 1):
            files = group.files
            size = get_human_readable_size(int(files[0].get('size', 0)))
            print(f"\nGroup {i} ({size} per file):")
            for file in files:
                print(f"- {file.get('name', 'Unknown')}")

    # Print details of folders containing only duplicates
    if scanner.duplicate_only_folders:
        print("\n=== Folders With Only Duplicates ===")
        for folder_id, folder in scanner.duplicate_only_folders.items():
            folder_name = folder.folder_info.get('name', 'Unknown Folder')
            num_files = len(folder.duplicate_file_ids)
            total_size = get_human_readable_size(folder.total_size)
            print(f"\n{folder_name}:")
            print(f"- Contains {num_files} duplicate files")
            print(f"- Total size: {total_size}")

    # Export to CSV
    if scanner.duplicate_groups:
        write_to_csv(scanner.duplicate_groups, drive_api)
        print(f"\nExported duplicate information to CSV file")

if __name__ == '__main__':
    main() 