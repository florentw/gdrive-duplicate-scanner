import argparse
import logging
from auth import get_service
from drive_api import DriveAPI
from scanner import DuplicateScanner
from export import write_to_csv
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

    # Initialize API and scanner
    drive_api = DriveAPI(service)
    scanner = DuplicateScanner(drive_api)

    # Scan for duplicates
    duplicate_groups = scanner.scan(delete=args.delete, force_refresh=args.refresh_cache)

    # Print summary
    total_duplicates = sum(len(group.files) - 1 for group in duplicate_groups)
    total_wasted = sum(group.wasted_space for group in duplicate_groups)
    
    print(f"\nFound {len(duplicate_groups)} duplicate groups")
    print(f"Total duplicate files: {total_duplicates}")
    print(f"Total wasted space: {get_human_readable_size(total_wasted)}")

    # Export to CSV
    if duplicate_groups:
        write_to_csv(duplicate_groups, drive_api)
        print(f"\nExported duplicate information to CSV file")

if __name__ == '__main__':
    main()
