import os
import sys
import logging
from src.auth import get_service
from src.drive_api import DriveAPI
from src.cache import MetadataCache
from src.scanner import DuplicateScanner
from src.export import write_to_csv
from src.config import setup_logging

def main():
    """Main function to run the duplicate file scanner."""
    # Setup logging
    setup_logging()
    logger = logging.getLogger('drive_scanner')
    
    try:
        # Get Google Drive service
        service = get_service()
        if not service:
            logger.error("Failed to initialize Google Drive service")
            return
        
        # Initialize cache and API
        cache = MetadataCache()
        drive_api = DriveAPI(service, cache)
        
        # Create scanner and find duplicates
        logger.info("Starting duplicate file scan")
        scanner = DuplicateScanner(drive_api, cache)
        scanner.scan()
        
        if not scanner.duplicate_groups:
            logger.info("No duplicate files found")
            return
        
        # Export results to CSV
        logger.info(f"Found {len(scanner.duplicate_groups)} groups of duplicate files")
        filename = write_to_csv(scanner.duplicate_groups, drive_api)
        if filename:
            logger.info(f"Results exported to {filename}")
        else:
            logger.error("Failed to export results to CSV")
            
    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
        return 1
    
    return 0

if __name__ == '__main__':
    sys.exit(main()) 