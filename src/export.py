import csv
from datetime import datetime
from typing import List, Dict
from drive_api import DriveAPI
from config import CSV_HEADERS
from utils import get_human_readable_size
import logging
from models import DuplicateGroup

def generate_csv_filename() -> str:
    """Generate a unique CSV filename with timestamp."""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    return f'duplicate_files_{timestamp}.csv'

def write_to_csv(duplicate_groups: List[DuplicateGroup], drive_api: DriveAPI) -> str:
    """Write duplicate file information to a CSV file."""
    filename = generate_csv_filename()
    logging.info(f"Starting CSV export to {filename} with {len(duplicate_groups)} groups")
    
    try:
        with open(filename, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=CSV_HEADERS)
            writer.writeheader()
            
            rows_written = 0
            for group_id, group in enumerate(duplicate_groups, 1):
                logging.debug(f"Processing group {group_id} with {len(group.files)} files")
                
                for i, file in enumerate(group.files):
                    file_meta = group.metadata.get(file['id'])
                    if not file_meta:
                        logging.warning(f"Missing metadata for file {file['id']} in group {group_id}")
                        continue
                        
                    # Get parent folder information
                    parent_id = file_meta.get('parents', [''])[0]
                    parent_meta = drive_api.get_file_metadata(parent_id) if parent_id else {}
                    if not parent_meta and parent_id:
                        logging.warning(f"Could not get metadata for parent folder {parent_id}")
                    
                    # For each file, write a row for every other file in the group
                    for j, other_file in enumerate(group.files):
                        if i == j:
                            continue
                            
                        other_meta = group.metadata.get(other_file['id'])
                        if not other_meta:
                            logging.warning(f"Missing metadata for duplicate file {other_file['id']} in group {group_id}")
                            continue
                            
                        other_parent_id = other_meta.get('parents', [''])[0]
                        other_parent_meta = drive_api.get_file_metadata(other_parent_id) if other_parent_id else {}
                        if not other_parent_meta and other_parent_id:
                            logging.warning(f"Could not get metadata for duplicate parent folder {other_parent_id}")
                        
                        row = {
                            'File Name': file_meta.get('name', ''),
                            'Full Path': f"{parent_meta.get('name', '')}/{file_meta.get('name', '')}",
                            'Size (Bytes)': file_meta.get('size', 0),
                            'Size (Human Readable)': get_human_readable_size(file_meta.get('size', 0)),
                            'File ID': file_meta.get('id', ''),
                            'MD5 Checksum': file_meta.get('md5Checksum', ''),
                            'Duplicate Group ID': group_id,
                            'Parent Folder': parent_meta.get('name', ''),
                            'Parent Folder ID': parent_id,
                            'Duplicate File Name': other_meta.get('name', ''),
                            'Duplicate File Path': f"{other_parent_meta.get('name', '')}/{other_meta.get('name', '')}",
                            'Duplicate File Size': other_meta.get('size', 0),
                            'Duplicate File ID': other_meta.get('id', '')
                        }
                        
                        writer.writerow(row)
                        rows_written += 1
                        
            logging.info(f"CSV export completed. Wrote {rows_written} rows to {filename}")
            return filename
    except IOError as e:
        logging.error(f"Error writing to CSV file: {e}")
        return None 