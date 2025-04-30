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
    
    try:
        with open(filename, 'w', newline='') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=CSV_HEADERS)
            writer.writeheader()
            
            for group_id, group in enumerate(duplicate_groups, 1):
                for i, file in enumerate(group.files):
                    file_meta = group.metadata.get(file['id'])
                    if not file_meta:
                        continue
                        
                    # Get parent folder information
                    parent_id = file_meta.get('parents', [''])[0]
                    parent_meta = drive_api.get_file_metadata(parent_id) if parent_id else {}
                    
                    # For each file, write a row for every other file in the group
                    for j, other_file in enumerate(group.files):
                        if i == j:
                            continue
                            
                        other_meta = group.metadata.get(other_file['id'])
                        if not other_meta:
                            continue
                            
                        other_parent_id = other_meta.get('parents', [''])[0]
                        other_parent_meta = drive_api.get_file_metadata(other_parent_id) if other_parent_id else {}
                        
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
                        
        return filename
    except IOError as e:
        logging.error(f"Error writing to CSV file: {e}")
        return None 