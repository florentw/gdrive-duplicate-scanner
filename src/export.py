import csv
from datetime import datetime
from typing import List, Dict
from drive_api import DriveAPI
from config import CSV_HEADERS
from utils import get_human_readable_size
import logging
from models import DuplicateGroup
from tqdm import tqdm

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
            total_files = sum(len(group.files) for group in duplicate_groups)
            
            # Create progress bar
            with tqdm(total=total_files, desc="Exporting duplicates", unit="files") as pbar:
                for group_id, group in enumerate(duplicate_groups, 1):
                    logging.debug(f"Processing group {group_id} with {len(group.files)} files")
                    
                    # Get all parent folder metadata at once to reduce API calls
                    parent_ids = set()
                    for file in group.files:
                        file_meta = group.metadata.get(file['id'])
                        if file_meta and 'parents' in file_meta:
                            parent_ids.update(file_meta['parents'])
                    
                    parent_metadata = {}
                    if parent_ids:
                        parent_metadata = drive_api.get_files_metadata_batch(list(parent_ids))
                    
                    # For each file, write a single row with all its duplicates
                    for i, file in enumerate(group.files):
                        file_meta = group.metadata.get(file['id'])
                        if not file_meta:
                            logging.warning(f"Missing metadata for file {file['id']} in group {group_id}")
                            pbar.update(1)
                            continue
                        
                        # Get parent folder information
                        parent_id = file_meta.get('parents', [''])[0]
                        parent_meta = parent_metadata.get(parent_id, {})
                        
                        # Get all duplicates for this file
                        duplicates = []
                        for other_file in group.files:
                            if other_file['id'] == file['id']:
                                continue
                                
                            other_meta = group.metadata.get(other_file['id'])
                            if not other_meta:
                                logging.warning(f"Missing metadata for duplicate file {other_file['id']} in group {group_id}")
                                continue
                                
                            other_parent_id = other_meta.get('parents', [''])[0]
                            other_parent_meta = parent_metadata.get(other_parent_id, {})
                            
                            duplicates.append({
                                'name': other_meta.get('name', ''),
                                'path': f"{other_parent_meta.get('name', '')}/{other_meta.get('name', '')}",
                                'size': other_meta.get('size', 0),
                                'id': other_meta.get('id', '')
                            })
                        
                        # Write a single row for this file and its duplicates
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
                            'Duplicate File Name': '; '.join(d['name'] for d in duplicates),
                            'Duplicate File Path': '; '.join(d['path'] for d in duplicates),
                            'Duplicate File Size': '; '.join(str(d['size']) for d in duplicates),
                            'Duplicate File ID': '; '.join(d['id'] for d in duplicates)
                        }
                        
                        writer.writerow(row)
                        rows_written += 1
                        pbar.update(1)
                        
            logging.info(f"CSV export completed. Wrote {rows_written} rows to {filename}")
            return filename
    except IOError as e:
        logging.error(f"Error writing to CSV file: {e}")
        return None 