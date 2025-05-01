import csv
from datetime import datetime
from typing import List, Dict, Optional, Set
from drive_api import DriveAPI
from config import CSV_HEADERS, logger
from utils import get_human_readable_size
from models import DuplicateGroup
from tqdm import tqdm


def generate_csv_filename() -> str:
    """Generate a unique CSV filename with timestamp."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"duplicate_files_{timestamp}.csv"


def get_parent_metadata(
    files: List[Dict], group: DuplicateGroup, drive_api: DriveAPI
) -> Dict[str, Dict]:
    """Get metadata for all parent folders in a batch.

    Args:
        files: List of file dictionaries
        group: DuplicateGroup containing file metadata
        drive_api: DriveAPI instance for fetching folder metadata

    Returns:
        Dict mapping folder IDs to their metadata
    """
    parent_ids = set()
    for file in files:
        file_meta = group.metadata.get(file["id"])
        if file_meta and "parents" in file_meta:
            parent_ids.update(file_meta["parents"])

    if not parent_ids:
        return {}

    return drive_api.get_files_metadata_batch(list(parent_ids))


def get_duplicate_info(
    file: Dict, group: DuplicateGroup, parent_metadata: Dict[str, Dict]
) -> List[Dict]:
    """Create a list of duplicate file information.

    Args:
        file: Current file dictionary
        group: DuplicateGroup containing all duplicates
        parent_metadata: Dict mapping folder IDs to their metadata

    Returns:
        List of dictionaries containing duplicate file information
    """
    duplicates = []
    for other_file in group.files:
        if other_file["id"] == file["id"]:
            continue

        other_meta = group.metadata.get(other_file["id"])
        if not other_meta:
            logger.warning(f"Missing metadata for duplicate file {other_file['id']}")
            continue

        other_parent_id = other_meta.get("parents", [""])[0]
        other_parent_meta = parent_metadata.get(other_parent_id, {})

        duplicates.append(
            {
                "name": other_meta.get("name", ""),
                "path": f"{other_parent_meta.get('name', '')}/{other_meta.get('name', '')}",
                "size": other_meta.get("size", 0),
                "id": other_meta.get("id", ""),
            }
        )

    return duplicates


def create_csv_row(
    file: Dict,
    file_meta: Dict,
    parent_meta: Dict,
    duplicates: List[Dict],
    group_id: int,
) -> Dict[str, str]:
    """Create a CSV row for a file and its duplicates.

    Args:
        file: Current file dictionary
        file_meta: Metadata for the current file
        parent_meta: Metadata for the parent folder
        duplicates: List of duplicate file information
        group_id: ID of the duplicate group

    Returns:
        Dictionary containing CSV row data
    """
    return {
        "File Name": file_meta.get("name", ""),
        "Full Path": f"{parent_meta.get('name', '')}/{file_meta.get('name', '')}",
        "Size (Bytes)": file_meta.get("size", 0),
        "Size (Human Readable)": get_human_readable_size(int(file_meta.get("size", 0))),
        "File ID": file_meta.get("id", ""),
        "MD5 Checksum": file_meta.get("md5Checksum", ""),
        "Duplicate Group ID": group_id,
        "Parent Folder": parent_meta.get("name", ""),
        "Parent Folder ID": file_meta.get("parents", [""])[0],
        "Duplicate File Name": "; ".join(d["name"] for d in duplicates),
        "Duplicate File Path": "; ".join(d["path"] for d in duplicates),
        "Duplicate File Size": "; ".join(str(d["size"]) for d in duplicates),
        "Duplicate File ID": "; ".join(d["id"] for d in duplicates),
    }


def write_to_csv(
    duplicate_groups: List[DuplicateGroup], drive_api: DriveAPI
) -> Optional[str]:
    """Write duplicate file information to a CSV file.

    Args:
        duplicate_groups: List of DuplicateGroup objects containing duplicate files
        drive_api: DriveAPI instance for fetching additional metadata

    Returns:
        str: Path to the generated CSV file, or None if an error occurred
    """
    filename = generate_csv_filename()
    logger.info(f"Starting CSV export to {filename}")

    try:
        with open(filename, "w", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=CSV_HEADERS)
            writer.writeheader()
            rows_written = 0

            # Calculate total files for progress bar
            total_files = sum(len(group.files) for group in duplicate_groups)

            # Process each group
            with tqdm(
                total=total_files, desc="Exporting duplicates", unit="files"
            ) as pbar:
                for group_id, group in enumerate(duplicate_groups, 1):
                    # Get all parent folder metadata in one batch
                    parent_metadata = get_parent_metadata(group.files, group, drive_api)

                    # Process each file in the group
                    for file in group.files:
                        file_meta = group.metadata.get(file["id"])
                        if not file_meta:
                            logger.warning(f"Missing metadata for file {file['id']}")
                            pbar.update(1)
                            continue

                        # Get parent folder metadata
                        parent_id = file_meta.get("parents", [""])[0]
                        parent_meta = parent_metadata.get(parent_id, {})

                        # Get duplicate information
                        duplicates = get_duplicate_info(file, group, parent_metadata)

                        # Create and write row
                        row = create_csv_row(
                            file, file_meta, parent_meta, duplicates, group_id
                        )
                        writer.writerow(row)
                        rows_written += 1
                        pbar.update(1)

            logger.info(
                f"CSV export completed. Wrote {rows_written} rows to {filename}"
            )
            return filename

    except IOError as e:
        logger.error(f"Error writing to CSV file: {e}")
        return None
