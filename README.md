# Google Drive Duplicate Finder

This script helps you find duplicate files in your Google Drive using the Google Drive API. 
Duplicates are determined based on the MD5 hash of files, which ensures high accuracy in duplicate detection. 
By comparing file hashes, we can be confident that the file content is identical, making this approach very reliable and safe.
It uses Google Drive API v3 for accessing and modifying files.

## Features

- Fetches all non-trashed files metadata from your Google Drive.
- Identifies duplicate files by comparing their hash values (MD5 checksums).
- Optionally moves duplicate files to trash.
- If it finds files with the same content but different names, it will ask you which file you want to keep.
- Logs each step of the process to help you track the script's progress and actions.
- Efficient batch processing of API requests to minimize API calls.
- Smart caching system to improve performance and reduce API usage.
- Automatic retry mechanism for failed API requests.
- Detailed folder analysis showing which folders contain duplicates.
- Identifies folders that only contain duplicate files.
- Exports duplicate information to CSV for further analysis.
- Comprehensive test suite ensuring reliability and correctness.

### Advanced Features

#### Batch Processing
- Processes API requests in batches to stay within Google's API limits
- Automatically handles batch failures with retry mechanism
- Configurable batch size (default: 100 requests per batch)

#### Caching System
- Implements a smart caching system to reduce API calls
- Cache expires after 24 hours by default
- Saves cache every 5 minutes if modified
- Handles cache invalidation for trashed files

#### Folder Analysis
- Shows total size of duplicates in each folder
- Identifies folders that only contain duplicate files
- Sorts folders by total size of duplicates
- Provides detailed folder metadata and statistics

#### CSV Export
- Exports duplicate pairs to CSV file
- Includes detailed file metadata
- Timestamps in filename for tracking different runs
- Comprehensive file information including paths and sizes

## ⚠️ Warning
Moving files to the trash is irreversible through this script. Be careful when using the `--delete` argument. If you accidentally move a file to the trash, you can manually restore it from the trash in Google Drive.

It is recommended to always backup important files before running the script.

This script is a tool that helps in managing duplicate files on your Google Drive. While the script has been designed to be as accurate and safe as possible, the ultimate responsibility for handling and deleting files lies with the user. 
Please use this tool responsibly and at your own risk. 
The author will not be held responsible for any data loss that may occur as a result of using this script.

## Installation

1. Clone the repository:
```bash
git clone https://github.com/florentw/gdrive-duplicate-scanner.git
cd gdrive-duplicate-scanner
```

2. Install the package in development mode:
```bash
pip install -e .
```

3. Setup Google Cloud Project:
	1. Create Google Cloud project.
	2. Enable the Google Drive API.
	3. Download the client configuration as described [here](https://developers.google.com/drive/api/v3/quickstart/python).
	4. Save the configuration file as `credentials.json` in the same directory as the script.
	5. Add your Google Account as a test user.

## Usage
Run the script without any arguments to only report duplicates:
```bash
python duplicate_scanner.py
```

Run the script with the `--delete` argument to move duplicates to the trash:
```bash
python duplicate_scanner.py --delete
```

Run the script with the `--refresh-cache` argument to force refresh the cache:
```bash
python duplicate_scanner.py --refresh-cache
```

### Cache Behavior
The script maintains a cache of file metadata to improve performance and reduce API calls. The cache:
- Persists between runs in `drive_metadata_cache.json`
- Is automatically invalidated when Google credentials change
- Can be manually refreshed using the `--refresh-cache` argument
- Is saved every 5 minutes when modified
- Uses atomic writes to prevent corruption

### Delete Behavior
When the --delete argument is used, the behavior is as follows:

- Duplicates with the same file name: One of the duplicate files is automatically moved to the trash without user intervention.

- Duplicates with different file names: The script will prompt the user to choose which file to move to the trash.

In both cases, the operation is reported in the console and the log file.

### Output
The script provides detailed output including:
- Total number of files scanned
- Number of duplicate groups found
- Total number of duplicate files
- Total wasted space
- List of folders containing duplicates
- List of folders containing only duplicates
- CSV export of duplicate pairs

## Running Tests

The project includes a comprehensive test suite to ensure reliability and correctness. To run the tests:

1. Make sure you have the package installed in development mode:
```bash
pip install -e .
```

2. Run all tests:
```bash
pytest tests/
```

3. Run a specific test:
```bash
pytest tests/test_duplicate_scanner.py::TestDuplicateScanner::test_name
```

4. Run tests with verbose output:
```bash
pytest -v tests/
```

5. Run tests with coverage report:
```bash
# Run tests with coverage collection
python3 -m coverage run -m pytest tests/

# Generate coverage report
python3 -m coverage report --show-missing
```

The test suite includes tests for:
- File metadata fetching and caching
- Batch processing and retry mechanisms
- Duplicate detection and handling
- CSV export functionality
- Cache operations and persistence
- Error handling and edge cases

## Logs
The script writes detailed logs to drive_scanner.log. Each run of the script appends to the log file.

## Cleanup Instructions

After you have finished using the application, it is recommended to perform the following cleanup steps:

1. Delete OAuth 2.0 Client ID: Go to [Google Project Credentials](https://console.cloud.google.com/apis/credentials) page. Locate the OAuth 2.0 Client ID credential and delete it.
2. Revoke Access: Go to your [Google Account Permissions](https://myaccount.google.com/permissions) page. Locate the application name you used for this project and click on it. Then, click on the "Remove Access" or "Revoke Access" button to revoke the permissions granted to the application.
3. Remove Test User (if applicable): If you added your Google Account as a test user to bypass the verification process.
4. Delete your project.
5. Delete the generated `token.json` file created locally on your project directory.
6. Delete the `credentials.json` file from your project directory.
7. Delete the cache file (`drive_metadata_cache.json`) if you want to remove all cached data.

By following these cleanup instructions, you can ensure that the application no longer has access to your Google Account and that any test user permissions are removed.

## About

This project is a complete rewrite of [moraneus/G-Drive-Remove-Duplicates](https://github.com/moraneus/G-Drive-Remove-Duplicates.git), with significant improvements in:
- Code organization and maintainability
- Performance optimization
- Test coverage
- Error handling
- Logging system
- Documentation
- CI/CD integration

