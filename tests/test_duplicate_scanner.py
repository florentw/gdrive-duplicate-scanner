import unittest
from unittest.mock import Mock, patch, MagicMock
import os
from collections import defaultdict
import sys
from pathlib import Path
import tempfile
import shutil
from io import StringIO
from datetime import datetime, timedelta
import csv

# Add parent directory to Python path to import duplicate_scanner
sys.path.append(str(Path(__file__).parent.parent))

from duplicate_scanner import (
    get_human_readable_size,
    handle_duplicate,
    write_to_csv,
    find_duplicates,
    MetadataCache,
    DriveAPI,
    BatchHandler,
    CACHE_EXPIRY_HOURS,
    BATCH_SIZE,
    SAVE_INTERVAL_MINUTES
)

class TestDuplicateScanner(unittest.TestCase):
    """Test suite for duplicate scanner functionality."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_service = Mock()
        self.mock_files_service = Mock()
        self.mock_service.files.return_value = self.mock_files_service
        self.test_dir = tempfile.mkdtemp()
        self.test_cache_file = os.path.join(self.test_dir, 'test_cache.json')
        self.test_cache = MetadataCache(self.test_cache_file)
        self.drive_api = DriveAPI(self.mock_service, self.test_cache)
        # Store original working directory
        self.original_dir = os.getcwd()
        # Change to test directory
        os.chdir(self.test_dir)

    def tearDown(self):
        """Clean up test fixtures after each test method."""
        # Remove any CSV files created during the test
        for file in os.listdir(self.test_dir):
            if file.startswith('drive_duplicates_') and file.endswith('.csv'):
                try:
                    os.remove(os.path.join(self.test_dir, file))
                except OSError:
                    pass
        
        # Change back to original directory
        os.chdir(self.original_dir)
        # Remove test directory
        shutil.rmtree(self.test_dir)

    def _setup_mock_files(self):
        """Helper to setup mock file data."""
        return [
            {'id': 'id1', 'name': 'file1.txt', 'md5Checksum': 'hash1', 'size': '1024'},
            {'id': 'id2', 'name': 'file2.txt', 'md5Checksum': 'hash1', 'size': '1024'},  # Duplicate
            {'id': 'id3', 'name': 'file3.txt', 'md5Checksum': 'hash2', 'size': '2048'}   # Unique
        ]

    def _setup_mock_metadata(self):
        """Helper to setup mock metadata."""
        return {
            'id1': {
                'id': 'id1',
                'name': 'file1.txt',
                'parents': ['parent1'],
                'size': '1024',
                'md5Checksum': 'hash1'
            },
            'id2': {
                'id': 'id2',
                'name': 'file2.txt',
                'parents': ['parent2'],
                'size': '1024',
                'md5Checksum': 'hash1'
            }
        }

    def test_get_human_readable_size(self):
        """Test size conversion to human readable format."""
        test_cases = [
            (0, "0.00 B"),
            (1023, "1023.00 B"),
            (1024, "1.00 KB"),
            (1024 * 1024, "1.00 MB"),
            (1024 * 1024 * 1024, "1.00 GB"),
            (-1, "Unknown size"),
            ("invalid", "Unknown size"),
        ]
        
        for input_size, expected_output in test_cases:
            with self.subTest(input_size=input_size):
                self.assertEqual(get_human_readable_size(input_size), expected_output)

    def test_metadata_cache_operations(self):
        """Test basic cache operations."""
        # Test setting and getting values
        self.test_cache.set('test_key', 'test_value')
        self.assertEqual(self.test_cache.get('test_key'), 'test_value')
        
        # Test updating multiple values
        self.test_cache.update({'key1': 'value1', 'key2': 'value2'})
        self.assertEqual(self.test_cache.get('key1'), 'value1')
        self.assertEqual(self.test_cache.get('key2'), 'value2')
        
        # Test removing values
        self.test_cache.remove(['key1'])
        self.assertIsNone(self.test_cache.get('key1'))
        
        # Test clearing cache
        self.test_cache.clear()
        self.assertIsNone(self.test_cache.get('test_key'))
        self.assertIsNone(self.test_cache.get('key2'))

    def test_metadata_cache_file_errors(self):
        """Test cache operations with file system errors."""
        # Test cache load with invalid file
        with patch('builtins.open', side_effect=IOError("File error")):
            cache = MetadataCache(self.test_cache_file)
            self.assertIsNone(cache.get('any_key'))

        # Test cache save with invalid file
        self.test_cache.set('test_key', 'test_value')
        with patch('builtins.open', side_effect=IOError("File error")):
            self.test_cache._save(force=True)
            # Cache should still work in memory
            self.assertEqual(self.test_cache.get('test_key'), 'test_value')

    def test_metadata_cache_context_manager(self):
        """Test cache context manager functionality."""
        with MetadataCache(self.test_cache_file) as cache:
            cache.set('test_key', 'test_value')
            self.assertEqual(cache.get('test_key'), 'test_value')
        
        # Cache should be saved after context exit
        new_cache = MetadataCache(self.test_cache_file)
        self.assertEqual(new_cache.get('test_key'), 'test_value')

    def test_drive_api_get_files_metadata_batch(self):
        """Test batch metadata fetching."""
        mock_files = [
            {'id': 'id1', 'name': 'file1.txt'},
            {'id': 'id2', 'name': 'file2.txt'}
        ]
        
        mock_responses = {
            'id1': {'id': 'id1', 'name': 'file1.txt', 'size': '1024'},
            'id2': {'id': 'id2', 'name': 'file2.txt', 'size': '2048'}
        }
        
        # Mock batch handler
        with patch('duplicate_scanner.BatchHandler') as mock_handler:
            mock_instance = mock_handler.return_value
            
            def mock_add_metadata_request(file_id):
                mock_instance.results[file_id] = mock_responses[file_id]
            
            def mock_execute():
                # Simulate successful batch execution
                pass
            
            mock_instance.results = {}
            mock_instance._cached_results = {}
            mock_instance.add_metadata_request.side_effect = mock_add_metadata_request
            mock_instance.execute.side_effect = mock_execute
            mock_instance.get_results.return_value = mock_responses
            
            result = self.drive_api.get_files_metadata_batch(['id1', 'id2'])
            
            self.assertEqual(result, mock_responses)
            self.assertEqual(mock_instance.add_metadata_request.call_count, 2)
            mock_instance.execute.assert_called_once()

    def test_drive_api_get_files_metadata_batch_retry(self):
        """Test batch metadata fetching with retries."""
        # Mock the service for retry
        mock_service = MagicMock()
        mock_get = MagicMock()
        mock_service.files.return_value.get.return_value = mock_get
        mock_get.execute.return_value = {'id': 'test_id', 'name': 'test_file'}
        
        # Create DriveAPI instance with mocked service
        self.drive_api = DriveAPI(mock_service, self.test_cache)
        
        # Mock the BatchHandler
        with patch('duplicate_scanner.BatchHandler') as mock_handler:
            mock_instance = mock_handler.return_value
            
            # First attempt fails
            def mock_add_metadata_request(file_id):
                mock_instance.results[file_id] = None
                mock_instance._failed_requests.add(file_id)
            
            def mock_execute():
                # Simulate batch execution failure
                raise Exception("Batch execution failed")
            
            mock_instance.results = {}
            mock_instance._failed_requests = set()
            mock_instance.add_metadata_request.side_effect = mock_add_metadata_request
            mock_instance.execute.side_effect = mock_execute
            mock_instance.get_failed_requests.return_value = {'test_id'}
            
            # Call the method
            result = self.drive_api.get_files_metadata_batch(['test_id'])
            
            # Verify the result
            self.assertIsNotNone(result)
            self.assertIn('test_id', result)
            self.assertEqual(result['test_id']['name'], 'test_file')
            
            # Verify retry behavior
            self.assertEqual(mock_get.execute.call_count, 1)  # One retry attempt

    def test_drive_api_move_files_to_trash_batch(self):
        """Test batch trash operations."""
        mock_files = ['id1', 'id2']
        
        # Mock successful trash operations
        with patch('duplicate_scanner.BatchHandler') as mock_handler:
            mock_instance = mock_handler.return_value
            mock_instance.results = {'id1': True, 'id2': True}
            
            result = self.drive_api.move_files_to_trash_batch(mock_files)
            
            self.assertEqual(result, {'id1': True, 'id2': True})
            self.assertEqual(mock_instance.add_trash_request.call_count, 2)
            mock_instance.execute.assert_called_once()

    def test_drive_api_move_files_to_trash_batch_errors(self):
        """Test batch trash operations with errors."""
        mock_files = ['id1', 'id2']
        
        # Mock failed trash operations
        with patch('duplicate_scanner.BatchHandler') as mock_handler:
            mock_instance = mock_handler.return_value
            mock_instance.results = {'id1': False, 'id2': True}
            
            result = self.drive_api.move_files_to_trash_batch(mock_files)
            
            self.assertEqual(result, {'id1': False, 'id2': True})
            self.assertEqual(mock_instance.add_trash_request.call_count, 2)
            mock_instance.execute.assert_called_once()

    def test_drive_api_get_file_metadata_cache(self):
        """Test that get_file_metadata caches results."""
        mock_metadata = {
            'id': 'test_id',
            'name': 'test_file.txt',
            'parents': ['parent_id'],
            'size': '1024',
            'md5Checksum': 'hash1',
            'mimeType': 'text/plain',
            'trashed': False
        }
        
        # Mock the service call
        mock_get = Mock()
        mock_get.execute.return_value = mock_metadata
        self.mock_files_service.get.return_value = mock_get
        
        # First call should make an API request
        result1 = self.drive_api.get_file_metadata('test_id')
        
        # Second call should use cached result
        result2 = self.drive_api.get_file_metadata('test_id')
        
        self.assertEqual(result1, result2)
        self.assertEqual(result1, mock_metadata)
        self.assertEqual(mock_get.execute.call_count, 1)  # Should only be called once

    def test_drive_api_get_file_metadata_error(self):
        """Test get_file_metadata error handling."""
        mock_get = Mock()
        mock_get.execute.side_effect = Exception("API Error")
        self.mock_service.files.return_value.get.return_value = mock_get
        
        result = self.drive_api.get_file_metadata('test_id')
        self.assertIsNone(result)

    def test_write_to_csv(self):
        """Test CSV export functionality."""
        mock_files = self._setup_mock_files()
        mock_metadata = self._setup_mock_metadata()
        
        # Mock file metadata fetching
        self.drive_api.get_file_metadata = Mock(side_effect=lambda x: mock_metadata.get(x))
        
        # Write duplicates to CSV
        csv_file = write_to_csv([(mock_files[0], mock_files[1])], self.drive_api)
        
        # Verify CSV file was created
        self.assertTrue(os.path.exists(csv_file))
        
        # Read and verify CSV contents
        with open(csv_file, 'r', newline='') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            
            self.assertEqual(len(rows), 2)  # Two rows for the duplicate pair
            self.assertEqual(rows[0]['File Name'], 'file1.txt')
            self.assertEqual(rows[1]['File Name'], 'file2.txt')

    def test_write_to_csv_file_error(self):
        """Test CSV export with file system errors."""
        mock_files = self._setup_mock_files()
        
        # Mock file system error
        with patch('builtins.open', side_effect=IOError("File error")):
            with self.assertRaises(IOError):
                write_to_csv([(mock_files[0], mock_files[1])], self.drive_api)

    def test_drive_api_list_files(self):
        """Test listing files from Drive."""
        mock_files = self._setup_mock_files()
        
        # Mock successful API response
        mock_list = Mock()
        mock_list.execute.return_value = {'files': mock_files}
        self.mock_files_service.list.return_value = mock_list
        
        # Test listing files
        result = self.drive_api.list_files()
        
        self.assertEqual(result, mock_files)
        self.mock_files_service.list.assert_called_once_with(
            q="trashed = false",
            pageSize=1000,
            fields="nextPageToken, files(id, name, size, md5Checksum, trashed, parents)",
            pageToken=None
        )

    def test_drive_api_list_files_error(self):
        """Test listing files with API errors."""
        # Mock API error
        mock_list = Mock()
        mock_list.execute.side_effect = Exception("API Error")
        self.mock_files_service.list.return_value = mock_list
        
        # Test error handling
        with self.assertRaises(Exception):
            self.drive_api.list_files()

    def test_handle_duplicate(self):
        """Test handling duplicate files."""
        mock_files = self._setup_mock_files()
        mock_metadata = self._setup_mock_metadata()
        duplicate_folders = defaultdict(set)
        
        # Mock metadata fetching
        self.drive_api.get_files_metadata_batch = Mock(return_value=mock_metadata)
        
        # Test handling duplicates
        handle_duplicate(self.drive_api, mock_files[0], mock_files[1], duplicate_folders)
        
        # Verify folder tracking
        self.assertIn('parent1', duplicate_folders)
        self.assertIn('parent2', duplicate_folders)
        self.assertIn('id1', duplicate_folders['parent1'])
        self.assertIn('id2', duplicate_folders['parent2'])

    def test_handle_duplicate_missing_metadata(self):
        """Test handling duplicates with missing metadata."""
        mock_files = self._setup_mock_files()
        duplicate_folders = defaultdict(set)
        
        # Mock failed metadata fetch
        self.drive_api.get_files_metadata_batch = Mock(return_value={})
        
        # Test error handling
        handle_duplicate(self.drive_api, mock_files[0], mock_files[1], duplicate_folders)
        
        # Verify no folders were tracked
        self.assertEqual(len(duplicate_folders), 0)

    def test_find_duplicates(self):
        """Test finding duplicate files."""
        # Mock file list response
        mock_files = [
            {'id': 'id1', 'name': 'file1.txt', 'size': '1024', 'md5Checksum': 'hash1', 'parents': ['folder1']},
            {'id': 'id2', 'name': 'file2.txt', 'size': '1024', 'md5Checksum': 'hash1', 'parents': ['folder2']},
            {'id': 'id3', 'name': 'file3.txt', 'size': '2048', 'md5Checksum': 'hash2', 'parents': ['folder1']}
        ]
        self.drive_api.list_files = Mock(return_value=mock_files)

        # Mock metadata responses
        def mock_get_metadata(file_id):
            return {
                'id': file_id,
                'name': f'file{file_id[-1]}.txt',
                'size': '1024',
                'md5Checksum': 'hash1',
                'parents': [f'folder{file_id[-1]}'],
                'mimeType': 'text/plain',
                'trashed': False
            }
        self.drive_api.get_file_metadata = Mock(side_effect=mock_get_metadata)
        
        # Mock batch metadata responses
        def mock_get_metadata_batch(file_ids):
            return {file_id: mock_get_metadata(file_id) for file_id in file_ids}
        self.drive_api.get_files_metadata_batch = Mock(side_effect=mock_get_metadata_batch)

        result = find_duplicates(self.drive_api)
        self.assertEqual(len(result), 1)  # One group of duplicates
        self.assertEqual(len(result[0]), 2)  # Two files in the group

    def test_find_duplicates_api_error(self):
        """Test finding duplicates with API errors."""
        # Mock API error
        self.drive_api.list_files = Mock(side_effect=Exception("API Error"))
        
        # Test error handling
        with self.assertRaises(Exception):
            find_duplicates(self.drive_api)

    def test_metadata_cache_expiry(self):
        """Test cache expiry functionality."""
        # Set a value with timestamp
        current_time = datetime.now()
        self.test_cache.set('test_key', {'data': 'test_value'})
        
        # Mock datetime to simulate cache expiry
        with patch('duplicate_scanner.datetime') as mock_datetime:
            # Mock now() to return a time after expiry
            mock_datetime.now.return_value = current_time + timedelta(hours=CACHE_EXPIRY_HOURS + 1)
            
            # Mock fromisoformat to return the original timestamp
            def mock_fromisoformat(timestamp_str):
                return current_time
            mock_datetime.fromisoformat.side_effect = mock_fromisoformat
            
            # Force cleanup of expired entries
            self.test_cache._cleanup_expired()
            
            # Value should be expired
            self.assertIsNone(self.test_cache.get('test_key'))

    def test_metadata_cache_persistence(self):
        """Test cache persistence across instances."""
        # Set values in first cache instance
        with MetadataCache(self.test_cache_file) as cache1:
            cache1.set('key1', 'value1')
            cache1.update({'key2': 'value2'})
        
        # Load in second instance
        with MetadataCache(self.test_cache_file) as cache2:
            self.assertEqual(cache2.get('key1'), 'value1')
            self.assertEqual(cache2.get('key2'), 'value2')

    def test_drive_api_list_files_pagination(self):
        """Test listing files with pagination."""
        # Mock paginated responses
        mock_list = Mock()
        mock_list.execute.side_effect = [
            {'files': [{'id': 'id1'}], 'nextPageToken': 'token1'},
            {'files': [{'id': 'id2'}], 'nextPageToken': None}
        ]
        self.mock_files_service.list.return_value = mock_list
        
        result = self.drive_api.list_files()
        
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]['id'], 'id1')
        self.assertEqual(result[1]['id'], 'id2')
        self.assertEqual(mock_list.execute.call_count, 2)

    def test_drive_api_list_files_cache(self):
        """Test file listing with cache."""
        mock_files = self._setup_mock_files()
        
        # Mock the service call
        mock_list = Mock()
        mock_list.execute.return_value = {'files': mock_files}
        self.mock_files_service.list.return_value = mock_list
        
        # First call should cache results
        result1 = self.drive_api.list_files()
        
        # Second call should use cache
        result2 = self.drive_api.list_files()
        
        self.assertEqual(result1, result2)
        self.assertEqual(result1, mock_files)
        self.assertEqual(mock_list.execute.call_count, 1)  # Should only be called once

    def test_drive_api_get_files_metadata_batch_cache(self):
        """Test batch metadata fetching with cache."""
        # Mock the service
        mock_service = MagicMock()
        mock_get = MagicMock()
        mock_service.files.return_value.get.return_value = mock_get
        mock_get.execute.return_value = {'id': 'test_id', 'name': 'test_file'}
        
        # Create DriveAPI instance with mocked service
        self.drive_api = DriveAPI(mock_service, self.test_cache)
        
        # Mock the BatchHandler
        with patch('duplicate_scanner.BatchHandler') as mock_handler:
            mock_instance = mock_handler.return_value
            
            # First call - should hit API
            def mock_add_metadata_request(file_id):
                mock_instance.results[file_id] = None
                mock_instance._failed_requests.add(file_id)
            
            def mock_execute():
                # Simulate batch execution failure to trigger retry
                raise Exception("Batch execution failed")
            
            mock_instance.results = {}
            mock_instance._failed_requests = set()
            mock_instance.add_metadata_request.side_effect = mock_add_metadata_request
            mock_instance.execute.side_effect = mock_execute
            mock_instance.get_failed_requests.return_value = {'test_id'}
            
            # First call - should hit API and cache result
            result1 = self.drive_api.get_files_metadata_batch(['test_id'])
            self.assertIn('test_id', result1)
            self.assertEqual(result1['test_id']['name'], 'test_file')
            
            # Second call - should use cache
            result2 = self.drive_api.get_files_metadata_batch(['test_id'])
            self.assertIn('test_id', result2)
            self.assertEqual(result2['test_id']['name'], 'test_file')
            
            # Verify API was called only once (during first call)
            # Verify API was called only once
            self.assertEqual(mock_get.execute.call_count, 1)

    def test_find_duplicates_cache(self):
        """Test finding duplicates with cached data."""
        # Mock the service
        mock_service = MagicMock()
        mock_list = MagicMock()
        mock_service.files.return_value.list.return_value = mock_list
        
        # Set up mock response for list_files
        mock_list.execute.return_value = {
            'files': [
                {'id': '1', 'name': 'file1', 'size': '100', 'md5Checksum': 'abc'},
                {'id': '2', 'name': 'file2', 'size': '100', 'md5Checksum': 'abc'}
            ]
        }
        
        # Create DriveAPI instance with mocked service
        self.drive_api = DriveAPI(mock_service, self.test_cache)
        
        # First call - should hit API
        result1 = self.drive_api.list_files()
        self.assertEqual(len(result1), 2)
        
        # Second call - should use cache
        result2 = self.drive_api.list_files()
        self.assertEqual(len(result2), 2)
        
        # Verify API was called only once
        self.assertEqual(mock_list.execute.call_count, 1)

    def test_drive_api_get_files_metadata_batch_size_limit(self):
        """Test batch size limits in metadata fetching."""
        # Create more files than BATCH_SIZE
        file_ids = [f'id{i}' for i in range(BATCH_SIZE + 10)]
        mock_responses = {f'id{i}': {'id': f'id{i}'} for i in range(len(file_ids))}
        
        with patch('duplicate_scanner.BatchHandler') as mock_handler:
            mock_instance = mock_handler.return_value
            
            def mock_add_metadata_request(file_id):
                mock_instance.results[file_id] = mock_responses[file_id]
            
            def mock_execute():
                # Simulate successful batch execution
                pass
            
            mock_instance.results = {}
            mock_instance._cached_results = {}
            mock_instance.add_metadata_request.side_effect = mock_add_metadata_request
            mock_instance.execute.side_effect = mock_execute
            mock_instance.get_results.return_value = mock_responses
            
            result = self.drive_api.get_files_metadata_batch(file_ids)
            
            # Should execute multiple batches
            self.assertGreater(mock_instance.execute.call_count, 1)
            self.assertEqual(len(result), len(file_ids))
            self.assertEqual(result, mock_responses)

    def test_find_duplicates_grouping(self):
        """Test duplicate file grouping logic."""
        # Mock file list response
        mock_files = [
            {'id': 'id1', 'name': 'file1.txt', 'size': '1024', 'md5Checksum': 'hash1', 'parents': ['folder1']},
            {'id': 'id2', 'name': 'file2.txt', 'size': '1024', 'md5Checksum': 'hash1', 'parents': ['folder2']},
            {'id': 'id3', 'name': 'file3.txt', 'size': '1024', 'md5Checksum': 'hash2', 'parents': ['folder1']},
            {'id': 'id4', 'name': 'file4.txt', 'size': '2048', 'md5Checksum': 'hash3', 'parents': ['folder2']}
        ]
        self.drive_api.list_files = Mock(return_value=mock_files)

        # Mock metadata responses
        def mock_get_metadata(file_id):
            return {
                'id': file_id,
                'name': f'file{file_id[-1]}.txt',
                'size': '1024' if file_id in ['id1', 'id2', 'id3'] else '2048',
                'md5Checksum': 'hash1' if file_id in ['id1', 'id2'] else ('hash2' if file_id == 'id3' else 'hash3'),
                'parents': [f'folder{file_id[-1]}'],
                'mimeType': 'text/plain',
                'trashed': False
            }
        self.drive_api.get_file_metadata = Mock(side_effect=mock_get_metadata)
        
        # Mock batch metadata responses
        def mock_get_metadata_batch(file_ids):
            return {file_id: mock_get_metadata(file_id) for file_id in file_ids}
        self.drive_api.get_files_metadata_batch = Mock(side_effect=mock_get_metadata_batch)

        result = find_duplicates(self.drive_api)
        self.assertEqual(len(result), 1)  # One group of duplicates
        self.assertEqual(len(result[0]), 2)  # Two files in the group

    def test_find_duplicates_folder_tracking(self):
        """Test duplicate folder tracking."""
        # Mock file list response
        mock_files = [
            {'id': 'id1', 'name': 'file1.txt', 'size': '1024', 'md5Checksum': 'hash1', 'parents': ['folder1']},
            {'id': 'id2', 'name': 'file2.txt', 'size': '1024', 'md5Checksum': 'hash1', 'parents': ['folder1', 'folder2']}
        ]
        self.drive_api.list_files = Mock(return_value=mock_files)

        # Mock metadata responses
        def mock_get_metadata(file_id):
            return {
                'id': file_id,
                'name': f'file{file_id[-1]}.txt',
                'size': '1024',
                'md5Checksum': 'hash1',
                'parents': ['folder1'] if file_id == 'id1' else ['folder1', 'folder2'],
                'mimeType': 'text/plain',
                'trashed': False
            }
        self.drive_api.get_file_metadata = Mock(side_effect=mock_get_metadata)
        
        # Mock batch metadata responses
        def mock_get_metadata_batch(file_ids):
            return {file_id: mock_get_metadata(file_id) for file_id in file_ids}
        self.drive_api.get_files_metadata_batch = Mock(side_effect=mock_get_metadata_batch)

        result = find_duplicates(self.drive_api)
        self.assertEqual(len(result), 1)  # One group of duplicates
        self.assertEqual(len(result[0]), 2)  # Two files in the group

    def test_write_to_csv_headers(self):
        """Test CSV export headers and format."""
        mock_files = self._setup_mock_files()
        mock_metadata = self._setup_mock_metadata()
        
        self.drive_api.get_file_metadata = Mock(side_effect=lambda x: mock_metadata.get(x))
        
        csv_file = write_to_csv([(mock_files[0], mock_files[1])], self.drive_api)
        
        with open(csv_file, 'r', newline='') as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames
            
            # Verify all required headers are present
            required_headers = [
                'File Name', 'Full Path', 'Size (Bytes)', 'Size (Human Readable)',
                'File ID', 'MD5 Checksum', 'Duplicate Group ID', 'Parent Folder',
                'Parent Folder ID', 'Duplicate File Name', 'Duplicate File Path',
                'Duplicate File Size', 'Duplicate File ID'
            ]
            self.assertEqual(set(headers), set(required_headers))

if __name__ == '__main__':
    unittest.main()
    