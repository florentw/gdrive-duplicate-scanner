import unittest
from unittest.mock import Mock, patch, MagicMock
import os
import sys
from pathlib import Path
import tempfile
import shutil
import csv
from datetime import datetime
import pytest
import json
import io

# Add parent directory to Python path to import modules
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.drive_api import DriveAPI
from src.batch import BatchHandler
from src.cache import MetadataCache
from src.models import DuplicateGroup, DuplicateFolder
from src.scanner import DuplicateScanner, DuplicateScannerWithFolders
from src.export import write_to_csv
from src.utils import get_human_readable_size
from src.config import BATCH_SIZE, METADATA_FIELDS, logger, MAX_RETRIES
# Import main from src for TestDuplicateScannerCLI
from src.duplicate_scanner import main as src_main 
# Import the root duplicate_scanner module to test its main
import duplicate_scanner as root_duplicate_scanner

class TestDuplicateScanner(unittest.TestCase):
    """Test suite for duplicate scanner functionality."""

    @pytest.fixture
    def mock_service(self):
        return Mock()

    def setUp(self):
        """Set up test fixtures before each test method."""
        # Create a proper mock service structure
        self.mock_files_service = Mock()
        self.mock_service = Mock()
        self.mock_service.files = Mock(return_value=self.mock_files_service)
        self.mock_service.new_batch_http_request = Mock(return_value=Mock())
        
        # Set up test directory and cache
        self.test_dir = tempfile.mkdtemp()
        self.test_cache_file = os.path.join(self.test_dir, 'test_cache.json')
        self.test_cache = MetadataCache(self.test_cache_file)
        
        # Initialize DriveAPI with cache
        self.drive_api = DriveAPI(self.mock_service, self.test_cache)
        
        # Store original working directory
        self.original_dir = os.getcwd()
        # Change to test directory
        os.chdir(self.test_dir)
        
        # Create a test cache file
        self.test_cache_data = {
            'files': [
                {'id': '1', 'name': 'test1.txt', 'size': '100', 'md5Checksum': 'abc'},
                {'id': '2', 'name': 'test2.txt', 'size': '200', 'md5Checksum': 'def'}
            ]
        }
        with open(self.test_cache_file, 'w') as f:
            json.dump(self.test_cache_data, f)

    def tearDown(self):
        """Clean up test fixtures after each test method."""
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
        mock_responses = {
            'id1': {'id': 'id1', 'name': 'file1.txt', 'size': '1024'},
            'id2': {'id': 'id2', 'name': 'file2.txt', 'size': '2048'}
        }
        
        # Mock batch handler
        mock_handler = Mock()
        mock_handler.results = mock_responses
        mock_handler.get_results.return_value = mock_responses
        mock_handler.get_failed_requests.return_value = set()
        mock_handler.get_statistics.return_value = {
            'total_requests': 2,
            'successful_requests': 2,
            'failed_requests': 0,
            'retry_count': 0
        }
        
        # Mock _get_batch_handler to return our mock handler
        with patch.object(DriveAPI, '_get_batch_handler', return_value=mock_handler):
            result = self.drive_api.get_files_metadata_batch(['id1', 'id2'])
            self.assertEqual(result, mock_responses)

    def test_drive_api_get_files_metadata_batch_retry(self):
        """Test batch metadata fetching with retries."""
        mock_response = {'id': 'test_id', 'name': 'test_file'}
        
        # Mock the service for retry
        mock_service = MagicMock()
        mock_get = MagicMock()
        mock_service.files.return_value.get.return_value = mock_get
        mock_get.execute.return_value = mock_response
        
        # Create DriveAPI instance with mocked service
        self.drive_api = DriveAPI(mock_service, self.test_cache)
        
        # Mock batch handler
        mock_handler = Mock()
        mock_handler.results = {}
        mock_handler.execute.side_effect = Exception("Batch execution failed")
        mock_handler.get_results.return_value = {}
        mock_handler.get_failed_requests.return_value = {'test_id'}
        mock_handler.get_statistics.return_value = {
            'total_requests': 1,
            'successful_requests': 0,
            'failed_requests': 1,
            'retry_count': 1
        }
        
        # Mock _get_batch_handler to return our mock handler
        with patch.object(DriveAPI, '_get_batch_handler', return_value=mock_handler), \
             patch.object(DriveAPI, 'get_file_metadata', return_value=None):  # Mock individual retry to return None
            result = self.drive_api.get_files_metadata_batch(['test_id'])
            self.assertEqual(result, {})

    def test_drive_api_move_files_to_trash_batch(self):
        """Test batch trash operations."""
        mock_files = ['id1', 'id2']
        mock_results = {'id1': True, 'id2': True}
        
        # Mock batch handler
        mock_handler = Mock()
        mock_handler.results = mock_results
        mock_handler.get_results.return_value = mock_results
        mock_handler.get_failed_requests.return_value = set()
        
        # Mock _get_batch_handler to return our mock handler
        with patch.object(DriveAPI, '_get_batch_handler', return_value=mock_handler):
            result = self.drive_api.move_files_to_trash_batch(mock_files)
            self.assertEqual(result, mock_results)
            mock_handler.execute.assert_called_once()

    def test_drive_api_move_files_to_trash_batch_errors(self):
        """Test batch trash operations with errors."""
        mock_files = ['id1', 'id2']
        mock_results = {'id2': True}
        
        # Mock batch handler
        mock_handler = Mock()
        mock_handler.results = mock_results
        mock_handler.get_results.return_value = mock_results
        mock_handler.get_failed_requests.return_value = {'id1'}
        
        # Mock _get_batch_handler to return our mock handler
        with patch.object(DriveAPI, '_get_batch_handler', return_value=mock_handler):
            result = self.drive_api.move_files_to_trash_batch(mock_files)
            self.assertEqual(result, {'id1': False, 'id2': True})
            mock_handler.execute.assert_called_once()

    def test_drive_api_get_file_metadata_cache(self):
        """Test file metadata caching."""
        mock_file = {'id': 'test_id', 'name': 'test_file.txt'}
        
        # Mock service response
        self.mock_files_service.get.return_value.execute.return_value = mock_file
        
        # First call should hit the API
        result = self.drive_api.get_file_metadata('test_id')
        self.assertEqual(result, mock_file)
        self.mock_files_service.get.assert_called_once()
        
        # Second call should use cache
        result = self.drive_api.get_file_metadata('test_id')
        self.assertEqual(result, mock_file)
        self.mock_files_service.get.assert_called_once()  # Still only called once

    def test_drive_api_get_file_metadata_error(self):
        """Test file metadata error handling."""
        self.mock_files_service.get.return_value.execute.side_effect = Exception("API Error")
        
        result = self.drive_api.get_file_metadata('test_id')
        self.assertIsNone(result)

    def test_write_to_csv(self):
        """Test CSV export functionality."""
        # Create a mock duplicate group
        files = [
            {'id': 'id1', 'name': 'file1.txt', 'size': '1024', 'parents': ['parent1']},
            {'id': 'id2', 'name': 'file2.txt', 'size': '1024', 'parents': ['parent2']}
        ]
        metadata = {
            'id1': {'id': 'id1', 'name': 'file1.txt', 'size': '1024', 'parents': ['parent1']},
            'id2': {'id': 'id2', 'name': 'file2.txt', 'size': '1024', 'parents': ['parent2']}
        }
        group = DuplicateGroup(files, metadata)
        
        # Mock parent folder metadata
        with patch.object(self.drive_api, 'get_file_metadata') as mock_get_metadata:
            mock_get_metadata.return_value = {'name': 'test_folder'}
            
            filename = write_to_csv([group], self.drive_api)
            self.assertIsNotNone(filename)
            self.assertTrue(os.path.exists(filename))
            
            # Clean up
            os.remove(filename)

    def test_write_to_csv_file_error(self):
        """Test CSV export error handling."""
        mock_pairs = [
            {
                'group_id': 1,
                'file': {'id': 'id1', 'name': 'file1.txt'},
                'duplicate': {'id': 'id2', 'name': 'file2.txt'}
            }
        ]
        
        # Mock file system error
        with patch('builtins.open', side_effect=IOError("File error")):
            write_to_csv(mock_pairs, self.drive_api)
            # Should not raise exception

    def test_drive_api_list_files(self):
        """Test listing files from Google Drive API."""
        # Mock API response
        self.mock_files_service.list.return_value.execute.return_value = {
            'files': [
                {'id': '1', 'name': 'test1.txt', 'size': '100', 'md5Checksum': 'abc'},
                {'id': '2', 'name': 'test2.txt', 'size': '200', 'md5Checksum': 'def'}
            ],
            'nextPageToken': None
        }

        # Test file listing
        files = self.drive_api.list_files()
        
        # Verify results
        self.assertEqual(len(files), 2)
        self.assertEqual(files[0]['id'], '1')
        self.assertEqual(files[1]['id'], '2')

        # Verify API call
        self.mock_files_service.list.assert_called_once()
        call_args = self.mock_files_service.list.call_args[1]
        self.assertIn(METADATA_FIELDS, call_args['fields'])
        self.assertEqual(call_args['q'], "trashed=false")
        self.assertEqual(call_args['spaces'], 'drive')

    def test_drive_api_list_files_error(self):
        """Test listing files error handling."""
        self.mock_files_service.list.return_value.execute.side_effect = Exception("API Error")
        
        result = self.drive_api.list_files()
        self.assertEqual(result, [])

    def test_duplicate_group(self):
        """Test DuplicateGroup class."""
        files = [
            {'id': 'id1', 'name': 'file1.txt', 'size': '1024'},
            {'id': 'id2', 'name': 'file2.txt', 'size': '1024'}
        ]
        metadata = {
            'id1': {'id': 'id1', 'name': 'file1.txt', 'size': '1024'},
            'id2': {'id': 'id2', 'name': 'file2.txt', 'size': '1024'}
        }
        
        group = DuplicateGroup(files, metadata)
        
        self.assertEqual(group.total_size, 2048)
        self.assertEqual(group.wasted_space, 1024)
        self.assertEqual(len(group.get_parent_folders()), 0)

    def test_duplicate_folder(self):
        """Test DuplicateFolder class."""
        folder_id = 'folder1'
        folder_meta = {'id': 'folder1', 'name': 'Test Folder'}
        duplicate_files = {'file1', 'file2'}
        all_files_metadata = {
            'file1': {'id': 'file1', 'size': '1024'},
            'file2': {'id': 'file2', 'size': '2048'}
        }
        
        folder = DuplicateFolder(folder_id, folder_meta, duplicate_files, all_files_metadata)
        
        # Test initial state
        self.assertEqual(folder.id, folder_id)
        self.assertEqual(folder.metadata, folder_meta)
        self.assertEqual(folder.duplicate_files, duplicate_files)
        self.assertEqual(folder.total_size, 3072) # 1024 + 2048
        
        # Test metadata update
        new_metadata = {
            'folder1': {
                'id': 'folder1',
                'name': 'Test Folder',
                'size': '3072',
                'mimeType': 'application/vnd.google-apps.folder'
            }
        }
        folder.update_metadata(new_metadata)
        self.assertEqual(folder.size, 3072)
        
        # Test duplicate only check
        folder.total_files = {'file1', 'file2'}
        self.assertTrue(folder.check_if_duplicate_only())
        folder.total_files = {'file1', 'file2', 'file3'}
        self.assertFalse(folder.check_if_duplicate_only())

    def test_duplicate_scanner(self):
        """Test DuplicateScanner class."""
        mock_files = self._setup_mock_files()
        
        # Mock file listing
        self.mock_files_service.list.return_value.execute.return_value = {
            'files': mock_files
        }
        
        # Mock metadata fetching
        def mock_get_metadata(file_id):
            return {
                'id': file_id,
                'name': f'file{file_id[-1]}.txt',
                'size': '1024',
                'md5Checksum': 'hash1' if file_id in ['id1', 'id2'] else 'hash2'
            }
        
        def mock_get_metadata_batch(file_ids):
            return {file_id: mock_get_metadata(file_id) for file_id in file_ids}
        
        with patch.object(self.drive_api, 'get_file_metadata', side_effect=mock_get_metadata), \
             patch.object(self.drive_api, 'get_files_metadata_batch', side_effect=mock_get_metadata_batch):
            
            scanner = DuplicateScanner(self.drive_api, self.test_cache)
            scanner.scan()
            
            self.assertEqual(len(scanner.duplicate_groups), 1)  # One group of duplicates
            self.assertEqual(len(scanner.duplicate_groups[0].files), 2)  # Two files in the group

    def test_batch_handler_operations(self):
        """Test BatchHandler operations and contract."""
        # Setup
        mock_service = MagicMock()
        mock_batch = MagicMock()
        mock_service.new_batch_http_request.return_value = mock_batch
        mock_increment = MagicMock()
        handler = BatchHandler(mock_service, self.test_cache, mock_increment)
        
        # Test adding requests
        file_ids = ['id1', 'id2', 'id3']
        for file_id in file_ids:
            handler.add_metadata_request(file_id)
        
        # Verify batch execution
        mock_batch.execute.return_value = None
        handler.execute()
        mock_batch.execute.assert_called_once()
        mock_increment.assert_called_once()  # Verify API request was counted
        
        # Test callback behavior
        for file_id in file_ids[:2]:  # First two succeed
            callback = mock_batch.add.call_args_list[file_ids.index(file_id)][1]['callback']
            callback(file_id, {'id': file_id, 'name': f'file{file_id}.txt'}, None)
        
        # Test error callback
        error_callback = mock_batch.add.call_args_list[2][1]['callback']
        error_callback('id3', None, Exception("API Error"))
        
        # Verify results
        results = handler.get_results()
        self.assertEqual(len(results), 2)
        self.assertTrue('id1' in results)
        self.assertTrue('id2' in results)
        self.assertFalse('id3' in results)
        
        # Verify failed requests
        failed = handler.get_failed_requests()
        self.assertEqual(len(failed), 1)
        self.assertTrue('id3' in failed)

    def test_batch_handler_retry(self):
        """Test BatchHandler retry behavior."""
        # Setup
        mock_service = MagicMock()
        mock_batch = MagicMock()
        mock_service.new_batch_http_request.return_value = mock_batch
        mock_increment = MagicMock()
        handler = BatchHandler(mock_service, self.test_cache, mock_increment)
        
        # Add a request
        handler.add_metadata_request('test_id')
        
        # Mock batch execution to fail twice then succeed
        mock_batch.execute.side_effect = [
            Exception("First failure"),
            Exception("Second failure"),
            None
        ]
        
        # Execute batch and verify retries
        handler.execute()
        self.assertEqual(mock_batch.execute.call_count, 3)
        mock_increment.assert_called_once()  # Should only increment once on final success

    def test_batch_handler_cache_interaction(self):
        """Test BatchHandler cache interaction."""
        # Setup
        mock_service = MagicMock()
        mock_batch = MagicMock()
        mock_service.new_batch_http_request.return_value = mock_batch
        mock_increment = MagicMock()
        handler = BatchHandler(mock_service, self.test_cache, mock_increment)
        
        # Test metadata request with cache
        file_id = 'test_id'
        handler.add_metadata_request(file_id)
        
        # Simulate successful response
        callback = mock_batch.add.call_args[1]['callback']
        response = {'id': file_id, 'name': 'test.txt'}
        callback(file_id, response, None)
        
        # Verify cache was updated
        cached = self.test_cache.get(file_id)
        self.assertEqual(cached, response)
        
        # Test trash request cache removal
        handler.add_trash_request(file_id)
        trash_callback = mock_batch.add.call_args[1]['callback']
        trash_callback(file_id, {'id': file_id, 'trashed': True}, None)
        
        # Verify cache was cleared
        self.assertIsNone(self.test_cache.get(file_id))

    def test_drive_api_batch_operations(self):
        """Test DriveAPI batch operations contract."""
        # Setup
        mock_service = MagicMock()
        api = DriveAPI(mock_service, self.test_cache)
        
        # Test metadata batch
        file_ids = ['id1', 'id2', 'id3']
        mock_responses = {
            'id1': {'id': 'id1', 'name': 'file1.txt'},
            'id2': {'id': 'id2', 'name': 'file2.txt'}
        }
        
        # Mock the batch handler
        mock_handler = Mock()
        mock_handler.results = mock_responses
        mock_handler.get_results.return_value = mock_responses
        mock_handler.get_failed_requests.return_value = {'id3'}
        mock_handler.execute.return_value = None
        mock_handler.get_statistics.return_value = {
            'total_requests': 3,
            'successful_requests': 2,
            'failed_requests': 1,
            'retry_count': 0
        }
        
        # Mock the get_file_metadata method to simulate individual retries
        with patch.object(DriveAPI, '_get_batch_handler', return_value=mock_handler), \
             patch.object(DriveAPI, 'get_file_metadata', return_value=None):
            
            # Test metadata batch
            result = api.get_files_metadata_batch(file_ids)
            self.assertEqual(result, mock_responses)
            
            # Reset mock responses for trash operation
            mock_responses = {'id1': True, 'id2': True}
            mock_handler.results = mock_responses
            mock_handler.get_results.return_value = mock_responses
            
            # Test trash batch
            trash_result = api.move_files_to_trash_batch(file_ids)
            self.assertTrue(trash_result['id1'])
            self.assertTrue(trash_result['id2'])
            self.assertFalse(trash_result['id3'])

    def test_drive_api_batch_size_limits(self):
        """Test DriveAPI batch size limits."""
        # Setup
        mock_service = MagicMock()
        mock_service.files().get().execute.return_value = {'id': 'test'}
        api = DriveAPI(mock_service)

        # Generate more file IDs than BATCH_SIZE
        file_ids = [f'id{i}' for i in range(BATCH_SIZE + 5)]
        
        # Mock batch handler
        mock_batch = MagicMock()
        mock_batch.execute.return_value = None
        mock_batch.get_results.return_value = {f'id{i}': {'id': f'id{i}'} for i in range(BATCH_SIZE)}
        mock_batch.get_statistics.return_value = {
            'total_requests': BATCH_SIZE,
            'successful_requests': BATCH_SIZE,
            'failed_requests': 0,
            'retry_count': 0
        }
        
        with patch('src.drive_api.BatchHandler', return_value=mock_batch):
            # Test metadata batch
            api.get_files_metadata_batch(file_ids)
            # Should be called twice: once for BATCH_SIZE items, once for remaining 5
            self.assertEqual(mock_batch.execute.call_count, 2)

    def test_write_to_csv_with_duplicate_groups(self):
        """Test writing to CSV with DuplicateGroup objects."""
        # Create test data
        files = [
            {'id': 'id1', 'name': 'file1.txt', 'size': '1024'},
            {'id': 'id2', 'name': 'file2.txt', 'size': '1024'}
        ]
        metadata = {
            'id1': {'id': 'id1', 'name': 'file1.txt', 'size': '1024', 'parents': ['folder1']},
            'id2': {'id': 'id2', 'name': 'file2.txt', 'size': '1024', 'parents': ['folder2']}
        }
        
        group = DuplicateGroup(files, metadata)
        groups = [group]
        
        # Mock DriveAPI
        mock_drive_api = MagicMock()
        mock_drive_api.get_files_metadata_batch.return_value = {
            'folder1': {'id': 'folder1', 'name': 'Folder 1'},
            'folder2': {'id': 'folder2', 'name': 'Folder 2'}
        }
        
        # Test CSV export
        filename = write_to_csv(groups, mock_drive_api)
        
        # Verify file was created
        self.assertIsNotNone(filename)
        self.assertTrue(os.path.exists(filename))
        
        # Verify CSV content
        with open(filename, 'r') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            
            # Should have 2 rows (one for each file)
            self.assertEqual(len(rows), 2)
            
            # Verify first row
            self.assertEqual(rows[0]['File Name'], 'file1.txt')
            self.assertEqual(rows[0]['Duplicate File Name'], 'file2.txt')
            self.assertEqual(rows[0]['Parent Folder'], 'Folder 1')
            self.assertEqual(rows[0]['Duplicate File Path'], 'Folder 2/file2.txt')
            
            # Verify second row
            self.assertEqual(rows[1]['File Name'], 'file2.txt')
            self.assertEqual(rows[1]['Duplicate File Name'], 'file1.txt')
            self.assertEqual(rows[1]['Parent Folder'], 'Folder 2')
            self.assertEqual(rows[1]['Duplicate File Path'], 'Folder 1/file1.txt')
        
        # Cleanup
        os.remove(filename)

    def test_scanner_with_cache(self):
        """Test scanner initialization and operation with cache."""
        # Create mock objects
        mock_drive_api = MagicMock()
        mock_cache = MagicMock()
        
        # Setup mock cache to return test files
        test_files = [
            {'id': 'id1', 'name': 'file1.txt', 'size': '1024', 'md5Checksum': 'abc'},
            {'id': 'id2', 'name': 'file2.txt', 'size': '1024', 'md5Checksum': 'abc'}
        ]
        mock_cache.get_all_files.return_value = test_files
        
        # Create scanner
        scanner = DuplicateScanner(mock_drive_api, mock_cache)
        
        # Test scan
        scanner.scan()
        
        # Verify cache was used
        mock_cache.get_all_files.assert_called_once()
        
        # Verify duplicate groups were found
        self.assertEqual(len(scanner.duplicate_groups), 1)
        self.assertEqual(len(scanner.duplicate_groups[0].files), 2)

    def test_main_script_flow(self):
        """Test the main script flow for the root duplicate_scanner.py."""
        mock_service_return = MagicMock() # What get_service returns

        # Mock for DriveAPI constructor and its instance
        mock_drive_api_constructor = MagicMock(spec=DriveAPI) 
        mock_drive_api_instance = MagicMock()
        mock_drive_api_constructor.return_value = mock_drive_api_instance

        # Mock for MetadataCache constructor and its instance
        mock_metadata_cache_constructor = MagicMock(spec=MetadataCache)
        mock_metadata_cache_instance = MagicMock()
        mock_metadata_cache_constructor.return_value = mock_metadata_cache_instance
        
        # Mock for DuplicateScannerWithFolders constructor and its instance
        mock_scanner_constructor = MagicMock(spec=DuplicateScannerWithFolders)
        mock_scanner_instance = MagicMock()
        mock_scanner_instance.duplicate_groups = [MagicMock(files=[{'size': '1024', 'name':'file1.txt'}], wasted_space=0)] 
        mock_scanner_instance.duplicate_files_in_folders = {} 
        mock_scanner_instance.duplicate_only_folders = {}   
        mock_scanner_constructor.return_value = mock_scanner_instance

        mock_write_csv_func = MagicMock()

        # Patch targets are for 'duplicate_scanner' (the root module)
        # sys.argv is set to ensure no actual CLI args interfere, or to pass specific test args
        with patch('sys.argv', ['duplicate_scanner.py']), \
             patch('duplicate_scanner.get_service', return_value=mock_service_return) as patched_get_service, \
             patch('duplicate_scanner.DriveAPI', mock_drive_api_constructor) as patched_drive_api_constructor, \
             patch('duplicate_scanner.MetadataCache', mock_metadata_cache_constructor) as patched_metadata_cache_constructor, \
             patch('duplicate_scanner.DuplicateScannerWithFolders', mock_scanner_constructor) as patched_scanner_constructor, \
             patch('duplicate_scanner.write_to_csv', mock_write_csv_func) as patched_write_csv, \
             patch('builtins.print') as mock_root_print: # Mock print for the root script
            
            root_duplicate_scanner.main() 
            
            patched_get_service.assert_called_once_with()
            patched_drive_api_constructor.assert_called_once_with(mock_service_return)
            patched_metadata_cache_constructor.assert_called_once_with() 
            patched_scanner_constructor.assert_called_once_with(mock_drive_api_instance, mock_metadata_cache_instance)
            mock_scanner_instance.scan.assert_called_once_with() 
            
            if mock_scanner_instance.duplicate_groups:
                patched_write_csv.assert_called_once_with(mock_scanner_instance.duplicate_groups, mock_drive_api_instance)
            else:
                patched_write_csv.assert_not_called()

    @patch('src.export.tqdm') # This should be fine as 'export' is 'src.export' which is used by both root and src main CSV exports
    def test_write_to_csv_optimized(self, mock_tqdm):
        """Test the optimized CSV export functionality."""
        # Setup mock progress bar
        mock_progress = MagicMock()
        mock_tqdm.return_value.__enter__.return_value = mock_progress
        
        # Create test data with multiple duplicates
        files = [
            {'id': 'id1', 'name': 'file1.txt', 'size': '1024', 'parents': ['folder1']},
            {'id': 'id2', 'name': 'file2.txt', 'size': '1024', 'parents': ['folder2']},
            {'id': 'id3', 'name': 'file3.txt', 'size': '1024', 'parents': ['folder3']}
        ]
        metadata = {
            'id1': {'id': 'id1', 'name': 'file1.txt', 'size': '1024', 'parents': ['folder1'], 'md5Checksum': 'abc123'},
            'id2': {'id': 'id2', 'name': 'file2.txt', 'size': '1024', 'parents': ['folder2'], 'md5Checksum': 'abc123'},
            'id3': {'id': 'id3', 'name': 'file3.txt', 'size': '1024', 'parents': ['folder3'], 'md5Checksum': 'abc123'}
        }
        group = DuplicateGroup(files, metadata)
        
        # Mock parent folder metadata
        folder_metadata = {
            'folder1': {'id': 'folder1', 'name': 'Folder 1'},
            'folder2': {'id': 'folder2', 'name': 'Folder 2'},
            'folder3': {'id': 'folder3', 'name': 'Folder 3'}
        }
        
        # Mock DriveAPI
        mock_drive_api = MagicMock()
        mock_drive_api.get_files_metadata_batch.return_value = folder_metadata
        
        # Test CSV export
        filename = write_to_csv([group], mock_drive_api)
        
        # Verify file was created
        self.assertIsNotNone(filename)
        self.assertTrue(os.path.exists(filename))
        
        # Verify progress bar was used correctly
        mock_tqdm.assert_called_once_with(total=3, desc="Exporting duplicates", unit="files")
        self.assertEqual(mock_progress.update.call_count, 3)  # Called once for each file
        
        # Verify CSV content
        with open(filename, 'r') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            
            # Should have 3 rows (one for each file)
            self.assertEqual(len(rows), 3)
            
            # Verify first row
            self.assertEqual(rows[0]['File Name'], 'file1.txt')
            self.assertEqual(rows[0]['Parent Folder'], 'Folder 1')
            self.assertEqual(rows[0]['MD5 Checksum'], 'abc123')
            self.assertEqual(rows[0]['Size (Bytes)'], '1024')
            
            # Verify duplicates are properly joined
            duplicate_names = rows[0]['Duplicate File Name'].split('; ')
            self.assertEqual(len(duplicate_names), 2)
            self.assertIn('file2.txt', duplicate_names)
            self.assertIn('file3.txt', duplicate_names)
            
            # Verify paths are properly joined
            duplicate_paths = rows[0]['Duplicate File Path'].split('; ')
            self.assertEqual(len(duplicate_paths), 2)
            self.assertIn('Folder 2/file2.txt', duplicate_paths)
            self.assertIn('Folder 3/file3.txt', duplicate_paths)
        
        # Cleanup
        os.remove(filename)

    @patch('src.export.tqdm')
    def test_write_to_csv_with_missing_metadata(self, mock_tqdm):
        """Test CSV export with missing metadata."""
        # Setup mock progress bar
        mock_progress = MagicMock()
        mock_tqdm.return_value.__enter__.return_value = mock_progress
        
        # Create test data with missing metadata
        files = [
            {'id': 'id1', 'name': 'file1.txt', 'size': '1024'},
            {'id': 'id2', 'name': 'file2.txt', 'size': '1024'}
        ]
        metadata = {
            'id1': {'id': 'id1', 'name': 'file1.txt', 'size': '1024', 'md5Checksum': 'abc123'}
            # id2 metadata is missing
        }
        group = DuplicateGroup(files, metadata)
        
        # Mock DriveAPI
        mock_drive_api = MagicMock()
        mock_drive_api.get_files_metadata_batch.return_value = {}
        
        # Test CSV export
        filename = write_to_csv([group], mock_drive_api)
        
        # Verify file was created
        self.assertIsNotNone(filename)
        self.assertTrue(os.path.exists(filename))
        
        # Verify progress bar was used correctly
        mock_tqdm.assert_called_once_with(total=2, desc="Exporting duplicates", unit="files")
        self.assertEqual(mock_progress.update.call_count, 2)  # Called for both files, even the missing one
        
        # Verify CSV content
        with open(filename, 'r') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            
            # Should have 1 row (only for file1.txt)
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]['File Name'], 'file1.txt')
            self.assertEqual(rows[0]['Duplicate File Name'], '')  # No duplicates due to missing metadata
        
        # Cleanup
        os.remove(filename)

    @patch('src.export.tqdm')
    def test_write_to_csv_with_empty_groups(self, mock_tqdm):
        """Test CSV export with empty duplicate groups."""
        # Setup mock progress bar
        mock_progress = MagicMock()
        mock_tqdm.return_value.__enter__.return_value = mock_progress
        
        # Create empty group
        group = DuplicateGroup([], {})
        
        # Mock DriveAPI
        mock_drive_api = MagicMock()
        
        # Test CSV export
        filename = write_to_csv([group], mock_drive_api)
        
        # Verify file was created
        self.assertIsNotNone(filename)
        self.assertTrue(os.path.exists(filename))
        
        # Verify progress bar was used correctly
        mock_tqdm.assert_called_once_with(total=0, desc="Exporting duplicates", unit="files")
        self.assertEqual(mock_progress.update.call_count, 0)  # Never called for empty group
        
        # Verify CSV content
        with open(filename, 'r') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            
            # Should have 0 rows
            self.assertEqual(len(rows), 0)
        
        # Cleanup
        os.remove(filename)

    def test_write_to_csv_file_error(self):
        """Test CSV export error handling."""
        # Create test data
        files = [{'id': 'id1', 'name': 'file1.txt', 'size': '1024'}]
        metadata = {'id1': {'id': 'id1', 'name': 'file1.txt', 'size': '1024', 'md5Checksum': 'abc123'}}
        group = DuplicateGroup(files, metadata)
        
        # Mock DriveAPI
        mock_drive_api = MagicMock()
        
        # Mock file system error
        with patch('builtins.open', side_effect=IOError("File error")):
            result = write_to_csv([group], mock_drive_api)
            self.assertIsNone(result)

    def test_drive_api_batch_size_logging(self):
        """Test batch size logging in metadata fetching."""
        # Setup
        mock_service = MagicMock()
        api = DriveAPI(mock_service)
        
        # Test with different batch sizes
        test_cases = [
            ([f'id{i}' for i in range(5)], 1),  # Single batch
            ([f'id{i}' for i in range(150)], 2),  # Two batches
            ([f'id{i}' for i in range(250)], 3),  # Three batches
        ]
        
        for file_ids, expected_batches in test_cases:
            # Reset API statistics
            api._total_batches_processed = 0
            api._total_batch_requests = 0
            api._total_batch_successes = 0
            api._total_batch_failures = 0
            api._total_batch_retries = 0
            api.api_request_count = 0
    
            # Mock batch handler
            mock_batch = MagicMock()
            mock_batch.execute.return_value = None
            mock_batch.get_results.return_value = {f'id{i}': {'id': f'id{i}'} for i in range(len(file_ids))}
            mock_batch.get_failed_requests.return_value = set()
            mock_batch.get_statistics.return_value = {
                'total_requests': len(file_ids),
                'successful_requests': len(file_ids),
                'failed_requests': 0,
                'retry_count': 0
            }
            
            with patch('src.drive_api.BatchHandler', return_value=mock_batch), \
                 patch('src.drive_api.logger.info') as mock_logging:
                
                # Call the method
                api.get_files_metadata_batch(file_ids)
                
                # Verify logging was called with correct batch information
                self.assertEqual(mock_logging.call_count, 2)  # Two log messages
                
                # Get both log messages
                first_message = mock_logging.call_args_list[0][0][0]
                second_message = mock_logging.call_args_list[1][0][0]
                
                # Verify first message (processing start)
                self.assertIn(str(len(file_ids)), first_message)  # Total files
                self.assertIn(str(expected_batches), first_message)  # Number of batches
                self.assertIn("avg", first_message.lower())  # Average batch size
                self.assertIn("files per batch", first_message.lower())  # Batch size info
                
                # Verify second message (completion)
                self.assertIn("successful", second_message.lower())  # Success rate
                self.assertIn("failed", second_message.lower())  # Failed requests
                self.assertIn("retries", second_message.lower())  # Retry count
                self.assertIn(str(expected_batches), second_message)  # Number of batches
                self.assertIn("100.0%", second_message)  # Success rate percentage
                
                # Reset mocks for next test case
                mock_logging.reset_mock()
                mock_batch.reset_mock()

    def test_drive_api_request_counting(self):
        """Test API request counting functionality."""
        # Setup
        mock_service = MagicMock()
        api = DriveAPI(mock_service)
        
        # Test single file metadata request
        mock_file = {'id': 'test_id', 'name': 'test_file.txt'}
        mock_service.files().get().execute.return_value = mock_file
        
        # First call should increment counter
        api.get_file_metadata('test_id')
        self.assertEqual(api.api_request_count, 1)
        
        # Second call should use cache and not increment counter
        api.get_file_metadata('test_id')
        self.assertEqual(api.api_request_count, 1)
        
        # Test batch metadata request
        mock_files = [
            {'id': 'id1', 'name': 'file1.txt'},
            {'id': 'id2', 'name': 'file2.txt'}
        ]
        mock_service.files().list().execute.return_value = {'files': mock_files}
        
        # List files should increment counter by 1 (just the actual list call)
        api.list_files()
        self.assertEqual(api.api_request_count, 2)
        
        # Second list should use cache and not increment counter
        api.list_files()
        self.assertEqual(api.api_request_count, 2)
        
        # Force refresh should increment counter by 1 again
        api.list_files(force_refresh=True)
        self.assertEqual(api.api_request_count, 3)

    def test_batch_handler_error_handling(self):
        """Test BatchHandler error handling and retry logic."""
        # Setup
        mock_service = MagicMock()
        mock_batch = MagicMock()
        mock_service.new_batch_http_request.return_value = mock_batch
        mock_increment = MagicMock()
        handler = BatchHandler(mock_service, self.test_cache, mock_increment)
        
        # Add some requests
        file_ids = ['id1', 'id2', 'id3']
        for file_id in file_ids:
            handler.add_metadata_request(file_id)
        
        # Test complete batch failure
        mock_batch.execute.side_effect = Exception("API Error")
        
        # Should raise after MAX_RETRIES attempts
        with self.assertRaises(Exception):
            handler.execute()
        
        # Verify retry attempts
        self.assertEqual(mock_batch.execute.call_count, MAX_RETRIES)
        self.assertEqual(handler._retry_count, MAX_RETRIES)
        mock_increment.assert_not_called()  # Should not increment on failure
        
        # Test partial batch failure
        mock_batch.execute.reset_mock()
        mock_batch.execute.side_effect = None
        mock_increment.reset_mock()
        
        # Simulate some failed callbacks
        for file_id in file_ids[:2]:  # First two succeed
            callback = mock_batch.add.call_args_list[file_ids.index(file_id)][1]['callback']
            callback(file_id, {'id': file_id}, None)
        
        # Last one fails
        error_callback = mock_batch.add.call_args_list[2][1]['callback']
        error_callback('id3', None, Exception("API Error"))
        
        # Execute batch
        handler.execute()
        mock_increment.assert_called_once()  # Should increment on successful batch execution
        
        # Verify error handling
        self.assertEqual(len(handler.get_failed_requests()), 1)
        self.assertTrue('id3' in handler.get_failed_requests())
        self.assertEqual(handler._failure_count, 1)
        self.assertEqual(handler._success_count, 2)

    def test_drive_api_batch_failure_handling(self):
        """Test DriveAPI handling of batch failures."""
        # Setup
        mock_service = MagicMock()
        api = DriveAPI(mock_service, self.test_cache)
        
        # Test complete batch failure with retry
        file_ids = ['id1', 'id2', 'id3']
        mock_batch = MagicMock()
        mock_batch.execute.side_effect = Exception("Batch Error")
        mock_batch.get_failed_requests.return_value = set(file_ids)
        mock_batch.get_statistics.return_value = {
            'total_requests': len(file_ids),
            'successful_requests': 0,
            'failed_requests': len(file_ids),
            'retry_count': MAX_RETRIES
        }
        
        # Mock individual retries to succeed for some files
        def mock_get_metadata(file_id):
            return {'id': file_id} if file_id != 'id3' else None
        
        with patch.object(DriveAPI, '_get_batch_handler', return_value=mock_batch), \
             patch.object(DriveAPI, 'get_file_metadata', side_effect=mock_get_metadata):
            
            results = api.get_files_metadata_batch(file_ids)
            
            # Verify results
            self.assertEqual(len(results), 2)  # Two successful retries
            self.assertTrue('id1' in results)
            self.assertTrue('id2' in results)
            self.assertFalse('id3' in results)  # Failed retry
            
            # Verify statistics
            stats = api.get_batch_statistics()
            self.assertEqual(stats['failed_requests'], len(file_ids))
            self.assertEqual(stats['retry_count'], MAX_RETRIES)

    def test_get_files_metadata_batch_multiple_batches(self):
        """Test get_files_metadata_batch with input forcing multiple batches."""
        file_ids = [f'id{i}' for i in range(BATCH_SIZE + 5)]
        
        # Mock BatchHandler instance and its methods
        mock_bh_instance = MagicMock(spec=BatchHandler)
        
        # Simulate two batches
        responses_batch1 = {f'id{i}': {'id': f'id{i}', 'name': f'file{i}'} for i in range(BATCH_SIZE)}
        responses_batch2 = {f'id{i}': {'id': f'id{i}', 'name': f'file{i}'} for i in range(BATCH_SIZE, BATCH_SIZE + 5)}

        # Configure side effects for get_results and get_failed_requests
        # First call (batch 1)
        mock_bh_instance.get_results.side_effect = [responses_batch1, responses_batch2]
        mock_bh_instance.get_failed_requests.side_effect = [set(), set()] # No failures in this test
        stats_batch1 = {'total_requests': BATCH_SIZE, 'successful_requests': BATCH_SIZE, 'failed_requests': 0, 'retry_count': 0}
        stats_batch2 = {'total_requests': 5, 'successful_requests': 5, 'failed_requests': 0, 'retry_count': 0}
        mock_bh_instance.get_statistics.side_effect = [stats_batch1, stats_batch2]

        def mock_get_cached_metadata_none(ids_to_check):
            return {}, set(ids_to_check) # Ensure all IDs are processed as uncached

        with patch.object(self.drive_api, '_get_cached_metadata', side_effect=mock_get_cached_metadata_none) as mock_cache_check, \
             patch('src.drive_api.BatchHandler', return_value=mock_bh_instance) as mock_batch_constructor:
            # Reset api_request_count for this specific test on the existing self.drive_api instance
            self.drive_api.api_request_count = 0 # Reset this as get_file_metadata (called by retry) increments it.
            self.drive_api._total_batches_processed = 0
            self.drive_api._total_batch_requests = 0
            self.drive_api._total_batch_successes = 0
            self.drive_api._total_batch_failures = 0
            self.drive_api._total_batch_retries = 0
            # Ensure batch_handler is reset for this test so a new one (our mock) is created
            self.drive_api.batch_handler = None

            results = self.drive_api.get_files_metadata_batch(file_ids)

            # Check that BatchHandler constructor was called (indirectly via _get_batch_handler)
            # It might be called once and reused, or multiple times if logic resets it.
            # For this test, we focus on execute calls.
            self.assertGreaterEqual(mock_batch_constructor.call_count, 1)
            
            # Check execute was called for each batch
            self.assertEqual(mock_bh_instance.execute.call_count, 2)
            
            # Verify combined results
            expected_results = {**responses_batch1, **responses_batch2}
            self.assertEqual(results, expected_results)
            self.assertEqual(len(results), BATCH_SIZE + 5)

    def test_get_files_metadata_batch_partial_failure_and_retry(self):
        """Test get_files_metadata_batch with partial failures and retries."""
        file_ids = ['id1', 'id2', 'id3', 'id4', 'id5']
        
        mock_bh_instance = MagicMock(spec=BatchHandler)
        
        # Batch 1 results: id1, id2 success; id3 fails
        batch1_success = {'id1': {'id': 'id1', 'name': 'file1'}, 'id2': {'id': 'id2', 'name': 'file2'}}
        batch1_failed_set = {'id3'}
        
        mock_bh_instance.get_results.return_value = batch1_success
        mock_bh_instance.get_failed_requests.return_value = batch1_failed_set
        mock_bh_instance.get_statistics.return_value = {'total_requests': 3, 'successful_requests': 2, 'failed_requests': 1, 'retry_count': 0}

        # Mock for DriveAPI.get_file_metadata (the retry mechanism)
        # id3 will be retried: succeed
        # Let's assume id4, id5 were part of a different scenario or not in the first batch for simplicity of this test.
        # We are testing the retry of 'id3'.
        
        # If get_file_metadata is called for 'id3', it will now raise an exception.
        def mock_individual_get_metadata(file_id_to_check):
            if file_id_to_check == 'id3':
                raise Exception("Simulated retry error for id3")
            # This mock should only be called for 'id3' in this test's logic.
            # If called for 'id1' or 'id2', it means they weren't handled by the batch mock as expected.
            self.fail(f"Unexpected call to get_file_metadata with {file_id_to_check}")
            return None # Should not be reached

        def mock_get_cached_metadata_none_for_retry(ids_to_check):
            return {}, set(ids_to_check)

        with patch.object(self.drive_api, '_get_cached_metadata', side_effect=mock_get_cached_metadata_none_for_retry) as mock_cache_check_retry, \
             patch('src.drive_api.BatchHandler', return_value=mock_bh_instance) as mock_bh_constructor_retry, \
             patch.object(self.drive_api, 'get_file_metadata', side_effect=mock_individual_get_metadata) as mock_retry_get_metadata, \
             patch('src.drive_api.logging.error') as mock_drive_api_logging_error: # Patch logger in drive_api
            
            # Reset api_request_count for this specific test on the existing self.drive_api instance
            self.drive_api.api_request_count = 0 
            self.drive_api._total_batches_processed = 0
            self.drive_api._total_batch_requests = 0
            self.drive_api._total_batch_successes = 0
            self.drive_api._total_batch_failures = 0
            self.drive_api._total_batch_retries = 0
            self.drive_api.batch_handler = None


            # We only pass IDs that would go into the first batch for this specific scenario
            results = self.drive_api.get_files_metadata_batch(['id1', 'id2', 'id3'])
            
            mock_bh_instance.execute.assert_called_once()
            mock_retry_get_metadata.assert_called_once_with('id3')
            
            # Expected: id1, id2 from batch success. id3 failed retry, so not present.
            expected_results = {
                'id1': {'id': 'id1', 'name': 'file1'},
                'id2': {'id': 'id2', 'name': 'file2'},
            }
            self.assertEqual(results, expected_results)
            # Verify that the specific error log from _handle_failed_requests was called
            logged_error_correctly = False
            for call_args_tuple in mock_drive_api_logging_error.call_args_list:
                # Ensure call_args_tuple.args is not empty and contains the log message string
                if call_args_tuple.args and isinstance(call_args_tuple.args[0], str):
                    log_message = call_args_tuple.args[0]
                    if f"Failed to get metadata for file id3" in log_message and "Simulated retry error for id3" in log_message:
                        logged_error_correctly = True
                        break
            self.assertTrue(logged_error_correctly, "Expected error log for failed retry of id3 was not found or incorrect.")

    def test_move_files_to_trash_batch_multiple_batches(self):
        """Test move_files_to_trash_batch with input forcing multiple batches."""
        file_ids = [f'id{i}' for i in range(BATCH_SIZE + 5)]
        
        mock_bh_instance = MagicMock(spec=BatchHandler)
        
        # Simulate two batches for trash operation
        # get_results for trash returns {file_id: True/False}
        trash_results_batch1 = {f'id{i}': True for i in range(BATCH_SIZE)}
        trash_results_batch2 = {f'id{i}': True for i in range(BATCH_SIZE, BATCH_SIZE + 5)}

        mock_bh_instance.get_results.side_effect = [trash_results_batch1, trash_results_batch2]
        mock_bh_instance.get_failed_requests.side_effect = [set(), set()] # No failures

        with patch('src.drive_api.BatchHandler', return_value=mock_bh_instance):
            self.drive_api.batch_handler = None # Ensure new mock handler is used
            results = self.drive_api.move_files_to_trash_batch(file_ids)
            
            self.assertEqual(mock_bh_instance.execute.call_count, 2)
            
            expected_results = {f'id{i}': True for i in range(BATCH_SIZE + 5)}
            self.assertEqual(results, expected_results)

    def test_move_files_to_trash_batch_partial_failure(self):
        """Test move_files_to_trash_batch with partial failures."""
        file_ids = ['id1', 'id2', 'id3_fail'] # id3_fail will fail
        
        mock_bh_instance = MagicMock(spec=BatchHandler)
        
        # Batch results: id1, id2 success (True); id3_fail is missing from get_results
        batch_success = {'id1': True, 'id2': True} 
        batch_failed_set = {'id3_fail'}
        
        mock_bh_instance.get_results.return_value = batch_success
        mock_bh_instance.get_failed_requests.return_value = batch_failed_set

        with patch('src.drive_api.BatchHandler', return_value=mock_bh_instance):
            self.drive_api.batch_handler = None # Ensure new mock handler is used
            results = self.drive_api.move_files_to_trash_batch(file_ids)
            
            mock_bh_instance.execute.assert_called_once()
            
            expected_results = {
                'id1': True,
                'id2': True,
                'id3_fail': False # Failed items should be marked as False
            }
            self.assertEqual(results, expected_results)

if __name__ == '__main__':
    unittest.main()

class TestDuplicateScannerCLI(unittest.TestCase):
    """Test suite for the command-line interface of duplicate_scanner.py."""

    @patch('src.duplicate_scanner.get_service') # Patches for src_main
    @patch('src.duplicate_scanner.DriveAPI')   # Patches for src_main
    @patch('src.duplicate_scanner.DuplicateScanner') # Patches for src_main
    @patch('src.duplicate_scanner.write_to_csv')     # Patches for src_main
    @patch('builtins.print')
    def test_main_basic_run(self, mock_print, mock_write_to_csv, mock_Scanner_class, mock_DriveAPI_class, mock_get_service): # For src_main
        """Test basic run with no arguments."""
        # Setup mocks
        mock_service_instance = MagicMock()
        mock_get_service.return_value = mock_service_instance
        mock_drive_api_instance = MagicMock()
        mock_DriveAPI_class.return_value = mock_drive_api_instance
        mock_scanner_instance = MagicMock()
        mock_scanner_instance.scan.return_value = [MagicMock(files=[1, 2], wasted_space=1024)] # Simulate some duplicates
        mock_Scanner_class.return_value = mock_scanner_instance

        with patch('sys.argv', ['src/duplicate_scanner.py']):
            src_main() # Call the aliased main from src

        mock_get_service.assert_called_once()
        mock_DriveAPI_class.assert_called_once_with(mock_service_instance)
        mock_Scanner_class.assert_called_once_with(mock_drive_api_instance)
        mock_scanner_instance.scan.assert_called_once_with(delete=False, force_refresh=False)
        mock_write_to_csv.assert_called_once()
        # Check for summary print statements
        self.assertTrue(any("Found 1 duplicate groups" in call_args[0][0] for call_args in mock_print.call_args_list if call_args[0]))
        self.assertTrue(any("Total duplicate files: 1" in call_args[0][0] for call_args in mock_print.call_args_list if call_args[0]))
        self.assertTrue(any("Total wasted space: 1.00 KB" in call_args[0][0] for call_args in mock_print.call_args_list if call_args[0]))


    @patch('src.duplicate_scanner.get_service')
    @patch('src.duplicate_scanner.DriveAPI')   # For src_main
    @patch('src.duplicate_scanner.DuplicateScanner') # For src_main
    @patch('src.duplicate_scanner.write_to_csv')     # For src_main
    @patch('builtins.print')
    def test_main_refresh_cache_argument(self, mock_print, mock_write_to_csv, mock_Scanner_class, mock_DriveAPI_class, mock_get_service): # For src_main
        """Test --refresh-cache argument."""
        mock_service_instance = MagicMock()
        mock_get_service.return_value = mock_service_instance
        mock_drive_api_instance = MagicMock()
        mock_DriveAPI_class.return_value = mock_drive_api_instance
        mock_scanner_instance = MagicMock()
        mock_scanner_instance.scan.return_value = []
        mock_Scanner_class.return_value = mock_scanner_instance

        with patch('sys.argv', ['src/duplicate_scanner.py', '--refresh-cache']):
            src_main() # Call the aliased main from src
        
        mock_scanner_instance.scan.assert_called_once_with(delete=False, force_refresh=True)


    @patch('src.duplicate_scanner.get_service')
    @patch('src.duplicate_scanner.DriveAPI')   # For src_main
    @patch('src.duplicate_scanner.DuplicateScanner') # For src_main
    @patch('src.duplicate_scanner.write_to_csv')     # For src_main
    @patch('builtins.print')
    def test_main_delete_argument(self, mock_print, mock_write_to_csv, mock_Scanner_class, mock_DriveAPI_class, mock_get_service): # For src_main
        """Test --delete argument."""
        mock_service_instance = MagicMock()
        mock_get_service.return_value = mock_service_instance
        mock_drive_api_instance = MagicMock()
        mock_DriveAPI_class.return_value = mock_drive_api_instance
        mock_scanner_instance = MagicMock()
        mock_scanner_instance.scan.return_value = [] # No duplicates for simplicity
        mock_Scanner_class.return_value = mock_scanner_instance

        with patch('sys.argv', ['src/duplicate_scanner.py', '--delete']):
            src_main() # Call the aliased main from src

        mock_scanner_instance.scan.assert_called_once_with(delete=True, force_refresh=False)


    @patch('src.duplicate_scanner.get_service')      # For src_main
    @patch('src.duplicate_scanner.logging.error') # For src_main
    @patch('builtins.print') # To see if any other print occurs
    def test_main_authentication_failure(self, mock_print, mock_logging_error, mock_get_service): # For src_main
        """Test authentication failure."""
        mock_get_service.return_value = None

        with patch('sys.argv', ['src/duplicate_scanner.py']):
            src_main() # Call the aliased main from src
        
        mock_get_service.assert_called_once()
        mock_logging_error.assert_called_with("Failed to get Google Drive service")
        # Ensure no other output like summary if auth fails
        self.assertFalse(any("Found" in call_args[0][0] for call_args in mock_print.call_args_list if call_args[0]))


    @patch('sys.stderr', new_callable=io.StringIO) # Capture stderr
    @patch('sys.exit') # To prevent test runner from exiting
    def test_main_invalid_argument(self, mock_exit, mock_stderr): # For src_main
        """Test with an invalid argument."""
        with patch('sys.argv', ['src/duplicate_scanner.py', '--invalid-option']):
            try:
                src_main() # Call the aliased main from src
            except SystemExit: # Argparse calls sys.exit on error
                pass
        
        self.assertTrue("unrecognized arguments: --invalid-option" in mock_stderr.getvalue())
        mock_exit.assert_called_with(2)

    # Note: src/duplicate_scanner.py currently implements --delete and --refresh-cache.
    # The subtask mentioned testing for --no-cache, --folders, --no-export.
    # test_main_refresh_cache_argument covers a similar functionality to --no-cache.
    # --folders and --no-export are not implemented in src/duplicate_scanner.py.
    # If they were, tests would follow a similar pattern to the ones above.
    # For example, for --no-export:
    # @patch('src.duplicate_scanner.get_service')
    # @patch('src.duplicate_scanner.DriveAPI')
    # @patch('src.duplicate_scanner.DuplicateScanner')
    # @patch('src.duplicate_scanner.write_to_csv') # Key mock
    # def test_main_no_export(self, mock_write_csv, ...):
    #    with patch('sys.argv', ['src/duplicate_scanner.py', '--no-export']):
    #        main()
    #    mock_write_csv.assert_not_called()

    # For --folders:
    # @patch('src.duplicate_scanner.get_service')
    # @patch('src.duplicate_scanner.DriveAPI')
    # @patch('src.duplicate_scanner.DuplicateScannerWithFolders') # Key mock
    # @patch('src.duplicate_scanner.DuplicateScanner') # Original
    # def test_main_folders(self, mock_original_scanner, mock_folder_scanner, ...):
    #    with patch('sys.argv', ['src/duplicate_scanner.py', '--folders']):
    #        main()
    #    mock_folder_scanner.assert_called_once()
    #    mock_original_scanner.assert_not_called()