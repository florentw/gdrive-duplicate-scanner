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

# Add parent directory to Python path to import modules
sys.path.append(str(Path(__file__).parent.parent))

from utils import get_human_readable_size
from cache import MetadataCache
from drive_api import DriveAPI
from batch import BatchHandler
from models import DuplicateGroup, DuplicateFolder
from scanner import DuplicateScanner
from export import write_to_csv
from config import CACHE_EXPIRY_HOURS, BATCH_SIZE, SAVE_INTERVAL_MINUTES

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
            if file.startswith('duplicate_files_') and file.endswith('.csv'):
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
        mock_responses = {
            'id1': {'id': 'id1', 'name': 'file1.txt', 'size': '1024'},
            'id2': {'id': 'id2', 'name': 'file2.txt', 'size': '2048'}
        }
        
        # Mock batch handler
        mock_handler = Mock()
        mock_handler.results = mock_responses
        mock_handler.get_results.return_value = mock_responses
        mock_handler.get_failed_requests.return_value = set()
        
        # Mock _get_batch_handler to return our mock handler
        with patch.object(DriveAPI, '_get_batch_handler', return_value=mock_handler):
            result = self.drive_api.get_files_metadata_batch(['id1', 'id2'])
            self.assertEqual(result, mock_responses)
            mock_handler.execute.assert_called_once()

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
        
        # Mock _get_batch_handler to return our mock handler
        with patch.object(DriveAPI, '_get_batch_handler', return_value=mock_handler):
            result = self.drive_api.get_files_metadata_batch(['test_id'])
            self.assertEqual(result, {'test_id': mock_response})

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
        mock_pairs = [
            {
                'group_id': 1,
                'file': {'id': 'id1', 'name': 'file1.txt', 'size': '1024'},
                'duplicate': {'id': 'id2', 'name': 'file2.txt', 'size': '1024'}
            }
        ]
        
        # Mock parent folder metadata
        with patch.object(self.drive_api, 'get_file_metadata') as mock_get_metadata:
            mock_get_metadata.return_value = {'name': 'test_folder'}
            
            write_to_csv(mock_pairs, self.drive_api)
            
            # Check if CSV file was created
            csv_files = [f for f in os.listdir(self.test_dir) if f.startswith('duplicate_files_') and f.endswith('.csv')]
            self.assertEqual(len(csv_files), 1)
            
            # Verify CSV content
            with open(os.path.join(self.test_dir, csv_files[0]), 'r') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                self.assertEqual(len(rows), 1)
                self.assertEqual(rows[0]['File Name'], 'file1.txt')
                self.assertEqual(rows[0]['Duplicate File Name'], 'file2.txt')

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
        """Test listing files from Google Drive."""
        mock_files = [
            {'id': 'id1', 'name': 'file1.txt'},
            {'id': 'id2', 'name': 'file2.txt'}
        ]
        
        # Mock API response
        self.mock_files_service.list.return_value.execute.return_value = {
            'files': mock_files
        }
        
        result = self.drive_api.list_files()
        
        self.assertEqual(result, mock_files)
        self.mock_files_service.list.assert_called_once()

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
        
        folder = DuplicateFolder(folder_id, folder_meta, duplicate_files)
        
        # Test initial state
        self.assertEqual(folder.folder_id, folder_id)
        self.assertEqual(folder.folder_meta, folder_meta)
        self.assertEqual(folder.duplicate_files, duplicate_files)
        self.assertEqual(folder.total_size, 0)
        
        # Test metadata update
        file_metadata = {
            'file1': {'size': '1024'},
            'file2': {'size': '2048'}
        }
        folder.update_metadata(file_metadata)
        self.assertEqual(folder.total_size, 3072)
        
        # Test duplicate only check
        self.assertTrue(folder.check_if_duplicate_only({'file1', 'file2'}))
        self.assertFalse(folder.check_if_duplicate_only({'file1', 'file2', 'file3'}))

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
            
            scanner = DuplicateScanner(self.drive_api)
            groups = scanner.scan()
            
            self.assertEqual(len(groups), 1)  # One group of duplicates
            self.assertEqual(len(groups[0].files), 2)  # Two files in the group

    def test_batch_handler_operations(self):
        """Test BatchHandler operations and contract."""
        # Setup
        mock_service = MagicMock()
        mock_batch = MagicMock()
        mock_service.new_batch_http_request.return_value = mock_batch
        handler = BatchHandler(mock_service, self.test_cache)
        
        # Test adding requests
        file_ids = ['id1', 'id2', 'id3']
        for file_id in file_ids:
            handler.add_metadata_request(file_id)
        
        # Verify batch execution
        mock_batch.execute.return_value = None
        handler.execute()
        mock_batch.execute.assert_called_once()
        
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
        handler = BatchHandler(mock_service, self.test_cache)
        
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

    def test_batch_handler_cache_interaction(self):
        """Test BatchHandler cache interaction."""
        # Setup
        mock_service = MagicMock()
        mock_batch = MagicMock()
        mock_service.new_batch_http_request.return_value = mock_batch
        handler = BatchHandler(mock_service, self.test_cache)
        
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
        
        # Mock the get_file_metadata method to simulate individual retries
        with patch.object(DriveAPI, '_get_batch_handler', return_value=mock_handler), \
             patch.object(DriveAPI, 'get_file_metadata', return_value=None):
            
            # Test metadata batch
            result = api.get_files_metadata_batch(file_ids)
            self.assertEqual(result['id1'], mock_responses['id1'])
            self.assertEqual(result['id2'], mock_responses['id2'])
            self.assertNotIn('id3', result)
            
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
        api = DriveAPI(mock_service, self.test_cache)
        
        # Generate more file IDs than BATCH_SIZE
        file_ids = [f'id{i}' for i in range(BATCH_SIZE + 5)]
        mock_responses = {f'id{i}': {'id': f'id{i}'} for i in range(BATCH_SIZE)}
        
        with patch.object(BatchHandler, 'execute') as mock_execute, \
             patch.object(BatchHandler, 'get_results', return_value=mock_responses):
            
            # Test metadata batch
            api.get_files_metadata_batch(file_ids)
            # Should be called twice: once for BATCH_SIZE items, once for remaining 5
            self.assertEqual(mock_execute.call_count, 2)

if __name__ == '__main__':
    unittest.main()
    