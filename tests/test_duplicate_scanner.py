import unittest
from unittest.mock import Mock, patch, MagicMock, call
import json
import os
from datetime import datetime
from collections import defaultdict
import sys
from pathlib import Path
import tempfile
import shutil
from io import StringIO

# Add parent directory to Python path to import duplicate_scanner
sys.path.append(str(Path(__file__).parent.parent))

from duplicate_scanner import (
    get_human_readable_size,
    get_file_metadata,
    handle_duplicate,
    write_to_csv,
    find_duplicates,
    get_cache_key,
    MetadataCache,
    DriveAPI,
    BatchHandler
)

class TestDuplicateScanner(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_service = Mock()
        # Create a temporary directory for test cache
        self.test_dir = tempfile.mkdtemp()
        self.test_cache_file = os.path.join(self.test_dir, 'test_cache.json')
        # Create a test-specific cache instance
        self.test_cache = MetadataCache(self.test_cache_file)
        # Create a DriveAPI instance with test cache
        self.drive_api = DriveAPI(self.mock_service, self.test_cache)

    def tearDown(self):
        """Clean up test fixtures after each test method."""
        # Remove the temporary directory and its contents
        shutil.rmtree(self.test_dir)

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

    def test_metadata_cache(self):
        """Test the MetadataCache class."""
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

    def test_get_file_metadata_caching(self):
        """Test that get_file_metadata caches results."""
        # Setup mock response
        mock_metadata = {
            'id': 'test_id',
            'name': 'test_file.txt',
            'parents': ['parent_id'],
            'size': '1024',
        }
        
        # Setup the mock chain
        mock_get = Mock()
        mock_get.execute.return_value = mock_metadata
        mock_files = Mock()
        mock_files.get.return_value = mock_get
        self.mock_service.files.return_value = mock_files

        # First call should make an API request
        result1 = get_file_metadata(self.drive_api, 'test_id')
        # Second call should use cached result
        result2 = get_file_metadata(self.drive_api, 'test_id')

        # Verify results are identical
        self.assertEqual(result1, result2)
        # Verify API was called only once with correct parameters
        expected_call = call(fileId='test_id', 
                           fields='id, name, parents, size, md5Checksum, mimeType, trashed')
        mock_files.get.assert_called_once_with(**expected_call.kwargs)
        mock_get.execute.assert_called_once()

    def test_get_file_metadata_error(self):
        """Test get_file_metadata error handling."""
        mock_get = Mock()
        mock_get.execute.side_effect = Exception("API Error")
        self.mock_service.files.return_value.get.return_value = mock_get
        
        result = get_file_metadata(self.drive_api, 'test_id')
        self.assertIsNone(result)

    @patch('builtins.open', create=True)
    def test_write_to_csv(self, mock_open):
        """Test CSV writing functionality."""
        # Setup mock files data
        file1 = {'id': 'id1', 'name': 'file1.txt', 'size': '1024', 'md5Checksum': 'hash1'}
        file2 = {'id': 'id2', 'name': 'file2.txt', 'size': '1024', 'md5Checksum': 'hash1'}
        duplicate_pairs = [(file1, file2)]

        # Mock get_file_metadata responses
        def mock_get_metadata(drive_api, file_id):
            metadata = {
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
            return metadata.get(file_id)

        # Mock file operations
        mock_file = MagicMock()
        mock_open.return_value.__enter__.return_value = mock_file

        with patch('duplicate_scanner.get_file_metadata', side_effect=mock_get_metadata):
            csv_filename = write_to_csv(duplicate_pairs, self.drive_api)
            
        # Verify filename format
        self.assertTrue(csv_filename.startswith('drive_duplicates_'))
        self.assertTrue(csv_filename.endswith('.csv'))
        
        # Verify file was opened for writing
        mock_open.assert_called_once()
        
        # Verify CSV content
        calls = mock_file.write.call_args_list
        self.assertTrue(len(calls) > 0)
        
        # Verify header was written
        header_call = calls[0][0][0]
        self.assertIn('File Name', header_call)
        self.assertIn('Full Path', header_call)
        self.assertIn('Size (Bytes)', header_call)
        self.assertIn('MD5 Checksum', header_call)

    def test_handle_duplicate(self):
        """Test duplicate file handling."""
        # Setup mock files
        file1 = {'id': 'id1', 'name': 'file1.txt'}
        file2 = {'id': 'id2', 'name': 'file2.txt'}
        duplicate_folders = defaultdict(set)

        # Mock get_files_metadata_batch response
        mock_metadata = {
            'id1': {
                'id': 'id1',
                'name': 'file1.txt',
                'parents': ['parent1'],
                'size': '1024'
            },
            'id2': {
                'id': 'id2',
                'name': 'file2.txt',
                'parents': ['parent2'],
                'size': '1024'
            }
        }

        with patch('duplicate_scanner.get_files_metadata_batch', return_value=mock_metadata):
            handle_duplicate(self.drive_api, file1, file2, duplicate_folders, delete=False)

        # Verify duplicate folders were updated
        self.assertEqual(duplicate_folders['parent1'], {'id1'})
        self.assertEqual(duplicate_folders['parent2'], {'id2'})

    def test_find_duplicates(self):
        """Test the main duplicate finding functionality."""
        # Mock file list
        mock_files = [
            {'id': 'id1', 'name': 'file1.txt', 'md5Checksum': 'hash1', 'size': '1024'},
            {'id': 'id2', 'name': 'file2.txt', 'md5Checksum': 'hash1', 'size': '1024'},  # Duplicate
            {'id': 'id3', 'name': 'file3.txt', 'md5Checksum': 'hash2', 'size': '2048'}   # Unique
        ]

        # Mock metadata responses
        mock_metadata = {
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

        # Capture stdout
        captured_output = StringIO()
        sys.stdout = captured_output

        try:
            # Mock API responses
            with patch.object(self.drive_api, 'list_files', return_value=mock_files), \
                 patch('duplicate_scanner.get_files_metadata_batch', return_value=mock_metadata), \
                 patch('duplicate_scanner._handle_group_deletion') as mock_handle_deletion:
                
                duplicate_groups = find_duplicates(self.drive_api, delete=False, force_refresh=True)
                
                # Verify duplicate groups
                self.assertEqual(len(duplicate_groups), 1)  # One group of duplicates
                self.assertEqual(len(duplicate_groups[0]), 2)  # Two files in the group
                self.assertEqual(duplicate_groups[0][0]['id'], 'id1')
                self.assertEqual(duplicate_groups[0][1]['id'], 'id2')

                # Verify output
                output = captured_output.getvalue()
                self.assertIn('Found duplicate group (2 files):', output)
                self.assertIn('file1.txt (Size: 1.00 KB)', output)
                self.assertIn('file2.txt (Size: 1.00 KB)', output)
                self.assertIn('Total files scanned: 3', output)
                self.assertIn('Duplicate groups found: 1', output)
                self.assertIn('Total duplicate files: 2', output)
                self.assertIn('Wasted space: 1.00 KB', output)
        finally:
            # Restore stdout
            sys.stdout = sys.__stdout__

    def test_drive_api_list_files(self):
        """Test the public DriveAPI.list_files method."""
        # Mock API response
        mock_files = [
            {'id': 'id1', 'name': 'file1.txt', 'size': '1024', 'md5Checksum': 'hash1'},
            {'id': 'id2', 'name': 'file2.txt', 'size': '1024', 'md5Checksum': 'hash2'}
        ]
        
        # Setup the mock chain properly
        mock_execute = Mock()
        mock_execute.execute.return_value = {
            'files': mock_files,
            'nextPageToken': None
        }
        
        mock_files_service = Mock()
        mock_files_service.list.return_value = mock_execute
        self.mock_service.files.return_value = mock_files_service

        # Test successful case
        result = self.drive_api.list_files(force_refresh=True)
        self.assertEqual(result, mock_files)
        mock_files_service.list.assert_called_once_with(
            q="trashed = false",
            pageSize=1000,
            fields="nextPageToken, files(id, name, size, md5Checksum, trashed, parents)",
            pageToken=None
        )

        # Test caching
        result2 = self.drive_api.list_files(force_refresh=False)
        self.assertEqual(result2, mock_files)
        # Should not make another API call
        mock_files_service.list.assert_called_once()

        # Test force refresh
        result3 = self.drive_api.list_files(force_refresh=True)
        self.assertEqual(result3, mock_files)
        # Should make another API call
        self.assertEqual(mock_files_service.list.call_count, 2)

if __name__ == '__main__':
    unittest.main()
    