import unittest
from unittest.mock import Mock, patch, MagicMock
import os
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
    MetadataCache,
    DriveAPI
)

class TestDuplicateScanner(unittest.TestCase):
    """Test suite for duplicate scanner functionality."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_service = Mock()
        self.test_dir = tempfile.mkdtemp()
        self.test_cache_file = os.path.join(self.test_dir, 'test_cache.json')
        self.test_cache = MetadataCache(self.test_cache_file)
        self.drive_api = DriveAPI(self.mock_service, self.test_cache)

    def tearDown(self):
        """Clean up test fixtures after each test method."""
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

    def test_get_file_metadata_caching(self):
        """Test that get_file_metadata caches results."""
        mock_metadata = {
            'id': 'test_id',
            'name': 'test_file.txt',
            'parents': ['parent_id'],
            'size': '1024',
        }
        
        mock_get = Mock()
        mock_get.execute.return_value = mock_metadata
        mock_files = Mock()
        mock_files.get.return_value = mock_get
        self.mock_service.files.return_value = mock_files

        # First call should make an API request
        result1 = get_file_metadata(self.drive_api, 'test_id')
        # Second call should use cached result
        result2 = get_file_metadata(self.drive_api, 'test_id')

        self.assertEqual(result1, result2)
        mock_files.get.assert_called_once_with(
            fileId='test_id', 
            fields='id, name, parents, size, md5Checksum, mimeType, trashed'
        )

    def test_get_file_metadata_error(self):
        """Test get_file_metadata error handling."""
        mock_get = Mock()
        mock_get.execute.side_effect = Exception("API Error")
        self.mock_service.files.return_value.get.return_value = mock_get
        
        result = get_file_metadata(self.drive_api, 'test_id')
        self.assertIsNone(result)

    def test_get_file_metadata_network_error(self):
        """Test get_file_metadata with network errors."""
        mock_get = Mock()
        mock_get.execute.side_effect = ConnectionError("Network error")
        self.mock_service.files.return_value.get.return_value = mock_get
        
        result = get_file_metadata(self.drive_api, 'test_id')
        self.assertIsNone(result)

    def test_write_to_csv(self):
        """Test CSV writing functionality."""
        file1 = {'id': 'id1', 'name': 'file1.txt', 'size': '1024', 'md5Checksum': 'hash1'}
        file2 = {'id': 'id2', 'name': 'file2.txt', 'size': '1024', 'md5Checksum': 'hash1'}
        duplicate_pairs = [(file1, file2)]

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

        mock_file = MagicMock()
        with patch('builtins.open', create=True) as mock_open, \
             patch('duplicate_scanner.get_file_metadata', side_effect=lambda _, file_id: mock_metadata.get(file_id)):
            mock_open.return_value.__enter__.return_value = mock_file
            csv_filename = write_to_csv(duplicate_pairs, self.drive_api)
            
        self.assertTrue(csv_filename.startswith('drive_duplicates_'))
        self.assertTrue(csv_filename.endswith('.csv'))
        
        # Verify CSV content
        calls = mock_file.write.call_args_list
        self.assertTrue(len(calls) > 0)
        header_call = calls[0][0][0]
        self.assertIn('File Name', header_call)
        self.assertIn('Full Path', header_call)
        self.assertIn('Size (Bytes)', header_call)
        self.assertIn('MD5 Checksum', header_call)

    def test_write_to_csv_file_error(self):
        """Test CSV writing with file system errors."""
        file1 = {'id': 'id1', 'name': 'file1.txt', 'size': '1024', 'md5Checksum': 'hash1'}
        file2 = {'id': 'id2', 'name': 'file2.txt', 'size': '1024', 'md5Checksum': 'hash1'}
        duplicate_pairs = [(file1, file2)]

        with patch('builtins.open', side_effect=IOError("File error")):
            with self.assertRaises(IOError):
                write_to_csv(duplicate_pairs, self.drive_api)

    def test_drive_api_list_files(self):
        """Test the public DriveAPI.list_files method."""
        mock_files = [
            {'id': 'id1', 'name': 'file1.txt', 'size': '1024', 'md5Checksum': 'hash1'},
            {'id': 'id2', 'name': 'file2.txt', 'size': '1024', 'md5Checksum': 'hash2'}
        ]
        
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
        mock_files_service.list.assert_called_once()

        # Test force refresh
        result3 = self.drive_api.list_files(force_refresh=True)
        self.assertEqual(result3, mock_files)
        self.assertEqual(mock_files_service.list.call_count, 2)

    def test_drive_api_list_files_error(self):
        """Test DriveAPI.list_files with API errors."""
        mock_execute = Mock()
        mock_execute.execute.side_effect = Exception("API Error")
        mock_files_service = Mock()
        mock_files_service.list.return_value = mock_execute
        self.mock_service.files.return_value = mock_files_service

        # Test API error with no cache
        with self.assertRaises(Exception):
            self.drive_api.list_files(force_refresh=True)

        # Test API error with cache fallback
        self.drive_api.cache.set('all_files', [{'id': 'cached'}])
        result = self.drive_api.list_files(force_refresh=True)
        self.assertEqual(result, [{'id': 'cached'}])

    def test_handle_duplicate(self):
        """Test duplicate file handling."""
        file1 = {'id': 'id1', 'name': 'file1.txt'}
        file2 = {'id': 'id2', 'name': 'file2.txt'}
        duplicate_folders = defaultdict(set)

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

        self.assertEqual(duplicate_folders['parent1'], {'id1'})
        self.assertEqual(duplicate_folders['parent2'], {'id2'})

    def test_handle_duplicate_missing_metadata(self):
        """Test handle_duplicate with missing metadata."""
        file1 = {'id': 'id1', 'name': 'file1.txt'}
        file2 = {'id': 'id2', 'name': 'file2.txt'}
        duplicate_folders = defaultdict(set)

        # Test with missing metadata
        with patch('duplicate_scanner.get_files_metadata_batch', return_value={}):
            handle_duplicate(self.drive_api, file1, file2, duplicate_folders, delete=False)
            self.assertEqual(duplicate_folders, {})

    def test_find_duplicates(self):
        """Test the main duplicate finding functionality."""
        mock_files = self._setup_mock_files()
        mock_metadata = self._setup_mock_metadata()

        # Capture stdout
        captured_output = StringIO()
        sys.stdout = captured_output

        try:
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
            sys.stdout = sys.__stdout__

    def test_find_duplicates_api_error(self):
        """Test find_duplicates with API errors."""
        # Test with API error in list_files
        with patch.object(self.drive_api, 'list_files', side_effect=Exception("API Error")):
            with self.assertRaises(Exception):
                find_duplicates(self.drive_api, delete=False, force_refresh=True)

        # Test with API error in get_files_metadata_batch
        mock_files = self._setup_mock_files()
        with patch.object(self.drive_api, 'list_files', return_value=mock_files), \
             patch('duplicate_scanner.get_files_metadata_batch', side_effect=Exception("API Error")):
            duplicate_groups = find_duplicates(self.drive_api, delete=False, force_refresh=True)
            self.assertEqual(duplicate_groups, [])  # Should return empty list on error

if __name__ == '__main__':
    unittest.main()
    