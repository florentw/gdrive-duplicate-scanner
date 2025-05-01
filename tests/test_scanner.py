import unittest
from unittest.mock import Mock, patch, MagicMock
import os
import json
from datetime import datetime
from src.cache import MetadataCache
from src.drive_api import DriveAPI
from src.scanner import BaseDuplicateScanner, DuplicateScanner, DuplicateScannerWithFolders
from src.models import DuplicateGroup, DuplicateFolder

class TestBaseDuplicateScanner(unittest.TestCase):
    def setUp(self):
        self.drive_api = Mock()
        self.cache = Mock()
        self.scanner = BaseDuplicateScanner(self.drive_api, self.cache)

    def test_filter_valid_files(self):
        test_files = [
            {'id': '1', 'size': '100', 'mimeType': 'text/plain'},
            {'id': '2', 'size': '0', 'mimeType': 'text/plain'},  # Should be filtered out
            {'id': '3', 'size': '200', 'mimeType': 'application/vnd.google-apps.document'},  # Should be filtered out
            {'id': '4', 'size': '300', 'mimeType': 'image/jpeg'}
        ]
        
        filtered = self.scanner._filter_valid_files(test_files)
        self.assertEqual(len(filtered), 2)
        self.assertEqual(filtered[0]['id'], '1')
        self.assertEqual(filtered[1]['id'], '4')

    def test_group_files_by_size(self):
        test_files = [
            {'id': '1', 'size': '100'},
            {'id': '2', 'size': '100'},
            {'id': '3', 'size': '200'},
        ]
        
        size_groups = self.scanner._group_files_by_size(test_files)
        self.assertEqual(len(size_groups), 2)
        self.assertEqual(len(size_groups['100']), 2)
        self.assertEqual(len(size_groups['200']), 1)

    def test_group_files_by_md5(self):
        test_files = [
            {'id': '1', 'md5Checksum': 'abc123'},
            {'id': '2', 'md5Checksum': 'abc123'},
            {'id': '3', 'md5Checksum': 'def456'},
            {'id': '4', 'md5Checksum': ''}  # Should be ignored
        ]
        
        md5_groups = self.scanner._group_files_by_md5(test_files)
        self.assertEqual(len(md5_groups), 2)
        self.assertEqual(len(md5_groups['abc123']), 2)
        self.assertEqual(len(md5_groups['def456']), 1)

    def test_process_duplicate_group(self):
        test_files = [
            {'id': '1', 'name': 'file1.txt', 'size': '100'},
            {'id': '2', 'name': 'file2.txt', 'size': '100'}
        ]
        test_metadata = {
            '1': {'id': '1', 'name': 'file1.txt', 'size': '100'},
            '2': {'id': '2', 'name': 'file2.txt', 'size': '100'}
        }
        
        self.scanner._process_duplicate_group(test_files, test_metadata)
        self.assertEqual(len(self.scanner.duplicate_groups), 1)
        self.assertEqual(len(self.scanner.duplicate_groups[0].files), 2)

class TestMetadataCache(unittest.TestCase):
    def setUp(self):
        self.test_cache_file = 'test_cache.json'
        self.test_cache = MetadataCache(self.test_cache_file)

    def tearDown(self):
        if os.path.exists(self.test_cache_file):
            os.remove(self.test_cache_file)

    def test_basic_cache_operations(self):
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

    def test_cache_persistence(self):
        """Test that cache persists to disk."""
        # Add some data
        test_data = {'test_key': 'test_value'}
        self.test_cache.update(test_data)
        
        # Force save to disk
        self.test_cache._save(force=True)
        
        # Create new cache instance
        new_cache = MetadataCache(self.test_cache_file)
        
        # Verify data was loaded
        self.assertEqual(new_cache.get('test_key'), 'test_value')

    def test_cache_key_mismatch(self):
        """Test cache behavior when credentials change."""
        # Add some data
        self.test_cache.set('test_key', 'test_value')
        
        # Mock get_cache_key to return different value
        with patch('src.cache.get_cache_key', return_value='different_key'):
            new_cache = MetadataCache(self.test_cache_file)
            self.assertIsNone(new_cache.get('test_key'))

    def test_cache_file_errors(self):
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

    def test_cache_context_manager(self):
        """Test cache context manager functionality."""
        with MetadataCache(self.test_cache_file) as cache:
            cache.set('test_key', 'test_value')
            self.assertEqual(cache.get('test_key'), 'test_value')
        
        # Cache should be saved after context exit
        new_cache = MetadataCache(self.test_cache_file)
        self.assertEqual(new_cache.get('test_key'), 'test_value')

    def test_cache_files_and_folders(self):
        """Test caching of files and folders."""
        test_files = [{'id': '1', 'name': 'file1'}, {'id': '2', 'name': 'file2'}]
        test_folders = [{'id': '3', 'name': 'folder1'}, {'id': '4', 'name': 'folder2'}]
        
        # Test caching files
        self.test_cache.cache_files(test_files)
        self.assertEqual(self.test_cache.get_all_files(), test_files)
        
        # Test caching folders
        self.test_cache.cache_folders(test_folders)
        self.assertEqual(self.test_cache.get_all_folders(), test_folders)

class TestDuplicateScanner(unittest.TestCase):
    def setUp(self):
        self.drive_api = Mock()
        self.cache = Mock()
        self.scanner = DuplicateScanner(self.drive_api, self.cache)

    def test_scan_with_cache(self):
        """Test scanner using cached data."""
        test_files = [
            {'id': '1', 'size': '100', 'md5Checksum': 'abc123', 'mimeType': 'text/plain'},
            {'id': '2', 'size': '100', 'md5Checksum': 'abc123', 'mimeType': 'text/plain'}
        ]
        self.cache.get_all_files.return_value = test_files
        
        self.scanner.scan()
        self.assertEqual(len(self.scanner.duplicate_groups), 1)
        self.cache.get_all_files.assert_called_once()
        self.drive_api.list_files.assert_not_called()

    def test_scan_without_cache(self):
        """Test scanner when cache is empty."""
        test_files = [
            {'id': '1', 'size': '100', 'md5Checksum': 'abc123', 'mimeType': 'text/plain'},
            {'id': '2', 'size': '100', 'md5Checksum': 'abc123', 'mimeType': 'text/plain'}
        ]
        self.cache.get_all_files.return_value = None
        self.drive_api.list_files.return_value = test_files
        
        self.scanner.scan()
        self.assertEqual(len(self.scanner.duplicate_groups), 1)
        self.cache.get_all_files.assert_called_once()
        self.drive_api.list_files.assert_called_once()
        self.cache.cache_files.assert_called_once_with(test_files)

class TestDuplicateScannerWithFolders(unittest.TestCase):
    def setUp(self):
        self.drive_api = Mock()
        self.cache = Mock()
        self.scanner = DuplicateScannerWithFolders(self.drive_api, self.cache)

    def test_scan_with_cache(self):
        """Test scanner with folders using cached data."""
        test_files = [
            {'id': '1', 'size': '100', 'md5Checksum': 'abc123', 'mimeType': 'text/plain', 'parents': ['folder1']},
            {'id': '2', 'size': '100', 'md5Checksum': 'abc123', 'mimeType': 'text/plain', 'parents': ['folder1']}
        ]
        test_folders = [
            {'id': 'folder1', 'name': 'Test Folder'}
        ]
        self.cache.get_all_files.return_value = test_files
        self.cache.get_all_folders.return_value = test_folders
        
        self.scanner.scan()
        self.assertEqual(len(self.scanner.duplicate_groups), 1)
        self.assertEqual(len(self.scanner.duplicate_files_in_folders), 1)
        self.cache.get_all_files.assert_called_once()
        self.cache.get_all_folders.assert_called_once()
        self.drive_api.list_all_files_and_folders.assert_not_called()

    def test_scan_without_cache(self):
        """Test scanner with folders when cache is empty."""
        test_files = [
            {'id': '1', 'size': '100', 'md5Checksum': 'abc123', 'mimeType': 'text/plain', 'parents': ['folder1']},
            {'id': '2', 'size': '100', 'md5Checksum': 'abc123', 'mimeType': 'text/plain', 'parents': ['folder1']}
        ]
        test_folders = [
            {'id': 'folder1', 'name': 'Test Folder'}
        ]
        self.cache.get_all_files.return_value = None
        self.cache.get_all_folders.return_value = None
        self.drive_api.list_all_files_and_folders.return_value = (test_files, test_folders)
        
        self.scanner.scan()
        self.assertEqual(len(self.scanner.duplicate_groups), 1)
        self.assertEqual(len(self.scanner.duplicate_files_in_folders), 1)
        self.cache.get_all_files.assert_called_once()
        self.cache.get_all_folders.assert_called_once()
        self.drive_api.list_all_files_and_folders.assert_called_once()
        self.cache.cache_files.assert_called_once_with(test_files)
        self.cache.cache_folders.assert_called_once_with(test_folders)

    def test_analyze_folder_structures(self):
        test_folders = [
            {'id': 'folder1', 'name': 'Test Folder 1'},
            {'id': 'folder2', 'name': 'Test Folder 2'}
        ]
        
        # Create duplicate groups that reference these folders
        self.scanner.duplicate_groups = [
            DuplicateGroup(
                files=[
                    {'id': '1', 'parents': ['folder1']},
                    {'id': '2', 'parents': ['folder2']}
                ],
                metadata={
                    '1': {'id': '1', 'parents': ['folder1']},
                    '2': {'id': '2', 'parents': ['folder2']}
                }
            )
        ]
        
        self.scanner._analyze_folder_structures(test_folders)
        self.assertEqual(len(self.scanner.duplicate_files_in_folders), 2)
        self.assertIn('folder1', self.scanner.duplicate_files_in_folders)
        self.assertIn('folder2', self.scanner.duplicate_files_in_folders)

    def test_duplicate_only_folders(self):
        """Test identification of folders containing only duplicate files."""
        # Setup test data
        test_files = [
            {'id': '1', 'size': '100', 'md5Checksum': 'abc123', 'mimeType': 'text/plain', 'parents': ['folder1']},
            {'id': '2', 'size': '100', 'md5Checksum': 'abc123', 'mimeType': 'text/plain', 'parents': ['folder1']},
            {'id': '3', 'size': '200', 'md5Checksum': 'def456', 'mimeType': 'text/plain', 'parents': ['folder2']},
            {'id': '4', 'size': '200', 'md5Checksum': 'def456', 'mimeType': 'text/plain', 'parents': ['folder2']}
        ]
        test_folders = [
            {'id': 'folder1', 'name': 'Test Folder 1'},
            {'id': 'folder2', 'name': 'Test Folder 2'}
        ]
        
        # Mock API responses
        self.drive_api.list_files.return_value = test_files
        self.cache.get_all_files.return_value = test_files
        self.cache.get_all_folders.return_value = test_folders
        
        # Run scan
        self.scanner.scan()
        
        # Verify results
        self.assertEqual(len(self.scanner.duplicate_groups), 2)  # Two groups of duplicates
        self.assertEqual(len(self.scanner.duplicate_files_in_folders), 2)  # Both folders have duplicates
        self.assertEqual(len(self.scanner.duplicate_only_folders), 2)  # Both folders contain only duplicates
        
        # Verify folder1
        folder1 = self.scanner.duplicate_only_folders['folder1']
        self.assertEqual(len(folder1.duplicate_files), 2)
        self.assertEqual(len(folder1.total_files), 2)
        self.assertTrue(folder1.check_if_duplicate_only())
        
        # Verify folder2
        folder2 = self.scanner.duplicate_only_folders['folder2']
        self.assertEqual(len(folder2.duplicate_files), 2)
        self.assertEqual(len(folder2.total_files), 2)
        self.assertTrue(folder2.check_if_duplicate_only())

if __name__ == '__main__':
    unittest.main() 