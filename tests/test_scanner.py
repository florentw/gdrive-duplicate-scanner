import unittest
from unittest.mock import Mock, patch
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

class TestDuplicateScanner(unittest.TestCase):
    def setUp(self):
        self.drive_api = Mock()
        self.cache = Mock()
        self.scanner = DuplicateScanner(self.drive_api, self.cache)

    def test_scan_with_cache(self):
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

if __name__ == '__main__':
    unittest.main() 