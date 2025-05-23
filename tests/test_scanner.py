import unittest
from unittest.mock import Mock, patch, MagicMock, mock_open
import os
import sys # Import sys for mocking
import json
from datetime import datetime
import tempfile # For TestMetadataCache, TestDuplicateScanner, TestDuplicateScannerWithFolders
import shutil   # For TestMetadataCache, TestDuplicateScanner, TestDuplicateScannerWithFolders
import logging

# Global logger for tests - configure once
# This can be a general logger for test outputs, distinct from application loggers being tested/mocked.
# For example, if you want to see logs from the test execution itself.
# However, for mocking application loggers (like 'drive_scanner' or 'src.scanner.logger'),
# it's better to do it within setUp or specific tests.
# test_execution_logger = logging.getLogger("TestRunner")
# test_execution_logger.setLevel(logging.DEBUG)
# if not test_execution_logger.handlers:
#     stream_handler = logging.StreamHandler(sys.stdout)
#     stream_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
#     test_execution_logger.addHandler(stream_handler)


class TestBaseDuplicateScanner(unittest.TestCase):
    def setUp(self):
        """Set up test fixtures for BaseDuplicateScanner tests."""
        self.drive_api_mock = Mock(name="DriveAPIMock_For_BaseScanner")
        
        # Mock the config module that BaseDuplicateScanner and its dependencies might import
        self.mock_config_for_base = Mock(name="ConfigMock_For_BaseScanner")
        self.mock_config_for_base.CACHE_FILE = "mock_cache_for_base_scanner.json" # Different from other classes
        self.mock_config_for_base.SAVE_INTERVAL_MINUTES = 5
        
        # Setup a specific logger for the scanner instance to use during tests
        # This logger instance will be passed to or patched into the scanner.
        self.scanner_logger = logging.getLogger('drive_scanner_for_base_tests')
        self.scanner_logger.handlers.clear() # Ensure no duplicate handlers from previous runs
        # To see logs from the scanner during tests, uncomment the next lines:
        # test_handler = logging.StreamHandler(sys.stdout)
        # test_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        # self.scanner_logger.addHandler(test_handler)
        self.scanner_logger.setLevel(logging.DEBUG) # Capture all levels for testing
        self.mock_config_for_base.logger = self.scanner_logger # If scanner gets logger from config

        self.mock_open_creds_for_base = mock_open(read_data='{"credentials_for_base": "data"}')

        # Patch sys.modules for 'config' and 'src.config'.
        # Patch 'open' in 'src.cache' and 'get_cache_key' in 'src.cache' for MetadataCache instantiation.
        # Patch 'logger' in 'src.scanner' if BaseDuplicateScanner imports a global logger from its module.
        # The 'create=True' for patching src.scanner.logger is important if src.scanner might not define it.
        with patch.dict(sys.modules, {
            'config': self.mock_config_for_base, 
            'src.config': self.mock_config_for_base 
        }), \
            patch('src.cache.open', self.mock_open_creds_for_base, create=True), \
            patch('src.cache.get_cache_key', return_value="mock_key_for_base_scanner_cache"), \
            patch('src.scanner.logger', self.scanner_logger, create=True): 

            from src.scanner import BaseDuplicateScanner # Deferred import
            from src.cache import MetadataCache      # Deferred import for spec
            
            # Provide a cache mock that conforms to MetadataCache's interface
            self.cache_mock = Mock(spec=MetadataCache, name="CacheInstanceMock_For_BaseScanner")
            self.scanner = BaseDuplicateScanner(drive_api=self.drive_api_mock, cache=self.cache_mock)
            # Crucially, ensure the scanner instance uses the logger we prepared.
            # This handles cases where the logger is passed in __init__ or set as an attribute.
            # If BaseDuplicateScanner itself imports 'from ..config import logger', the patch of src.config.logger handles it.
            # If BaseDuplicateScanner imports 'from .config import logger' (less likely for a base class), direct patching of module is needed.
            # If BaseDuplicateScanner does 'import logging; self.logger = logging.getLogger(...)' then this line is key:
            self.scanner.logger = self.scanner_logger 
        
        # Reset duplicate_groups for each test, as it's an instance variable modified by methods
        self.scanner.duplicate_groups = []


    def tearDown(self):
        """Clean up after each test method for BaseDuplicateScanner."""
        # Clear handlers from the specific logger instance to prevent interference between tests
        if hasattr(self, 'scanner_logger') and self.scanner_logger:
            self.scanner_logger.handlers.clear()
        pass

    # --- Tests for _filter_valid_files (Copied from previous reports) ---
    def test_filter_valid_files_empty_files(self):
        files = [
            {'id': '1', 'name': 'empty_file.txt', 'size': '0', 'mimeType': 'text/plain'},
            {'id': '2', 'name': 'another_empty_file.txt', 'size': '0', 'mimeType': 'application/pdf'},
        ]
        expected_valid_files = []
        self.assertEqual(self.scanner._filter_valid_files(files), expected_valid_files)

    def test_filter_valid_files_google_workspace_files(self):
        files = [
            {'id': '1', 'name': 'google_doc.gdoc', 'size': '1024', 'mimeType': 'application/vnd.google-apps.document'},
            {'id': '2', 'name': 'google_sheet.gsheet', 'size': '2048', 'mimeType': 'application/vnd.google-apps.spreadsheet'},
        ]
        expected_valid_files = []
        self.assertEqual(self.scanner._filter_valid_files(files), expected_valid_files)

    def test_filter_valid_files_valid_files(self):
        files = [
            {'id': '1', 'name': 'report.docx', 'size': '1024', 'mimeType': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'},
            {'id': '2', 'name': 'image.jpg', 'size': '2048', 'mimeType': 'image/jpeg'},
        ]
        expected_valid_files = files
        self.assertEqual(self.scanner._filter_valid_files(files), expected_valid_files)

    def test_filter_valid_files_mixed_list(self):
        files = [
            {'id': '1', 'name': 'empty_file.txt', 'size': '0', 'mimeType': 'text/plain'}, 
            {'id': '2', 'name': 'google_doc.gdoc', 'size': '1024', 'mimeType': 'application/vnd.google-apps.document'}, 
            {'id': '3', 'name': 'report.docx', 'size': '1024', 'mimeType': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'},
            {'id': '4', 'name': 'image.jpg', 'size': '2048', 'mimeType': 'image/jpeg'},
        ]
        expected_valid_files = [files[2], files[3]]
        self.assertEqual(self.scanner._filter_valid_files(files), expected_valid_files)

    def test_filter_valid_files_missing_size_or_mime_type(self):
        files = [
            {'id': '1', 'name': 'file_missing_mime.txt', 'size': '100'}, 
            {'id': '2', 'name': 'file_missing_size.txt', 'mimeType': 'text/plain'}, 
            {'id': '3', 'name': 'file_missing_both.txt'}, 
            {'id': '4', 'name': 'empty_file_missing_mime.txt', 'size': '0'}, 
            {'id': '5', 'name': 'google_doc_missing_size.gdoc', 'mimeType': 'application/vnd.google-apps.document'},
        ]
        # Implementation detail: file.get('size', '0') == '0' filters if size missing or '0'
        # file.get('mimeType', '').startswith(...) handles missing mimeType as non-Google
        expected_valid_files = [files[0]]
        self.assertEqual(self.scanner._filter_valid_files(files), expected_valid_files)

    # --- Tests for _group_files_by_size (Copied from previous reports) ---
    def test_group_files_by_size_no_files(self):
        self.assertEqual(self.scanner._group_files_by_size([]), {})

    def test_group_files_by_size_all_unique_sizes(self):
        files = [{'id': '1', 'size': '100'}, {'id': '2', 'size': '200'}]
        expected = {'100': [files[0]], '200': [files[1]]}
        self.assertEqual(self.scanner._group_files_by_size(files), expected)

    def test_group_files_by_size_some_same_sizes(self):
        f = [{'id': '1', 'size': '100'}, {'id': '2', 'size': '200'}, {'id': '3', 'size': '100'}]
        expected = {'100': [f[0], f[2]], '200': [f[1]]}
        result = self.scanner._group_files_by_size(f)
        self.assertCountEqual(result.keys(), expected.keys())
        for k_size in expected: self.assertCountEqual(result[k_size], expected[k_size])

    def test_group_files_by_size_all_same_size(self):
        f = [{'id': '1', 'size': '100'}, {'id': '2', 'size': '100'}]
        expected = {'100': f}
        result = self.scanner._group_files_by_size(f)
        self.assertCountEqual(result.keys(), expected.keys())
        for k_size in expected: self.assertCountEqual(result[k_size], expected[k_size])
        
    def test_group_files_by_size_files_missing_size(self):
        f = [{'id': '1', 'size': '100'}, {'id': '2' }, {'id': '3', 'size': '0'}] # File '2' is missing size
        expected = {'100': [f[0]], '0': [f[1], f[2]]} # file.get('size', '0')
        result = self.scanner._group_files_by_size(f)
        self.assertCountEqual(result.keys(), expected.keys())
        for k_size in expected: self.assertCountEqual(result[k_size], expected[k_size])

    # --- Tests for _group_files_by_md5 (Copied from previous reports) ---
    def test_group_files_by_md5_no_files(self):
        self.assertEqual(self.scanner._group_files_by_md5([]), {})

    def test_group_files_by_md5_all_unique_hashes(self):
        f = [{'id': '1', 'md5Checksum': 'md5_1'}, {'id': '2', 'md5Checksum': 'md5_2'}]
        expected = {'md5_1': [f[0]], 'md5_2': [f[1]]}
        self.assertEqual(self.scanner._group_files_by_md5(f), expected)

    def test_group_files_by_md5_some_same_hashes(self):
        f = [{'id': '1', 'md5Checksum': 'A'}, {'id': '2', 'md5Checksum': 'B'}, {'id': '3', 'md5Checksum': 'A'}]
        expected = {'A': [f[0], f[2]], 'B': [f[1]]}
        result = self.scanner._group_files_by_md5(f)
        self.assertCountEqual(result.keys(), expected.keys())
        for k_md5 in expected: self.assertCountEqual(result[k_md5], expected[k_md5])

    def test_group_files_by_md5_all_same_hash(self):
        f = [{'id': '1', 'md5Checksum': 'A'}, {'id': '2', 'md5Checksum': 'A'}]
        expected = {'A': f}
        result = self.scanner._group_files_by_md5(f)
        self.assertCountEqual(result.keys(), expected.keys())
        for k_md5 in expected: self.assertCountEqual(result[k_md5], expected[k_md5])
        
    def test_group_files_by_md5_missing_or_empty_hash(self):
        f = [{'id': '1', 'md5Checksum': 'A'}, {'id': '2'}, {'id': '3', 'md5Checksum': ''}, {'id': '4', 'md5Checksum': 'A'}]
        expected = {'A': [f[0], f[3]]} 
        self.assertEqual(self.scanner._group_files_by_md5(f), expected)

    # --- Tests for _process_duplicate_group (Copied, needs DuplicateGroup or mock) ---
    @patch('src.scanner.DuplicateGroup') 
    def test_process_duplicate_group_adds_group(self, MockDuplicateGroup):
        self.scanner.duplicate_groups = [] 
        file_list = [{'id': '1', 'name': 'f1'}, {'id': '2', 'name': 'f2'}]
        metadata_dict = {'1': file_list[0], '2': file_list[1]}
        mock_group_instance = MockDuplicateGroup.return_value 
        
        self.scanner._process_duplicate_group(file_list, metadata_dict)
        
        MockDuplicateGroup.assert_called_once_with(file_list, metadata_dict)
        self.assertEqual(len(self.scanner.duplicate_groups), 1)
        self.assertIs(self.scanner.duplicate_groups[0], mock_group_instance)

    @patch('src.scanner.DuplicateGroup')
    def test_process_duplicate_group_less_than_two_files(self, MockDuplicateGroup):
        self.scanner.duplicate_groups = []
        self.scanner._process_duplicate_group([{'id': '1'}], {'1': {'id': '1'}})
        MockDuplicateGroup.assert_not_called()
        self.assertEqual(len(self.scanner.duplicate_groups), 0)

    # --- Tests for _scan_for_duplicates (Copied from previous reports) ---
    def test_scan_for_duplicates_no_files(self):
        self.scanner.duplicate_groups = []
        self.scanner._scan_for_duplicates([])
        self.assertEqual(len(self.scanner.duplicate_groups), 0)

    def test_scan_for_duplicates_no_duplicates(self):
        self.scanner.duplicate_groups = []
        files = [
            {'id': '1', 'size': '100', 'mimeType': 'text/plain', 'md5Checksum': 'md5_1'},
            {'id': '2', 'size': '200', 'mimeType': 'text/plain', 'md5Checksum': 'md5_2'},
        ]
        self.scanner._scan_for_duplicates(files)
        self.assertEqual(len(self.scanner.duplicate_groups), 0)

    def test_scan_for_duplicates_simple_pair(self):
        self.scanner.duplicate_groups = []
        f = [
            {'id': '1', 'size': '100', 'mimeType': 'text/plain', 'md5Checksum': 'A'},
            {'id': '2', 'size': '100', 'mimeType': 'text/plain', 'md5Checksum': 'A'},
        ]
        self.scanner._scan_for_duplicates(f)
        self.assertEqual(len(self.scanner.duplicate_groups), 1)
        self.assertCountEqual([file['id'] for file in self.scanner.duplicate_groups[0].files], ['1', '2'])

    def test_scan_for_duplicates_multiple_groups_and_unique_files(self):
        self.scanner.duplicate_groups = []
        f = [
            {'id': '1', 'size': '100', 'mimeType': 'text/plain', 'md5Checksum': 'A'}, 
            {'id': '2', 'size': '200', 'mimeType': 'text/plain', 'md5Checksum': 'B'}, 
            {'id': '3', 'size': '100', 'mimeType': 'text/plain', 'md5Checksum': 'A'}, 
            {'id': '4', 'size': '300', 'mimeType': 'text/plain', 'md5Checksum': 'C'}, 
            {'id': '5', 'size': '200', 'mimeType': 'text/plain', 'md5Checksum': 'B'}, 
            {'id': '6', 'size': '100', 'mimeType': 'text/plain', 'md5Checksum': 'A'}, 
        ]
        self.scanner._scan_for_duplicates(f)
        self.assertEqual(len(self.scanner.duplicate_groups), 2)
        # Sorting key for deterministic assertion of groups
        sorted_groups = sorted(self.scanner.duplicate_groups, key=lambda g: g.files[0]['md5Checksum'])
        self.assertCountEqual([file['id'] for file in sorted_groups[0].files], ['1', '3', '6']) 
        self.assertCountEqual([file['id'] for file in sorted_groups[1].files], ['2', '5'])
        
    def test_scan_for_duplicates_ignores_filtered_files(self):
        self.scanner.duplicate_groups = []
        f = [
            {'id': '1', 'size': '100', 'mimeType': 'text/plain', 'md5Checksum': 'A'}, 
            {'id': '2', 'size': '0',   'mimeType': 'text/plain', 'md5Checksum': 'A'}, 
            {'id': '3', 'size': '100', 'mimeType': 'application/vnd.google-apps.document', 'md5Checksum': 'A'},
            {'id': '4', 'size': '100', 'mimeType': 'text/plain', 'md5Checksum': 'A'}, 
        ]
        self.scanner._scan_for_duplicates(f)
        self.assertEqual(len(self.scanner.duplicate_groups), 1)
        self.assertCountEqual([file['id'] for file in self.scanner.duplicate_groups[0].files], ['1', '4'])


# --- TestMetadataCache ---
class TestMetadataCache(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()
        self.test_cache_file = os.path.join(self.test_dir, 'metadata_cache_test_file.json')
        
        self.mock_config_for_cache = Mock(name="ConfigMock_For_MetadataCache")
        self.mock_config_for_cache.CACHE_FILE = self.test_cache_file
        self.mock_config_for_cache.SAVE_INTERVAL_MINUTES = 0 
        
        self.mock_open_creds_for_cache = mock_open(read_data='{"credentials_for_cache": "data"}')

        with patch.dict(sys.modules, {'config': self.mock_config_for_cache, 'src.config': self.mock_config_for_cache}):
            with patch('src.cache.open', self.mock_open_creds_for_cache, create=True), \
                 patch('src.cache.get_cache_key', return_value="mock_key_for_metadata_cache_class"):
                from src.cache import MetadataCache
                self.cache_under_test = MetadataCache(cache_file=self.test_cache_file)
                self.cache_under_test.clear() 

    def tearDown(self):
        if hasattr(self, 'test_dir') and os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)

    def test_set_get_clear_operations(self):
        self.assertIsNone(self.cache_under_test.get('mykey'))
        self.cache_under_test.set('mykey', 'myvalue')
        self.assertEqual(self.cache_under_test.get('mykey'), 'myvalue')
        self.cache_under_test.clear()
        self.assertIsNone(self.cache_under_test.get('mykey'))

    def test_update_remove_operations(self):
        self.cache_under_test.update({'key1': 'val1', 'key2': 'val2'})
        self.assertEqual(self.cache_under_test.get('key1'), 'val1')
        self.cache_under_test.remove(['key1'])
        self.assertIsNone(self.cache_under_test.get('key1'))
        self.assertEqual(self.cache_under_test.get('key2'), 'val2')

    def test_persistence_operations(self):
        self.cache_under_test.set('persist_key', 'persist_value')
        self.cache_under_test._save(force=True)
        
        with patch.dict(sys.modules, {'config': self.mock_config_for_cache, 'src.config': self.mock_config_for_cache}):
            with patch('src.cache.open', self.mock_open_creds_for_cache, create=True), \
                 patch('src.cache.get_cache_key', return_value="mock_key_for_metadata_cache_class"):
                from src.cache import MetadataCache
                new_cache_instance = MetadataCache(cache_file=self.test_cache_file)
        self.assertEqual(new_cache_instance.get('persist_key'), 'persist_value')

    def test_cache_files_and_folders_data(self):
        files_data = [{'id': 'f1', 'name': 'file1.txt'}]
        folders_data = [{'id': 'd1', 'name': 'folder1'}]
        self.cache_under_test.cache_files(files_data)
        self.assertEqual(self.cache_under_test.get_all_files(), files_data)
        self.cache_under_test.cache_folders(folders_data)
        self.assertEqual(self.cache_under_test.get_all_folders(), folders_data)

# --- TestDuplicateScanner (Derived class) ---
class TestDuplicateScanner(unittest.TestCase): 
    def setUp(self):
        self.drive_api_mock = Mock(name="DriveAPIMock_For_DerivedScanner")
        self.test_dir = tempfile.mkdtemp()
        self.test_cache_file = os.path.join(self.test_dir, 'derived_dup_scanner_cache.json')

        self.mock_config_derived = Mock(name="ConfigMock_For_DerivedScanner")
        self.mock_config_derived.CACHE_FILE = self.test_cache_file
        self.mock_config_derived.SAVE_INTERVAL_MINUTES = 0
        self.scanner_logger_derived = logging.getLogger('drive_scanner_derived_tests')
        self.scanner_logger_derived.handlers.clear()
        self.mock_config_derived.logger = self.scanner_logger_derived

        self.mock_open_creds_derived = mock_open(read_data='{"credentials_derived": "data"}')

        with patch.dict(sys.modules, {'config': self.mock_config_derived, 'src.config': self.mock_config_derived}):
            with patch('src.cache.open', self.mock_open_creds_derived, create=True), \
                 patch('src.cache.get_cache_key', return_value="mock_key_derived_scanner"):
                from src.scanner import DuplicateScanner 
                from src.cache import MetadataCache
                self.actual_cache_instance = MetadataCache(cache_file=self.test_cache_file)
                self.actual_cache_instance.clear()
                self.derived_scanner = DuplicateScanner(self.drive_api_mock, self.actual_cache_instance)
                self.derived_scanner.logger = self.scanner_logger_derived

    def tearDown(self):
        if hasattr(self, 'test_dir') and os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
        if hasattr(self, 'scanner_logger_derived'):
            self.scanner_logger_derived.handlers.clear()

    def test_derived_scan_when_cache_empty(self):
        api_files_data = [
            {'id': 'api_f1', 'name': 'f1.txt', 'md5Checksum': 'h1', 'size': '100', 'mimeType': 'text/plain'},
            {'id': 'api_f2', 'name': 'f2.txt', 'md5Checksum': 'h1', 'size': '100', 'mimeType': 'text/plain'},
        ]
        self.drive_api_mock.list_files = Mock(return_value=api_files_data)
        
        self.derived_scanner.scan()
            
        self.drive_api_mock.list_files.assert_called_once()
        self.assertEqual(len(self.derived_scanner.duplicate_groups), 1)
        self.assertCountEqual([f['id'] for f in self.derived_scanner.duplicate_groups[0].files], ['api_f1', 'api_f2'])
        self.assertEqual(self.actual_cache_instance.get_all_files(), api_files_data)

    def test_derived_scan_uses_cache(self):
        cached_files_data = [
            {'id': 'cf1', 'name': 'cf1.txt', 'md5Checksum': 'ch1', 'size': '200', 'mimeType': 'text/plain'},
            {'id': 'cf2', 'name': 'cf2.txt', 'md5Checksum': 'ch1', 'size': '200', 'mimeType': 'text/plain'},
        ]
        self.actual_cache_instance.cache_files(cached_files_data)
        self.drive_api_mock.list_files = Mock()

        self.derived_scanner.scan()

        self.drive_api_mock.list_files.assert_not_called()
        self.assertEqual(len(self.derived_scanner.duplicate_groups), 1)
        self.assertCountEqual([f['id'] for f in self.derived_scanner.duplicate_groups[0].files], ['cf1', 'cf2'])

    def test_derived_scan_force_refresh_overrides_cache(self):
        self.actual_cache_instance.cache_files([{'id': 'old', 'name': 'old.txt', 'md5Checksum': 'old_h', 'size': '50', 'mimeType':'text/plain'}])
        new_api_data = [
            {'id': 'new1', 'name': 'new1.txt', 'md5Checksum': 'new_h', 'size': '300', 'mimeType': 'text/plain'},
            {'id': 'new2', 'name': 'new2.txt', 'md5Checksum': 'new_h', 'size': '300', 'mimeType': 'text/plain'},
        ]
        self.drive_api_mock.list_files = Mock(return_value=new_api_data)

        self.derived_scanner.scan(force_refresh=True)

        self.drive_api_mock.list_files.assert_called_once_with(force_refresh=True)
        self.assertEqual(self.actual_cache_instance.get_all_files(), new_api_data)
        self.assertEqual(len(self.derived_scanner.duplicate_groups), 1)
        self.assertCountEqual([f['id'] for f in self.derived_scanner.duplicate_groups[0].files], ['new1', 'new2'])

# --- TestDuplicateScannerWithFolders (Derived class) ---
class TestDuplicateScannerWithFolders(unittest.TestCase):
    def setUp(self):
        self.drive_api_mock = Mock(name="DriveAPIMock_For_FoldersScanner")
        self.test_dir = tempfile.mkdtemp()
        self.test_cache_file = os.path.join(self.test_dir, 'folders_scan_cache.json')

        self.mock_config_folders_derived = Mock(name="ConfigMock_For_FoldersScanner")
        self.mock_config_folders_derived.CACHE_FILE = self.test_cache_file
        self.mock_config_folders_derived.SAVE_INTERVAL_MINUTES = 0
        self.scanner_logger_folders = logging.getLogger('drive_scanner_folders_tests')
        self.scanner_logger_folders.handlers.clear()
        self.mock_config_folders_derived.logger = self.scanner_logger_folders

        self.mock_open_creds_folders_derived = mock_open(read_data='{"credentials_folders_derived": "data"}')

        with patch.dict(sys.modules, {'config': self.mock_config_folders_derived, 'src.config': self.mock_config_folders_derived}):
            with patch('src.cache.open', self.mock_open_creds_folders_derived, create=True), \
                 patch('src.cache.get_cache_key', return_value="mock_key_folders_scanner_derived"):
                from src.scanner import DuplicateScannerWithFolders 
                from src.cache import MetadataCache
                # from src.models import DuplicateFolder # For spec if needed for self.scanner.duplicate_files_in_folders
                self.actual_cache_instance_folders = MetadataCache(cache_file=self.test_cache_file)
                self.actual_cache_instance_folders.clear()
                self.folders_scanner = DuplicateScannerWithFolders(self.drive_api_mock, self.actual_cache_instance_folders)
                self.folders_scanner.logger = self.scanner_logger_folders
                # Reset collections that might be modified by scan
                self.folders_scanner.duplicate_groups = []
                self.folders_scanner.duplicate_files_in_folders = {}
                self.folders_scanner.duplicate_only_folders = {}

    def tearDown(self):
        if hasattr(self, 'test_dir') and os.path.exists(self.test_dir):
            shutil.rmtree(self.test_dir)
        if hasattr(self, 'scanner_logger_folders'):
            self.scanner_logger_folders.handlers.clear()

    def test_folders_scan_cache_empty(self):
        api_files = [
            {'id': 'f1', 'name': 'file1.txt', 'md5Checksum': 'h1', 'size': '100', 'mimeType': 'text/plain', 'parents': ['d1']},
            {'id': 'f2', 'name': 'file2.txt', 'md5Checksum': 'h1', 'size': '100', 'mimeType': 'text/plain', 'parents': ['d1']},
        ]
        api_folders = [{'id': 'd1', 'name': 'Folder1'}]
        self.drive_api_mock.list_all_files_and_folders = Mock(return_value=(api_files, api_folders))
        
        with patch.object(self.folders_scanner, '_analyze_folder_structures') as mock_analyze:
            self.folders_scanner.scan()
        
        self.drive_api_mock.list_all_files_and_folders.assert_called_once()
        self.assertEqual(len(self.folders_scanner.duplicate_groups), 1)
        self.assertCountEqual([f['id'] for f in self.folders_scanner.duplicate_groups[0].files], ['f1', 'f2'])
        self.assertEqual(self.actual_cache_instance_folders.get_all_files(), api_files)
        self.assertEqual(self.actual_cache_instance_folders.get_all_folders(), api_folders)
        mock_analyze.assert_called_once_with(api_folders, api_files) 

    def test_folders_scan_uses_cache(self):
        cached_f = [{'id': 'cf1', 'name': 'cf1.txt', 'md5Checksum': 'ch1', 'size': '200', 'mimeType': 'text/plain', 'parents':['cd1']}]
        cached_d = [{'id': 'cd1', 'name': 'CachedFolder1'}]
        self.actual_cache_instance_folders.cache_files(cached_f)
        self.actual_cache_instance_folders.cache_folders(cached_d)
        self.drive_api_mock.list_all_files_and_folders = Mock()

        with patch.object(self.folders_scanner, '_analyze_folder_structures') as mock_analyze:
            self.folders_scanner.scan()

        self.drive_api_mock.list_all_files_and_folders.assert_not_called()
        self.assertEqual(len(self.folders_scanner.duplicate_groups), 0) 
        mock_analyze.assert_called_once_with(cached_d, cached_f)

if __name__ == '__main__':
    unittest.main() 