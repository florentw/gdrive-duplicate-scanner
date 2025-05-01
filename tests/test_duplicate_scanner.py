import unittest
from unittest.mock import Mock, patch, MagicMock
import os
import sys
from pathlib import Path
import tempfile
import shutil

# Add parent directory to Python path to import modules
sys.path.append(str(Path(__file__).parent.parent))

from src.duplicate_scanner import DuplicateScanner
from src.cache import Cache
from src.config import logger

class TestDuplicateScanner(unittest.TestCase):
    """Test suite for duplicate scanner functionality."""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.mock_service = Mock()
        self.mock_files_service = Mock()
        self.mock_service.files.return_value = self.mock_files_service
        self.test_dir = tempfile.mkdtemp()
        self.test_cache_file = os.path.join(self.test_dir, 'test_cache.json')
        self.test_cache = Cache(self.test_dir)
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

    def test_scanner_initialization(self):
        """Test that the DuplicateScanner can be initialized."""
        with patch('src.duplicate_scanner.get_service', return_value=self.mock_service):
            scanner = DuplicateScanner()
            assert scanner is not None
            assert hasattr(scanner, 'drive_service')
            assert hasattr(scanner, 'cache')
            assert hasattr(scanner, 'logger')

    def test_scan_files(self):
        """Test that scan_files returns a dictionary."""
        mock_files = self._setup_mock_files()
        self.mock_files_service.list.return_value.execute.return_value = {
            'files': mock_files
        }
        
        with patch('src.duplicate_scanner.get_service', return_value=self.mock_service):
            scanner = DuplicateScanner()
            result = scanner.scan_files()
            assert isinstance(result, dict)
            assert 'files_by_size' in result
            assert 'total_files' in result
            assert 'total_size' in result

    def test_find_duplicates(self):
        """Test finding duplicate files."""
        mock_files = self._setup_mock_files()
        files_by_size = {
            1024: ['id1', 'id2'],
            2048: ['id3']
        }
        
        def mock_get_file(file_id):
            for file in mock_files:
                if file['id'] == file_id:
                    return file
            return None
            
        self.mock_files_service.get.return_value.execute.side_effect = mock_get_file
        
        with patch('src.duplicate_scanner.get_service', return_value=self.mock_service):
            scanner = DuplicateScanner()
            result = scanner.find_duplicates(files_by_size)
            assert len(result) == 1  # One group of duplicates
            assert result[0]['size'] == 1024
            assert len(result[0]['files']) == 2  # Two files in the group 