import pytest
from src.scanner import DuplicateScanner

def test_scanner_initialization():
    """Test that the DuplicateScanner can be initialized."""
    scanner = DuplicateScanner()
    assert scanner is not None
    assert hasattr(scanner, 'drive_service')
    assert hasattr(scanner, 'cache')
    assert hasattr(scanner, 'logger') 