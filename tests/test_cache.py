import pytest
from src.cache import Cache

def test_cache_initialization():
    """Test that the Cache can be initialized."""
    cache = Cache()
    assert cache is not None
    assert hasattr(cache, 'cache_dir')
    assert hasattr(cache, 'logger')

def test_cache_file_operations():
    """Test basic cache file operations."""
    cache = Cache()
    test_data = {'test': 'data'}
    
    # Test saving and loading
    cache.save_to_cache('test_file', test_data)
    loaded_data = cache.load_from_cache('test_file')
    assert loaded_data == test_data 