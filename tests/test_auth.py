import unittest
from unittest.mock import patch, mock_open, MagicMock, create_autospec
import os
import pickle
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import Resource, build
from google.auth.transport.requests import Request
from src.auth import get_service

# Mock discovery doc with minimum required fields
MOCK_DISCOVERY_DOC = '''
{
    "rootUrl": "https://www.googleapis.com/",
    "servicePath": "drive/v3/",
    "baseUrl": "https://www.googleapis.com/drive/v3/",
    "batchPath": "batch/drive/v3",
    "version": "v3",
    "name": "drive"
}
'''

class TestAuth(unittest.TestCase):
    """Test suite for authentication functionality."""

    def setUp(self):
        """Set up test fixtures."""
        # Create a properly configured mock credentials object
        self.mock_creds = MagicMock(spec=Credentials)
        self.mock_creds.valid = True
        self.mock_creds.expired = False
        self.mock_creds.refresh_token = True
        
        # Create a mock service
        self.mock_service = create_autospec(Resource, instance=True)

    def test_get_service_with_valid_token(self):
        """Test get_service when token.json exists and is valid."""
        with patch('os.path.exists', return_value=True), \
             patch('os.chmod'), \
             patch('builtins.open', mock_open()), \
             patch('pickle.load', return_value=self.mock_creds), \
             patch('googleapiclient.discovery._retrieve_discovery_doc', return_value=MOCK_DISCOVERY_DOC), \
             patch('src.auth.build', autospec=True) as mock_build:
            
            mock_build.return_value = self.mock_service
            service = get_service()
            
            self.assertIsNotNone(service)
            mock_build.assert_called_once_with('drive', 'v3', credentials=self.mock_creds)

    def test_get_service_with_expired_token(self):
        """Test get_service when token exists but is expired."""
        # Configure mock credentials for expired token scenario
        mock_creds = MagicMock(spec=Credentials)
        mock_creds.valid = False
        mock_creds.expired = True
        mock_creds.refresh_token = True

        mock_request = MagicMock(spec=Request)

        with patch('os.path.exists', return_value=True), \
             patch('os.chmod'), \
             patch('builtins.open', mock_open()), \
             patch('pickle.load', return_value=mock_creds), \
             patch('pickle.dump'), \
             patch('src.auth.Request', return_value=mock_request), \
             patch('googleapiclient.discovery._retrieve_discovery_doc', return_value=MOCK_DISCOVERY_DOC), \
             patch('src.auth.build', autospec=True) as mock_build:
            
            # After refresh, credentials should be valid
            def mock_refresh(request):
                mock_creds.valid = True
                mock_creds.expired = False
            mock_creds.refresh.side_effect = mock_refresh
            mock_build.return_value = self.mock_service
            
            service = get_service()
            
            self.assertIsNotNone(service)
            mock_creds.refresh.assert_called_once_with(mock_request)
            mock_build.assert_called_once_with('drive', 'v3', credentials=mock_creds)

    def test_get_service_with_no_token(self):
        """Test get_service when token.json doesn't exist."""
        mock_flow = MagicMock()
        # Configure mock flow to return valid credentials
        mock_flow.run_local_server.return_value = self.mock_creds

        with patch('os.path.exists', return_value=False), \
             patch('os.chmod'), \
             patch('builtins.open', mock_open()), \
             patch('google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file',
                   return_value=mock_flow), \
             patch('pickle.dump'), \
             patch('googleapiclient.discovery._retrieve_discovery_doc', return_value=MOCK_DISCOVERY_DOC), \
             patch('src.auth.build', autospec=True) as mock_build:
            
            mock_build.return_value = self.mock_service
            service = get_service()
            
            self.assertIsNotNone(service)
            mock_flow.run_local_server.assert_called_once_with(port=0)
            mock_build.assert_called_once_with('drive', 'v3', credentials=self.mock_creds)

    def test_get_service_with_invalid_credentials_file(self):
        """Test get_service when credentials.json is missing or invalid."""
        with patch('os.path.exists', return_value=False), \
             patch('google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file',
                   side_effect=FileNotFoundError):
            
            service = get_service()
            self.assertIsNone(service)

    def test_get_service_with_build_error(self):
        """Test get_service when building the service fails."""
        with patch('os.path.exists', return_value=True), \
             patch('os.chmod'), \
             patch('builtins.open', mock_open()), \
             patch('pickle.load', return_value=self.mock_creds), \
             patch('googleapiclient.discovery._retrieve_discovery_doc', return_value=MOCK_DISCOVERY_DOC), \
             patch('src.auth.build', side_effect=Exception("API Error")):
            
            service = get_service()
            self.assertIsNone(service) 