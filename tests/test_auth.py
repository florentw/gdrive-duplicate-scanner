import unittest
from unittest.mock import patch, mock_open, MagicMock, create_autospec
import os
import pickle
import logging # For checking log calls
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import Resource, build
from google.auth.transport.requests import Request
from google.auth import exceptions as google_auth_exceptions # For RefreshError
from auth import get_service

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
             patch('auth.build', autospec=True) as mock_build:
            
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
             patch('auth.Request', return_value=mock_request), \
             patch('googleapiclient.discovery._retrieve_discovery_doc', return_value=MOCK_DISCOVERY_DOC), \
             patch('auth.build', autospec=True) as mock_build:
            
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
             patch('auth.build', autospec=True) as mock_build:
            
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
             patch('auth.build', side_effect=Exception("API Error")):
            
            service = get_service()
            self.assertIsNone(service)

    @patch('logging.error') # To check error logging
    def test_get_service_token_refresh_failure(self, mock_logging_error):
        """Test get_service when token refresh fails."""
        mock_creds_expired = MagicMock(spec=Credentials)
        mock_creds_expired.valid = False
        mock_creds_expired.expired = True
        mock_creds_expired.refresh_token = True
        mock_creds_expired.refresh.side_effect = google_auth_exceptions.RefreshError("Refresh failed")

        mock_request_instance = MagicMock(spec=Request)

        # Mock open to simulate reading the token file, then pickle.load to return the mock credentials
        with patch('os.path.exists', return_value=True), \
             patch('builtins.open', mock_open()) as mock_file_open, \
             patch('pickle.load', return_value=mock_creds_expired) as mock_pickle_load, \
             patch('auth.Request', return_value=mock_request_instance) as mock_auth_request:
            
            service = get_service()
            
            mock_file_open.assert_called_once_with('token.json', 'rb')
            mock_pickle_load.assert_called_once_with(mock_file_open.return_value)
            self.assertIsNone(service)
            mock_creds_expired.refresh.assert_called_once_with(mock_request_instance)
            self.assertTrue(any("Error refreshing token" in call.args[0] for call in mock_logging_error.call_args_list))

    @patch('logging.error') # To check error logging
    def test_get_service_save_token_failure(self, mock_logging_error):
        """Test get_service when saving a new token fails."""
        mock_flow = MagicMock()
        mock_new_creds = MagicMock(spec=Credentials)
        mock_flow.run_local_server.return_value = mock_new_creds
        
        # Simulate successful build after credentials obtained
        mock_service_instance = MagicMock(spec=Resource)

        with patch('os.path.exists', return_value=False), \
             patch('google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file', return_value=mock_flow) as mock_from_secrets, \
             patch('builtins.open', mock_open()) as mock_file_open, \
             patch('pickle.dump', side_effect=IOError("Failed to save token")) as mock_pickle_dump, \
             patch('os.chmod') as mock_chmod, \
             patch('auth.build', return_value=mock_service_instance) as mock_build: # build should not be called if saving token fails and returns None

            service = get_service()

            self.assertIsNone(service) # As per src/auth.py, it returns None if saving token fails
            mock_from_secrets.assert_called_once_with('credentials.json', ['https://www.googleapis.com/auth/drive'])
            mock_flow.run_local_server.assert_called_once_with(port=0)
            # pickle.dump should be called to attempt saving
            mock_pickle_dump.assert_called_once_with(mock_new_creds, mock_file_open.return_value)
            # os.chmod should not be called if pickle.dump fails before it
            # mock_chmod.assert_called_once_with('token.json', 0o600) # This line is not reached
            self.assertTrue(any("Error saving token" in call.args[0] for call in mock_logging_error.call_args_list))
            mock_build.assert_not_called() # Build should not be called if saving token results in None return

    @patch('logging.error') # To check error logging
    def test_get_service_flow_generic_exception(self, mock_logging_error):
        """Test get_service when InstalledAppFlow.from_client_secrets_file raises a generic exception."""
        with patch('os.path.exists', return_value=False), \
             patch('google_auth_oauthlib.flow.InstalledAppFlow.from_client_secrets_file',
                   side_effect=Exception("Generic flow error")):
            
            service = get_service()
            self.assertIsNone(service)
            self.assertTrue(any("Error during authentication flow" in call.args[0] for call in mock_logging_error.call_args_list))


if __name__ == '__main__':
    unittest.main()