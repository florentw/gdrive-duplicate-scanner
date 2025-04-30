import os
import pickle
import logging
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from config import SCOPES

def get_service():
    """Authorize and return Google Drive service."""
    creds = None
    token_file = 'token.json'
    
    # Check file permissions
    if os.path.exists(token_file):
        try:
            # Ensure token file has correct permissions
            os.chmod(token_file, 0o600)
            with open(token_file, 'rb') as token:
                creds = pickle.load(token)
        except (pickle.PickleError, IOError, PermissionError) as e:
            logging.error(f"Error loading credentials: {e}")
            creds = None

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
                # Save refreshed credentials
                with open(token_file, 'wb') as token:
                    pickle.dump(creds, token)
                os.chmod(token_file, 0o600)  # Set secure permissions
            except Exception as e:
                logging.error(f"Error refreshing credentials: {e}")
                creds = None
        else:
            try:
                flow = InstalledAppFlow.from_client_secrets_file(
                    'credentials.json', SCOPES)
                creds = flow.run_local_server(port=0)
                # Save the credentials for the next run
                with open(token_file, 'wb') as token:
                    pickle.dump(creds, token)
                os.chmod(token_file, 0o600)  # Set secure permissions
            except Exception as e:
                logging.error(f"Error getting new credentials: {e}")
                return None

    try:
        # Build the Drive API service
        service = build('drive', 'v3', credentials=creds)
        return service
    except Exception as e:
        logging.error(f"Error building Drive API service: {e}")
        return None 