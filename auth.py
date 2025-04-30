import os
import pickle
import logging
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import UnknownApiNameOrVersion

# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/drive']

def get_service():
    """Gets an authorized Google Drive API service instance.
    
    Returns:
        googleapiclient.discovery.Resource: An authorized Google Drive API service instance.
        None if authentication fails.
    """
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first time.
    if os.path.exists('token.json'):
        try:
            with open('token.json', 'rb') as token:
                creds = pickle.load(token)
        except Exception as e:
            logging.error(f"Error loading token: {e}")
            return None

    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except Exception as e:
                logging.error(f"Error refreshing token: {e}")
                return None
        else:
            try:
                flow = InstalledAppFlow.from_client_secrets_file(
                    'credentials.json', SCOPES)
                creds = flow.run_local_server(port=0)
            except FileNotFoundError:
                logging.error("credentials.json not found")
                return None
            except Exception as e:
                logging.error(f"Error during authentication flow: {e}")
                return None

        # Save the credentials for the next run
        try:
            with open('token.json', 'wb') as token:
                pickle.dump(creds, token)
            # Ensure the token file has the correct permissions
            os.chmod('token.json', 0o600)
        except Exception as e:
            logging.error(f"Error saving token: {e}")
            return None

    try:
        # Build the service
        service = build('drive', 'v3', credentials=creds)
        return service
    except Exception as e:
        logging.error(f"Error building service: {e}")
        return None 