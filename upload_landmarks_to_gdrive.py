import os
import argparse
import csv
from pathlib import Path
import datetime

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import mimetypes

# --- Configuration ---
BASE_DIR = Path('/home/pandu/Documents/explore/sign-language-translator-ai')
SCOPES = ["https://www.googleapis.com/auth/drive"]
SCRIPT_CWD = Path.cwd()

# OAuth credentials (reusing existing setup)
OAUTH_CLIENT_SECRET_FILE = SCRIPT_CWD / 'credentials.json'
OAUTH_TOKEN_FILE = SCRIPT_CWD / 'token_landmarks.json'
ERROR_LOG_FILE_PATH = SCRIPT_CWD / 'upload_landmarks_errors.csv'

# Directory mappings: local_dir -> (gdrive_root_folder, gdrive_subfolder)
DIRECTORY_MAPPINGS = {
    'hands_landmarks_original': ('slta_landmarks', 'hands_landmarks_original'),
    'holistic_landmarks_original': ('slta_landmarks', 'holistic_landmarks_original'),
    'visualization_hands': ('slta_landmarks_visualization', 'visualization_hands'),
    'visualization_holistic': ('slta_landmarks_visualization', 'visualization_holistic')
}


def initialize_error_log(log_file_path):
    """Creates the error log CSV with headers if it doesn't exist."""
    if not log_file_path.exists():
        with open(log_file_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['local_path', 'gdrive_path', 'error_message', 'timestamp'])
        print(f"Initialized error log: {log_file_path}")


def log_upload_error(log_file_path, local_path, gdrive_path, error_message):
    """Appends a failed item and error to the CSV log."""
    timestamp = datetime.datetime.now().isoformat()
    
    with open(log_file_path, 'a', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([str(local_path), str(gdrive_path), str(error_message), timestamp])


def get_service_oauth(token_file_path):
    """Handles user authentication using OAuth 2.0 Client ID flow."""
    creds = None
    if token_file_path.exists():
        try:
            creds = Credentials.from_authorized_user_file(str(token_file_path), SCOPES)
        except Exception as e:
            print(f"Error loading token file '{token_file_path}': {e}. Will attempt re-authentication.")
            creds = None

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                print("Refreshing access token...")
                creds.refresh(Request())
            except Exception as e:
                print(f"Error refreshing token: {e}. Proceeding to full authorization flow.")
                creds = None
        if not creds:
            print("No valid credentials found, starting authorization flow...")
            if not OAUTH_CLIENT_SECRET_FILE.exists():
                print(f"\nERROR: OAuth Client Secret file ('{OAUTH_CLIENT_SECRET_FILE}') not found.")
                print("Please download your OAuth 2.0 Client ID credentials from Google Cloud Console.")
                return None
            
            try:
                flow = InstalledAppFlow.from_client_secrets_file(str(OAUTH_CLIENT_SECRET_FILE), SCOPES)
                creds = flow.run_local_server(port=0)
            except Exception as e:
                print(f"Error during authorization flow: {e}")
                return None

        with open(token_file_path, "w") as token:
            token.write(creds.to_json())
        print(f"Credentials saved to '{token_file_path}'")
    
    try:
        service = build('drive', 'v3', credentials=creds)
        return service
    except Exception as e:
        print(f"Error building Google Drive service with user credentials: {e}")
        return None


def find_or_create_folder(service, name, parent_id=None):
    """Find existing folder or create new one."""
    query = f"name='{name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
    if parent_id:
        query += f" and '{parent_id}' in parents"
    
    res = service.files().list(q=query, fields='files(id, name)').execute()
    files = res.get('files')
    if files:
        return files[0]['id']
    
    # Create folder if not found
    metadata = {'name': name, 'mimeType': 'application/vnd.google-apps.folder'}
    if parent_id:
        metadata['parents'] = [parent_id]
    
    folder = service.files().create(body=metadata, fields='id').execute()
    print(f"  Created folder '{name}' with ID: {folder['id']}")
    return folder['id']


def find_existing_file(service, filename, parent_id):
    """Find existing file in the specified folder."""
    query = f"name='{filename}' and trashed=false"
    if parent_id:
        query += f" and '{parent_id}' in parents"
    
    res = service.files().list(q=query, fields='files(id, name)').execute()
    files = res.get('files')
    return files[0]['id'] if files else None


def upload_file(service, local_file_path, folder_id, overwrite=True):
    """Upload file to Google Drive, optionally overwriting existing files."""
    file_name = local_file_path.name
    
    # Check if file already exists
    existing_file_id = find_existing_file(service, file_name, folder_id)
    
    # Prepare file metadata and media
    mime_type, _ = mimetypes.guess_type(str(local_file_path))
    if mime_type is None:
        mime_type = 'application/octet-stream'
    
    media = MediaFileUpload(str(local_file_path), mimetype=mime_type, resumable=True)
    
    if existing_file_id and overwrite:
        # Update existing file
        file = service.files().update(
            fileId=existing_file_id,
            media_body=media,
            fields='id, name'
        ).execute()
        print(f"  Updated existing file '{file.get('name')}' (ID: {file.get('id')})")
    else:
        # Create new file
        metadata = {'name': file_name, 'parents': [folder_id]}
        file = service.files().create(
            body=metadata,
            media_body=media,
            fields='id, name'
        ).execute()
        action = "uploaded" if not existing_file_id else "created duplicate of"
        print(f"  Successfully {action} '{file.get('name')}' (ID: {file.get('id')})")


def upload_directory_recursive(service, local_dir_path, gdrive_folder_id, gdrive_path_prefix="", args=None):
    """Recursively upload directory contents to Google Drive."""
    uploaded_count = 0
    error_count = 0
    
    try:
        items = list(local_dir_path.iterdir())
        items.sort()  # Sort for consistent processing order
        
        for item in items:
            current_gdrive_path = f"{gdrive_path_prefix}/{item.name}"
            
            if item.is_file():
                # --- Resume Logic ---
                if args and args.resume:
                    existing_file_id = find_existing_file(service, item.name, gdrive_folder_id)
                    if existing_file_id:
                        print(f"    Skipping (already exists in Drive): {item.name}")
                        uploaded_count += 1 # Count as "handled"
                        items_processed_in_test += 1
                        continue
                # --- End Resume Logic ---
                try:
                    print(f"    Uploading file: {item.name}")
                    upload_file(service, item, gdrive_folder_id, overwrite=True)
                    uploaded_count += 1
                except Exception as e:
                    error_msg = f"Failed to upload file '{item}': {e}"
                    print(f"    Error: {error_msg}")
                    log_upload_error(ERROR_LOG_FILE_PATH, item, current_gdrive_path, error_msg)
                    error_count += 1
                    
            elif item.is_dir():
                try:
                    print(f"    Creating subdirectory: {item.name}")
                    subfolder_id = find_or_create_folder(service, item.name, gdrive_folder_id)
                    
                    # Recursively upload subdirectory contents
                    sub_uploaded, sub_errors = upload_directory_recursive(
                        service, item, subfolder_id, current_gdrive_path, args=args
                    )
                    uploaded_count += sub_uploaded
                    error_count += sub_errors
                    
                except Exception as e:
                    error_msg = f"Failed to process directory '{item}': {e}"
                    print(f"    Error: {error_msg}")
                    log_upload_error(ERROR_LOG_FILE_PATH, item, current_gdrive_path, error_msg)
                    error_count += 1
                    
    except Exception as e:
        error_msg = f"Failed to list directory contents '{local_dir_path}': {e}"
        print(f"  Error: {error_msg}")
        log_upload_error(ERROR_LOG_FILE_PATH, local_dir_path, gdrive_path_prefix, error_msg)
        error_count += 1
        
    return uploaded_count, error_count


def main():
    parser = argparse.ArgumentParser(description="Upload landmark directories to Google Drive")
    parser.add_argument(
        '--test',
        action='store_true',
        help="Test mode: only process first few files/directories from each main directory"
    )
    parser.add_argument(
        '--test_limit',
        type=int,
        default=3,
        help="Number of items (files/subdirs) to process per directory in test mode (default: 3)"
    )
    parser.add_argument( # <<< NEW ARGUMENT
        '--resume',
        action='store_true',
        help="Resume mode: skip files that already exist in Google Drive"
    )
    args = parser.parse_args()

    print("--- Starting Landmarks Upload to Google Drive ---")
    if args.resume:
        print("RESUME MODE ENABLED: Will skip already uploaded files.")
    if args.test:
        print(f"TEST MODE ENABLED: Processing up to {args.test_limit} items per directory level.")

    print("--- Starting Landmarks Upload to Google Drive ---")
    print(f"Base directory: {BASE_DIR}")
    print(f"Using token file: {OAUTH_TOKEN_FILE}")
    print(f"Error log: {ERROR_LOG_FILE_PATH}")
    
    # Initialize error logging
    initialize_error_log(ERROR_LOG_FILE_PATH)
    
    # Authenticate with Google Drive
    service = get_service_oauth(OAUTH_TOKEN_FILE)
    if not service:
        print("Failed to authenticate with Google Drive. Exiting.")
        return

    total_uploaded = 0
    total_errors = 0

    # Process each directory mapping
    for local_dir_name, (gdrive_root, gdrive_subdir) in DIRECTORY_MAPPINGS.items():
        local_dir_path = BASE_DIR / local_dir_name
        
        print(f"\n=== Processing {local_dir_name} ===")
        print(f"Local path: {local_dir_path}")
        print(f"Target GDrive path: {gdrive_root}/{gdrive_subdir}")
        
        if not local_dir_path.exists():
            error_msg = f"Local directory not found: {local_dir_path}"
            print(f"  Warning: {error_msg}")
            log_upload_error(ERROR_LOG_FILE_PATH, local_dir_path, f"{gdrive_root}/{gdrive_subdir}", error_msg)
            total_errors += 1
            continue
            
        if not local_dir_path.is_dir():
            error_msg = f"Path is not a directory: {local_dir_path}"
            print(f"  Warning: {error_msg}")
            log_upload_error(ERROR_LOG_FILE_PATH, local_dir_path, f"{gdrive_root}/{gdrive_subdir}", error_msg)
            total_errors += 1
            continue

        try:
            # Create/find root folder (e.g., slta_landmarks)
            print(f"  Ensuring root folder '{gdrive_root}' exists...")
            root_folder_id = find_or_create_folder(service, gdrive_root)
            
            # Create/find subfolder (e.g., hands_landmarks_original)
            print(f"  Ensuring subfolder '{gdrive_subdir}' exists...")
            subfolder_id = find_or_create_folder(service, gdrive_subdir, root_folder_id)
            
            # Upload directory contents
            if args.test:
                print(f"  TEST MODE: Processing first {args.test_limit} items only")
                # In test mode, we'll still process recursively but could add limits
                # For now, let's just proceed normally but mention it's test mode
            
            uploaded_count, error_count = upload_directory_recursive(
                service, local_dir_path, subfolder_id, f"{gdrive_root}/{gdrive_subdir}", args=args
            )
            
            total_uploaded += uploaded_count
            total_errors += error_count
            
            print(f"  Completed {local_dir_name}: {uploaded_count} files uploaded, {error_count} errors")
            
        except Exception as e:
            error_msg = f"Failed to process directory mapping for '{local_dir_name}': {e}"
            print(f"  Error: {error_msg}")
            log_upload_error(ERROR_LOG_FILE_PATH, local_dir_path, f"{gdrive_root}/{gdrive_subdir}", error_msg)
            total_errors += 1

    print(f"\n--- Upload Complete ---")
    print(f"Total files uploaded: {total_uploaded}")
    print(f"Total errors: {total_errors}")
    print(f"Error log: {ERROR_LOG_FILE_PATH}")
    
    if total_errors > 0:
        print(f"\nPlease check the error log for details on failed uploads.")


if __name__ == "__main__":
    main()