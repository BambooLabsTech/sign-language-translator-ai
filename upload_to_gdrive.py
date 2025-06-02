import os
import pandas as pd
import argparse
import csv # For writing to the error log
from pathlib import Path # For easier path management

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import mimetypes # Moved import to top

# --- Base Configuration (paths that don't change per part) ---
BASE_CSV_DIR = Path('/home/pandu/Documents/explore/sign-language-translator-ai')
VIDEO_BASE_DIR = Path('/home/pandu/Documents/explore/sign-language-translator-ai/videos')
GDRIVE_ROOT_FOLDER_NAME = 'slta_dataset'
DEFAULT_NUM_TEST_FILES = 5
SCOPES = ["https://www.googleapis.com/auth/drive"]
SCRIPT_CWD = Path.cwd() # Directory where the script is run, for token and log files

# --- OAuth 2.0 Client Secret File (Shared by all parts) ---
OAUTH_CLIENT_SECRET_FILE = SCRIPT_CWD / 'credentials.json'


def initialize_error_log(log_file_path, df_columns):
    """Creates the error log CSV with headers if it doesn't exist."""
    if not log_file_path.exists():
        with open(log_file_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(list(df_columns) + ['upload_error_message', 'timestamp'])
        print(f"Initialized error log: {log_file_path}")

def log_upload_error(log_file_path, row_data, error_message, df_columns):
    """Appends a failed item and error to the CSV log."""
    import datetime # Local import to keep it close to usage
    timestamp = datetime.datetime.now().isoformat()
    
    # Ensure row_data is a list in the order of df_columns
    ordered_row_values = [row_data.get(col, '') for col in df_columns]

    with open(log_file_path, 'a', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(ordered_row_values + [str(error_message), timestamp])

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
        # Optional: Verify by getting user info
        # about = service.about().get(fields="user").execute()
        # print(f"Authenticated as: {about['user']['emailAddress']}")
        return service
    except Exception as e:
        print(f"Error building Google Drive service with user credentials: {e}")
        return None

def find_or_create_folder(service, name, parent_id=None):
    query = f"name='{name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
    if parent_id:
        query += f" and '{parent_id}' in parents"
    
    res = service.files().list(q=query, fields='files(id, name)').execute()
    files = res.get('files')
    if files:
        return files[0]['id']
    
    # print(f"Folder '{name}' not found by this script instance. Creating...")
    metadata = {'name': name, 'mimeType': 'application/vnd.google-apps.folder'}
    if parent_id:
        metadata['parents'] = [parent_id]
    
    folder = service.files().create(body=metadata, fields='id').execute()
    print(f"  Created folder '{name}' with ID: {folder['id']} (by this script instance or found if created concurrently)")
    return folder['id']

def upload_file(service, local_file_path, folder_id):
    file_name = local_file_path.name
    metadata = {'name': file_name, 'parents': [folder_id]}
    mime_type, _ = mimetypes.guess_type(str(local_file_path))
    if mime_type is None:
        mime_type = 'application/octet-stream'

    media = MediaFileUpload(str(local_file_path), mimetype=mime_type, resumable=True)
    file = service.files().create(body=metadata, media_body=media, fields='id, name').execute()
    print(f"  Successfully uploaded '{file.get('name')}' (ID: {file.get('id')})")


def main():
    parser = argparse.ArgumentParser(description="Upload ASL videos to Google Drive using OAuth 2.0 (Part-based).")
    parser.add_argument(
        '--part_num',
        type=int,
        required=True,
        help="Part number for this script instance (e.g., 1, 2, 3). Determines CSV, token, and log files."
    )
    parser.add_argument(
        '--mode',
        choices=['test', 'full'],
        default='full', # Default to full, as 'test' might be less useful for split parts
        help="Mode of operation: 'test' for a sample, 'full' for all in the part (default: full)."
    )
    parser.add_argument(
        '--num_test_files',
        type=int,
        default=DEFAULT_NUM_TEST_FILES,
        help=f"Number of files to upload in 'test' mode (default: {DEFAULT_NUM_TEST_FILES})."
    )
    args = parser.parse_args()

    # --- Dynamic Configuration based on part_num ---
    PART_NUM = args.part_num
    CSV_FILE_PATH = BASE_CSV_DIR / f"splitted_asl_part{PART_NUM}.csv"
    OAUTH_TOKEN_FILE = SCRIPT_CWD / f"token_part{PART_NUM}.json"
    ERROR_LOG_FILE_PATH = SCRIPT_CWD / f"upload_errors_part{PART_NUM}.csv"

    print(f"--- Starting Google Drive Upload Script (OAuth 2.0) for PART {PART_NUM} ---")
    print(f"Reading CSV from: {CSV_FILE_PATH}")
    print(f"Using token file: {OAUTH_TOKEN_FILE}")
    print(f"Logging errors to: {ERROR_LOG_FILE_PATH}")

    service = get_service_oauth(OAUTH_TOKEN_FILE)
    if not service:
        print("Failed to authenticate or build Google Drive service. Exiting.")
        return

    try:
        df = pd.read_csv(CSV_FILE_PATH)
        print(f"Successfully loaded CSV: {CSV_FILE_PATH} ({len(df)} rows)")
    except FileNotFoundError:
        print(f"Error: CSV file not found at {CSV_FILE_PATH}")
        return
    except Exception as e:
        print(f"Error reading CSV file '{CSV_FILE_PATH}': {e}")
        return

    # Store original DataFrame column order for logging
    original_df_columns = df.columns.tolist()
    initialize_error_log(ERROR_LOG_FILE_PATH, original_df_columns)

    initial_rows = len(df)
    df_filtered = df[(df['is_valid'] == True) & (df['is_duplicate'] == False)].copy()
    
    if len(df_filtered) < initial_rows:
        print(f"Warning: Sanity check filtered out {initial_rows - len(df_filtered)} rows from part {PART_NUM} CSV.")
    
    if df_filtered.empty:
        print(f"No valid data to process in part {PART_NUM} CSV after filtering.")
        return

    if args.mode == 'test':
        num_files_to_sample = min(args.num_test_files, len(df_filtered))
        if num_files_to_sample > 0:
            df_to_process = df_filtered.sample(n=num_files_to_sample, random_state=42) # Use different random_state per part if truly random sampling is desired for testing
        else:
            df_to_process = pd.DataFrame()
        print(f"--- Test Mode: Attempting to process {len(df_to_process)} random files from part {PART_NUM} ---")
    else: # args.mode == 'full'
        df_to_process = df_filtered
        print(f"--- Full Mode: Attempting to process {len(df_to_process)} files from part {PART_NUM} ---")

    if df_to_process.empty:
        print(f"No files selected for processing in part {PART_NUM}. Exiting.")
        return

    # Common root folder - multiple scripts might try to create it, find_or_create_folder handles this
    try:
        # print(f"Ensuring root folder '{GDRIVE_ROOT_FOLDER_NAME}' exists in Google Drive...")
        root_folder_id = find_or_create_folder(service, GDRIVE_ROOT_FOLDER_NAME)
    except Exception as e:
        print(f"Could not ensure Google Drive root folder '{GDRIVE_ROOT_FOLDER_NAME}'. Aborting part {PART_NUM}. Error: {e}")
        log_upload_error(ERROR_LOG_FILE_PATH, {"script_error": "Root folder creation failed"}, f"Root folder '{GDRIVE_ROOT_FOLDER_NAME}' creation/finding failed: {e}", ["script_error"])
        return
        
    total_files_to_process = len(df_to_process)
    processed_count = 0
    skipped_missing_local_file = 0
    error_count = 0

    for i, row_tuple in enumerate(df_to_process.iterrows()):
        index, row_series = row_tuple # iterrows() yields (index, Series)
        row_dict = row_series.to_dict() # Convert Series to dict for easier logging

        print(f"\nPart {PART_NUM} - Processing file {i + 1}/{total_files_to_process}:")
        print(f"  CSV row details: id={row_dict.get('id')}, category='{row_dict.get('category')}', dataset_split='{row_dict.get('dataset_split')}', filename='{row_dict.get('filename')}'")

        local_video_path = VIDEO_BASE_DIR / str(row_dict.get('filename', ''))

        if not row_dict.get('filename') or not local_video_path.exists():
            err_msg = f"Local video file not found or filename missing: {local_video_path}"
            print(f"  Warning: {err_msg}. Skipping.")
            log_upload_error(ERROR_LOG_FILE_PATH, row_dict, err_msg, original_df_columns)
            skipped_missing_local_file += 1
            error_count +=1
            continue

        try:
            split_folder_name = str(row_dict.get('dataset_split'))
            split_folder_id = find_or_create_folder(service, split_folder_name, root_folder_id)
            
            category_folder_name = str(row_dict.get('category'))
            category_folder_id = find_or_create_folder(service, category_folder_name, split_folder_id)
            
            print(f"  Target GDrive path: {GDRIVE_ROOT_FOLDER_NAME}/{split_folder_name}/{category_folder_name}/{local_video_path.name}")
            upload_file(service, local_video_path, category_folder_id)
            processed_count += 1
        except Exception as e:
            err_msg_upload = f"Failed to process/upload '{row_dict.get('filename')}': {e}"
            print(f"  {err_msg_upload}. Logging and Continuing.")
            log_upload_error(ERROR_LOG_FILE_PATH, row_dict, err_msg_upload, original_df_columns)
            error_count += 1
            # Continue to the next file

    print(f"\n--- Script Part {PART_NUM} Finished ---")
    print(f"Summary for Part {PART_NUM}:")
    print(f"  Total files from CSV (after filter): {len(df_filtered)}")
    print(f"  Files selected for processing ({args.mode} mode): {total_files_to_process}")
    print(f"  Successfully uploaded: {processed_count}")
    print(f"  Skipped (local file not found/filename missing): {skipped_missing_local_file}")
    print(f"  Failed during GDrive operation (logged to error file): {error_count - skipped_missing_local_file}") # Adjust if skipped is already part of error_count
    print(f"  Total errors logged: {error_count}")
    print(f"  Error log for this part: {ERROR_LOG_FILE_PATH}")

if __name__ == "__main__":
    main()