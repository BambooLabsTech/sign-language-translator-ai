import os
import json
import random
import math
import mimetypes
from collections import defaultdict
import numpy as np
from tqdm import tqdm # For progress bars

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload

# --- Configuration ---
DATASET_DIR = '/home/pandu/.cache/kagglehub/datasets/risangbaskoro/wlasl-processed/versions/5'
JSON_FILE = os.path.join(DATASET_DIR, 'WLASL_v0.3.json')
VIDEO_DIR = os.path.join(DATASET_DIR, 'videos')
CREDENTIALS_FILE = 'credentials.json' # Assumed to be in the same dir as script
TOKEN_FILE = 'token.json'             # Will be created after first auth

# Google Drive Configuration
DRIVE_BASE_FOLDER_NAME = 'SLTA_DATASET'
SPLIT_NAMES = ['TRAIN', 'VAL', 'TEST']
TRAIN_RATIO = 0.75
VAL_RATIO = 0.15
TEST_RATIO = 0.10 # Adjusted slightly to ensure sum <= 1.0

# Google API Scopes (Ensure Drive write access)
SCOPES = ["https://www.googleapis.com/auth/drive"]

# --- Google Drive Helper Functions ---

def authenticate():
    """Handles user authentication for Google Drive."""
    creds = None
    if os.path.exists(TOKEN_FILE):
        try:
            creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
        except Exception as e:
            print(f"Error loading token.json: {e}. Re-authenticating.")
            creds = None # Force re-authentication if token is invalid

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                print("Refreshing access token...")
                creds.refresh(Request())
            except Exception as e:
                print(f"Error refreshing token: {e}")
                print("Proceeding to full authorization flow.")
                creds = None
        if not creds:
            print("Starting authorization flow...")
            if not os.path.exists(CREDENTIALS_FILE):
                print(f"\nERROR: {CREDENTIALS_FILE} not found.")
                print("Please download OAuth 2.0 Client credentials from Google")
                print(f"Cloud Console and save as {CREDENTIALS_FILE}")
                return None
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)

        with open(TOKEN_FILE, "w") as token:
            token.write(creds.to_json())
        print(f"Credentials saved to {TOKEN_FILE}")

    return creds

def get_or_create_folder(service, folder_name, parent_id=None):
    """Finds a folder by name or creates it if it doesn't exist."""
    query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder'"
    if parent_id:
        query += f" and '{parent_id}' in parents"
    else:
        # Search in root if no parent specified (though we usually will specify)
        query += " and 'root' in parents"
    query += " and trashed=false"

    try:
        response = service.files().list(q=query, spaces='drive', fields='files(id, name)').execute()
        files = response.get('files', [])
        if files:
            print(f"Found existing folder: '{folder_name}' (ID: {files[0].get('id')})")
            return files[0].get('id')
        else:
            print(f"Creating folder: '{folder_name}'" + (f" inside parent ID {parent_id}" if parent_id else " in root"))
            file_metadata = {
                'name': folder_name,
                'mimeType': 'application/vnd.google-apps.folder'
            }
            if parent_id:
                file_metadata['parents'] = [parent_id]

            folder = service.files().create(body=file_metadata, fields='id').execute()
            print(f"Created folder '{folder_name}' with ID: {folder.get('id')}")
            return folder.get('id')
    except HttpError as error:
        print(f"An error occurred finding/creating folder '{folder_name}': {error}")
        return None
    except Exception as e:
         print(f"An unexpected error occurred finding/creating folder '{folder_name}': {e}")
         return None

def upload_file_to_folder(service, local_path, parent_folder_id, drive_filename=None):
    """Uploads a single file to a specific Google Drive folder."""
    if not os.path.exists(local_path):
        print(f"Error: Local file not found: '{local_path}'")
        return None

    file_name = drive_filename or os.path.basename(local_path)
    mime_type, _ = mimetypes.guess_type(local_path)
    if mime_type is None:
        mime_type = 'application/octet-stream'

    file_metadata = {'name': file_name, 'parents': [parent_folder_id]}
    media = MediaFileUpload(local_path, mimetype=mime_type, resumable=True)

    try:
        # print(f"  Uploading '{file_name}' to folder ID {parent_folder_id}...")
        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id, name'
        ).execute()
        # print(f"  Successfully uploaded '{file.get('name')}' (ID: {file.get('id')})")
        return file.get('id')
    except HttpError as error:
        print(f"\nAn error occurred uploading '{file_name}': {error}")
        # Consider retries or logging failures here
        return None
    except Exception as e:
        print(f"\nAn unexpected error occurred uploading '{file_name}': {e}")
        return None

# --- Data Processing Functions ---

def calculate_splits(total_items, train_r, val_r, test_r):
    """Calculates the number of items for train, val, test splits."""
    if total_items == 0:
        return 0, 0, 0
    if total_items == 1:
        return 1, 0, 0 # Assign single item to train
    if total_items == 2:
        return 1, 1, 0 # Assign one to train, one to val

    # Ensure ratios sum reasonably (adjust test if needed)
    if train_r + val_r + test_r > 1.0:
         print(f"Warning: Ratios sum to > 1.0 ({train_r + val_r + test_r}). Adjusting test ratio down.")
         test_r = max(0, 1.0 - train_r - val_r)

    n_train = math.floor(total_items * train_r)
    n_val = math.floor(total_items * val_r)

    # Adjust n_val slightly if train+val is too large due to flooring artifacts
    # and there are enough items for test
    if n_train + n_val >= total_items -1 and total_items > 2: # need at least 1 for test
        n_val = max(0, total_items - n_train - 1) # Ensure at least 1 left for test

    # Assign remaining to test, ensuring it's at least 0
    n_test = max(0, total_items - n_train - n_val)

    # Distribute any rounding remainder (usually 0 or 1 item)
    # Prioritize train, then val, then test if sum is still less
    remainder = total_items - (n_train + n_val + n_test)
    if remainder > 0:
       n_train += 1
       remainder -= 1
    if remainder > 0 and n_val > 0: # Add to val only if val is not zero
        n_val += 1
        remainder -=1
    if remainder > 0: # Add any left over to test
        n_test += 1


    # Final sanity check
    if n_train + n_val + n_test != total_items:
       print(f"Error in split calculation! Total={total_items}, Split={n_train},{n_val},{n_test}. Sum={n_train+n_val+n_test}")
       # Fallback: give all but one to train, one to val (if possible)
       n_train = max(1, total_items -1) if total_items > 1 else total_items
       n_val = 1 if total_items > 1 else 0
       n_test = 0

    return n_train, n_val, n_test


# --- Main Execution Logic ---

def main():
    print("--- WLASL Data Preparation and Google Drive Upload ---")

    # 1. Authenticate with Google Drive
    print("\nStep 1: Authenticating with Google Drive...")
    creds = authenticate()
    if not creds:
        print("Authentication failed. Exiting.")
        return
    try:
        service = build("drive", "v3", credentials=creds)
        print("Google Drive service created successfully.")
    except Exception as e:
        print(f"Failed to build Drive service: {e}")
        return

    # 2. Load JSON Data
    print(f"\nStep 2: Loading JSON data from {JSON_FILE}...")
    try:
        with open(JSON_FILE, 'r') as f:
            wlasl_data = json.load(f)
        print(f"Loaded {len(wlasl_data)} gloss entries.")
    except FileNotFoundError:
        print(f"Error: JSON file not found at {JSON_FILE}")
        return
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from {JSON_FILE}")
        return

    # 3. Identify Existing Video Files
    print(f"\nStep 3: Identifying existing video files in {VIDEO_DIR}...")
    if not os.path.isdir(VIDEO_DIR):
        print(f"Error: Video directory not found at {VIDEO_DIR}")
        return
    try:
        existing_videos = {f for f in os.listdir(VIDEO_DIR) if f.endswith('.mp4')}
        print(f"Found {len(existing_videos)} .mp4 files locally.")
    except OSError as e:
        print(f"Error accessing video directory {VIDEO_DIR}: {e}")
        return

    # 4. Filter JSON and Group by Gloss (only existing videos)
    print("\nStep 4: Filtering JSON data and grouping by gloss...")
    gloss_to_existing_videos = defaultdict(list)
    total_instances_in_json = 0
    included_instances = 0
    missing_video_count = 0

    for entry in tqdm(wlasl_data, desc="Processing Glosses"):
        gloss = entry.get('gloss')
        instances = entry.get('instances', [])
        if not gloss or not instances:
            continue

        for instance in instances:
            total_instances_in_json += 1
            video_id = instance.get('video_id')
            if not video_id:
                continue

            video_filename = f"{video_id}.mp4"
            if video_filename in existing_videos:
                gloss_to_existing_videos[gloss].append(video_filename)
                included_instances += 1
            else:
                missing_video_count += 1

    print(f"Processed {total_instances_in_json} instances mentioned in JSON.")
    print(f"Found {included_instances} instances with corresponding video files.")
    print(f"{missing_video_count} instances skipped due to missing video files.")
    print(f"Data grouped for {len(gloss_to_existing_videos)} glosses with available videos.")

    # 5. Split Data into Train/Val/Test
    print("\nStep 5: Splitting data into TRAIN/VAL/TEST sets...")
    split_data = {'TRAIN': defaultdict(list), 'VAL': defaultdict(list), 'TEST': defaultdict(list)}
    total_files_to_upload = 0

    for gloss, video_list in tqdm(gloss_to_existing_videos.items(), desc="Splitting Glosses"):
        num_videos = len(video_list)
        if num_videos == 0:
            continue

        # Shuffle for random assignment
        random.shuffle(video_list)

        n_train, n_val, n_test = calculate_splits(num_videos, TRAIN_RATIO, VAL_RATIO, TEST_RATIO)
        # print(f"  Gloss '{gloss}': {num_videos} videos -> Train={n_train}, Val={n_val}, Test={n_test}") # Debug print

        split_data['TRAIN'][gloss] = video_list[:n_train]
        split_data['VAL'][gloss] = video_list[n_train : n_train + n_val]
        split_data['TEST'][gloss] = video_list[n_train + n_val :]

        total_files_to_upload += num_videos

    print(f"Data split complete. Total files to potentially upload: {total_files_to_upload}")
    print(f"  Train set: {sum(len(v) for v in split_data['TRAIN'].values())} files across {len(split_data['TRAIN'])} glosses")
    print(f"  Val set:   {sum(len(v) for v in split_data['VAL'].values())} files across {len(split_data['VAL'])} glosses")
    print(f"  Test set:  {sum(len(v) for v in split_data['TEST'].values())} files across {len(split_data['TEST'])} glosses")


    # 6. Create Google Drive Folders and Upload
    print(f"\nStep 6: Creating Google Drive folder structure under '{DRIVE_BASE_FOLDER_NAME}' and uploading files...")

    # Get/Create Base Folder
    base_folder_id = get_or_create_folder(service, DRIVE_BASE_FOLDER_NAME, parent_id='root') # Create in root
    if not base_folder_id:
        print("Failed to get or create base folder. Exiting.")
        return

    # Get/Create Split Folders
    split_folder_ids = {}
    for split_name in SPLIT_NAMES:
        folder_id = get_or_create_folder(service, split_name, parent_id=base_folder_id)
        if not folder_id:
            print(f"Failed to get or create {split_name} folder. Skipping this split.")
            continue
        split_folder_ids[split_name] = folder_id

    # Iterate through splits, glosses, and files to upload
    upload_count = 0
    upload_errors = 0
    with tqdm(total=total_files_to_upload, desc="Uploading Videos", unit="file") as pbar:
        for split_name, gloss_map in split_data.items():
            if split_name not in split_folder_ids:
                num_skipped = sum(len(v) for v in gloss_map.values())
                print(f"Skipping {num_skipped} files for split '{split_name}' due to folder creation error.")
                pbar.update(num_skipped)
                continue

            parent_split_folder_id = split_folder_ids[split_name]
            # print(f"\nProcessing {split_name} split...")

            for gloss, video_filenames in gloss_map.items():
                if not video_filenames: # Skip if a gloss ended up with 0 videos in this split
                    continue

                # Sanitize gloss name for folder creation if necessary (e.g., replace slashes)
                safe_gloss_name = gloss.replace('/', '_').replace('\\', '_') # Basic sanitization

                # Get/Create Gloss Folder inside the split folder
                gloss_folder_id = get_or_create_folder(service, safe_gloss_name, parent_id=parent_split_folder_id)
                if not gloss_folder_id:
                    print(f"  Failed to get/create folder for gloss '{safe_gloss_name}' in {split_name}. Skipping {len(video_filenames)} files.")
                    pbar.update(len(video_filenames))
                    upload_errors += len(video_filenames)
                    continue

                # print(f"  Uploading {len(video_filenames)} videos for gloss '{gloss}' to {split_name}/{safe_gloss_name}...")
                for video_filename in video_filenames:
                    local_file_path = os.path.join(VIDEO_DIR, video_filename)
                    uploaded_id = upload_file_to_folder(service, local_file_path, gloss_folder_id)
                    if uploaded_id:
                        upload_count += 1
                    else:
                        upload_errors += 1
                    pbar.update(1) # Update progress bar for each file attempt

    print("\n--- Upload Summary ---")
    print(f"Attempted to upload: {total_files_to_upload} files")
    print(f"Successfully uploaded: {upload_count} files")
    print(f"Upload errors/skips: {upload_errors} files")
    print("--- Script finished ---")


if __name__ == "__main__":
    # Set random seed for reproducibility of splits (optional)
    random.seed(42)
    main()