import os
import pandas as pd
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

def get_service():
    creds = service_account.Credentials.from_service_account_file(
        'sa_credentials.json',
        scopes=['https://www.googleapis.com/auth/drive']
    )
    return build('drive', 'v3', credentials=creds)

def find_or_create_folder(service, name, parent_id=None):
    query= f"name='{name}' and mimeType='application/vnd.google-apps.folder'"
    if parent_id:
        query += f" and '{parent_id}' in parents"

    res = service.files().list(q=query, fields='files(id)').execute()
    files = res.get('files')
    if files:
        return files[0]['id']
    metadata = {
        'name': name,
        'mimeType': 'application/vnd.google-apps.folder',
        'parents': [parent_id] if parent_id else []
    }
    folder = service.files().create(body=metadata, fields='id').execute()
    return folder['id']

def upload_file(service, path, folder_id):
    metadata = {'name': os.path.basename(path), 'parents': [folder_id]}
    media = MediaFileUpload(path, resumable=True)
    service.files().create(body=metadata, media_body=media).execute()

def main():
    service = get_service()
    df = pd.read_csv('splitted_asl.csv')
    root_id = find_or_create_folder(service, 'slta_dataset')

    for _, row in df.iterrows():
        local_path = f"./downloads/{row['filename']}"
        if not os.path.exists(local_path):
            continue
        split_id = find_or_create_folder(service, row['dataset_split'], root_id)
        category_id = find_or_create_folder(service, row['category'], split_id)
        try:
            upload_file(service, local_path, category_id)
        except Exception as e:
            print(f"Error uploading {local_path}: {e}")

if __name__ == "__main__":
    main()