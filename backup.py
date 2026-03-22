import os
import io
import datetime
import json

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseUpload
from google.auth.transport.requests import Request
import pandas as pd

SCOPES = ['https://www.googleapis.com/auth/drive.file']
CREDENTIALS_FILE = os.path.join(os.path.dirname(__file__), 'credentials.json')
TOKEN_FILE = os.path.join(os.path.dirname(__file__), 'token.json')
BACKUP_FOLDER_NAME = 'MartaBackup'
REDIRECT_URI = 'http://localhost:5000/backup/oauth2callback'


def get_flow():
    return Flow.from_client_secrets_file(
        CREDENTIALS_FILE,
        scopes=SCOPES,
        redirect_uri=REDIRECT_URI
    )


def get_credentials():
    if not os.path.exists(TOKEN_FILE):
        return None
    with open(TOKEN_FILE) as f:
        token_data = json.load(f)
    creds = Credentials.from_authorized_user_info(token_data, SCOPES)
    return creds


def save_credentials(creds):
    with open(TOKEN_FILE, 'w') as f:
        f.write(creds.to_json())


def is_authorized():
    creds = get_credentials()
    if creds is None:
        return False
    if creds.expired and creds.refresh_token:
        try:
            creds.refresh(Request())
            save_credentials(creds)
            return True
        except Exception:
            return False
    return creds.valid


def get_drive_service():
    creds = get_credentials()
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
        save_credentials(creds)
    return build('drive', 'v3', credentials=creds)


def _get_or_create_backup_folder(service):
    results = service.files().list(
        q=f"name='{BACKUP_FOLDER_NAME}' and mimeType='application/vnd.google-apps.folder' and trashed=false",
        fields='files(id, name)'
    ).execute()
    files = results.get('files', [])
    if files:
        return files[0]['id']
    folder = service.files().create(
        body={'name': BACKUP_FOLDER_NAME, 'mimeType': 'application/vnd.google-apps.folder'},
        fields='id'
    ).execute()
    return folder['id']


def run_backup(db_path):
    """
    Uploads marta.db and an Excel export (3 sheets) to Google Drive.
    Returns a result dict.
    """
    import functions as f

    service = get_drive_service()
    folder_id = _get_or_create_backup_folder(service)
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    uploaded = []

    # 1. Upload raw SQLite file
    db_filename = f'marta_{timestamp}.db'
    media = MediaFileUpload(db_path, mimetype='application/octet-stream', resumable=False)
    db_file = service.files().create(
        body={'name': db_filename, 'parents': [folder_id]},
        media_body=media,
        fields='id,name'
    ).execute()
    uploaded.append(db_file['name'])

    # 2. Upload Excel with 3 sheets (human-readable)
    excel_buffer = io.BytesIO()
    with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
        f.get_clients_data().to_excel(writer, sheet_name='Clientes', index=False)
        f.get_articles_data().to_excel(writer, sheet_name='Articulos', index=False)
        f.get_orders_data().to_excel(writer, sheet_name='Pedidos', index=False)
    excel_buffer.seek(0)

    excel_filename = f'marta_{timestamp}.xlsx'
    media = MediaIoBaseUpload(
        excel_buffer,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        resumable=False
    )
    excel_file = service.files().create(
        body={'name': excel_filename, 'parents': [folder_id]},
        media_body=media,
        fields='id,name'
    ).execute()
    uploaded.append(excel_file['name'])

    return {'timestamp': timestamp, 'files': uploaded}


def get_backup_history(limit=10):
    """Returns list of recent backup files from Drive, most recent first."""
    try:
        service = get_drive_service()
        folder_id = _get_or_create_backup_folder(service)
        results = service.files().list(
            q=f"'{folder_id}' in parents and trashed=false",
            orderBy='createdTime desc',
            pageSize=limit,
            fields='files(id, name, createdTime, size)'
        ).execute()
        return results.get('files', [])
    except Exception:
        return []


def disconnect():
    """Remove saved token, effectively disconnecting Drive."""
    if os.path.exists(TOKEN_FILE):
        os.remove(TOKEN_FILE)
