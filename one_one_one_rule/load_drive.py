from __future__ import annotations

from pathlib import Path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials as OAuthCredentials
from google.oauth2.service_account import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

from .perf import log_perf

_SCOPES = [
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/drive",
]
_SHEET_MIME = "application/vnd.google-apps.spreadsheet"


def _escape_drive_query(text: str) -> str:
    # Escape quotes for Drive search queries.
    return text.replace("'", "\\'")


@log_perf
def _get_service_account_credentials(service_account_file: Path):
    # Load service account credentials from JSON key.
    return Credentials.from_service_account_file(
        service_account_file, scopes=_SCOPES
    )


@log_perf
def _get_oauth_credentials(oauth_client_file: Path, oauth_token_file: Path):
    # Load or refresh OAuth credentials, storing a token file for reuse.
    creds = None
    if oauth_token_file.exists():
        creds = OAuthCredentials.from_authorized_user_file(
            str(oauth_token_file), scopes=_SCOPES
        )

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                str(oauth_client_file), scopes=_SCOPES
            )
            creds = flow.run_local_server(port=0)
        oauth_token_file.parent.mkdir(parents=True, exist_ok=True)
        oauth_token_file.write_text(creds.to_json())

    return creds


@log_perf
def get_drive_service(
    *,
    auth_mode: str = "oauth",
    service_account_file: Path | None = None,
    oauth_client_file: Path | None = None,
    oauth_token_file: Path | None = None,
):
    # Build the Drive API client with OAuth or Service Account auth.
    auth_mode = (auth_mode or "service_account").lower()
    if auth_mode == "oauth":
        if not oauth_client_file or not oauth_client_file.exists():
            raise ValueError("OAuth client file not found. Set GOOGLE_OAUTH_CLIENT_FILE.")
        token_file = oauth_token_file or Path("config/oauth_token.json")
        creds = _get_oauth_credentials(oauth_client_file, token_file)
    else:
        if not service_account_file or not service_account_file.exists():
            raise ValueError(
                "Service account file not found. Set GOOGLE_SERVICE_ACCOUNT_FILE."
            )
        creds = _get_service_account_credentials(service_account_file)

    return build("drive", "v3", credentials=creds)


@log_perf
def find_existing_file_id(service, name: str, folder_id: str | None) -> str | None:
    # Find a file by name (and optional parent folder).
    escaped_name = _escape_drive_query(name)
    query = f"name='{escaped_name}' and trashed=false"
    if folder_id:
        query += f" and '{folder_id}' in parents"

    result = (
        service.files()
        .list(
            q=query,
            fields="files(id,name)",
            pageSize=10,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        )
        .execute()
    )
    files = result.get("files", [])
    if not files:
        return None
    return files[0].get("id")


@log_perf
def find_folder_id(service, name: str, parent_id: str | None = None) -> str | None:
    # Find a folder by name (and optional parent).
    escaped_name = _escape_drive_query(name)
    query = (
        "mimeType='application/vnd.google-apps.folder'"
        f" and name='{escaped_name}' and trashed=false"
    )
    if parent_id:
        query += f" and '{parent_id}' in parents"

    result = (
        service.files()
        .list(
            q=query,
            fields="files(id,name)",
            pageSize=10,
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
        )
        .execute()
    )
    files = result.get("files", [])
    if not files:
        return None
    return files[0].get("id")


@log_perf
def create_folder(service, name: str, parent_id: str | None = None) -> str:
    # Create a folder and return its ID.
    metadata = {
        "name": name,
        "mimeType": "application/vnd.google-apps.folder",
    }
    if parent_id:
        metadata["parents"] = [parent_id]

    created = (
        service.files()
        .create(
            body=metadata,
            fields="id",
            supportsAllDrives=True,
        )
        .execute()
    )
    return created.get("id")


@log_perf
def upload_csv(
    file_path: Path,
    service_account_file: Path | None,
    file_name: str,
    folder_id: str | None = None,
    file_id: str | None = None,
    *,
    auth_mode: str = "oauth",
    oauth_client_file: Path | None = None,
    oauth_token_file: Path | None = None,
    convert_to_sheets: bool = True,
) -> str:
    # Convenience wrapper to upload CSV content.
    return upload_file(
        file_path=file_path,
        service_account_file=service_account_file,
        file_name=file_name,
        folder_id=folder_id,
        file_id=file_id,
        auth_mode=auth_mode,
        oauth_client_file=oauth_client_file,
        oauth_token_file=oauth_token_file,
        mime_type="text/csv",
        convert_to_sheets=convert_to_sheets,
    )


@log_perf
def upload_file(
    file_path: Path,
    service_account_file: Path | None,
    file_name: str,
    folder_id: str | None = None,
    file_id: str | None = None,
    *,
    auth_mode: str = "oauth",
    oauth_client_file: Path | None = None,
    oauth_token_file: Path | None = None,
    mime_type: str = "application/octet-stream",
    convert_to_sheets: bool = False,
) -> str:
    """
    Uploads a file to Google Drive.

    IMPORTANT: For Service Accounts, 'folder_id' is REQUIRED when creating a new file.
    Service Accounts have 0 storage quota and cannot create files in their own root.
    """
    # Use Drive API to create or update the target file.
    service = get_drive_service(
        auth_mode=auth_mode,
        service_account_file=service_account_file,
        oauth_client_file=oauth_client_file,
        oauth_token_file=oauth_token_file,
    )
    media = MediaFileUpload(str(file_path), mimetype=mime_type, resumable=True)

    # 1. Try to find existing file if ID not provided
    if not file_id:
        file_id = find_existing_file_id(service, file_name, folder_id)

    # 2. UPDATE existing file (This is fine without folder_id)
    if file_id and convert_to_sheets:
        try:
            meta = (
                service.files()
                .get(
                    fileId=file_id,
                    fields="id,mimeType",
                    supportsAllDrives=True,
                )
                .execute()
            )
            if meta.get("mimeType") != _SHEET_MIME:
                file_id = None
        except Exception:
            file_id = None

    if file_id:
        print(f"Updating existing file: {file_name} (ID: {file_id})")
        updated = (
            service.files()
            .update(
                fileId=file_id,
                media_body=media,
                supportsAllDrives=True,
            )
            .execute()
        )
        return updated.get("id")

    # 3. CREATE new file (MUST have folder_id)
    # --- FIX: Prevent creating in root for Service Account ---
    if auth_mode != "oauth" and not folder_id:
        raise ValueError(
            "folder_id is required to create a new file with a Service Account "
            "(Service Accounts do not have their own storage quota)."
        )
    # ---------------------------------------------------------

    metadata = {"name": file_name}
    if folder_id:
        metadata["parents"] = [folder_id]
    if convert_to_sheets:
        metadata["mimeType"] = _SHEET_MIME

    print(f"Creating new file: {file_name} in folder {folder_id}")
    created = (
        service.files()
        .create(
            body=metadata,
            media_body=media,
            fields="id",
            supportsAllDrives=True,
        )
        .execute()
    )
    return created.get("id")
