import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(ROOT))

try:
    from one_one_one_rule.settings import load_settings
    from one_one_one_rule.load_drive import upload_csv
except ImportError:
    print("❌ Error: ไม่พบ Module ใน one_one_one_rule/. ตรวจสอบว่าคุณรัน Script จาก Root Folder")
    sys.exit(1)


def upload_file_to_drive(file_path: Path, file_name_override: str | None = None) -> str | None:
    settings = load_settings()
    file_name = file_name_override or file_path.name

    try:
        file_id = upload_csv(
            file_path=file_path,
            service_account_file=settings.drive_service_account_file,
            file_name=file_name,
            folder_id=settings.drive_folder_id,
            file_id=settings.drive_file_id,
            auth_mode=settings.drive_auth_mode,
            oauth_client_file=settings.drive_oauth_client_file,
            oauth_token_file=settings.drive_oauth_token_file,
            convert_to_sheets=settings.drive_convert_to_sheets,
        )
        print(f"✅ อัปโหลดสำเร็จ! File ID: {file_id}")
        return file_id
    except Exception as e:
        print(f"❌ เกิดข้อผิดพลาด: {e}")
        return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload a file to Google Drive (uses .env settings)")
    parser.add_argument(
        "--file",
        default="test_upload_oauth.txt",
        help="Path to a local file to upload",
    )
    parser.add_argument(
        "--file-name",
        default="",
        help="Optional Drive file name override",
    )
    args = parser.parse_args()

    file_path = Path(args.file)
    if not file_path.exists():
        file_path.write_text("ไฟล์ทดสอบการอัปโหลดด้วย OAuth 2.0 สำหรับ Gmail ส่วนตัว", encoding="utf-8")

    upload_file_to_drive(file_path, file_name_override=args.file_name or None)
