import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

from .perf import log_perf

# Load environment variables from config/.env once.
load_dotenv("config/.env", override=False)


@log_perf
def _env_bool(key: str, default: bool = False) -> bool:
    # Parse common truthy values from environment variables.
    raw = os.getenv(key)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


@dataclass(frozen=True)
class Settings:
    # Centralized pipeline settings loaded from .env.
    timezone: str
    output_dir: Path
    output_basename: str
    write_dated_copy: bool
    undervalued_threshold: float
    overvalued_threshold: float
    peg_max: float
    pe_sector_max_mult: float
    margin_of_safety_min: float

    use_wikipedia_tickers: bool
    ticker_fallback_file: Path

    upload_to_drive: bool
    drive_auth_mode: str
    drive_service_account_file: Path
    drive_oauth_client_file: Path
    drive_oauth_token_file: Path
    drive_convert_to_sheets: bool
    drive_folder_id: str | None
    drive_file_name: str
    drive_file_id: str | None

    log_dir: Path
    log_file_prefix: str
    upload_log_to_drive: bool


@log_perf
def load_settings() -> Settings:
    # Build settings with sane defaults to keep the pipeline reproducible.
    timezone = os.getenv("PIPELINE_TIMEZONE", "Asia/Bangkok")
    output_dir = Path(os.getenv("OUTPUT_DIR", "data"))
    output_basename = os.getenv("OUTPUT_BASENAME", "nasdaq100_latest.csv")
    write_dated_copy = _env_bool("WRITE_DATED_COPY", True)

    undervalued_threshold = float(os.getenv("UNDERVALUED_THRESHOLD", "0.90"))
    overvalued_threshold = float(os.getenv("OVERVALUED_THRESHOLD", "1.10"))
    peg_max = float(os.getenv("PEG_MAX", "1.0"))
    pe_sector_max_mult = float(os.getenv("PE_SECTOR_MAX_MULT", "1.0"))
    margin_of_safety_min = float(os.getenv("MARGIN_OF_SAFETY_MIN", "0.0"))

    use_wikipedia_tickers = _env_bool("USE_WIKIPEDIA_TICKERS", True)
    ticker_fallback_file = Path(os.getenv("TICKER_FALLBACK_FILE", "data/nasdaq100_tickers.csv"))

    upload_to_drive = _env_bool("UPLOAD_TO_DRIVE", True)
    drive_auth_mode = os.getenv("GOOGLE_DRIVE_AUTH_MODE", "oauth").strip().lower()
    drive_service_account_file = Path(
        os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "config/service_account.json")
    )
    drive_oauth_client_file = Path(
        os.getenv("GOOGLE_OAUTH_CLIENT_FILE", "config/credentials.json")
    )
    drive_oauth_token_file = Path(
        os.getenv("GOOGLE_OAUTH_TOKEN_FILE", "config/oauth_token.json")
    )
    drive_convert_to_sheets = _env_bool("GOOGLE_DRIVE_CONVERT_TO_SHEETS", True)
    drive_folder_id = os.getenv("GOOGLE_DRIVE_FOLDER_ID", "").strip() or None
    drive_file_name = os.getenv("GOOGLE_DRIVE_FILE_NAME", "nasdaq100_valuations.csv")
    drive_file_id = os.getenv("GOOGLE_DRIVE_FILE_ID", "").strip() or None

    log_dir = Path(os.getenv("LOG_DIR", "data/logs"))
    log_file_prefix = os.getenv("LOG_FILE_PREFIX", "pipeline").strip() or "pipeline"
    upload_log_to_drive = _env_bool("UPLOAD_LOG_TO_DRIVE", True)

    return Settings(
        timezone=timezone,
        output_dir=output_dir,
        output_basename=output_basename,
        write_dated_copy=write_dated_copy,
        undervalued_threshold=undervalued_threshold,
        overvalued_threshold=overvalued_threshold,
        peg_max=peg_max,
        pe_sector_max_mult=pe_sector_max_mult,
        margin_of_safety_min=margin_of_safety_min,
        use_wikipedia_tickers=use_wikipedia_tickers,
        ticker_fallback_file=ticker_fallback_file,
        upload_to_drive=upload_to_drive,
        drive_service_account_file=drive_service_account_file,
        drive_auth_mode=drive_auth_mode,
        drive_oauth_client_file=drive_oauth_client_file,
        drive_oauth_token_file=drive_oauth_token_file,
        drive_convert_to_sheets=drive_convert_to_sheets,
        drive_folder_id=drive_folder_id,
        drive_file_name=drive_file_name,
        drive_file_id=drive_file_id,
        log_dir=log_dir,
        log_file_prefix=log_file_prefix,
        upload_log_to_drive=upload_log_to_drive,
    )
