from __future__ import annotations

from datetime import datetime, timezone as dt_timezone
import logging
from zoneinfo import ZoneInfo

import pandas as pd

from .settings import load_settings
from .perf import log_perf
from .load_drive import upload_csv, upload_file
from .extract_tickers import load_nasdaq100_tickers
from .transform_valuation import (
    ValuationThresholds,
    apply_valuation,
    cleanse_fundamentals,
    fetch_fundamentals,
)


@log_perf
def _build_output(df: pd.DataFrame, as_of_date: str, run_ts_utc: str) -> pd.DataFrame:
    # Insert run metadata and order columns for consistent downstream use.
    df = df.copy()
    df.insert(0, "as_of_date", as_of_date)
    df.insert(1, "run_ts_utc", run_ts_utc)

    ordered = [
        "as_of_date",
        "run_ts_utc",
        "ticker",
        "company",
        "sector",
        "price",
        "peg_ratio",
        "peg_ratio_source",
        "trailing_pe",
        "sector_median_pe",
        "pe_median_used",
        "forward_pe",
        "earnings_growth",
        "trailing_eps",
        "forward_eps",
        "book_value_per_share",
        "graham_value",
        "margin_of_safety",
        "peg_pass",
        "pe_vs_sector_pass",
        "margin_of_safety_pass",
        "valuation_hunter",
        "fair_value",
        "fair_value_source",
        "valuation",
        "pct_diff",
        "currency",
        "market_cap",
        "target_mean_price",
    ]

    cols = [c for c in ordered if c in df.columns] + [c for c in df.columns if c not in ordered]
    return df[cols]


@log_perf
def _setup_logger(log_path) -> logging.Logger:
    # Configure a file+console logger for the pipeline run.
    logger = logging.getLogger("pipeline")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    return logger


@log_perf
def main() -> None:
    settings = load_settings()
    timezone = ZoneInfo(settings.timezone)

    # Use configured timezone for business date; keep UTC for traceability.
    now_local = datetime.now(timezone)
    as_of_date = now_local.date().isoformat()
    run_ts_utc = (
        datetime.now(dt_timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )

    # Initialize log file for this run.
    settings.log_dir.mkdir(parents=True, exist_ok=True)
    log_name = f"{settings.log_file_prefix}_{as_of_date}.log"
    log_path = settings.log_dir / log_name
    logger = _setup_logger(log_path)

    logger.info("Pipeline start")
    logger.info("as_of_date=%s run_ts_utc=%s", as_of_date, run_ts_utc)

    try:
        # 1) Extract tickers.
        tickers = load_nasdaq100_tickers(
            settings.use_wikipedia_tickers,
            settings.ticker_fallback_file,
        )
        logger.info("Loaded tickers: %d", len(tickers))

        # 2) Fetch fundamentals and compute valuations.
        fundamentals = fetch_fundamentals(tickers)
        logger.info("Fetched fundamentals rows: %d", len(fundamentals))
        fundamentals = cleanse_fundamentals(fundamentals)
        logger.info("After cleansing rows: %d", len(fundamentals))
        thresholds = ValuationThresholds(
            undervalued=settings.undervalued_threshold,
            overvalued=settings.overvalued_threshold,
            peg_max=settings.peg_max,
            pe_sector_max_mult=settings.pe_sector_max_mult,
            margin_of_safety_min=settings.margin_of_safety_min,
        )
        valued = apply_valuation(fundamentals, thresholds)
        output = _build_output(valued, as_of_date, run_ts_utc)

        # 3) Write local outputs (latest + optional dated copy).
        settings.output_dir.mkdir(parents=True, exist_ok=True)
        latest_path = settings.output_dir / settings.output_basename
        output.to_csv(latest_path, index=False)
        logger.info("Wrote latest CSV: %s", latest_path)

        dated_name = None
        dated_path = None
        if settings.write_dated_copy:
            dated_name = f"nasdaq100_valuations_{as_of_date}.csv"
            dated_path = settings.output_dir / dated_name
            if dated_path != latest_path:
                output.to_csv(dated_path, index=False)
            logger.info("Wrote dated CSV: %s", dated_path)

        # 4) Upload data (and optional log) to Google Drive.
        if settings.upload_to_drive:
            if settings.drive_auth_mode == "service_account" and not settings.drive_service_account_file.exists():
                raise RuntimeError(
                    "ไม่พบไฟล์ service account ที่ระบุใน GOOGLE_SERVICE_ACCOUNT_FILE"
                )
            upload_path = latest_path
            upload_name = settings.drive_file_name
            if settings.write_dated_copy and dated_path and dated_name:
                upload_path = dated_path
                upload_name = dated_name

            file_id = upload_csv(
                file_path=upload_path,
                service_account_file=settings.drive_service_account_file,
                file_name=upload_name,
                folder_id=settings.drive_folder_id,
                file_id=settings.drive_file_id,
                auth_mode=settings.drive_auth_mode,
                oauth_client_file=settings.drive_oauth_client_file,
                oauth_token_file=settings.drive_oauth_token_file,
                convert_to_sheets=settings.drive_convert_to_sheets,
            )
            logger.info("Uploaded data to Drive: %s", file_id)

        if settings.upload_log_to_drive:
            log_id = upload_file(
                file_path=log_path,
                service_account_file=settings.drive_service_account_file,
                file_name=log_name,
                folder_id=settings.drive_folder_id,
                auth_mode=settings.drive_auth_mode,
                oauth_client_file=settings.drive_oauth_client_file,
                oauth_token_file=settings.drive_oauth_token_file,
                mime_type="text/plain",
                convert_to_sheets=False,
            )
            logger.info("Uploaded log to Drive: %s", log_id)

        logger.info("Pipeline finished")
    except Exception:
        logger.exception("Pipeline failed")
        raise


if __name__ == "__main__":
    main()
