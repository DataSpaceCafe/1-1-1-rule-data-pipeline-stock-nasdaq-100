from __future__ import annotations

from pathlib import Path

import pandas as pd

from .perf import log_perf

_WIKI_URL = "https://en.wikipedia.org/wiki/Nasdaq-100"


def _normalize_ticker(raw: str) -> str:
    # Normalize tickers to the format used by Yahoo Finance.
    ticker = raw.strip().upper()
    if not ticker:
        return ""
    # Yahoo Finance uses '-' instead of '.' for some tickers (e.g., BRK.B -> BRK-B)
    return ticker.replace(".", "-")


@log_perf
def _tickers_from_wikipedia() -> list[str]:
    # Scrape the Nasdaq-100 constituents table from Wikipedia.
    tables = pd.read_html(_WIKI_URL)
    for table in tables:
        if not len(table.columns):
            continue
        columns = {str(col).strip().lower(): col for col in table.columns}
        for candidate in ("ticker", "ticker symbol", "symbol"):
            if candidate in columns:
                col = columns[candidate]
                tickers = (
                    table[col]
                    .astype(str)
                    .map(_normalize_ticker)
                    .dropna()
                    .tolist()
                )
                tickers = [t for t in tickers if t]
                if len(tickers) >= 80:
                    return sorted(set(tickers))
    return []


@log_perf
def _tickers_from_file(path: Path) -> list[str]:
    # Load tickers from a local CSV fallback file.
    if not path.exists():
        return []
    df = pd.read_csv(path)
    if df.empty:
        return []
    col = df.columns[0]
    if "symbol" in [c.lower() for c in df.columns]:
        for c in df.columns:
            if c.lower() == "symbol":
                col = c
                break
    tickers = df[col].astype(str).map(_normalize_ticker).tolist()
    tickers = [t for t in tickers if t]
    return sorted(set(tickers))


@log_perf
def load_nasdaq100_tickers(use_wikipedia: bool, fallback_file: Path) -> list[str]:
    # Prefer Wikipedia; fall back to a local list if needed.
    tickers: list[str] = []
    if use_wikipedia:
        try:
            tickers = _tickers_from_wikipedia()
        except Exception:
            tickers = []

    if not tickers:
        tickers = _tickers_from_file(fallback_file)

    if not tickers:
        raise RuntimeError(
            "ไม่พบรายชื่อ Nasdaq-100: โปรดเปิดใช้ Wikipedia หรือใส่ไฟล์รายชื่อใน data/nasdaq100_tickers.csv"
        )

    return tickers
