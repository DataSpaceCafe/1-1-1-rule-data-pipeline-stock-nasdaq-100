import io
from pathlib import Path

import pandas as pd
import requests
import certifi


WIKI_URL = "https://en.wikipedia.org/wiki/Nasdaq-100"
OUTPUT_PATH = Path("data/nasdaq100_tickers.csv")


def fetch_tickers() -> list[str]:
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/121.0.0.0 Safari/537.36"
        )
    }
    resp = requests.get(WIKI_URL, headers=headers, timeout=20, verify=certifi.where())
    resp.raise_for_status()

    tables = pd.read_html(io.StringIO(resp.text))
    component_table = None
    for table in tables:
        cols = [str(c).strip().lower() for c in table.columns]
        if any(c in cols for c in ("ticker", "ticker symbol", "symbol")):
            component_table = table
            break

    if component_table is None:
        raise RuntimeError("ไม่พบตารางรายชื่อ Nasdaq-100 จาก Wikipedia")

    ticker_col = None
    for c in component_table.columns:
        if str(c).strip().lower() in ("ticker", "ticker symbol", "symbol"):
            ticker_col = c
            break
    if ticker_col is None:
        ticker_col = component_table.columns[0]

    tickers = component_table[ticker_col].astype(str).str.strip().str.upper()
    tickers = tickers.str.replace(".", "-", regex=False)
    unique = sorted(set(t for t in tickers.tolist() if t))
    return unique


def main() -> None:
    tickers = fetch_tickers()
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"Symbol": tickers}).to_csv(OUTPUT_PATH, index=False)
    print(f"Updated {OUTPUT_PATH} with {len(tickers)} tickers")


if __name__ == "__main__":
    main()
