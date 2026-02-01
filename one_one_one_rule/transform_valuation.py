from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd
import pandera.pandas as pa
import yfinance as yf

from .perf import log_perf

def _safe_float(value) -> float:
    # Convert to float safely; return NaN on failure.
    try:
        if value is None:
            return float("nan")
        return float(value)
    except Exception:
        return float("nan")


def _first_value(*values):
    # Return the first non-empty value.
    for value in values:
        if value is None:
            continue
        if isinstance(value, (int, float)) and np.isnan(value):
            continue
        return value
    return float("nan")


@dataclass(frozen=True)
class ValuationThresholds:
    undervalued: float
    overvalued: float
    peg_max: float
    pe_sector_max_mult: float
    margin_of_safety_min: float


def _fetch_snapshot(ticker: str) -> dict:
    # Pull a single ticker snapshot from yfinance.
    info = {}
    fast = {}
    yf_ticker = yf.Ticker(ticker)
    try:
        info = yf_ticker.info or {}
    except Exception:
        info = {}
    try:
        fast = yf_ticker.fast_info or {}
    except Exception:
        fast = {}

    price = _first_value(
        fast.get("last_price"),
        fast.get("regular_market_price"),
        info.get("currentPrice"),
        info.get("regularMarketPrice"),
    )
    market_cap = _first_value(
        fast.get("market_cap"),
        info.get("marketCap"),
    )

    currency = _first_value(
        fast.get("currency"),
        info.get("currency"),
    )

    return {
        "ticker": ticker,
        "company": info.get("shortName") or info.get("longName") or "",
        "sector": info.get("sector") or "Unknown",
        "price": _safe_float(price),
        "market_cap": _safe_float(market_cap),
        "currency": currency or "",
        "trailing_pe": _safe_float(info.get("trailingPE")),
        "forward_pe": _safe_float(info.get("forwardPE")),
        "trailing_eps": _safe_float(info.get("trailingEps")),
        "forward_eps": _safe_float(info.get("forwardEps")),
        "earnings_growth": _safe_float(info.get("earningsGrowth")),
        "peg_ratio": _safe_float(info.get("pegRatio")),
        "book_value_per_share": _safe_float(info.get("bookValue")),
        "target_mean_price": _safe_float(info.get("targetMeanPrice")),
    }


@log_perf
def fetch_fundamentals(tickers: list[str]) -> pd.DataFrame:
    # Batch fetch fundamentals for all tickers.
    rows = []
    for ticker in tickers:
        rows.append(_fetch_snapshot(ticker))
    return pd.DataFrame(rows)


@log_perf
def cleanse_fundamentals(df: pd.DataFrame) -> pd.DataFrame:
    # Normalize symbols, coerce types, and remove invalid values.
    df = df.copy()

    if "ticker" in df.columns:
        df["ticker"] = (
            df["ticker"]
            .astype(str)
            .str.strip()
            .str.upper()
            .str.replace(".", "-", regex=False)
        )
        df = df[df["ticker"].notna() & (df["ticker"] != "")]
        df = df.drop_duplicates(subset=["ticker"], keep="first")

    schema = pa.DataFrameSchema(
        {
            "ticker": pa.Column(str, nullable=True, coerce=True),
            "company": pa.Column(str, nullable=True, coerce=True),
            "sector": pa.Column(str, nullable=True, coerce=True),
            "price": pa.Column(float, nullable=True, coerce=True),
            "market_cap": pa.Column(float, nullable=True, coerce=True),
            "trailing_pe": pa.Column(float, nullable=True, coerce=True),
            "forward_pe": pa.Column(float, nullable=True, coerce=True),
            "trailing_eps": pa.Column(float, nullable=True, coerce=True),
            "forward_eps": pa.Column(float, nullable=True, coerce=True),
            "earnings_growth": pa.Column(float, nullable=True, coerce=True),
            "peg_ratio": pa.Column(float, nullable=True, coerce=True),
            "book_value_per_share": pa.Column(float, nullable=True, coerce=True),
            "target_mean_price": pa.Column(float, nullable=True, coerce=True),
        },
        coerce=True,
        strict=False,
    )

    try:
        df = schema.validate(df, lazy=True)
    except pa.errors.SchemaErrors as exc:
        df = exc.data

    df.replace([np.inf, -np.inf], np.nan, inplace=True)

    if "price" in df.columns:
        df.loc[df["price"] <= 0, "price"] = np.nan
    if "market_cap" in df.columns:
        df.loc[df["market_cap"] <= 0, "market_cap"] = np.nan
    if "sector" in df.columns:
        df["sector"] = df["sector"].replace("", "Unknown").fillna("Unknown")

    return df


def _select_fair_value(row: pd.Series, sector_median_pe, overall_median_pe, sector_median_fpe, overall_median_fpe):
    # Choose the best available fair value estimate.
    graham_value = row.get("graham_value")
    if pd.notna(graham_value) and graham_value > 0:
        return graham_value, "graham_value"

    target_mean_price = row.get("target_mean_price")
    if pd.notna(target_mean_price) and target_mean_price > 0:
        return target_mean_price, "target_mean_price"

    trailing_eps = row.get("trailing_eps")
    if pd.notna(trailing_eps) and trailing_eps > 0:
        pe = sector_median_pe.get(row.get("sector"))
        if pd.isna(pe):
            pe = overall_median_pe
        if pd.notna(pe) and pe > 0:
            return trailing_eps * pe, "sector_median_trailing_pe"

    forward_eps = row.get("forward_eps")
    if pd.notna(forward_eps) and forward_eps > 0:
        pe = sector_median_fpe.get(row.get("sector"))
        if pd.isna(pe):
            pe = overall_median_fpe
        if pd.notna(pe) and pe > 0:
            return forward_eps * pe, "sector_median_forward_pe"

    return float("nan"), "missing"


def _compute_graham_value(row: pd.Series) -> float:
    # Graham number based on EPS and book value.
    eps = row.get("trailing_eps")
    book_value = row.get("book_value_per_share")
    if pd.notna(eps) and pd.notna(book_value) and eps > 0 and book_value > 0:
        return float(np.sqrt(22.5 * eps * book_value))
    return float("nan")


def _compute_peg_ratio(row: pd.Series) -> tuple[float, str]:
    # Use reported PEG when available, otherwise derive it.
    peg_ratio = row.get("peg_ratio")
    if pd.notna(peg_ratio) and peg_ratio > 0:
        return float(peg_ratio), "reported"

    trailing_pe = row.get("trailing_pe")
    earnings_growth = row.get("earnings_growth")
    if pd.notna(trailing_pe) and trailing_pe > 0 and pd.notna(earnings_growth) and earnings_growth > 0:
        growth_pct = earnings_growth * 100 if earnings_growth <= 1 else earnings_growth
        if growth_pct > 0:
            return float(trailing_pe) / float(growth_pct), "derived"

    return float("nan"), "missing"


def _pass_fail_unknown(valid: bool, condition: bool) -> str:
    # Normalize boolean checks into pass/fail/unknown.
    if not valid:
        return "unknown"
    return "pass" if condition else "fail"


@log_perf
def apply_valuation(df: pd.DataFrame, thresholds: ValuationThresholds) -> pd.DataFrame:
    # Compute valuation metrics and classification labels.
    sector_median_pe = df.groupby("sector")["trailing_pe"].median()
    overall_median_pe = df["trailing_pe"].median()
    sector_median_fpe = df.groupby("sector")["forward_pe"].median()
    overall_median_fpe = df["forward_pe"].median()

    df = df.copy()

    graham_values = []
    peg_values = []
    peg_sources = []
    for _, row in df.iterrows():
        graham_values.append(_compute_graham_value(row))
        peg_value, peg_source = _compute_peg_ratio(row)
        peg_values.append(peg_value)
        peg_sources.append(peg_source)

    df["graham_value"] = graham_values
    df["peg_ratio"] = peg_values
    df["peg_ratio_source"] = peg_sources
    df["sector_median_pe"] = df["sector"].map(sector_median_pe)
    df["pe_median_used"] = df["sector_median_pe"].fillna(overall_median_pe)

    fair_values = []
    fair_sources = []
    for _, row in df.iterrows():
        fair_value, source = _select_fair_value(
            row,
            sector_median_pe,
            overall_median_pe,
            sector_median_fpe,
            overall_median_fpe,
        )
        fair_values.append(fair_value)
        fair_sources.append(source)

    df["fair_value"] = fair_values
    df["fair_value_source"] = fair_sources

    df["margin_of_safety"] = np.where(
        df["graham_value"] > 0,
        (df["graham_value"] - df["price"]) / df["graham_value"],
        np.nan,
    )

    peg_valid = df["peg_ratio"].notna() & (df["peg_ratio"] > 0)
    df["peg_pass"] = [
        _pass_fail_unknown(valid, value < thresholds.peg_max)
        for valid, value in zip(peg_valid, df["peg_ratio"])
    ]

    pe_valid = df["trailing_pe"].notna() & df["pe_median_used"].notna()
    df["pe_vs_sector_pass"] = [
        _pass_fail_unknown(
            valid,
            pe <= (median_pe * thresholds.pe_sector_max_mult),
        )
        for valid, pe, median_pe in zip(pe_valid, df["trailing_pe"], df["pe_median_used"])
    ]

    mos_valid = df["margin_of_safety"].notna()
    df["margin_of_safety_pass"] = [
        _pass_fail_unknown(valid, mos >= thresholds.margin_of_safety_min)
        for valid, mos in zip(mos_valid, df["margin_of_safety"])
    ]

    def hunter_classify(row: pd.Series) -> str:
        checks = [
            row.get("peg_pass"),
            row.get("pe_vs_sector_pass"),
            row.get("margin_of_safety_pass"),
        ]
        if any(value == "unknown" for value in checks):
            return "unknown"
        if all(value == "pass" for value in checks):
            return "pass"
        return "fail"

    df["valuation_hunter"] = df.apply(hunter_classify, axis=1)

    def classify(row: pd.Series) -> str:
        price = row.get("price")
        fair_value = row.get("fair_value")
        if pd.isna(price) or pd.isna(fair_value) or fair_value <= 0:
            return "unknown"
        if price <= fair_value * thresholds.undervalued:
            return "undervalued"
        if price >= fair_value * thresholds.overvalued:
            return "overvalued"
        return "fair"

    df["valuation"] = df.apply(classify, axis=1)
    df["pct_diff"] = (df["price"] - df["fair_value"]) / df["fair_value"]

    return df
