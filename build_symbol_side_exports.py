#!/usr/bin/env python3
"""Export flow_true-style CSVs by side for a given symbol."""

from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from zoneinfo import ZoneInfo


FLOW_HEADER = [
    "date",
    "time",
    "underlying_symbol",
    "side",
    "strike",
    "type",
    "expiry",
    "DTE",
    "option_chain_id",
    "diff",
    "ewma_nbbo_bid",
    "ewma_nbbo_ask",
    "underlying_price",
    "size",
    "premium",
    "volume",
    "open_interest",
    "bid_vol",
    "no_side_vol",
    "mid_vol",
    "ask_vol",
    "multi_vol",
    "stock_multi_vol",
    "implied_volatility",
    "delta",
    "theta",
    "gamma",
    "theo",
    "rho",
    "vega",
    "next_earnings_date",
    "bearish_or_bullish",
    "report_flags",
    "exchange",
    "upstream_condition_detail",
    "tags",
    "sector",
    "industry_type",
    "nbbo_bid",
    "nbbo_ask",
    "canceled",
    "er_time",
    "full_name",
    "marketcap",
    "option_type",
    "price",
    "string",
]

SIDE_ORDER = ("ASK", "BID", "NO_SIDE", "MID")
SIDE_TOKEN = {
    "ASK": "ask_side",
    "BID": "bid_side",
    "NO_SIDE": "no_side",
    "MID": "mid_side",
}
SIDE_SENTIMENT = {
    "ASK": "bullish",
    "BID": "bearish",
    "NO_SIDE": "neutral",
    "MID": "neutral",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build per-side flow CSV exports for a symbol from the raw EOD report."
    )
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--output-dir", default=Path.cwd(), type=Path)
    parser.add_argument("--timezone", default="America/Sao_Paulo")
    return parser.parse_args()


def parse_utc_datetime(value: str, target_timezone: str) -> datetime:
    text = value.strip()
    if text.endswith("+00"):
        text = f"{text}:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        timezone_normalized = text.replace("+00:00", "+0000")
        for date_format in ("%Y-%m-%d %H:%M:%S.%f%z", "%Y-%m-%d %H:%M:%S%z"):
            try:
                parsed = datetime.strptime(timezone_normalized, date_format)
                break
            except ValueError:
                continue
        else:
            raise
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(ZoneInfo(target_timezone))


def option_type_code(option_type: str) -> str:
    normalized = option_type.strip().lower()
    if normalized == "call":
        return "C"
    if normalized == "put":
        return "P"
    return normalized[:1].upper()


def infer_direction(side: str, option_code: str) -> str:
    if side in {"ASK", "BID"} and option_code in {"C", "P"}:
        bullish_combo = ((side == "ASK") and (option_code == "C")) or (
            (side == "BID") and (option_code == "P")
        )
        return "bullish" if bullish_combo else "bearish"
    return SIDE_SENTIMENT.get(side, "neutral")


def bool_text(value: str) -> str:
    normalized = (value or "").strip().lower()
    return "true" if normalized in {"t", "true", "1", "yes"} else "false"


def format_strike_for_string(value: str) -> str:
    try:
        return f"{Decimal(value):.2f}"
    except InvalidOperation:
        return value


def format_expiry_for_string(value: str) -> str:
    try:
        return datetime.strptime(value, "%Y-%m-%d").strftime("%m/%d/%Y")
    except ValueError:
        return value


def dte(expiry: str, trade_date: datetime) -> str:
    try:
        expiry_date = datetime.strptime(expiry, "%Y-%m-%d").date()
    except ValueError:
        return ""
    return str((expiry_date - trade_date.date()).days)


def percent_diff(strike: str, underlying_price: str) -> str:
    try:
        strike_value = float(strike)
        underlying_value = float(underlying_price)
    except ValueError:
        return ""
    if underlying_value == 0:
        return ""
    return str((strike_value - underlying_value) / underlying_value * 100)


def side_volume_fields(side: str, volume: str) -> tuple[str, str, str, str]:
    bid_vol = volume if side == "BID" else "0"
    no_side_vol = volume if side == "NO_SIDE" else "0"
    mid_vol = volume if side == "MID" else "0"
    ask_vol = volume if side == "ASK" else "0"
    return bid_vol, no_side_vol, mid_vol, ask_vol


def row_to_flow(raw: dict[str, str], target_timezone: str) -> dict[str, str] | None:
    side = raw.get("side", "").strip().upper()
    if side not in SIDE_ORDER:
        return None

    trade_datetime = parse_utc_datetime(raw["executed_at"], target_timezone)
    option_code = option_type_code(raw.get("option_type", ""))
    volume = raw.get("volume", "")
    bid_vol, no_side_vol, mid_vol, ask_vol = side_volume_fields(side, volume)
    sentiment = infer_direction(side, option_code)
    tag = SIDE_TOKEN[side]

    return {
        "date": trade_datetime.strftime("%m/%d/%Y"),
        "time": trade_datetime.strftime("%H:%M:%S"),
        "underlying_symbol": raw.get("underlying_symbol", ""),
        "side": side,
        "strike": raw.get("strike", ""),
        "type": option_code,
        "expiry": raw.get("expiry", ""),
        "DTE": dte(raw.get("expiry", ""), trade_datetime),
        "option_chain_id": raw.get("option_chain_id", ""),
        "diff": percent_diff(raw.get("strike", ""), raw.get("underlying_price", "")),
        "ewma_nbbo_bid": raw.get("ewma_nbbo_bid", ""),
        "ewma_nbbo_ask": raw.get("ewma_nbbo_ask", ""),
        "underlying_price": raw.get("underlying_price", ""),
        "size": raw.get("size", ""),
        "premium": raw.get("premium", ""),
        "volume": volume,
        "open_interest": raw.get("open_interest", ""),
        "bid_vol": bid_vol,
        "no_side_vol": no_side_vol,
        "mid_vol": mid_vol,
        "ask_vol": ask_vol,
        "multi_vol": "0",
        "stock_multi_vol": "0",
        "implied_volatility": raw.get("implied_volatility", ""),
        "delta": raw.get("delta", ""),
        "theta": raw.get("theta", ""),
        "gamma": raw.get("gamma", ""),
        "theo": raw.get("theo", ""),
        "rho": raw.get("rho", ""),
        "vega": raw.get("vega", ""),
        "next_earnings_date": "",
        "bearish_or_bullish": sentiment,
        "report_flags": raw.get("report_flags", ""),
        "exchange": raw.get("exchange", ""),
        "upstream_condition_detail": raw.get("upstream_condition_detail", ""),
        "tags": f"{tag},{sentiment},etf",
        "sector": raw.get("sector", ""),
        "industry_type": "",
        "nbbo_bid": raw.get("nbbo_bid", ""),
        "nbbo_ask": raw.get("nbbo_ask", ""),
        "canceled": bool_text(raw.get("canceled", "")),
        "er_time": "",
        "full_name": "",
        "marketcap": "",
        "option_type": raw.get("option_type", ""),
        "price": raw.get("price", ""),
        "string": (
            f"{raw.get('underlying_symbol', '')} "
            f"${format_strike_for_string(raw.get('strike', ''))} "
            f"{option_code} {format_expiry_for_string(raw.get('expiry', ''))}"
        ),
    }


def output_name(date_label: str, symbol: str, side: str) -> str:
    token = SIDE_TOKEN[side]
    return f"flow_true_{date_label}_50_ETF_{symbol}_{token}_FULL_{date_label}.csv"


def main() -> int:
    args = parse_args()
    symbol = args.symbol.upper().strip()
    if not args.input.exists():
        raise FileNotFoundError(f"Input file not found: {args.input}")

    date_label = "unknown_date"
    if "bot-eod-report-" in args.input.name and args.input.suffix == ".csv":
        date_label = args.input.stem.replace("bot-eod-report-", "")

    rows_by_side: dict[str, list[dict[str, str]]] = {side: [] for side in SIDE_ORDER}

    with args.input.open(newline="", encoding="utf-8", errors="replace") as handle:
        reader = csv.DictReader(handle)
        for raw in reader:
            if raw.get("underlying_symbol", "").upper() != symbol:
                continue
            flow = row_to_flow(raw, args.timezone)
            if flow is None:
                continue
            rows_by_side[flow["side"]].append(flow)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    for side in SIDE_ORDER:
        rows = rows_by_side[side]
        rows.sort(key=lambda item: (item["date"], item["time"], item["option_chain_id"]), reverse=True)
        out_path = args.output_dir / output_name(date_label, symbol, side)
        with out_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=FLOW_HEADER)
            writer.writeheader()
            writer.writerows(rows)
        print(f"{side}: {len(rows)} -> {out_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
