#!/usr/bin/env python3
"""Build full-day EWZ ASK/BID flow exports plus missing-trade deltas."""

from __future__ import annotations

import argparse
import csv
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from zoneinfo import ZoneInfo


PROJECT_DIR = Path(__file__).resolve().parent
DEFAULT_EWZ_INPUT = PROJECT_DIR / "bot-eod-report-2026-04-21.EWZ.csv"
DEFAULT_EXISTING_FILES = [
    Path("/Users/leonardocarneiro/Downloads/flow_true_2026-03-20_50_ETF_EWZ_mid_side,no_side,bid_side.csv"),
    Path("/Users/leonardocarneiro/Downloads/flow_true_2026-03-20_50_ETF_EWZ_mid_side,no_side,ask_side.csv"),
]

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

DEFAULT_OUTPUTS = {
    "ASK": {
        "full": "flow_true_2026-03-20_50_ETF_EWZ_ask_side_FULL_2026-04-21.csv",
        "missing": "flow_true_2026-03-20_50_ETF_EWZ_ask_side_MISSING_2026-04-21.csv",
    },
    "BID": {
        "full": "flow_true_2026-03-20_50_ETF_EWZ_bid_side_FULL_2026-04-21.csv",
        "missing": "flow_true_2026-03-20_50_ETF_EWZ_bid_side_MISSING_2026-04-21.csv",
    },
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create full and missing ASK/BID flow CSVs from complete EWZ trade data."
    )
    parser.add_argument("--input", type=Path, default=DEFAULT_EWZ_INPUT)
    parser.add_argument("--output-dir", type=Path, default=PROJECT_DIR)
    parser.add_argument(
        "--existing",
        action="append",
        type=Path,
        default=None,
        help="Existing partial flow CSV. Can be passed more than once.",
    )
    return parser.parse_args()


def normalize_number(value: str | None) -> str:
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    try:
        number = Decimal(text)
    except InvalidOperation:
        try:
            number = Decimal(str(round(float(text), 8)))
        except (TypeError, ValueError, InvalidOperation):
            return text
    rounded = number.quantize(Decimal("0.000001")).normalize()
    return format(rounded, "f")


def parse_utc_datetime(value: str) -> datetime:
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
    return parsed.astimezone(ZoneInfo("America/Sao_Paulo"))


def option_type_code(option_type: str) -> str:
    normalized = option_type.strip().lower()
    if normalized == "call":
        return "C"
    if normalized == "put":
        return "P"
    return normalized[:1].upper()


def infer_direction(side: str, option_code: str) -> str:
    bullish_combo = ((side == "ASK") and (option_code == "C")) or (
        (side == "BID") and (option_code == "P")
    )
    return "bullish" if bullish_combo else "bearish"


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


def read_existing_defaults(paths: list[Path]) -> dict[str, str]:
    defaults: dict[str, str] = {}
    default_columns = [
        "next_earnings_date",
        "industry_type",
        "er_time",
        "full_name",
        "marketcap",
    ]
    for path in paths:
        if not path.exists():
            continue
        with path.open(newline="", encoding="utf-8", errors="replace") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                for column in default_columns:
                    if column not in defaults and row.get(column):
                        defaults[column] = row[column]
                if all(column in defaults for column in default_columns):
                    return defaults
                break
    return defaults


def flow_key(row: dict[str, str]) -> tuple[str, str, str, str, str, str, str, str]:
    return (
        row.get("side", "").strip().upper(),
        row.get("option_chain_id", "").strip(),
        normalize_number(row.get("strike")),
        row.get("type", "").strip().upper(),
        row.get("expiry", "").strip(),
        normalize_number(row.get("price")),
        normalize_number(row.get("size")),
        normalize_number(row.get("premium")),
    )


def existing_trade_counts(paths: list[Path]) -> Counter[tuple[str, str, str, str, str, str, str, str]]:
    counts: Counter[tuple[str, str, str, str, str, str, str, str]] = Counter()
    for path in paths:
        if not path.exists():
            raise FileNotFoundError(f"Existing CSV not found: {path}")
        with path.open(newline="", encoding="utf-8", errors="replace") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                counts[flow_key(row)] += 1
    return counts


def transform_raw_row(row: dict[str, str], defaults: dict[str, str]) -> dict[str, str] | None:
    side = row.get("side", "").strip().upper()
    if side not in {"ASK", "BID"}:
        return None

    trade_datetime = parse_utc_datetime(row["executed_at"])
    option_code = option_type_code(row.get("option_type", ""))
    direction = infer_direction(side, option_code)
    side_tag = "ask_side" if side == "ASK" else "bid_side"
    volume = row.get("volume", "")

    return {
        "date": trade_datetime.strftime("%m/%d/%Y"),
        "time": trade_datetime.strftime("%H:%M:%S"),
        "underlying_symbol": row.get("underlying_symbol", ""),
        "side": side,
        "strike": row.get("strike", ""),
        "type": option_code,
        "expiry": row.get("expiry", ""),
        "DTE": dte(row.get("expiry", ""), trade_datetime),
        "option_chain_id": row.get("option_chain_id", ""),
        "diff": percent_diff(row.get("strike", ""), row.get("underlying_price", "")),
        "ewma_nbbo_bid": row.get("ewma_nbbo_bid", ""),
        "ewma_nbbo_ask": row.get("ewma_nbbo_ask", ""),
        "underlying_price": row.get("underlying_price", ""),
        "size": row.get("size", ""),
        "premium": row.get("premium", ""),
        "volume": volume,
        "open_interest": row.get("open_interest", ""),
        "bid_vol": volume if side == "BID" else "0",
        "no_side_vol": "0",
        "mid_vol": "0",
        "ask_vol": volume if side == "ASK" else "0",
        "multi_vol": "0",
        "stock_multi_vol": "0",
        "implied_volatility": row.get("implied_volatility", ""),
        "delta": row.get("delta", ""),
        "theta": row.get("theta", ""),
        "gamma": row.get("gamma", ""),
        "theo": row.get("theo", ""),
        "rho": row.get("rho", ""),
        "vega": row.get("vega", ""),
        "next_earnings_date": defaults.get("next_earnings_date", ""),
        "bearish_or_bullish": direction,
        "report_flags": row.get("report_flags", ""),
        "exchange": row.get("exchange", ""),
        "upstream_condition_detail": row.get("upstream_condition_detail", ""),
        "tags": f"{side_tag},{direction},etf",
        "sector": row.get("sector", ""),
        "industry_type": defaults.get("industry_type", ""),
        "nbbo_bid": row.get("nbbo_bid", ""),
        "nbbo_ask": row.get("nbbo_ask", ""),
        "canceled": bool_text(row.get("canceled", "")),
        "er_time": defaults.get("er_time", ""),
        "full_name": defaults.get("full_name", ""),
        "marketcap": defaults.get("marketcap", ""),
        "option_type": row.get("option_type", ""),
        "price": row.get("price", ""),
        "string": (
            f"{row.get('underlying_symbol', '')} "
            f"${format_strike_for_string(row.get('strike', ''))} "
            f"{option_code} {format_expiry_for_string(row.get('expiry', ''))}"
        ),
    }


def write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=FLOW_HEADER)
        writer.writeheader()
        writer.writerows(rows)


def main() -> int:
    args = parse_args()
    existing_paths = args.existing or DEFAULT_EXISTING_FILES
    defaults = read_existing_defaults(existing_paths)
    existing_counts = existing_trade_counts(existing_paths)

    full_rows: dict[str, list[dict[str, str]]] = {"ASK": [], "BID": []}
    missing_rows: dict[str, list[dict[str, str]]] = {"ASK": [], "BID": []}

    with args.input.open(newline="", encoding="utf-8", errors="replace") as handle:
        reader = csv.DictReader(handle)
        for raw_row in reader:
            flow_row = transform_raw_row(raw_row, defaults)
            if flow_row is None:
                continue
            side = flow_row["side"]
            full_rows[side].append(flow_row)

            key = flow_key(flow_row)
            if existing_counts[key] > 0:
                existing_counts[key] -= 1
            else:
                missing_rows[side].append(flow_row)

    args.output_dir.mkdir(parents=True, exist_ok=True)
    for side in ("ASK", "BID"):
        full_rows[side].sort(key=lambda item: (item["date"], item["time"], item["option_chain_id"]), reverse=True)
        missing_rows[side].sort(key=lambda item: (item["date"], item["time"], item["option_chain_id"]), reverse=True)

        write_csv(args.output_dir / DEFAULT_OUTPUTS[side]["full"], full_rows[side])
        write_csv(args.output_dir / DEFAULT_OUTPUTS[side]["missing"], missing_rows[side])

        print(f"{side}_full_rows={len(full_rows[side])}")
        print(f"{side}_missing_rows={len(missing_rows[side])}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
