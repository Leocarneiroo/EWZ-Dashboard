#!/usr/bin/env python3
"""Process daily ZIP drops into extracted CSVs, flow exports, and OI slices."""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
from zipfile import ZipFile

from build_symbol_side_exports import FLOW_HEADER, output_name, row_to_flow


DEFAULT_SYMBOLS = ("EWZ", "SPY")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Extract daily ZIP files and build symbol-specific outputs."
    )
    parser.add_argument("--date-dir", required=True, type=Path)
    parser.add_argument(
        "--symbols",
        nargs="+",
        default=list(DEFAULT_SYMBOLS),
        help="Underlying symbols to process from bot and chain OI reports.",
    )
    parser.add_argument("--timezone", default="America/Sao_Paulo")
    return parser.parse_args()


def unzip_single_csv(zip_path: Path) -> Path:
    with ZipFile(zip_path) as archive:
        csv_names = [name for name in archive.namelist() if name.lower().endswith(".csv")]
        if not csv_names:
            raise ValueError(f"No CSV found in {zip_path}")
        name = csv_names[0]
        out_path = zip_path.with_name(Path(name).name)
        out_path.write_bytes(archive.read(name))
        return out_path


def build_flow_exports(bot_csv: Path, symbol: str, out_dir: Path, timezone_name: str) -> list[Path]:
    date_label = bot_csv.stem.replace("bot-eod-report-", "")
    rows_by_side: dict[str, list[dict[str, str]]] = {
        "ASK": [],
        "BID": [],
        "NO_SIDE": [],
        "MID": [],
    }

    with bot_csv.open(newline="", encoding="utf-8", errors="replace") as handle:
        reader = csv.DictReader(handle)
        for raw in reader:
            if raw.get("underlying_symbol", "").strip().upper() != symbol:
                continue
            flow = row_to_flow(raw, timezone_name)
            if flow is None:
                continue
            rows_by_side[flow["side"]].append(flow)

    outputs: list[Path] = []
    for side, rows in rows_by_side.items():
        rows.sort(key=lambda item: (item["date"], item["time"], item["option_chain_id"]), reverse=True)
        out_path = out_dir / output_name(date_label, symbol, side)
        with out_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=FLOW_HEADER)
            writer.writeheader()
            writer.writerows(rows)
        outputs.append(out_path)
    return outputs


def filter_chain_oi(chain_csv: Path, symbols: list[str], out_dir: Path) -> list[Path]:
    with chain_csv.open(newline="", encoding="utf-8", errors="replace") as handle:
        reader = csv.DictReader(handle)
        fieldnames = reader.fieldnames or []
        buckets = {symbol: [] for symbol in symbols}
        for row in reader:
            symbol = str(row.get("underlying_symbol", "")).strip().upper()
            if symbol in buckets:
                buckets[symbol].append(row)

    outputs: list[Path] = []
    for symbol, rows in buckets.items():
        out_path = out_dir / f"chain-oi-changes-{symbol}-{chain_csv.stem.replace('chain-oi-changes-', '')}.csv"
        with out_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        outputs.append(out_path)
    return outputs


def filter_darkpool(dp_csv: Path, symbols: list[str], out_dir: Path) -> list[Path]:
    with dp_csv.open(newline="", encoding="utf-8", errors="replace") as handle:
        reader = csv.DictReader(handle)
        fieldnames = reader.fieldnames or []
        buckets = {symbol: [] for symbol in symbols}
        for row in reader:
            symbol = str(row.get("ticker", "")).strip().upper()
            if symbol in buckets:
                buckets[symbol].append(row)

    outputs: list[Path] = []
    date_label = dp_csv.stem.replace("dp-eod-report-", "")
    for symbol, rows in buckets.items():
        out_path = out_dir / f"dp-eod-report-{symbol}-{date_label}.csv"
        with out_path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        outputs.append(out_path)
    return outputs


def main() -> int:
    args = parse_args()
    date_dir = args.date_dir.expanduser().resolve()
    if not date_dir.exists():
        raise FileNotFoundError(f"Date directory not found: {date_dir}")

    symbols = [symbol.upper().strip() for symbol in args.symbols]
    processed_dir = date_dir / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)

    extracted: dict[str, Path] = {}
    for stem in ("bot-eod-report", "dp-eod-report", "chain-oi-changes"):
        zip_path = next(date_dir.glob(f"{stem}-*.zip"), None)
        if zip_path is None:
            continue
        extracted[stem] = unzip_single_csv(zip_path)

    if "bot-eod-report" not in extracted:
        raise FileNotFoundError("bot-eod-report ZIP not found in date dir.")
    if "chain-oi-changes" not in extracted:
        raise FileNotFoundError("chain-oi-changes ZIP not found in date dir.")
    if "dp-eod-report" not in extracted:
        raise FileNotFoundError("dp-eod-report ZIP not found in date dir.")

    all_outputs: list[Path] = []
    for symbol in symbols:
        all_outputs.extend(
            build_flow_exports(extracted["bot-eod-report"], symbol, processed_dir, args.timezone)
        )
    all_outputs.extend(filter_chain_oi(extracted["chain-oi-changes"], symbols, processed_dir))
    all_outputs.extend(filter_darkpool(extracted["dp-eod-report"], symbols, processed_dir))

    print("Generated outputs:")
    for path in all_outputs:
        print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
