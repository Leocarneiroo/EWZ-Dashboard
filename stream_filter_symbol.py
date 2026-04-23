#!/usr/bin/env python3
"""Filter giant CSV files by underlying symbol using streaming (constant memory)."""

from __future__ import annotations

import argparse
import csv
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Filter a large CSV by symbol without loading full file in memory."
    )
    parser.add_argument("input_csv", type=Path, help="Input CSV path")
    parser.add_argument("--symbol", required=True, help="Symbol to filter (e.g., EWZ)")
    parser.add_argument(
        "--symbol-column",
        default="underlying_symbol",
        help="Column name containing symbol (default: underlying_symbol)",
    )
    parser.add_argument("--output", type=Path, default=None, help="Output CSV path")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    input_path = args.input_csv
    symbol = args.symbol.strip().upper()

    if not input_path.exists():
        raise SystemExit(f"Input not found: {input_path}")

    output_path = args.output
    if output_path is None:
        output_path = input_path.with_name(f"{input_path.stem}.{symbol}{input_path.suffix}")

    matched = 0
    total = 0

    with input_path.open("r", encoding="utf-8", errors="replace", newline="") as fin:
        reader = csv.DictReader(fin)
        if not reader.fieldnames:
            raise SystemExit("CSV has no header")
        if args.symbol_column not in reader.fieldnames:
            available = ", ".join(reader.fieldnames)
            raise SystemExit(
                f"Column '{args.symbol_column}' not found. Available columns: {available}"
            )

        with output_path.open("w", encoding="utf-8", newline="") as fout:
            writer = csv.DictWriter(fout, fieldnames=reader.fieldnames)
            writer.writeheader()

            for row in reader:
                total += 1
                if (row.get(args.symbol_column) or "").upper() == symbol:
                    writer.writerow(row)
                    matched += 1

    print(f"input={input_path}")
    print(f"output={output_path}")
    print(f"total_rows={total}")
    print(f"matched_rows={matched}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
