#!/usr/bin/env python3
"""Run daily processing pipeline and publish dashboard to GitHub Pages docs/."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Process daily files, generate EWZ dashboard, and publish docs/index.html."
    )
    parser.add_argument(
        "--date-dir",
        required=True,
        type=Path,
        help="Directory containing daily ZIP files (ex: 2026-04-23).",
    )
    parser.add_argument(
        "--ticker",
        default="EWZ",
        help="Ticker to generate dashboard for.",
    )
    parser.add_argument(
        "--site-dir",
        default=Path("docs"),
        type=Path,
        help="GitHub Pages site directory.",
    )
    return parser.parse_args()


def run(cmd: list[str], cwd: Path) -> None:
    completed = subprocess.run(cmd, cwd=cwd, check=False)
    if completed.returncode != 0:
        raise RuntimeError(f"Command failed ({completed.returncode}): {' '.join(cmd)}")


def main() -> int:
    args = parse_args()
    repo_dir = Path(__file__).resolve().parent
    date_dir = args.date_dir.expanduser().resolve()
    date_label = date_dir.name
    ticker = args.ticker.upper().strip()
    processed_dir = date_dir / "processed"

    if not date_dir.exists():
        raise FileNotFoundError(f"Date directory not found: {date_dir}")

    run(
        [sys.executable, "process_daily_reports.py", "--date-dir", str(date_dir)],
        cwd=repo_dir,
    )

    ask_path = processed_dir / f"flow_true_{date_label}_50_ETF_{ticker}_ask_side_FULL_{date_label}.csv"
    bid_path = processed_dir / f"flow_true_{date_label}_50_ETF_{ticker}_bid_side_FULL_{date_label}.csv"
    oi_path = processed_dir / f"chain-oi-changes-{ticker}-{date_label}.csv"
    output_html = processed_dir / f"{ticker}_delta_volume_dashboard_{date_label}.html"

    run(
        [
            sys.executable,
            "ewz_delta_dashboard.py",
            "--ticker",
            ticker,
            "--flow-files",
            str(ask_path),
            str(bid_path),
            "--oi-change-file",
            str(oi_path),
            "--output",
            str(output_html),
        ],
        cwd=repo_dir,
    )

    run(
        [
            sys.executable,
            "publish_github_pages.py",
            "--dashboard-html",
            str(output_html),
            "--date",
            date_label,
            "--site-dir",
            str(args.site_dir),
        ],
        cwd=repo_dir,
    )

    print()
    print("Daily pipeline completed.")
    print(f"Date: {date_label}")
    print(f"Dashboard: {output_html}")
    print("GitHub Pages source updated in docs/")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
