#!/usr/bin/env python3
"""Run EWZ daily pipeline for today's folder, supporting ZIP or CSV inputs."""

from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import datetime
from pathlib import Path

from process_daily_reports import build_flow_exports, filter_chain_oi, filter_darkpool, unzip_single_csv

REQUIRED_STEMS = ("bot-eod-report", "dp-eod-report", "chain-oi-changes")
DEFAULT_SYMBOLS = ("EWZ", "SPY")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run daily EWZ pipeline using today's date folder (ZIP preferred, CSV fallback)."
    )
    parser.add_argument(
        "--date-dir",
        type=Path,
        default=None,
        help="Date directory. Default: today's folder YYYY-MM-DD in current repo.",
    )
    parser.add_argument("--symbols", nargs="+", default=list(DEFAULT_SYMBOLS))
    parser.add_argument("--timezone", default="America/Sao_Paulo")
    parser.add_argument("--site-dir", type=Path, default=Path("docs"))
    parser.add_argument("--no-publish", action="store_true", help="Skip publish_github_pages.py step.")
    return parser.parse_args()


def run(cmd: list[str], cwd: Path) -> None:
    completed = subprocess.run(cmd, cwd=cwd, check=False)
    if completed.returncode != 0:
        raise RuntimeError(f"Command failed ({completed.returncode}): {' '.join(cmd)}")


def resolve_today_dir(repo_dir: Path, date_dir: Path | None) -> Path:
    if date_dir is not None:
        return date_dir.expanduser().resolve()
    today = datetime.now().strftime("%Y-%m-%d")
    return (repo_dir / today).resolve()


def latest_date_dir(repo_dir: Path) -> Path | None:
    candidates = [
        path for path in repo_dir.iterdir() if path.is_dir() and len(path.name) == 10
    ]
    dated: list[tuple[datetime, Path]] = []
    for path in candidates:
        try:
            dt = datetime.strptime(path.name, "%Y-%m-%d")
        except ValueError:
            continue
        dated.append((dt, path.resolve()))
    if not dated:
        return None
    dated.sort(key=lambda item: item[0], reverse=True)
    return dated[0][1]


def resolve_source_csv(date_dir: Path, stem: str) -> Path:
    zip_match = next(date_dir.glob(f"{stem}-*.zip"), None)
    if zip_match is not None:
        return unzip_single_csv(zip_match)

    csv_match = next(date_dir.glob(f"{stem}-*.csv"), None)
    if csv_match is not None:
        return csv_match

    raise FileNotFoundError(
        f"Missing input for {stem}: expected {stem}-*.zip or {stem}-*.csv in {date_dir}"
    )


def main() -> int:
    args = parse_args()
    repo_dir = Path(__file__).resolve().parent
    date_dir = resolve_today_dir(repo_dir, args.date_dir)
    default_date_mode = args.date_dir is None

    if not date_dir.exists():
        if not default_date_mode:
            raise FileNotFoundError(f"Date directory not found: {date_dir}")
        fallback_dir = latest_date_dir(repo_dir)
        if fallback_dir is None:
            raise FileNotFoundError(
                f"Date directory not found: {date_dir}. No YYYY-MM-DD folders were found for fallback."
            )
        print(f"Today's folder not found ({date_dir.name}). Using latest available: {fallback_dir.name}")
        date_dir = fallback_dir

    symbols = [symbol.upper().strip() for symbol in args.symbols]
    date_label = date_dir.name
    processed_dir = date_dir / "processed"
    processed_dir.mkdir(parents=True, exist_ok=True)

    sources = {stem: resolve_source_csv(date_dir, stem) for stem in REQUIRED_STEMS}

    all_outputs: list[Path] = []
    for symbol in symbols:
        all_outputs.extend(
            build_flow_exports(sources["bot-eod-report"], symbol, processed_dir, args.timezone)
        )
    all_outputs.extend(filter_chain_oi(sources["chain-oi-changes"], symbols, processed_dir))
    all_outputs.extend(filter_darkpool(sources["dp-eod-report"], symbols, processed_dir))

    output_html_by_symbol: dict[str, Path] = {}
    for ticker in symbols:
        ask_path = processed_dir / f"flow_true_{date_label}_50_ETF_{ticker}_ask_side_FULL_{date_label}.csv"
        bid_path = processed_dir / f"flow_true_{date_label}_50_ETF_{ticker}_bid_side_FULL_{date_label}.csv"
        oi_path = processed_dir / f"chain-oi-changes-{ticker}-{date_label}.csv"
        dp_path = processed_dir / f"dp-eod-report-{ticker}-{date_label}.csv"
        output_html = processed_dir / f"{ticker}_delta_volume_dashboard_{date_label}.html"
        output_html_by_symbol[ticker] = output_html

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
                "--dp-file",
                str(dp_path),
                "--output",
                str(output_html),
            ],
            cwd=repo_dir,
        )

    if not args.no_publish:
        publish_cmd = [
            sys.executable,
            "publish_github_pages.py",
            "--date",
            date_label,
            "--site-dir",
            str(args.site_dir),
        ]
        for ticker in sorted(output_html_by_symbol.keys()):
            publish_cmd.extend(["--dashboard", f"{ticker}={output_html_by_symbol[ticker]}"])
        run(publish_cmd, cwd=repo_dir)

    print("EWZ runner completed.")
    print(f"Date: {date_label}")
    for stem, src in sources.items():
        print(f"Input {stem}: {src}")
    print(f"Processed dir: {processed_dir}")
    for ticker in sorted(output_html_by_symbol.keys()):
        print(f"{ticker} dashboard: {output_html_by_symbol[ticker]}")
    if args.no_publish:
        print("Publish skipped (--no-publish).")
    else:
        print("Docs published in docs/.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
