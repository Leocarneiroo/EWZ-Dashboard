#!/usr/bin/env python3
"""Publish a generated dashboard HTML into docs/ for GitHub Pages."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Copy dashboard HTML to docs/index.html and keep a dated history copy."
    )
    parser.add_argument(
        "--dashboard-html",
        required=True,
        type=Path,
        help="Path to generated dashboard HTML.",
    )
    parser.add_argument(
        "--date",
        required=True,
        help="Session date label (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--site-dir",
        default=Path("docs"),
        type=Path,
        help="GitHub Pages site directory.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    dashboard_html = args.dashboard_html.expanduser().resolve()
    site_dir = args.site_dir.expanduser().resolve()
    history_dir = site_dir / "history"

    if not dashboard_html.exists():
        raise FileNotFoundError(f"Dashboard HTML not found: {dashboard_html}")

    site_dir.mkdir(parents=True, exist_ok=True)
    history_dir.mkdir(parents=True, exist_ok=True)

    html_content = dashboard_html.read_text(encoding="utf-8")

    index_path = site_dir / "index.html"
    dated_path = history_dir / f"EWZ_delta_volume_dashboard_{args.date}.html"
    latest_meta_path = site_dir / "latest.json"

    index_path.write_text(html_content, encoding="utf-8")
    dated_path.write_text(html_content, encoding="utf-8")
    latest_meta_path.write_text(
        json.dumps(
            {
                "date": args.date,
                "latest_dashboard": index_path.name,
                "history_file": f"history/{dated_path.name}",
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    print(f"Published GitHub Pages index: {index_path}")
    print(f"Archived dashboard: {dated_path}")
    print(f"Wrote metadata: {latest_meta_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
