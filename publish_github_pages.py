#!/usr/bin/env python3
"""Publish generated dashboards into docs/ for GitHub Pages."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Copy multiple dashboard HTML files to docs/ and build selector index."
    )
    parser.add_argument(
        "--dashboard",
        action="append",
        required=True,
        help="Dashboard mapping in format SYMBOL=/abs/or/rel/path/to/file.html",
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


def parse_dashboard_mapping(entries: list[str]) -> dict[str, Path]:
    mapping: dict[str, Path] = {}
    for entry in entries:
        if "=" not in entry:
            raise ValueError(f"Invalid --dashboard value '{entry}'. Use SYMBOL=path.html")
        symbol, path_text = entry.split("=", 1)
        key = symbol.strip().upper()
        if not key:
            raise ValueError(f"Invalid symbol in --dashboard value '{entry}'")
        path = Path(path_text.strip()).expanduser().resolve()
        mapping[key] = path
    return mapping


def build_selector_index(symbol_to_path: dict[str, str], default_symbol: str, date_label: str) -> str:
    options = "\n".join(
        f'<option value="{symbol}">{symbol}</option>' for symbol in sorted(symbol_to_path.keys())
    )
    payload = json.dumps(symbol_to_path, ensure_ascii=False)
    return f"""<!doctype html>
<html lang="pt-BR">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>EWZ/SPY Dashboard</title>
  <style>
    :root {{
      --bg: #080b0c;
      --panel: #101518;
      --line: #223038;
      --text: #e6ece9;
      --muted: #91a097;
      --accent: #d8c37a;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      color: var(--text);
      background:
        radial-gradient(1200px 600px at 0% 0%, #1c292f 0%, transparent 55%),
        radial-gradient(1000px 500px at 100% 100%, #2d1f1d 0%, transparent 60%),
        var(--bg);
      font-family: "Space Grotesk", -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }}
    .wrap {{
      max-width: 1320px;
      margin: 0 auto;
      padding: 18px 16px;
      display: grid;
      gap: 12px;
    }}
    .bar {{
      display: flex;
      justify-content: space-between;
      align-items: center;
      flex-wrap: wrap;
      gap: 10px;
      border: 1px solid var(--line);
      background: linear-gradient(170deg, rgba(16,21,24,.95), rgba(8,11,12,.95));
      border-radius: 10px;
      padding: 12px;
    }}
    .title {{
      font-size: 18px;
      font-weight: 700;
      letter-spacing: .2px;
    }}
    .date {{
      color: var(--muted);
      font-size: 12px;
      font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
    }}
    .controls {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
    }}
    label {{
      color: var(--muted);
      font-size: 12px;
      text-transform: uppercase;
    }}
    select {{
      border: 1px solid var(--line);
      background: var(--panel);
      color: var(--text);
      border-radius: 8px;
      padding: 8px 10px;
      font-weight: 600;
      min-width: 100px;
      outline: none;
    }}
    iframe {{
      width: 100%;
      height: calc(100vh - 110px);
      border: 1px solid var(--line);
      border-radius: 10px;
      background: #050708;
    }}
    @media (max-width: 860px) {{
      iframe {{ height: calc(100vh - 140px); }}
      .title {{ font-size: 16px; }}
    }}
  </style>
</head>
<body>
  <main class="wrap">
    <section class="bar">
      <div>
        <div class="title">Options Flow Dashboard</div>
        <div class="date">Última atualização: {date_label}</div>
      </div>
      <div class="controls">
        <label for="symbol">Símbolo</label>
        <select id="symbol">
          {options}
        </select>
      </div>
    </section>
    <iframe id="frame" title="Dashboard"></iframe>
  </main>
  <script>
    const dashboards = {payload};
    const selector = document.getElementById("symbol");
    const frame = document.getElementById("frame");
    const defaultSymbol = "{default_symbol}";

    function pickInitialSymbol() {{
      const hashSymbol = location.hash.replace("#", "").toUpperCase();
      if (dashboards[hashSymbol]) return hashSymbol;
      return dashboards[defaultSymbol] ? defaultSymbol : Object.keys(dashboards)[0];
    }}

    function render(symbol) {{
      if (!dashboards[symbol]) return;
      selector.value = symbol;
      frame.src = dashboards[symbol];
      if (location.hash !== "#" + symbol) {{
        history.replaceState(null, "", "#" + symbol);
      }}
    }}

    selector.addEventListener("change", () => render(selector.value));
    window.addEventListener("hashchange", () => render(pickInitialSymbol()));
    render(pickInitialSymbol());
  </script>
</body>
</html>
"""


def main() -> int:
    args = parse_args()
    dashboard_html_by_symbol = parse_dashboard_mapping(args.dashboard)
    site_dir = args.site_dir.expanduser().resolve()
    history_dir = site_dir / "history"
    latest_dir = site_dir / "latest"

    for symbol, dashboard_html in dashboard_html_by_symbol.items():
        if not dashboard_html.exists():
            raise FileNotFoundError(f"Dashboard HTML not found ({symbol}): {dashboard_html}")

    site_dir.mkdir(parents=True, exist_ok=True)
    history_dir.mkdir(parents=True, exist_ok=True)
    latest_dir.mkdir(parents=True, exist_ok=True)

    index_path = site_dir / "index.html"
    latest_meta_path = site_dir / "latest.json"
    latest_map: dict[str, str] = {}
    history_map: dict[str, str] = {}

    for symbol in sorted(dashboard_html_by_symbol.keys()):
        html_content = dashboard_html_by_symbol[symbol].read_text(encoding="utf-8")
        latest_path = latest_dir / f"{symbol}.html"
        history_path = history_dir / f"{symbol}_delta_volume_dashboard_{args.date}.html"
        latest_path.write_text(html_content, encoding="utf-8")
        history_path.write_text(html_content, encoding="utf-8")
        latest_map[symbol] = f"latest/{latest_path.name}"
        history_map[symbol] = f"history/{history_path.name}"

    default_symbol = "EWZ" if "EWZ" in latest_map else sorted(latest_map.keys())[0]
    index_path.write_text(
        build_selector_index(latest_map, default_symbol, args.date),
        encoding="utf-8",
    )

    latest_meta_path.write_text(
        json.dumps(
            {
                "date": args.date,
                "default_symbol": default_symbol,
                "symbols": sorted(latest_map.keys()),
                "latest_dashboards": latest_map,
                "history_files": history_map,
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    print(f"Published GitHub Pages index: {index_path}")
    for symbol in sorted(history_map.keys()):
        print(f"{symbol} latest: {latest_map[symbol]}")
        print(f"{symbol} history: {history_map[symbol]}")
    print(f"Wrote metadata: {latest_meta_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
