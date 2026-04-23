#!/usr/bin/env python3
"""Generate a minimal EWZ delta-volume dashboard from flow CSV files."""

from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

import pandas as pd


DEFAULT_TICKER = "EWZ"
DEFAULT_FLOW_FILES = [
    "flow_true_2026-03-20_50_ETF_EWZ_ask_side_FULL_2026-04-21.csv",
    "flow_true_2026-03-20_50_ETF_EWZ_bid_side_FULL_2026-04-21.csv",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build delta volume imbalance dashboard HTML from flow CSV files."
    )
    parser.add_argument("--ticker", default=DEFAULT_TICKER)
    parser.add_argument(
        "--flow-files",
        nargs="*",
        default=DEFAULT_FLOW_FILES,
        help="One or more flow CSV files (ASK/BID).",
    )
    parser.add_argument(
        "--output",
        default=f"{DEFAULT_TICKER}_delta_volume_dashboard.html",
        help="Output HTML file path.",
    )
    return parser.parse_args()


def parse_canceled(series: pd.Series) -> pd.Series:
    normalized = series.fillna("").astype(str).str.strip().str.lower()
    return normalized.isin({"true", "t", "1", "yes"})


def calculate_from_flow(flow_paths: list[Path]) -> dict[str, float | int | str]:
    frames = [pd.read_csv(path) for path in flow_paths]
    flow = pd.concat(frames, ignore_index=True)

    required = {"side", "size", "delta"}
    missing = required - set(flow.columns)
    if missing:
        raise ValueError(f"Flow CSV missing required columns: {sorted(missing)}")

    flow["side"] = flow["side"].astype(str).str.upper().str.strip()
    flow["size"] = pd.to_numeric(flow["size"], errors="coerce").fillna(0.0)
    flow["delta"] = pd.to_numeric(flow["delta"], errors="coerce").fillna(0.0)
    flow["price"] = pd.to_numeric(flow.get("price", 0), errors="coerce").fillna(0.0)
    flow["premium"] = pd.to_numeric(flow.get("premium", 0), errors="coerce").fillna(0.0)

    if "canceled" in flow.columns:
        flow = flow[~parse_canceled(flow["canceled"])].copy()

    ask_mask = flow["side"] == "ASK"
    bid_mask = flow["side"] == "BID"
    ignored_mask = ~flow["side"].isin(["ASK", "BID"])

    flow["delta_volume"] = 0.0
    flow.loc[ask_mask, "delta_volume"] = flow.loc[ask_mask, "size"] * flow.loc[ask_mask, "delta"] * 100
    flow.loc[bid_mask, "delta_volume"] = -flow.loc[bid_mask, "size"] * flow.loc[bid_mask, "delta"] * 100

    bullish = float(flow.loc[flow["delta_volume"] > 0, "delta_volume"].sum())
    bearish = float(flow.loc[flow["delta_volume"] < 0, "delta_volume"].abs().sum())
    total = bullish + bearish

    option_type_col = "option_type" if "option_type" in flow.columns else "type"
    option_kind = flow[option_type_col].astype(str).str.upper().str[0]
    call_mask = option_kind == "C"
    put_mask = option_kind == "P"

    # Two bi-color columns:
    # CALLS = ASK (green) + BID (red)
    # PUTS  = ASK (red) + BID (green)
    calls_ask_green = float(flow.loc[ask_mask & call_mask, "delta_volume"].clip(lower=0).sum())
    calls_bid_red = float(flow.loc[bid_mask & call_mask, "delta_volume"].abs().sum())
    puts_ask_red = float(flow.loc[ask_mask & put_mask, "delta_volume"].abs().sum())
    puts_bid_green = float(flow.loc[bid_mask & put_mask, "delta_volume"].clip(lower=0).sum())

    return {
        "bullish_dv": bullish,
        "bearish_dv": bearish,
        "imbalance": bullish - bearish,
        "pct_bullish": round(bullish / total * 100, 1) if total else 0.0,
        "pct_bearish": round(bearish / total * 100, 1) if total else 0.0,
        "n_contracts": int(len(flow)),
        "n_mid_ignored": int(ignored_mask.sum()),
        "n_ask": int(ask_mask.sum()),
        "n_bid": int(bid_mask.sum()),
        "n_calls": int(call_mask.sum()),
        "n_puts": int(put_mask.sum()),
        "calls_ask_green": calls_ask_green,
        "calls_bid_red": calls_bid_red,
        "puts_ask_red": puts_ask_red,
        "puts_bid_green": puts_bid_green,
        "premium_total": float(flow["premium"].sum()),
        "price_avg": float(flow["price"].mean()) if len(flow) else 0.0,
        "mode": "flow",
    }


def generate_html(ticker: str, data: dict[str, float | int | str]) -> str:
    imbalance = float(data["imbalance"])
    pct_bull = float(data["pct_bullish"])
    pct_bear = float(data["pct_bearish"])
    bullish = float(data["bullish_dv"])
    bearish = float(data["bearish_dv"])

    sentiment = "Bullish" if pct_bull >= 50 else "Bearish"
    sentiment_color = "#1f9e78" if sentiment == "Bullish" else "#d05e34"
    imbalance_color = "#1f9e78" if imbalance >= 0 else "#d05e34"
    imbalance_sign = "+" if imbalance >= 0 else ""
    today = date.today().strftime("%d/%m/%Y")
    mid_note = (
        f"{int(data['n_mid_ignored']):,} MID/NO_SIDE ignorados"
        if int(data["n_mid_ignored"]) > 0
        else "Sem MID/NO_SIDE"
    )

    chart_payload = {
        "donut": [pct_bull, pct_bear],
        "columnsGreen": [float(data["calls_ask_green"]), float(data["puts_bid_green"])],
        "columnsRed": [float(data["calls_bid_red"]), float(data["puts_ask_red"])],
    }

    return f"""<!doctype html>
<html lang="pt-BR">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{ticker} Delta Volume Dashboard</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;700&family=IBM+Plex+Mono:wght@400;500&display=swap" rel="stylesheet">
  <script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
  <style>
    :root {{
      --bg: #090b0c;
      --bg-2: #111719;
      --line: #253035;
      --text: #e6ece8;
      --muted: #8a9890;
      --bull: #1f9e78;
      --bear: #d05e34;
      --accent: #d8c37a;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Space Grotesk", sans-serif;
      color: var(--text);
      background:
        radial-gradient(1200px 600px at 0% 0%, #1d2a2f 0%, transparent 55%),
        radial-gradient(900px 400px at 100% 100%, #2b1c1a 0%, transparent 58%),
        var(--bg);
      min-height: 100vh;
    }}
    .layout {{
      max-width: 1080px;
      margin: 0 auto;
      padding: 28px 20px 40px;
      display: grid;
      gap: 16px;
    }}
    .hero {{
      display: grid;
      gap: 8px;
      border: 1px solid var(--line);
      background: linear-gradient(160deg, rgba(17,23,25,.92), rgba(9,11,12,.95));
      padding: 18px;
      border-radius: 10px;
    }}
    .hero-top {{
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 10px;
      flex-wrap: wrap;
    }}
    .ticker {{
      font-size: 28px;
      line-height: 1;
      letter-spacing: 0;
      font-weight: 700;
    }}
    .stamp {{
      font-family: "IBM Plex Mono", monospace;
      color: var(--muted);
      font-size: 12px;
    }}
    .imbalance {{
      font-size: clamp(32px, 7vw, 58px);
      font-weight: 700;
      color: {imbalance_color};
      line-height: 1;
    }}
    .sub {{
      color: var(--muted);
      font-size: 13px;
    }}
    .status {{
      color: {sentiment_color};
      font-weight: 700;
    }}
    .grid {{
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 10px;
    }}
    .kpi {{
      border: 1px solid var(--line);
      background: var(--bg-2);
      border-radius: 10px;
      padding: 12px;
      min-height: 84px;
    }}
    .k {{
      color: var(--muted);
      font-size: 11px;
      margin-bottom: 8px;
      text-transform: uppercase;
    }}
    .v {{
      font-size: 22px;
      font-weight: 700;
      line-height: 1.1;
    }}
    .panel {{
      border: 1px solid var(--line);
      background: var(--bg-2);
      border-radius: 10px;
      padding: 14px;
    }}
    .chart-grid {{
      display: grid;
      grid-template-columns: 1.1fr 1fr;
      gap: 12px;
      align-items: stretch;
    }}
    .chart-wrap {{
      height: 260px;
    }}
    .small {{
      font-family: "IBM Plex Mono", monospace;
      color: var(--muted);
      font-size: 11px;
    }}
    @media (max-width: 860px) {{
      .grid {{
        grid-template-columns: repeat(2, minmax(0, 1fr));
      }}
      .chart-grid {{
        grid-template-columns: 1fr;
      }}
      .chart-wrap {{
        height: 230px;
      }}
    }}
  </style>
</head>
<body>
  <main class="layout">
    <section class="hero">
      <div class="hero-top">
        <div class="ticker">{ticker} Delta Volume</div>
        <div class="stamp">{today}</div>
      </div>
      <div class="imbalance">{imbalance_sign}{imbalance:,.0f}</div>
      <div class="sub">Order Flow Sentiment: <span class="status">{pct_bull:.1f}% {sentiment}</span> · {mid_note}</div>
    </section>

    <section class="grid">
      <div class="kpi"><div class="k">Bullish Delta Vol</div><div class="v" style="color:var(--bull)">{bullish:,.0f}</div></div>
      <div class="kpi"><div class="k">Bearish Delta Vol</div><div class="v" style="color:var(--bear)">{bearish:,.0f}</div></div>
      <div class="kpi"><div class="k">Trades</div><div class="v">{int(data["n_contracts"]):,}</div></div>
      <div class="kpi"><div class="k">Premium Total</div><div class="v">{float(data["premium_total"]):,.0f}</div></div>
    </section>

    <section class="panel chart-grid">
      <div>
        <div class="small">Bullish vs Bearish (%)</div>
        <div class="chart-wrap"><canvas id="sentimentChart"></canvas></div>
      </div>
      <div>
        <div class="small">CALLS/PUTS bi-color flow</div>
        <div class="chart-wrap"><canvas id="structureChart"></canvas></div>
      </div>
    </section>
  </main>

  <script>
    const payload = {json.dumps(chart_payload)};
    new Chart(document.getElementById("sentimentChart"), {{
      type: "doughnut",
      data: {{
        labels: ["Bullish", "Bearish"],
        datasets: [{{
          data: payload.donut,
          backgroundColor: ["#1f9e78", "#d05e34"],
          borderColor: ["#146e54", "#8f3f23"],
          borderWidth: 1.5
        }}]
      }},
      options: {{
        responsive: true,
        maintainAspectRatio: false,
        cutout: "62%",
        plugins: {{
          legend: {{ labels: {{ color: "#dce4df" }} }}
        }}
      }}
    }});

    new Chart(document.getElementById("structureChart"), {{
      type: "bar",
      data: {{
        labels: ["CALLS", "PUTS"],
        datasets: [{{
          label: "Green side",
          data: payload.columnsGreen,
          backgroundColor: ["#1f9e78", "#1f9e78"],
          borderColor: ["#146e54", "#146e54"],
          borderWidth: 1,
          stack: "flow"
        }},
        {{
          label: "Red side",
          data: payload.columnsRed,
          backgroundColor: ["#d05e34", "#d05e34"],
          borderColor: ["#8f3f23", "#8f3f23"],
          borderWidth: 1
        }}]
      }},
      options: {{
        responsive: true,
        maintainAspectRatio: false,
        plugins: {{ legend: {{ labels: {{ color: "#dce4df" }} }} }},
        scales: {{
          x: {{ stacked: true, ticks: {{ color: "#dce4df" }}, grid: {{ color: "rgba(255,255,255,.06)" }} }},
          y: {{ stacked: true, ticks: {{ color: "#dce4df" }}, grid: {{ color: "rgba(255,255,255,.06)" }} }}
        }}
      }}
    }});
  </script>
</body>
</html>"""


def main() -> int:
    args = parse_args()
    flow_paths = [Path(path) for path in args.flow_files]
    missing = [str(path) for path in flow_paths if not path.exists()]
    if missing:
        raise FileNotFoundError(f"Flow file(s) not found: {missing}")

    data = calculate_from_flow(flow_paths)
    html = generate_html(args.ticker, data)

    output_path = Path(args.output)
    output_path.write_text(html, encoding="utf-8")

    sign = "+" if float(data["imbalance"]) >= 0 else ""
    print(f"Ticker: {args.ticker}")
    print(f"Bullish Delta Vol: {float(data['bullish_dv']):,.0f} ({float(data['pct_bullish']):.1f}%)")
    print(f"Bearish Delta Vol: {float(data['bearish_dv']):,.0f} ({float(data['pct_bearish']):.1f}%)")
    print(f"Imbalance: {sign}{float(data['imbalance']):,.0f}")
    print(f"Trades: {int(data['n_contracts']):,}")
    print(f"HTML generated: {output_path.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
