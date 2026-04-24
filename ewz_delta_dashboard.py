#!/usr/bin/env python3
"""Generate a minimal EWZ delta-volume dashboard from flow CSV files."""

from __future__ import annotations

import argparse
import csv
import json
from datetime import date
from pathlib import Path
from zipfile import ZipFile

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
        "--oi-change-file",
        default=None,
        help="Optional chain OI changes CSV or ZIP file.",
    )
    parser.add_argument(
        "--dp-file",
        default=None,
        help="Optional darkpool DP EOD CSV or ZIP file.",
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


def load_csv_from_path(path: Path) -> list[dict[str, str]]:
    if path.suffix.lower() == ".zip":
        with ZipFile(path) as archive:
            names = [name for name in archive.namelist() if name.lower().endswith(".csv")]
            if not names:
                raise ValueError(f"No CSV found inside ZIP: {path}")
            with archive.open(names[0]) as handle:
                reader = csv.DictReader(
                    line.decode("utf-8", errors="replace") for line in handle
                )
                return list(reader)

    with path.open(newline="", encoding="utf-8", errors="replace") as handle:
        return list(csv.DictReader(handle))


def summarize_oi_change(oi_path: Path | None, ticker: str) -> dict[str, object] | None:
    if oi_path is None:
        return None

    rows = load_csv_from_path(oi_path)
    symbol_rows = [
        row for row in rows if str(row.get("underlying_symbol", "")).strip().upper() == ticker.upper()
    ]
    if not symbol_rows:
        return None

    records: list[dict[str, object]] = []
    opening_contracts = 0.0
    closing_contracts = 0.0
    bullish_open = 0.0
    bearish_open = 0.0
    neutral_open = 0.0
    bullish_close = 0.0
    bearish_close = 0.0
    neutral_close = 0.0

    for row in symbol_rows:
        last_oi = float(row.get("last_oi") or 0.0)
        curr_oi = float(row.get("curr_oi") or 0.0)
        volume = float(row.get("volume") or 0.0)
        prev_bid = float(row.get("prev_bid_volume") or 0.0)
        prev_ask = float(row.get("prev_ask_volume") or 0.0)
        diff = curr_oi - last_oi

        if prev_ask > prev_bid:
            bias = "bullish"
        elif prev_bid > prev_ask:
            bias = "bearish"
        else:
            bias = "neutral"

        opening = max(diff, 0.0)
        closing = max(-diff, 0.0)
        opening_contracts += opening
        closing_contracts += closing

        if bias == "bullish":
            bullish_open += opening
            bullish_close += closing
        elif bias == "bearish":
            bearish_open += opening
            bearish_close += closing
        else:
            neutral_open += opening
            neutral_close += closing

        records.append(
            {
                "option_symbol": row.get("option_symbol", ""),
                "strike": float(row.get("strike") or 0.0),
                "dte": int(float(row.get("dte") or 0.0)),
                "volume": volume,
                "prev_ask_volume": prev_ask,
                "prev_bid_volume": prev_bid,
                "opened": opening,
                "closed": closing,
                "net_change": diff,
                "bias": bias,
            }
        )

    top_open = sorted(records, key=lambda item: float(item["opened"]), reverse=True)[:5]
    top_close = sorted(records, key=lambda item: float(item["closed"]), reverse=True)[:5]

    return {
        "rows": len(records),
        "opening_contracts": opening_contracts,
        "closing_contracts": closing_contracts,
        "net_contracts": opening_contracts - closing_contracts,
        "bullish_open": bullish_open,
        "bearish_open": bearish_open,
        "neutral_open": neutral_open,
        "bullish_close": bullish_close,
        "bearish_close": bearish_close,
        "neutral_close": neutral_close,
        "top_open": top_open,
        "top_close": top_close,
    }


def summarize_darkpool(dp_path: Path | None, ticker: str) -> dict[str, object] | None:
    if dp_path is None:
        return None

    rows = load_csv_from_path(dp_path)
    symbol_rows = [row for row in rows if str(row.get("ticker", "")).strip().upper() == ticker.upper()]
    if not symbol_rows:
        return None

    frame = pd.DataFrame(symbol_rows)
    for column in [
        "size",
        "volume",
        "premium",
        "price",
        "nbbo_ask",
        "nbbo_bid",
        "nbbo_ask_quantity",
        "nbbo_bid_quantity",
    ]:
        frame[column] = pd.to_numeric(frame.get(column), errors="coerce").fillna(0.0)

    if "canceled" in frame.columns:
        frame = frame.loc[~parse_canceled(frame["canceled"])].copy()

    frame["off_mid"] = 0.0
    spread = frame["nbbo_ask"] - frame["nbbo_bid"]
    valid_spread = spread > 0
    mid = (frame["nbbo_ask"] + frame["nbbo_bid"]) / 2.0
    frame.loc[valid_spread, "off_mid"] = frame.loc[valid_spread, "price"] - mid.loc[valid_spread]

    above_ask = int((frame["price"] > frame["nbbo_ask"]).sum())
    below_bid = int((frame["price"] < frame["nbbo_bid"]).sum())
    inside_nbbo = int(((frame["price"] >= frame["nbbo_bid"]) & (frame["price"] <= frame["nbbo_ask"])).sum())
    executed_regular = int((frame.get("trade_settlement", "").astype(str).str.lower() == "regular").sum())
    extended_hours = int(frame.get("ext_hour_sold_codes", "").astype(str).str.contains("extended", case=False, na=False).sum())

    top_rows = (
        frame.sort_values(["premium", "size"], ascending=False)
        .head(5)[["executed_at", "price", "size", "premium", "nbbo_bid", "nbbo_ask"]]
        .to_dict(orient="records")
    )

    return {
        "rows": int(len(frame)),
        "premium_total": float(frame["premium"].sum()),
        "size_total": float(frame["size"].sum()),
        "volume_total": float(frame["volume"].sum()),
        "avg_price": float(frame["price"].mean()) if len(frame) else 0.0,
        "avg_off_mid": float(frame["off_mid"].mean()) if len(frame) else 0.0,
        "above_ask": above_ask,
        "below_bid": below_bid,
        "inside_nbbo": inside_nbbo,
        "regular_settlement": executed_regular,
        "extended_hours": extended_hours,
        "top_rows": top_rows,
    }


def build_bot_dp_comparison(flow_data: dict[str, float | int | str], dp_summary: dict[str, object] | None) -> dict[str, object] | None:
    if not dp_summary:
        return None

    bot_trades = int(flow_data["n_contracts"])
    dp_trades = int(dp_summary["rows"])
    bot_premium = float(flow_data["premium_total"])
    dp_premium = float(dp_summary["premium_total"])
    bot_vs_dp_premium = bot_premium / dp_premium if dp_premium else None

    similarities = [
        "Mesmo ativo e mesma data da sessao.",
        "Ambos capturam atividade grande/institucional.",
        "Os dois ajudam a confirmar onde houve concentracao de capital.",
    ]
    differences = [
        "BOT = fluxo de opcoes; DP = blocos de acoes em dark pool.",
        "BOT traz lado/estrutura de calls e puts; DP nao traz cadeia de opcoes.",
        "Premium BOT e premium DP nao sao metricas identicas, entao a leitura deve ser paralela e nao somada.",
    ]

    return {
        "bot_trades": bot_trades,
        "dp_trades": dp_trades,
        "bot_premium": bot_premium,
        "dp_premium": dp_premium,
        "premium_ratio": bot_vs_dp_premium,
        "similarities": similarities,
        "differences": differences,
    }


def explain_option_symbol(option_symbol: str) -> str:
    text = option_symbol.strip().upper()
    if not text:
        return ""

    root_end = next((index for index, char in enumerate(text) if char.isdigit()), -1)
    if root_end < 0:
        return ""
    tail = text[root_end:]
    if len(tail) < 15:
        return ""

    yymmdd = tail[:6]
    cp_flag = tail[6]
    strike_digits = tail[7:15]

    if not (yymmdd.isdigit() and strike_digits.isdigit()):
        return ""

    year = 2000 + int(yymmdd[:2])
    month = int(yymmdd[2:4])
    day = int(yymmdd[4:6])
    if month < 1 or month > 12 or day < 1 or day > 31:
        return ""

    strike = int(strike_digits) / 1000.0
    option_type = "Call" if cp_flag == "C" else "Put" if cp_flag == "P" else cp_flag
    return f"{option_type} {strike:.2f} {year:04d}-{month:02d}-{day:02d}"


def render_oi_rows(rows: list[dict[str, object]], mode: str) -> str:
    if not rows:
        return '<div class="oi-empty">Sem contratos relevantes.</div>'

    label = "Abertos" if mode == "open" else "Fechados"
    cells = []
    for row in rows:
        amount = float(row["opened"] if mode == "open" else row["closed"])
        if amount <= 0:
            continue
        option_symbol = str(row["option_symbol"])
        option_explain = explain_option_symbol(option_symbol)
        bias = str(row["bias"])
        bias_class = {
            "bullish": "bull",
            "bearish": "bear",
        }.get(bias, "neutral")
        explain_html = f'<span class="oi-explain"> | {option_explain}</span>' if option_explain else ""
        cells.append(
            f"""
            <div class="oi-row">
              <div class="oi-contract">{option_symbol}{explain_html}</div>
              <div class="oi-meta">{label}: {amount:,.0f} · Bias: <span class="{bias_class}">{bias}</span></div>
            </div>
            """
        )
    return "".join(cells) if cells else '<div class="oi-empty">Sem contratos relevantes.</div>'


def render_dp_rows(rows: list[dict[str, object]]) -> str:
    if not rows:
        return '<div class="oi-empty">Sem prints relevantes.</div>'

    cells = []
    for row in rows:
        cells.append(
            f"""
            <div class="oi-row">
              <div class="oi-contract">{row["executed_at"]}</div>
              <div class="oi-meta">Price: {float(row["price"]):.2f} · Size: {float(row["size"]):,.0f} · Premium: {float(row["premium"]):,.0f} · NBBO {float(row["nbbo_bid"]):.2f}/{float(row["nbbo_ask"]):.2f}</div>
            </div>
            """
        )
    return "".join(cells)


def generate_html(
    ticker: str,
    data: dict[str, float | int | str],
    oi_summary: dict[str, object] | None,
    dp_summary: dict[str, object] | None,
    bot_dp_comparison: dict[str, object] | None,
) -> str:
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
        "oiOpen": [
            float(oi_summary["bullish_open"]) if oi_summary else 0.0,
            float(oi_summary["bearish_open"]) if oi_summary else 0.0,
            float(oi_summary["neutral_open"]) if oi_summary else 0.0,
        ],
        "oiClose": [
            float(oi_summary["bullish_close"]) if oi_summary else 0.0,
            float(oi_summary["bearish_close"]) if oi_summary else 0.0,
            float(oi_summary["neutral_close"]) if oi_summary else 0.0,
        ],
    }

    oi_panel = ""
    if oi_summary:
        net_contracts = float(oi_summary["net_contracts"])
        net_class = "bull" if net_contracts >= 0 else "bear"
        net_sign = "+" if net_contracts >= 0 else ""
        oi_panel = f"""
    <section class="panel">
      <div class="panel-head">
        <div class="small">OI-CHANGE | posições abertas x fechadas</div>
        <div class="small">Contratos monitorados: {int(oi_summary["rows"]):,}</div>
      </div>
      <section class="grid oi-grid">
        <div class="kpi"><div class="k">Aberturas</div><div class="v">{float(oi_summary["opening_contracts"]):,.0f}</div></div>
        <div class="kpi"><div class="k">Fechamentos</div><div class="v">{float(oi_summary["closing_contracts"]):,.0f}</div></div>
        <div class="kpi"><div class="k">Saldo OI</div><div class="v {net_class}">{net_sign}{net_contracts:,.0f}</div></div>
        <div class="kpi"><div class="k">Bull vs Bear Open</div><div class="v"><span class="bull">{float(oi_summary["bullish_open"]):,.0f}</span> / <span class="bear">{float(oi_summary["bearish_open"]):,.0f}</span></div></div>
      </section>
      <div class="chart-grid oi-chart-grid">
        <div>
          <div class="small">Abertura/fechamento por bias</div>
          <div class="chart-wrap"><canvas id="oiChart"></canvas></div>
        </div>
        <div class="oi-lists">
          <div>
            <div class="small">Top aberturas</div>
            <div class="oi-list">{render_oi_rows(oi_summary["top_open"], "open")}</div>
          </div>
          <div>
            <div class="small">Top fechamentos</div>
            <div class="oi-list">{render_oi_rows(oi_summary["top_close"], "close")}</div>
          </div>
        </div>
      </div>
    </section>
"""

    dp_panel = ""
    if dp_summary and bot_dp_comparison:
        premium_ratio = bot_dp_comparison["premium_ratio"]
        ratio_text = f"{premium_ratio:.2f}x" if premium_ratio is not None else "N/A"
        similarities_html = "".join(
            f'<div class="compare-item bull">{item}</div>' for item in bot_dp_comparison["similarities"]
        )
        differences_html = "".join(
            f'<div class="compare-item bear">{item}</div>' for item in bot_dp_comparison["differences"]
        )
        dp_panel = f"""
    <section class="panel">
      <div class="panel-head">
        <div class="small">DP-EOD | dark pool separado do BOT</div>
        <div class="small">Prints monitorados: {int(dp_summary["rows"]):,}</div>
      </div>
      <section class="grid">
        <div class="kpi"><div class="k">DP Premium</div><div class="v">{float(dp_summary["premium_total"]):,.0f}</div></div>
        <div class="kpi"><div class="k">DP Size</div><div class="v">{float(dp_summary["size_total"]):,.0f}</div></div>
        <div class="kpi"><div class="k">DP Avg Price</div><div class="v">{float(dp_summary["avg_price"]):.2f}</div></div>
        <div class="kpi"><div class="k">Inside NBBO</div><div class="v">{int(dp_summary["inside_nbbo"]):,}</div></div>
      </section>
      <div class="compare-grid">
        <div class="compare-box">
          <div class="small">BOT x DP | semelhancas</div>
          {similarities_html}
        </div>
        <div class="compare-box">
          <div class="small">BOT x DP | diferencas</div>
          {differences_html}
        </div>
      </div>
      <section class="grid compare-kpis">
        <div class="kpi"><div class="k">BOT Trades</div><div class="v">{int(bot_dp_comparison["bot_trades"]):,}</div></div>
        <div class="kpi"><div class="k">DP Prints</div><div class="v">{int(bot_dp_comparison["dp_trades"]):,}</div></div>
        <div class="kpi"><div class="k">BOT Premium</div><div class="v">{float(bot_dp_comparison["bot_premium"]):,.0f}</div></div>
        <div class="kpi"><div class="k">BOT/DP Premium</div><div class="v">{ratio_text}</div></div>
      </section>
      <div style="margin-top:12px">
        <div class="small">Top prints DP</div>
        <div class="oi-list">{render_dp_rows(dp_summary["top_rows"])}</div>
      </div>
    </section>
"""

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
      --neutral: #93a29a;
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
    .panel-head {{
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 12px;
      margin-bottom: 12px;
      flex-wrap: wrap;
    }}
    .chart-grid {{
      display: grid;
      grid-template-columns: 1.1fr 1fr;
      gap: 12px;
      align-items: stretch;
    }}
    .oi-chart-grid {{
      grid-template-columns: 1fr 1fr;
      margin-top: 12px;
    }}
    .chart-wrap {{
      height: 260px;
    }}
    .small {{
      font-family: "IBM Plex Mono", monospace;
      color: var(--muted);
      font-size: 11px;
    }}
    .bull {{ color: var(--bull); }}
    .bear {{ color: var(--bear); }}
    .neutral {{ color: var(--neutral); }}
    .oi-lists {{
      display: grid;
      grid-template-columns: 1fr;
      gap: 12px;
    }}
    .oi-list {{
      display: grid;
      gap: 8px;
      margin-top: 8px;
    }}
    .oi-row {{
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 10px;
      background: rgba(255,255,255,.02);
    }}
    .oi-contract {{
      font-family: "IBM Plex Mono", monospace;
      font-size: 12px;
      color: var(--text);
    }}
    .oi-explain {{
      color: var(--muted);
      font-family: "Space Grotesk", sans-serif;
      font-size: 11px;
      font-weight: 500;
    }}
    .oi-meta, .oi-empty {{
      margin-top: 4px;
      color: var(--muted);
      font-size: 12px;
    }}
    .compare-grid {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 12px;
      margin-top: 12px;
    }}
    .compare-box {{
      border: 1px solid var(--line);
      border-radius: 10px;
      padding: 12px;
      background: rgba(255,255,255,.02);
      display: grid;
      gap: 8px;
    }}
    .compare-item {{
      font-size: 13px;
      line-height: 1.35;
      color: var(--text);
    }}
    @media (max-width: 860px) {{
      .grid {{
        grid-template-columns: repeat(2, minmax(0, 1fr));
      }}
      .chart-grid,
      .oi-chart-grid,
      .compare-grid {{
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
{oi_panel}
{dp_panel}
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
          borderWidth: 1,
          stack: "flow"
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

    if (document.getElementById("oiChart")) {{
      new Chart(document.getElementById("oiChart"), {{
        type: "bar",
        data: {{
          labels: ["Bullish", "Bearish", "Neutral"],
          datasets: [{{
            label: "Aberturas",
            data: payload.oiOpen,
            backgroundColor: ["#1f9e78", "#d05e34", "#93a29a"],
            borderWidth: 0
          }},
          {{
            label: "Fechamentos",
            data: payload.oiClose,
            backgroundColor: ["#146e54", "#8f3f23", "#55635c"],
            borderWidth: 0
          }}]
        }},
        options: {{
          responsive: true,
          maintainAspectRatio: false,
          plugins: {{ legend: {{ labels: {{ color: "#dce4df" }} }} }},
          scales: {{
            x: {{ ticks: {{ color: "#dce4df" }}, grid: {{ color: "rgba(255,255,255,.06)" }} }},
            y: {{ ticks: {{ color: "#dce4df" }}, grid: {{ color: "rgba(255,255,255,.06)" }} }}
          }}
        }}
      }});
    }}
  </script>
</body>
</html>"""


def main() -> int:
    args = parse_args()
    flow_paths = [Path(path) for path in args.flow_files]
    missing = [str(path) for path in flow_paths if not path.exists()]
    if missing:
        raise FileNotFoundError(f"Flow file(s) not found: {missing}")

    oi_path = Path(args.oi_change_file) if args.oi_change_file else None
    if oi_path and not oi_path.exists():
        raise FileNotFoundError(f"OI change file not found: {oi_path}")
    dp_path = Path(args.dp_file) if args.dp_file else None
    if dp_path and not dp_path.exists():
        raise FileNotFoundError(f"DP file not found: {dp_path}")

    data = calculate_from_flow(flow_paths)
    oi_summary = summarize_oi_change(oi_path, args.ticker)
    dp_summary = summarize_darkpool(dp_path, args.ticker)
    bot_dp_comparison = build_bot_dp_comparison(data, dp_summary)
    html = generate_html(args.ticker, data, oi_summary, dp_summary, bot_dp_comparison)

    output_path = Path(args.output)
    output_path.write_text(html, encoding="utf-8")

    sign = "+" if float(data["imbalance"]) >= 0 else ""
    print(f"Ticker: {args.ticker}")
    print(f"Bullish Delta Vol: {float(data['bullish_dv']):,.0f} ({float(data['pct_bullish']):.1f}%)")
    print(f"Bearish Delta Vol: {float(data['bearish_dv']):,.0f} ({float(data['pct_bearish']):.1f}%)")
    print(f"Imbalance: {sign}{float(data['imbalance']):,.0f}")
    print(f"Trades: {int(data['n_contracts']):,}")
    if oi_summary:
        net_contracts = float(oi_summary["net_contracts"])
        net_sign = "+" if net_contracts >= 0 else ""
        print(f"OI Opened: {float(oi_summary['opening_contracts']):,.0f}")
        print(f"OI Closed: {float(oi_summary['closing_contracts']):,.0f}")
        print(f"OI Net: {net_sign}{net_contracts:,.0f}")
    if dp_summary:
        print(f"DP Prints: {int(dp_summary['rows']):,}")
        print(f"DP Premium: {float(dp_summary['premium_total']):,.0f}")
    print(f"HTML generated: {output_path.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
