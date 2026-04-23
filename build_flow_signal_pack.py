#!/usr/bin/env python3
"""Build advanced options-flow indicators from flow_true-style CSV files.

Indicators implemented:
1) SMFI (Smart Money Flow Index)
2) Put/Call premium ratio (+ optional Z-score with 30d history)
3) GEX by strike
4) FMS (Flow Momentum Score) on rolling time windows
5) UAS (Unusual Activity Score)
6) DEX (Delta Exposure)
7) Composite score from SMFI/DEX/PC/FMS
"""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd


PROJECT_DIR = Path(__file__).resolve().parent
DEFAULT_EWZ_FILES = [
    PROJECT_DIR / "flow_true_2026-03-20_50_ETF_EWZ_ask_side_FULL_2026-04-21.csv",
    PROJECT_DIR / "flow_true_2026-03-20_50_ETF_EWZ_bid_side_FULL_2026-04-21.csv",
]

TRUE_VALUES = {"true", "t", "1", "yes"}
SIDE_VALUES = {"ASK", "BID"}
TYPE_MAP = {"C": "CALL", "P": "PUT", "CALL": "CALL", "PUT": "PUT"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build advanced option-flow signal pack (JSON, CSVs, HTML dashboard)."
    )
    parser.add_argument("--symbol", default="EWZ", help="Underlying symbol label.")
    parser.add_argument(
        "--flow-files",
        nargs="+",
        type=Path,
        default=DEFAULT_EWZ_FILES,
        help="One or more flow_true CSVs (usually ASK and BID FULL files).",
    )
    parser.add_argument(
        "--window-minutes",
        type=int,
        default=30,
        help="FMS aggregation window in minutes.",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=20,
        help="Top-N rows saved for UAS trades/clusters and GEX extremes.",
    )
    parser.add_argument(
        "--pc-history-file",
        type=Path,
        default=None,
        help=(
            "Optional CSV with historical put/call ratios. "
            "Expected columns: 'pc_ratio' (required) and optional date column."
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=PROJECT_DIR,
        help="Directory for generated files.",
    )
    parser.add_argument(
        "--output-prefix",
        default=None,
        help=(
            "Output filename prefix. Default: "
            "<symbol>_flow_signals_<session-date>."
        ),
    )
    parser.add_argument(
        "--skip-html",
        action="store_true",
        help="Skip HTML dashboard generation.",
    )
    return parser.parse_args()


def parse_canceled(series: pd.Series) -> pd.Series:
    normalized = series.fillna("").astype(str).str.strip().str.lower()
    return normalized.isin(TRUE_VALUES)


def infer_option_type(df: pd.DataFrame) -> pd.Series:
    if "option_type" in df.columns:
        raw = df["option_type"]
    elif "type" in df.columns:
        raw = df["type"]
    else:
        return pd.Series(index=df.index, dtype="object")
    return raw.astype(str).str.upper().str.strip().map(TYPE_MAP)


def load_flow(flow_paths: list[Path]) -> pd.DataFrame:
    usecols = {
        "date",
        "time",
        "side",
        "strike",
        "DTE",
        "size",
        "premium",
        "volume",
        "open_interest",
        "delta",
        "gamma",
        "underlying_price",
        "option_type",
        "type",
        "canceled",
    }
    frames: list[pd.DataFrame] = []
    for path in flow_paths:
        if not path.exists():
            raise FileNotFoundError(f"Flow file not found: {path}")
        frame = pd.read_csv(path, usecols=lambda column: column in usecols)
        frame["source_file"] = path.name
        frames.append(frame)

    flow = pd.concat(frames, ignore_index=True)
    if len(flow) == 0:
        raise ValueError("No rows loaded from flow files.")

    flow["side"] = flow["side"].astype(str).str.upper().str.strip()
    flow["option_type_norm"] = infer_option_type(flow)
    flow = flow[
        flow["side"].isin(SIDE_VALUES) & flow["option_type_norm"].isin(["CALL", "PUT"])
    ].copy()

    numeric_cols = [
        "strike",
        "DTE",
        "size",
        "premium",
        "volume",
        "open_interest",
        "delta",
        "gamma",
        "underlying_price",
    ]
    for column in numeric_cols:
        flow[column] = pd.to_numeric(flow[column], errors="coerce")

    if "canceled" in flow.columns:
        flow = flow.loc[~parse_canceled(flow["canceled"])].copy()

    flow["premium"] = flow["premium"].fillna(0.0).clip(lower=0.0)
    flow["size"] = flow["size"].fillna(0.0)
    flow["delta"] = flow["delta"].fillna(0.0)
    flow["volume"] = flow["volume"].fillna(0.0)
    flow["open_interest"] = flow["open_interest"].fillna(0.0)
    flow["gamma"] = flow["gamma"].fillna(0.0)
    flow["underlying_price"] = flow["underlying_price"].ffill().bfill().fillna(0.0)

    timestamps = pd.to_datetime(
        flow["date"].astype(str) + " " + flow["time"].astype(str),
        format="%m/%d/%Y %H:%M:%S",
        errors="coerce",
    )
    flow["ts"] = timestamps
    flow = flow.dropna(subset=["ts"]).sort_values("ts").reset_index(drop=True)
    if len(flow) == 0:
        raise ValueError("No valid timestamp rows after parsing date/time.")

    bullish = (
        ((flow["side"] == "ASK") & (flow["option_type_norm"] == "CALL"))
        | ((flow["side"] == "BID") & (flow["option_type_norm"] == "PUT"))
    )
    flow["direction"] = np.where(bullish, 1, -1).astype(int)
    flow["direction_label"] = np.where(bullish, "bullish", "bearish")
    return flow


def compute_smfi(flow: pd.DataFrame) -> dict[str, float]:
    weights = flow["premium"] * flow["delta"].abs()
    denominator = float(weights.sum())
    if denominator == 0:
        return {"smfi": 0.0, "weight_sum": 0.0}
    numerator = float((weights * flow["direction"]).sum())
    return {"smfi": numerator / denominator, "weight_sum": denominator}


def compute_put_call_ratio(flow: pd.DataFrame) -> dict[str, float]:
    put_premium = float(flow.loc[flow["option_type_norm"] == "PUT", "premium"].sum())
    call_premium = float(flow.loc[flow["option_type_norm"] == "CALL", "premium"].sum())
    ratio = put_premium / call_premium if call_premium else math.nan
    return {
        "put_premium": put_premium,
        "call_premium": call_premium,
        "pc_ratio": ratio,
    }


def compute_pc_zscore(pc_ratio: float, history_path: Path | None) -> dict[str, float | None]:
    result: dict[str, float | None] = {"pc_zscore_30d": None, "pc_hist_mean_30d": None, "pc_hist_std_30d": None}
    if history_path is None:
        return result
    if not history_path.exists():
        raise FileNotFoundError(f"PC history file not found: {history_path}")

    hist = pd.read_csv(history_path)
    if "pc_ratio" not in hist.columns:
        raise ValueError("PC history CSV must contain a 'pc_ratio' column.")
    hist["pc_ratio"] = pd.to_numeric(hist["pc_ratio"], errors="coerce")
    recent = hist["pc_ratio"].dropna().tail(30)
    if len(recent) < 2 or not np.isfinite(pc_ratio):
        return result

    mean = float(recent.mean())
    std = float(recent.std(ddof=0))
    z = float((pc_ratio - mean) / std) if std > 0 else None
    result["pc_zscore_30d"] = z
    result["pc_hist_mean_30d"] = mean
    result["pc_hist_std_30d"] = std
    return result


def compute_dex(flow: pd.DataFrame) -> dict[str, float]:
    raw = flow["delta"] * flow["size"] * 100.0 * flow["direction"]
    dex = float(raw.sum())
    abs_base = float((flow["delta"].abs() * flow["size"] * 100.0).sum())
    normalized = float(np.clip(dex / abs_base, -1.0, 1.0)) if abs_base else 0.0
    return {"dex": dex, "dex_abs_base": abs_base, "dex_norm": normalized}


def compute_fms(flow: pd.DataFrame, window_minutes: int) -> pd.DataFrame:
    frame = flow.copy()
    frame["window"] = frame["ts"].dt.floor(f"{window_minutes}min")
    frame["bull_prem"] = np.where(frame["direction"] == 1, frame["premium"], 0.0)
    frame["bear_prem"] = np.where(frame["direction"] == -1, frame["premium"], 0.0)

    grouped = (
        frame.groupby("window", as_index=False)[["bull_prem", "bear_prem"]]
        .sum()
        .sort_values("window")
    )
    denominator = grouped["bull_prem"] + grouped["bear_prem"]
    grouped["fms"] = np.where(
        denominator > 0,
        (grouped["bull_prem"] - grouped["bear_prem"]) / denominator,
        0.0,
    )
    grouped["delta_fms"] = grouped["fms"].diff().fillna(0.0)

    sign = np.sign(grouped["fms"])
    prev_sign = sign.shift(1).fillna(0.0)
    grouped["reversal"] = (sign * prev_sign) < 0
    return grouped


def compute_uas(flow: pd.DataFrame, top_n: int) -> tuple[pd.DataFrame, pd.DataFrame]:
    frame = flow.copy()
    oi_safe = frame["open_interest"].replace(0.0, np.nan)
    frame["vol_oi"] = frame["volume"] / oi_safe
    frame["uas"] = (
        np.log(frame["premium"].clip(lower=1.0))
        * frame["vol_oi"].fillna(0.0)
        * frame["delta"].abs()
    )
    frame["uas"] = frame["uas"].replace([np.inf, -np.inf], np.nan).fillna(0.0)

    trade_cols = [
        "ts",
        "side",
        "option_type_norm",
        "strike",
        "DTE",
        "size",
        "premium",
        "volume",
        "open_interest",
        "delta",
        "vol_oi",
        "direction_label",
        "uas",
    ]
    top_trades = frame.sort_values("uas", ascending=False).head(top_n)[trade_cols].copy()

    clusters = (
        frame.groupby(["strike", "DTE", "option_type_norm", "direction_label"], as_index=False)
        .agg(
            uas_sum=("uas", "sum"),
            trades=("uas", "size"),
            premium_sum=("premium", "sum"),
            mean_vol_oi=("vol_oi", "mean"),
        )
        .sort_values(["uas_sum", "premium_sum"], ascending=False)
        .head(top_n)
    )
    return top_trades, clusters


def compute_gex(flow: pd.DataFrame, top_n: int) -> tuple[pd.DataFrame, dict[str, float | str | None]]:
    frame = flow.copy()
    frame["gex"] = (
        frame["gamma"]
        * frame["open_interest"]
        * 100.0
        * (frame["underlying_price"] ** 2)
        * frame["direction"]
    )
    by_strike = (
        frame.groupby("strike", as_index=False)["gex"]
        .sum()
        .sort_values("gex", ascending=False)
    )
    total = float(frame["gex"].sum())

    positive = by_strike[by_strike["gex"] > 0]
    pin_strike = float(positive.iloc[0]["strike"]) if len(positive) else None
    pin_gex = float(positive.iloc[0]["gex"]) if len(positive) else None

    regime = "positive_gex_compression" if total > 0 else "negative_gex_expansion" if total < 0 else "neutral"
    meta = {
        "gex_total": total,
        "gex_regime": regime,
        "pin_strike": pin_strike,
        "pin_gex": pin_gex,
    }
    extremes = pd.concat([by_strike.head(top_n), by_strike.tail(top_n).sort_values("gex")], ignore_index=True)
    return extremes, meta


def classify_score(score: float) -> str:
    if score > 0.3:
        return "bullish_strong"
    if score > 0.1:
        return "bullish_moderate"
    if score >= -0.1:
        return "neutral"
    if score >= -0.3:
        return "bearish_moderate"
    return "bearish_strong"


def compute_composite(smfi: float, dex_norm: float, pc_ratio: float, fms_last: float) -> dict[str, float | str]:
    # Normalization assumptions:
    # - DEX: already bounded by abs-base ratio in [-1, 1]
    # - PC: tanh(1 - pc_ratio), so >1 drags score down, <1 lifts score
    # - FMS: already in [-1, 1]
    norm_pc = float(np.tanh(1.0 - pc_ratio)) if np.isfinite(pc_ratio) else 0.0
    score = (0.35 * smfi) + (0.25 * dex_norm) + (0.25 * norm_pc) + (0.15 * fms_last)
    return {
        "composite_score": float(score),
        "composite_label": classify_score(float(score)),
        "norm_pc_component": norm_pc,
    }


def build_alerts(
    smfi: float,
    fms: pd.DataFrame,
    clusters: pd.DataFrame,
    gex_meta: dict[str, float | str | None],
) -> list[str]:
    alerts: list[str] = []
    if smfi >= 0.1:
        alerts.append(f"SMFI bullish bias ({smfi:.3f}).")
    elif smfi <= -0.1:
        alerts.append(f"SMFI bearish bias ({smfi:.3f}).")
    else:
        alerts.append(f"SMFI near neutral ({smfi:.3f}).")

    if len(fms):
        latest = fms.iloc[-1]
        alerts.append(
            f"FMS latest {latest['window']:%H:%M}: {latest['fms']:.3f} "
            f"(delta {latest['delta_fms']:+.3f})."
        )
        reversal_rows = fms[fms["reversal"]]
        if len(reversal_rows):
            rev = reversal_rows.iloc[-1]
            alerts.append(f"FMS reversal detected at {rev['window']:%H:%M}.")

    if len(clusters):
        top = clusters.iloc[0]
        alerts.append(
            "Top unusual cluster: "
            f"{top['direction_label']} {top['option_type_norm']} strike {top['strike']} "
            f"DTE {top['DTE']} (UAS sum {top['uas_sum']:.2f})."
        )

    regime = gex_meta["gex_regime"]
    if regime == "positive_gex_compression":
        alerts.append("Total GEX positive: lower-volatility/pin tendency.")
    elif regime == "negative_gex_expansion":
        alerts.append("Total GEX negative: higher-volatility/expansion tendency.")

    if gex_meta["pin_strike"] is not None:
        alerts.append(
            f"Potential pin strike near {gex_meta['pin_strike']:.2f} "
            f"(GEX {gex_meta['pin_gex']:.2f})."
        )
    return alerts


def json_safe(value: Any) -> Any:
    if isinstance(value, (np.floating, np.integer)):
        value = value.item()
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    if isinstance(value, (pd.Timestamp, np.datetime64)):
        return pd.Timestamp(value).isoformat()
    return value


def df_for_csv(df: pd.DataFrame) -> pd.DataFrame:
    copy = df.copy()
    for col in copy.columns:
        if pd.api.types.is_datetime64_any_dtype(copy[col]):
            copy[col] = copy[col].dt.strftime("%Y-%m-%d %H:%M:%S")
    return copy


def write_html_dashboard(
    out_path: Path,
    summary: dict[str, Any],
    fms: pd.DataFrame,
    gex: pd.DataFrame,
    clusters: pd.DataFrame,
) -> None:
    def fmt_number(x: Any, decimals: int = 3) -> str:
        if x is None:
            return "N/A"
        if isinstance(x, float) and (math.isnan(x) or math.isinf(x)):
            return "N/A"
        return f"{x:,.{decimals}f}"

    def fmt_money(x: Any) -> str:
        if x is None:
            return "N/A"
        value = float(x)
        if abs(value) >= 1e9:
            return f"${value / 1e9:,.2f}B"
        if abs(value) >= 1e6:
            return f"${value / 1e6:,.2f}M"
        if abs(value) >= 1e3:
            return f"${value / 1e3:,.1f}K"
        return f"${value:,.0f}"

    score = float(summary["composite_score"])
    score_pct = max(0.0, min(100.0, (score + 1.0) * 50.0))
    score_color = "#16a34a" if score > 0.1 else "#dc2626" if score < -0.1 else "#f59e0b"

    fms_rows = df_for_csv(fms.tail(12)).to_dict(orient="records")
    gex_rows = df_for_csv(gex.head(20)).to_dict(orient="records")
    cluster_rows = df_for_csv(clusters.head(12)).to_dict(orient="records")

    def table(headers: list[str], rows: list[dict[str, Any]], formatter: dict[str, Any] | None = None) -> str:
        formatter = formatter or {}
        body_rows = []
        for row in rows:
            tds = []
            for header in headers:
                value = row.get(header, "")
                if header in formatter:
                    value = formatter[header](value)
                tds.append(f"<td>{value}</td>")
            body_rows.append("<tr>" + "".join(tds) + "</tr>")
        if not body_rows:
            body_rows.append("<tr><td colspan='{0}'>No data</td></tr>".format(len(headers)))
        head = "".join(f"<th>{h}</th>" for h in headers)
        return f"<table><thead><tr>{head}</tr></thead><tbody>{''.join(body_rows)}</tbody></table>"

    alerts_html = "".join(f"<li>{line}</li>" for line in summary["alerts"])
    html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{summary['symbol']} Flow Signals Dashboard</title>
  <style>
    :root {{
      --bg: #0b1220;
      --panel: #111b2f;
      --line: #22314f;
      --text: #e6edf8;
      --muted: #9fb2d3;
      --green: #16a34a;
      --red: #dc2626;
      --amber: #f59e0b;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      font-family: "Space Grotesk", -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      color: var(--text);
      background:
        radial-gradient(800px 450px at 0% 0%, rgba(22,163,74,0.18), transparent 60%),
        radial-gradient(900px 450px at 100% 100%, rgba(220,38,38,0.16), transparent 65%),
        var(--bg);
      min-height: 100vh;
      padding: 20px;
    }}
    .wrap {{
      max-width: 1200px;
      margin: 0 auto;
      display: grid;
      gap: 14px;
    }}
    .panel {{
      background: linear-gradient(180deg, rgba(17,27,47,0.92), rgba(17,27,47,0.8));
      border: 1px solid var(--line);
      border-radius: 12px;
      padding: 14px;
    }}
    .title {{
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 12px;
      flex-wrap: wrap;
    }}
    .title h1 {{
      margin: 0;
      font-size: 26px;
      line-height: 1.1;
    }}
    .muted {{ color: var(--muted); font-size: 12px; }}
    .kpis {{
      display: grid;
      grid-template-columns: repeat(6, minmax(0, 1fr));
      gap: 10px;
    }}
    .kpi {{
      background: rgba(11,18,32,0.7);
      border: 1px solid var(--line);
      border-radius: 10px;
      padding: 10px;
    }}
    .kpi .k {{ color: var(--muted); font-size: 11px; margin-bottom: 6px; text-transform: uppercase; }}
    .kpi .v {{ font-size: 22px; font-weight: 700; }}
    .score {{
      height: 12px;
      border-radius: 999px;
      background: #1e293b;
      overflow: hidden;
      border: 1px solid var(--line);
      margin-top: 8px;
    }}
    .score > span {{
      display: block;
      height: 100%;
      width: {score_pct:.2f}%;
      background: {score_color};
    }}
    .grid {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 14px;
    }}
    ul {{ margin: 8px 0 0 20px; padding: 0; }}
    li {{ margin-bottom: 6px; }}
    table {{
      width: 100%;
      border-collapse: collapse;
      font-size: 12px;
      margin-top: 10px;
    }}
    th, td {{
      text-align: left;
      padding: 8px;
      border-bottom: 1px solid var(--line);
      white-space: nowrap;
    }}
    th {{ color: var(--muted); font-size: 11px; text-transform: uppercase; }}
    @media (max-width: 980px) {{
      .kpis {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
      .grid {{ grid-template-columns: 1fr; }}
    }}
  </style>
</head>
<body>
  <div class="wrap">
    <section class="panel">
      <div class="title">
        <h1>{summary['symbol']} Flow Signals</h1>
        <div class="muted">Session: {summary['session_date']} | Trades: {summary['total_trades']:,}</div>
      </div>
      <div class="kpis" style="margin-top:12px;">
        <div class="kpi"><div class="k">Composite</div><div class="v">{fmt_number(summary['composite_score'])}</div></div>
        <div class="kpi"><div class="k">Label</div><div class="v" style="font-size:18px">{summary['composite_label']}</div></div>
        <div class="kpi"><div class="k">SMFI</div><div class="v">{fmt_number(summary['smfi'])}</div></div>
        <div class="kpi"><div class="k">DEX</div><div class="v">{fmt_number(summary['dex'], 0)}</div></div>
        <div class="kpi"><div class="k">Put/Call</div><div class="v">{fmt_number(summary['pc_ratio'])}</div></div>
        <div class="kpi"><div class="k">GEX Total</div><div class="v" style="font-size:18px">{fmt_money(summary['gex_total'])}</div></div>
      </div>
      <div class="score"><span></span></div>
      <div class="muted" style="margin-top:8px;">Scale -1 (bearish) to +1 (bullish)</div>
    </section>

    <section class="panel">
      <h3 style="margin:0">Alerts</h3>
      <ul>{alerts_html}</ul>
    </section>

    <section class="grid">
      <div class="panel">
        <h3 style="margin:0">FMS ({summary['window_minutes']}m windows)</h3>
        {table(
            ["window", "bull_prem", "bear_prem", "fms", "delta_fms", "reversal"],
            fms_rows,
            formatter={
                "bull_prem": fmt_money,
                "bear_prem": fmt_money,
                "fms": lambda x: fmt_number(float(x), 3),
                "delta_fms": lambda x: fmt_number(float(x), 3),
            },
        )}
      </div>
      <div class="panel">
        <h3 style="margin:0">Top UAS Clusters</h3>
        {table(
            ["strike", "DTE", "option_type_norm", "direction_label", "uas_sum", "trades", "premium_sum", "mean_vol_oi"],
            cluster_rows,
            formatter={
                "uas_sum": lambda x: fmt_number(float(x), 2),
                "premium_sum": fmt_money,
                "mean_vol_oi": lambda x: fmt_number(float(x), 2),
            },
        )}
      </div>
    </section>

    <section class="panel">
      <h3 style="margin:0">GEX by Strike (Extremes)</h3>
      {table(
          ["strike", "gex"],
          gex_rows,
          formatter={"gex": fmt_money},
      )}
    </section>
  </div>
</body>
</html>
"""
    out_path.write_text(html, encoding="utf-8")


def main() -> int:
    args = parse_args()
    flow_files = [path.expanduser().resolve() for path in args.flow_files]
    flow = load_flow(flow_files)

    smfi_metrics = compute_smfi(flow)
    pc_metrics = compute_put_call_ratio(flow)
    pc_z = compute_pc_zscore(pc_metrics["pc_ratio"], args.pc_history_file)
    dex_metrics = compute_dex(flow)
    fms = compute_fms(flow, args.window_minutes)
    uas_trades, uas_clusters = compute_uas(flow, args.top_n)
    gex_extremes, gex_meta = compute_gex(flow, args.top_n)

    fms_last = float(fms.iloc[-1]["fms"]) if len(fms) else 0.0
    composite = compute_composite(
        smfi_metrics["smfi"],
        dex_metrics["dex_norm"],
        pc_metrics["pc_ratio"],
        fms_last,
    )

    session_date = flow["ts"].dt.date.max().isoformat()
    output_prefix = args.output_prefix or f"{args.symbol.lower()}_flow_signals_{session_date}"
    output_dir = args.output_dir.expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    summary: dict[str, Any] = {
        "symbol": args.symbol.upper(),
        "session_date": session_date,
        "window_minutes": args.window_minutes,
        "flow_files": [str(path) for path in flow_files],
        "total_trades": int(len(flow)),
        "spot_last": float(flow["underlying_price"].iloc[-1]),
        "premium_total": float(flow["premium"].sum()),
        "smfi": float(smfi_metrics["smfi"]),
        "smfi_weight_sum": float(smfi_metrics["weight_sum"]),
        "dex": float(dex_metrics["dex"]),
        "dex_norm": float(dex_metrics["dex_norm"]),
        "pc_ratio": float(pc_metrics["pc_ratio"]) if np.isfinite(pc_metrics["pc_ratio"]) else None,
        "put_premium": float(pc_metrics["put_premium"]),
        "call_premium": float(pc_metrics["call_premium"]),
        "pc_zscore_30d": pc_z["pc_zscore_30d"],
        "pc_hist_mean_30d": pc_z["pc_hist_mean_30d"],
        "pc_hist_std_30d": pc_z["pc_hist_std_30d"],
        "fms_last": fms_last,
        "fms_delta_last": float(fms.iloc[-1]["delta_fms"]) if len(fms) else 0.0,
        "fms_reversal_count": int(fms["reversal"].sum()) if len(fms) else 0,
        "gex_total": float(gex_meta["gex_total"]),
        "gex_regime": gex_meta["gex_regime"],
        "pin_strike": gex_meta["pin_strike"],
        "pin_gex": gex_meta["pin_gex"],
        "composite_score": float(composite["composite_score"]),
        "composite_label": str(composite["composite_label"]),
        "norm_pc_component": float(composite["norm_pc_component"]),
    }
    summary["alerts"] = build_alerts(
        summary["smfi"],
        fms,
        uas_clusters,
        {
            "gex_regime": summary["gex_regime"],
            "pin_strike": summary["pin_strike"],
            "pin_gex": summary["pin_gex"],
        },
    )

    summary_safe = {key: json_safe(value) for key, value in summary.items()}

    summary_path = output_dir / f"{output_prefix}_summary.json"
    fms_path = output_dir / f"{output_prefix}_fms_{args.window_minutes}m.csv"
    gex_path = output_dir / f"{output_prefix}_gex_by_strike.csv"
    uas_trades_path = output_dir / f"{output_prefix}_top_uas_trades.csv"
    uas_clusters_path = output_dir / f"{output_prefix}_top_uas_clusters.csv"
    html_path = output_dir / f"{output_prefix}_dashboard.html"

    summary_path.write_text(
        json.dumps(summary_safe, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    df_for_csv(fms).to_csv(fms_path, index=False)
    df_for_csv(gex_extremes).to_csv(gex_path, index=False)
    df_for_csv(uas_trades).to_csv(uas_trades_path, index=False)
    df_for_csv(uas_clusters).to_csv(uas_clusters_path, index=False)

    if not args.skip_html:
        write_html_dashboard(html_path, summary_safe, fms, gex_extremes, uas_clusters)

    outputs = {
        "summary_json": str(summary_path),
        "fms_csv": str(fms_path),
        "gex_csv": str(gex_path),
        "uas_trades_csv": str(uas_trades_path),
        "uas_clusters_csv": str(uas_clusters_path),
    }
    if not args.skip_html:
        outputs["dashboard_html"] = str(html_path)

    print(json.dumps({"summary": summary_safe, "outputs": outputs}, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
