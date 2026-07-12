#!/usr/bin/env python3
"""Build a today-only IV curves dashboard for selected symbols."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


DEFAULT_SYMBOLS = ("EWZ", "SPY")
SMILE_BINS = [-0.55, -0.45, -0.35, -0.25, -0.15, -0.05, 0.05, 0.15, 0.25, 0.35, 0.45, 0.55]
SMILE_LABELS = ["-0.50", "-0.40", "-0.30", "-0.20", "-0.10", "0.00", "0.10", "0.20", "0.30", "0.40", "0.50"]
TERM_BINS = [0, 7, 14, 30, 45, 60, 90, 120, 180, 270, 365, 730]
TERM_LABELS = ["0-7", "8-14", "15-30", "31-45", "46-60", "61-90", "91-120", "121-180", "181-270", "271-365", "366+"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a today-only IV curves HTML dashboard from processed flow CSV files."
    )
    parser.add_argument("--date-dir", required=True, type=Path, help="Date folder with processed outputs.")
    parser.add_argument("--symbols", nargs="+", default=list(DEFAULT_SYMBOLS), help="Symbols to include.")
    parser.add_argument("--output", type=Path, default=None, help="Optional output HTML path.")
    return parser.parse_args()


def parse_canceled(series: pd.Series) -> pd.Series:
    normalized = series.fillna("").astype(str).str.strip().str.lower()
    return normalized.isin({"true", "t", "1", "yes"})


def weighted_mean(frame: pd.DataFrame, value_col: str, weight_col: str) -> float:
    weights = frame[weight_col].fillna(0.0)
    values = frame[value_col].fillna(0.0)
    total_weight = float(weights.sum())
    if total_weight <= 0:
        return float(values.mean()) if len(values) else 0.0
    return float((values * weights).sum() / total_weight)


def load_symbol_flow(processed_dir: Path, symbol: str) -> tuple[pd.DataFrame, str]:
    pattern = f"flow_true_*_ETF_{symbol}_*_side_FULL_*.csv"
    flow_paths = sorted(processed_dir.glob(pattern))
    if not flow_paths:
        raise FileNotFoundError(f"No processed flow files found for {symbol} in {processed_dir}")

    frames = [pd.read_csv(path) for path in flow_paths]
    flow = pd.concat(frames, ignore_index=True)
    if "canceled" in flow.columns:
        flow = flow[~parse_canceled(flow["canceled"])].copy()

    numeric_cols = [
        "implied_volatility",
        "delta",
        "DTE",
        "premium",
        "size",
        "underlying_price",
        "strike",
    ]
    for col in numeric_cols:
        flow[col] = pd.to_numeric(flow.get(col), errors="coerce")

    flow = flow.dropna(subset=["implied_volatility", "delta", "DTE"]).copy()
    flow["weight"] = pd.to_numeric(flow["premium"], errors="coerce").fillna(0.0)
    zero_weight = flow["weight"] <= 0
    flow.loc[zero_weight, "weight"] = pd.to_numeric(flow.loc[zero_weight, "size"], errors="coerce").fillna(0.0)

    flow["session_date"] = pd.to_datetime(flow["date"], format="%m/%d/%Y", errors="coerce")
    session_date = flow["session_date"].dropna().max()
    if pd.isna(session_date):
        session_label = flow_paths[-1].stem.split("FULL_")[-1]
    else:
        session_label = session_date.strftime("%Y-%m-%d")

    return flow, session_label


def build_smile_curve(flow: pd.DataFrame) -> dict[str, object]:
    near_3m = flow[(flow["DTE"] >= 45) & (flow["DTE"] <= 135)].copy()
    near_3m["delta_bucket"] = pd.cut(
        near_3m["delta"],
        bins=SMILE_BINS,
        labels=SMILE_LABELS,
        include_lowest=True,
    )

    points: list[dict[str, float | int | str]] = []
    for label in SMILE_LABELS:
        bucket = near_3m[near_3m["delta_bucket"] == label]
        if bucket.empty:
            continue
        points.append(
            {
                "label": label,
                "iv": weighted_mean(bucket, "implied_volatility", "weight") * 100.0,
                "count": int(len(bucket)),
            }
        )

    atm = near_3m[(near_3m["delta"].abs() >= 0.45) & (near_3m["delta"].abs() <= 0.55)]
    call_25 = near_3m[(near_3m["delta"] >= 0.20) & (near_3m["delta"] <= 0.30)]
    put_25 = near_3m[(near_3m["delta"] >= -0.30) & (near_3m["delta"] <= -0.20)]

    atm_iv = weighted_mean(atm, "implied_volatility", "weight") * 100.0 if not atm.empty else None
    call_25_iv = weighted_mean(call_25, "implied_volatility", "weight") * 100.0 if not call_25.empty else None
    put_25_iv = weighted_mean(put_25, "implied_volatility", "weight") * 100.0 if not put_25.empty else None

    return {
        "points": points,
        "window_label": "45-135 DTE",
        "sample_size": int(len(near_3m)),
        "atm_iv": atm_iv,
        "call_25_iv": call_25_iv,
        "put_25_iv": put_25_iv,
        "risk_reversal": (call_25_iv - put_25_iv) if call_25_iv is not None and put_25_iv is not None else None,
        "put_skew_ratio": (put_25_iv / atm_iv) if put_25_iv is not None and atm_iv not in (None, 0) else None,
        "call_skew_ratio": (call_25_iv / atm_iv) if call_25_iv is not None and atm_iv not in (None, 0) else None,
    }


def build_term_curve(flow: pd.DataFrame) -> dict[str, object]:
    atmish = flow[(flow["delta"].abs() >= 0.40) & (flow["delta"].abs() <= 0.60)].copy()
    atmish["dte_bucket"] = pd.cut(
        atmish["DTE"],
        bins=TERM_BINS,
        labels=TERM_LABELS,
        include_lowest=True,
        right=True,
    )

    points: list[dict[str, float | int | str]] = []
    for label in TERM_LABELS:
        bucket = atmish[atmish["dte_bucket"] == label]
        if bucket.empty:
            continue
        points.append(
            {
                "label": label,
                "iv": weighted_mean(bucket, "implied_volatility", "weight") * 100.0,
                "count": int(len(bucket)),
            }
        )

    return {
        "points": points,
        "window_label": "|delta| 0.40-0.60",
        "sample_size": int(len(atmish)),
    }


def fmt_metric(value: float | None, suffix: str = "") -> str:
    if value is None:
        return "n/a"
    return f"{value:.2f}{suffix}"


def build_symbol_summary(flow: pd.DataFrame, symbol: str, session_label: str) -> dict[str, object]:
    smile = build_smile_curve(flow)
    term = build_term_curve(flow)
    spot = flow["underlying_price"].dropna()
    return {
        "symbol": symbol,
        "session_label": session_label,
        "n_trades": int(len(flow)),
        "spot": float(spot.iloc[-1]) if len(spot) else None,
        "smile": smile,
        "term": term,
        "kpis": {
            "spot": fmt_metric(float(spot.iloc[-1]), "") if len(spot) else "n/a",
            "atm_iv": fmt_metric(smile["atm_iv"], "%"),
            "put_25_iv": fmt_metric(smile["put_25_iv"], "%"),
            "call_25_iv": fmt_metric(smile["call_25_iv"], "%"),
            "risk_reversal": fmt_metric(smile["risk_reversal"], " pts"),
            "put_skew_ratio": fmt_metric(smile["put_skew_ratio"], "x"),
            "call_skew_ratio": fmt_metric(smile["call_skew_ratio"], "x"),
        },
    }


def build_html(symbol_summaries: list[dict[str, object]], folder_label: str, output_name: str) -> str:
    payload = json.dumps(symbol_summaries, ensure_ascii=False)
    title = "Curvas do Dia | SPY e EWZ"
    return f"""<!doctype html>
<html lang="pt-BR">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{title}</title>
  <style>
    :root {{
      --bg: #0a0d10;
      --panel: #11171d;
      --panel-2: #162029;
      --line: #223140;
      --text: #e8eeec;
      --muted: #94a6ab;
      --accent: #d8bc73;
      --blue: #7aa2c7;
      --red: #c55b6a;
      --green: #85b788;
    }}
    * {{ box-sizing: border-box; }}
    body {{
      margin: 0;
      color: var(--text);
      background:
        radial-gradient(1200px 700px at 0% 0%, rgba(32, 55, 74, 0.45), transparent 55%),
        radial-gradient(900px 500px at 100% 0%, rgba(74, 44, 32, 0.35), transparent 60%),
        linear-gradient(180deg, #080b0d, #0d1116);
      font-family: "Space Grotesk", -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }}
    .wrap {{
      max-width: 1440px;
      margin: 0 auto;
      padding: 24px 16px 36px;
    }}
    .hero {{
      border: 1px solid var(--line);
      border-radius: 18px;
      padding: 20px;
      background: linear-gradient(160deg, rgba(17,23,29,.96), rgba(10,13,16,.96));
      box-shadow: 0 20px 70px rgba(0,0,0,.24);
      margin-bottom: 16px;
    }}
    .eyebrow {{
      color: var(--accent);
      text-transform: uppercase;
      letter-spacing: 0.12em;
      font-size: 11px;
      margin-bottom: 8px;
    }}
    h1 {{
      margin: 0 0 8px;
      font-size: 34px;
      line-height: 1.05;
    }}
    .sub {{
      color: var(--muted);
      font-size: 14px;
      line-height: 1.5;
      max-width: 920px;
    }}
    .grid {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 16px;
      margin-top: 16px;
    }}
    .symbol-card {{
      border: 1px solid var(--line);
      border-radius: 18px;
      background: linear-gradient(180deg, rgba(17,23,29,.96), rgba(11,16,20,.96));
      overflow: hidden;
    }}
    .symbol-head {{
      padding: 18px 18px 8px;
      display: flex;
      justify-content: space-between;
      align-items: flex-end;
      gap: 12px;
      flex-wrap: wrap;
    }}
    .symbol-title {{
      font-size: 28px;
      font-weight: 700;
    }}
    .symbol-meta {{
      color: var(--muted);
      font-size: 13px;
    }}
    .kpis {{
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 10px;
      padding: 0 18px 18px;
    }}
    .kpi {{
      border: 1px solid rgba(255,255,255,.06);
      background: rgba(255,255,255,.03);
      border-radius: 14px;
      padding: 12px;
      min-height: 78px;
    }}
    .k {{
      color: var(--muted);
      font-size: 11px;
      text-transform: uppercase;
      letter-spacing: 0.08em;
    }}
    .v {{
      font-size: 23px;
      font-weight: 700;
      margin-top: 8px;
    }}
    .charts {{
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 16px;
      padding: 0 18px 18px;
    }}
    .panel {{
      border: 1px solid rgba(255,255,255,.05);
      border-radius: 16px;
      background: linear-gradient(180deg, rgba(22,32,41,.92), rgba(12,18,24,.92));
      padding: 14px;
    }}
    .panel-title {{
      font-size: 14px;
      font-weight: 700;
      margin-bottom: 4px;
    }}
    .panel-sub {{
      color: var(--muted);
      font-size: 12px;
      margin-bottom: 10px;
    }}
    .canvas-wrap {{
      height: 280px;
    }}
    .foot {{
      color: var(--muted);
      font-size: 12px;
      margin-top: 16px;
    }}
    @media (max-width: 1080px) {{
      .grid, .charts {{ grid-template-columns: 1fr; }}
      .kpis {{ grid-template-columns: 1fr 1fr; }}
    }}
    @media (max-width: 640px) {{
      h1 {{ font-size: 28px; }}
      .kpis {{ grid-template-columns: 1fr; }}
      .symbol-title {{ font-size: 24px; }}
    }}
  </style>
</head>
<body>
  <main class="wrap">
    <section class="hero">
      <div class="eyebrow">Today-Only Curves</div>
      <h1>SPY e EWZ | Curvas do dia</h1>
      <div class="sub">
        Painel local com smile/skew e term structure usando apenas a sessão mais recente disponível nos arquivos processados.
        Pasta analisada: <strong>{folder_label}</strong>. Arquivo gerado: <strong>{output_name}</strong>.
      </div>
    </section>
    <section class="grid" id="cards"></section>
    <div class="foot">
      Smile/skew usa proxy de 45-135 DTE. Term structure usa opções ATM-ish com |delta| entre 0.40 e 0.60.
      Como a base vem de fluxo negociado, isso é uma leitura de tape/prints, não uma surface oficial de fechamento.
    </div>
  </main>
  <script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
  <script>
    const cards = {payload};
    const root = document.getElementById("cards");

    function chartConfig(titleColor, labels, values, pointCounts) {{
      return {{
        type: "line",
        data: {{
          labels,
          datasets: [{{
            data: values,
            borderColor: titleColor,
            backgroundColor: titleColor,
            borderWidth: 2.5,
            tension: 0.25,
            pointRadius: 3,
            pointHoverRadius: 5
          }}]
        }},
        options: {{
          responsive: true,
          maintainAspectRatio: false,
          plugins: {{
            legend: {{ display: false }},
            tooltip: {{
              callbacks: {{
                label(ctx) {{
                  const count = pointCounts[ctx.dataIndex];
                  return `IV ${{ctx.parsed.y.toFixed(2)}}% | prints ${{count}}`;
                }}
              }}
            }}
          }},
          scales: {{
            x: {{
              ticks: {{ color: "#94a6ab" }},
              grid: {{ color: "rgba(255,255,255,0.04)" }}
            }},
            y: {{
              ticks: {{
                color: "#94a6ab",
                callback(value) {{ return value.toFixed(0) + "%"; }}
              }},
              grid: {{ color: "rgba(255,255,255,0.05)" }}
            }}
          }}
        }}
      }};
    }}

    function renderCard(card, index) {{
      const smileLabels = card.smile.points.map(point => point.label);
      const smileValues = card.smile.points.map(point => point.iv);
      const smileCounts = card.smile.points.map(point => point.count);
      const termLabels = card.term.points.map(point => point.label);
      const termValues = card.term.points.map(point => point.iv);
      const termCounts = card.term.points.map(point => point.count);
      const accent = card.symbol === "SPY" ? "#7aa2c7" : "#c55b6a";
      const accent2 = card.symbol === "SPY" ? "#85b788" : "#d8bc73";

      const node = document.createElement("article");
      node.className = "symbol-card";
      node.innerHTML = `
        <div class="symbol-head">
          <div>
            <div class="symbol-title">${{card.symbol}}</div>
            <div class="symbol-meta">Sessão ${{card.session_label}} | ${{card.n_trades.toLocaleString("pt-BR")}} prints válidos</div>
          </div>
          <div class="symbol-meta">Spot ${{card.kpis.spot}}</div>
        </div>
        <div class="kpis">
          <div class="kpi"><div class="k">ATM IV</div><div class="v">${{card.kpis.atm_iv}}</div></div>
          <div class="kpi"><div class="k">25d Put IV</div><div class="v">${{card.kpis.put_25_iv}}</div></div>
          <div class="kpi"><div class="k">25d Call IV</div><div class="v">${{card.kpis.call_25_iv}}</div></div>
          <div class="kpi"><div class="k">Risk Reversal</div><div class="v">${{card.kpis.risk_reversal}}</div></div>
        </div>
        <div class="charts">
          <div class="panel">
            <div class="panel-title">Smile / Skew</div>
            <div class="panel-sub">${{card.smile.window_label}} | amostra ${{card.smile.sample_size.toLocaleString("pt-BR")}}</div>
            <div class="canvas-wrap"><canvas id="smile-${{index}}"></canvas></div>
          </div>
          <div class="panel">
            <div class="panel-title">ATM Term Structure</div>
            <div class="panel-sub">${{card.term.window_label}} | amostra ${{card.term.sample_size.toLocaleString("pt-BR")}}</div>
            <div class="canvas-wrap"><canvas id="term-${{index}}"></canvas></div>
          </div>
        </div>
      `;

      root.appendChild(node);
      new Chart(document.getElementById(`smile-${{index}}`), chartConfig(accent, smileLabels, smileValues, smileCounts));
      new Chart(document.getElementById(`term-${{index}}`), chartConfig(accent2, termLabels, termValues, termCounts));
    }}

    cards.forEach(renderCard);
  </script>
</body>
</html>
"""


def main() -> int:
    args = parse_args()
    date_dir = args.date_dir.expanduser().resolve()
    processed_dir = date_dir / "processed"
    if not processed_dir.exists():
        raise FileNotFoundError(f"Processed directory not found: {processed_dir}")

    symbol_summaries: list[dict[str, object]] = []
    session_labels: set[str] = set()
    for symbol in [item.upper().strip() for item in args.symbols]:
        flow, session_label = load_symbol_flow(processed_dir, symbol)
        symbol_summaries.append(build_symbol_summary(flow, symbol, session_label))
        session_labels.add(session_label)

    default_session = sorted(session_labels)[-1] if session_labels else date_dir.name
    output_path = args.output
    if output_path is None:
        output_path = processed_dir / f"today_iv_curves_{default_session}.html"
    else:
        output_path = output_path.expanduser().resolve()

    output_path.write_text(
        build_html(symbol_summaries, date_dir.name, output_path.name),
        encoding="utf-8",
    )

    print(f"Generated HTML: {output_path}")
    for card in symbol_summaries:
        print(
            f"{card['symbol']} | session {card['session_label']} | "
            f"ATM IV {card['kpis']['atm_iv']} | "
            f"25d put {card['kpis']['put_25_iv']} | "
            f"25d call {card['kpis']['call_25_iv']}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
