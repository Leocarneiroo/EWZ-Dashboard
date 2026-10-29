#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
================================================================================
 BACKTEST — EWZ x Market Breadth (SMA 20 / 50 / 200)
================================================================================
Regra do James:
    COMPRA  EWZ quando o breadth de SMA 20, 50 e 200 caem ABAIXO de 20
    VENDE   EWZ quando o breadth de SMA 20, 50 e 200 sobem ACIMA de 80

O sinal de breadth NÃO vem do EWZ — vem da cesta de 40 ações da B3 do seu
indicador TradingView ("Market Breadth EWZ - 20/50/200"). Por isso este script
consome o CSV exportado do próprio TradingView, que já contém:
    - o preço do EWZ (série principal do gráfico), e
    - as três linhas de breadth (Breadth 20 / Breadth 50 / Breadth 200).

--------------------------------------------------------------------------------
COMO GERAR O CSV NO TRADINGVIEW (1 arquivo, tudo junto)
--------------------------------------------------------------------------------
 1. Abra o gráfico do EWZ (idealmente em Diário para um backtest desde 2017;
    o indicador usa tf="60" por padrão, mas 60m não tem histórico até 2017).
 2. Aplique o indicador "Market Breadth EWZ - 20/50/200" nesse gráfico.
 3. Menu (⋮ / seta de exportar no topo do gráfico) -> "Exportar dados do gráfico".
 4. Marque para incluir os plots dos indicadores. Exporte em CSV (ISO time).
 5. Faça upload do CSV aqui — ou rode este script localmente:
        python backtest_breadth_ewz.py meu_export.csv

Alternativa (2 arquivos): se preferir, exporte o EWZ e o breadth separados e
passe os dois:  python backtest_breadth_ewz.py ewz.csv breadth.csv
--------------------------------------------------------------------------------

Dependências: pandas, numpy, matplotlib  (openpyxl é opcional, p/ saída .xlsx)
    pip install pandas numpy matplotlib openpyxl
================================================================================
"""

from __future__ import annotations
import sys
import re
import argparse
import numpy as np
import pandas as pd

# =============================================================================
# PARÂMETROS (ajuste aqui ou via linha de comando)
# =============================================================================
CFG = dict(
    entry_threshold = 20.0,   # compra quando breadth < este valor
    exit_threshold  = 80.0,   # vende quando breadth > este valor
    lines           = [20, 50, 200],  # quais breadths usam a regra. Ex.: [20] = igual ao bgcolor do indicador
    mode            = "all",  # "all" = TODOS os breadths precisam cruzar; "any" = QUALQUER um
    execution       = "next_close",  # "next_close" (sem look-ahead) ou "same_close"
    cost_bps        = 5.0,    # custo por perna (ida/volta), em basis points. 5 = 0,05%
    rf_annual       = 0.0,    # taxa livre de risco anual p/ Sharpe (deixe 0 ou use CDI/SELIC se quiser)
    periods_per_year= None,   # None = inferido pela mediana do espaçamento das datas
)

# =============================================================================
# LEITURA / DETECÇÃO DE COLUNAS
# =============================================================================
def _read_csv_flex(path: str) -> pd.DataFrame:
    """Lê CSV do TradingView tolerando separador ',' ou ';' e decimal ',' ou '.'."""
    for sep in (",", ";", "\t"):
        try:
            df = pd.read_csv(path, sep=sep)
            if df.shape[1] >= 2:
                return df
        except Exception:
            pass
    return pd.read_csv(path)


def _find_col(cols, *patterns):
    """Acha a primeira coluna cujo nome casa (case-insensitive) com algum padrão."""
    low = {c: str(c).lower() for c in cols}
    for pat in patterns:
        rx = re.compile(pat, re.I)
        for c in cols:
            if rx.search(low[c]):
                return c
    return None


def _to_datetime(s: pd.Series) -> pd.Series:
    # TradingView costuma exportar epoch (segundos) ou ISO. Tenta ambos.
    if pd.api.types.is_numeric_dtype(s):
        # heurística: > 10^11 provavelmente é milissegundos
        unit = "ms" if float(pd.to_numeric(s, errors="coerce").dropna().median()) > 1e11 else "s"
        return pd.to_datetime(s, unit=unit, utc=True).dt.tz_localize(None)
    dt = pd.to_datetime(s, utc=True, errors="coerce")
    return dt.dt.tz_localize(None)


def _to_float(s: pd.Series) -> pd.Series:
    if s.dtype == object:
        s = s.astype(str).str.strip()
        comma = s.str.contains(",", regex=False)
        both = comma & s.str.contains(".", regex=False)
        s = s.where(~both, s.str.replace(".", "", regex=False))
        s = s.str.replace(",", ".", regex=False)
    return pd.to_numeric(s, errors="coerce")


def load_stock_screener_breadth20(directory: str) -> pd.DataFrame:
    """Aproxima b20 usando closes dos snapshots stock-screener disponíveis."""
    from pathlib import Path

    files = sorted(Path(directory).glob("20??-??-??/stock-screener-*.csv"))
    if not files:
        files = sorted(Path(directory).glob("stock-screener-*.csv"))
    if not files:
        raise SystemExit("ERRO: nenhum stock-screener-*.csv encontrado.")

    frames = []
    for path in files:
        x = pd.read_csv(path, usecols=["date", "ticker", "close"])
        x["ticker"] = x["ticker"].astype(str).str.upper()
        x["close"] = _to_float(x["close"])
        frames.append(x.dropna(subset=["ticker", "close"]))
    common = set.intersection(*(set(x["ticker"]) for x in frames))
    if "EWZ" not in common:
        raise SystemExit("ERRO: EWZ não aparece em todos os snapshots.")

    prices = pd.concat([x[x["ticker"].isin(common)] for x in frames], ignore_index=True)
    prices["date"] = pd.to_datetime(prices["date"], errors="coerce")
    pivot = prices.pivot_table(index="date", columns="ticker", values="close", aggfunc="last").sort_index()
    sma20 = pivot.rolling(20, min_periods=20).mean()
    valid = sma20.notna().all(axis=1)
    b20 = (pivot > sma20).mean(axis=1).mul(100).where(valid).rename("b20")
    out = pd.DataFrame({"price": pivot["EWZ"], "b20": b20}).dropna(subset=["price"])
    print(f"[aproximação] {len(common)} ativos comuns; {len(out)} datas; b20 válido em {out['b20'].notna().sum()} datas")
    return out


def load_data(price_path: str, breadth_path: str | None = None,
              required_lines=(20, 50, 200)) -> pd.DataFrame:
    """
    Retorna DataFrame indexado por data com colunas:
        price, b20, b50, b200
    Aceita 1 arquivo (tudo junto) ou 2 (preço + breadth), fazendo merge por data.
    """
    df = _read_csv_flex(price_path)
    cols = list(df.columns)

    tcol = _find_col(cols, r"^time$", r"^date", r"data", r"datetime", r"timestamp")
    if tcol is None:
        tcol = cols[0]
    date = _to_datetime(df[tcol])

    # preço do EWZ: 'close'/'Close'; se não houver, tenta uma coluna 'EWZ' ou a 1ª numérica plausível
    pcol = _find_col(cols, r"^close$", r"\bclose\b", r"\bewz\b", r"pre[cç]o", r"\bprice\b")
    if pcol is None:
        num = [c for c in cols if c != tcol and _to_float(df[c]).notna().mean() > 0.8]
        pcol = num[0] if num else None
    price = _to_float(df[pcol]) if pcol else pd.Series(np.nan, index=df.index)

    # breadths no mesmo arquivo?
    b20c  = _find_col(cols, r"breadth.*20\b", r"\b20\b.*breadth", r"^b20", r"20/50/200.*20")
    b50c  = _find_col(cols, r"breadth.*50\b", r"\b50\b.*breadth", r"^b50")
    b200c = _find_col(cols, r"breadth.*200\b", r"\b200\b.*breadth", r"^b200")

    out = pd.DataFrame({"date": date, "price": price})
    if b20c:  out["b20"]  = _to_float(df[b20c])
    if b50c:  out["b50"]  = _to_float(df[b50c])
    if b200c: out["b200"] = _to_float(df[b200c])

    # segundo arquivo com o breadth?
    if breadth_path and not {"b20", "b50", "b200"}.issubset(out.columns):
        bdf = _read_csv_flex(breadth_path)
        bcols = list(bdf.columns)
        btc = _find_col(bcols, r"^time$", r"^date", r"data", r"datetime") or bcols[0]
        bdate = _to_datetime(bdf[btc])
        b = pd.DataFrame({"date": bdate})
        m20  = _find_col(bcols, r"20\b");  m50 = _find_col(bcols, r"50\b"); m200 = _find_col(bcols, r"200\b")
        if m20:  b["b20"]  = _to_float(bdf[m20])
        if m50:  b["b50"]  = _to_float(bdf[m50])
        if m200: b["b200"] = _to_float(bdf[m200])
        out = out.drop(columns=[c for c in ("b20","b50","b200") if c in out], errors="ignore")
        out = pd.merge_asof(out.sort_values("date"), b.sort_values("date"), on="date", direction="backward",
                            tolerance=pd.Timedelta("1D"))

    out = out.dropna(subset=["date"]).drop_duplicates("date").set_index("date").sort_index()

    missing = [f"b{line}" for line in required_lines if f"b{line}" not in out.columns]
    if missing:
        raise SystemExit(
            "ERRO: não encontrei as colunas de breadth {} no(s) arquivo(s).\n"
            "      Exporte do TradingView com os plots do indicador incluídos, "
            "ou passe um 2º CSV com as três linhas de breadth.".format(missing)
        )
    return out


# =============================================================================
# GERAÇÃO DE SINAL + BACKTEST (long/flat, sem look-ahead)
# =============================================================================
def run_backtest(data: pd.DataFrame, cfg: dict) -> dict:
    lines = cfg["lines"]
    colmap = {20: "b20", 50: "b50", 200: "b200"}
    use = [colmap[l] for l in lines]

    B = data[use]
    below = B < cfg["entry_threshold"]
    above = B > cfg["exit_threshold"]

    if cfg["mode"] == "all":
        entry_sig = below.all(axis=1)
        exit_sig  = above.all(axis=1)
    else:  # "any"
        entry_sig = below.any(axis=1)
        exit_sig  = above.any(axis=1)

    # barras sem breadth completo não geram sinal
    valid = B.notna().all(axis=1)
    entry_sig &= valid
    exit_sig  &= valid

    # máquina de estados: flat -> long na entrada; long -> flat na saída
    n = len(data)
    pos = np.zeros(n, dtype=float)   # posição vigente NAQUELA barra
    state = 0
    for i in range(n):
        if state == 0 and entry_sig.iloc[i]:
            state = 1
        elif state == 1 and exit_sig.iloc[i]:
            state = 0
        pos[i] = state
    data = data.copy()
    data["signal_pos"] = pos

    # execução: no próximo fechamento (sem look-ahead) ou no mesmo fechamento
    exec_pos = pd.Series(pos, index=data.index)
    if cfg["execution"] == "next_close":
        exec_pos = exec_pos.shift(1).fillna(0.0)
    data["position"] = exec_pos

    # retornos
    ret = data["price"].pct_change().fillna(0.0)
    trades_change = data["position"].diff().abs().fillna(data["position"].abs())
    cost = trades_change * (cfg["cost_bps"] / 1e4)   # custo aplicado quando muda a posição
    strat_ret = data["position"] * ret - cost
    data["ret_asset"] = ret
    data["ret_strat"] = strat_ret
    data["equity_strat"] = (1 + strat_ret).cumprod()
    data["equity_bh"]    = (1 + ret).cumprod()

    # ------- log de trades -------
    trades = []
    entering = (data["position"] > 0) & (data["position"].shift(1).fillna(0) == 0)
    exiting  = (data["position"] == 0) & (data["position"].shift(1).fillna(0) > 0)
    entry_dates = list(data.index[entering])
    exit_dates  = list(data.index[exiting])
    for k, ed in enumerate(entry_dates):
        xd = exit_dates[k] if k < len(exit_dates) else data.index[-1]
        seg = data.loc[ed:xd]
        pe = data["price"].loc[ed]
        px = data["price"].loc[xd]
        tr = px / pe - 1
        trades.append(dict(
            entrada=ed.date(), saida=xd.date(),
            preco_entrada=round(float(pe), 4), preco_saida=round(float(px), 4),
            retorno_pct=round(float(tr) * 100, 2),
            dias=int((xd - ed).days),
            aberto=(k >= len(exit_dates)),
        ))
    trades_df = pd.DataFrame(trades)

    return dict(data=data, trades=trades_df,
                entry_sig=entry_sig, exit_sig=exit_sig)


# =============================================================================
# MÉTRICAS
# =============================================================================
def _infer_ppy(idx: pd.DatetimeIndex) -> float:
    if len(idx) < 3:
        return 252.0
    med = np.median(np.diff(idx.values).astype("timedelta64[s]").astype(float))
    day = 86400.0
    if med >= 0.7 * day:           # diário
        return 252.0
    return (6.5 * 3600.0 / med) * 252.0  # intraday (pregão ~6.5h)


def metrics(res: dict, cfg: dict) -> pd.DataFrame:
    d = res["data"]
    ppy = cfg["periods_per_year"] or _infer_ppy(d.index)
    out = {}

    for name, r, eq in [("Estratégia", d["ret_strat"], d["equity_strat"]),
                        ("Buy & Hold", d["ret_asset"], d["equity_bh"])]:
        total = eq.iloc[-1] - 1
        yrs = (d.index[-1] - d.index[0]).days / 365.25
        cagr = eq.iloc[-1] ** (1 / yrs) - 1 if yrs > 0 else np.nan
        vol = r.std() * np.sqrt(ppy)
        rf_per = (1 + cfg["rf_annual"]) ** (1 / ppy) - 1
        excess = r - rf_per
        sharpe = excess.mean() / r.std() * np.sqrt(ppy) if r.std() > 0 else np.nan
        downside = r[r < 0].std()
        sortino = excess.mean() / downside * np.sqrt(ppy) if downside > 0 else np.nan
        dd = eq / eq.cummax() - 1
        maxdd = dd.min()
        out[name] = dict(
            retorno_total_pct = round(total * 100, 1),
            cagr_pct = round(cagr * 100, 2),
            vol_anual_pct = round(vol * 100, 2),
            sharpe = round(sharpe, 3),
            sortino = round(sortino, 3),
            max_drawdown_pct = round(maxdd * 100, 1),
            calmar = round((cagr / abs(maxdd)), 3) if maxdd < 0 else np.nan,
        )

    # métricas específicas da estratégia
    expo = (d["position"] > 0).mean()
    t = res["trades"]
    if len(t):
        wins = t["retorno_pct"] > 0
        out["Estratégia"].update(
            exposicao_pct = round(expo * 100, 1),
            n_trades = int(len(t)),
            win_rate_pct = round(wins.mean() * 100, 1),
            retorno_medio_trade_pct = round(t["retorno_pct"].mean(), 2),
            melhor_trade_pct = round(t["retorno_pct"].max(), 2),
            pior_trade_pct = round(t["retorno_pct"].min(), 2),
            dias_medios_trade = round(t["dias"].mean(), 1),
            profit_factor = round(
                t.loc[wins, "retorno_pct"].sum() / abs(t.loc[~wins, "retorno_pct"].sum()), 2
            ) if (~wins).any() and t.loc[~wins, "retorno_pct"].sum() != 0 else np.nan,
        )
    else:
        out["Estratégia"].update(exposicao_pct=round(expo*100,1), n_trades=0)

    return pd.DataFrame(out).T


# =============================================================================
# SAÍDAS (relatório, gráfico, arquivos)
# =============================================================================
def make_outputs(res: dict, mtx: pd.DataFrame, cfg: dict, prefix: str = "backtest_ewz"):
    d = res["data"]; t = res["trades"]

    print("\n" + "=" * 74)
    print(" BACKTEST — EWZ x Breadth 20/50/200")
    print("=" * 74)
    print(f" Período      : {d.index[0].date()}  ->  {d.index[-1].date()}  ({len(d)} barras)")
    print(f" Regra        : COMPRA breadth {cfg['lines']} < {cfg['entry_threshold']:.0f}  ({cfg['mode']})")
    print(f"                VENDE  breadth {cfg['lines']} > {cfg['exit_threshold']:.0f}  ({cfg['mode']})")
    print(f" Execução     : {cfg['execution']}   |   custo {cfg['cost_bps']:.1f} bps/perna")
    print("-" * 74)
    with pd.option_context("display.width", 120, "display.max_columns", 20):
        print(mtx.to_string())
    print("-" * 74)
    if len(t):
        print(f" {len(t)} trades:")
        with pd.option_context("display.width", 120, "display.max_rows", 200):
            print(t.to_string(index=False))
    else:
        print(" >> NENHUM trade gerado. Com 'all' + linha 200, os extremos <20 e >80")
        print("    do breadth de 200 SMA quase nunca ocorrem juntos. Tente mode='any',")
        print("    lines=[20] (igual ao bgcolor do indicador) ou afrouxe os limiares.")
    print("=" * 74 + "\n")

    # trade log
    if len(t):
        try:
            t.to_excel(f"{prefix}_trades.xlsx", index=False)
            print(f"[ok] trades -> {prefix}_trades.xlsx")
        except Exception:
            t.to_csv(f"{prefix}_trades.csv", index=False)
            print(f"[ok] trades -> {prefix}_trades.csv")

    # gráfico
    try:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(13, 8), sharex=True,
                                       gridspec_kw={"height_ratios": [2, 1]})
        ax1.plot(d.index, d["equity_strat"], lw=1.8, label="Estratégia")
        ax1.plot(d.index, d["equity_bh"], lw=1.2, alpha=.7, label="Buy & Hold EWZ")
        inpos = d["position"] > 0
        ax1.fill_between(d.index, 0, 1, where=inpos, transform=ax1.get_xaxis_transform(),
                         color="tab:green", alpha=.08, label="comprado")
        ax1.set_yscale("log"); ax1.set_ylabel("Equity (log, base 1)")
        ax1.legend(loc="upper left"); ax1.set_title("EWZ x Breadth — curva de capital")
        ax1.grid(alpha=.3)
        for c, lab, col in [("b20","Breadth 20","tab:blue"),
                            ("b50","Breadth 50","tab:orange"),
                            ("b200","Breadth 200","tab:red")]:
            if c in d: ax2.plot(d.index, d[c], lw=1, label=lab, color=col)
        ax2.axhline(cfg["entry_threshold"], ls="--", color="green", alpha=.6)
        ax2.axhline(cfg["exit_threshold"], ls="--", color="red", alpha=.6)
        ax2.set_ylabel("% breadth"); ax2.legend(loc="upper left", ncol=3, fontsize=8)
        ax2.grid(alpha=.3)
        fig.tight_layout()
        fig.savefig(f"{prefix}_equity.png", dpi=130)
        print(f"[ok] gráfico -> {prefix}_equity.png")
    except Exception as e:
        print(f"[aviso] gráfico não gerado: {e}")


# =============================================================================
# MAIN
# =============================================================================
def main():
    ap = argparse.ArgumentParser(description="Backtest EWZ x Breadth 20/50/200")
    ap.add_argument("price_csv", nargs="?", help="CSV do TradingView (EWZ + breadths)")
    ap.add_argument("breadth_csv", nargs="?", default=None, help="(opcional) CSV só com os breadths")
    ap.add_argument("--stock-screener-dir", help="calcula b20 aproximado a partir dos snapshots")
    ap.add_argument("--entry", type=float, default=CFG["entry_threshold"])
    ap.add_argument("--exit", type=float, default=CFG["exit_threshold"])
    ap.add_argument("--lines", default=",".join(map(str, CFG["lines"])),
                    help="ex.: 20,50,200  ou  20")
    ap.add_argument("--mode", choices=["all", "any"], default=CFG["mode"])
    ap.add_argument("--exec", dest="execution", choices=["next_close", "same_close"],
                    default=CFG["execution"])
    ap.add_argument("--cost", type=float, default=CFG["cost_bps"], help="bps por perna")
    args = ap.parse_args()

    cfg = dict(CFG)
    cfg.update(entry_threshold=args.entry, exit_threshold=args.exit,
               lines=[int(x) for x in str(args.lines).split(",") if x.strip()],
               mode=args.mode, execution=args.execution, cost_bps=args.cost)

    if args.stock_screener_dir:
        data = load_stock_screener_breadth20(args.stock_screener_dir)
    elif args.price_csv:
        data = load_data(args.price_csv, args.breadth_csv, required_lines=cfg["lines"])
    else:
        ap.error("informe price_csv ou --stock-screener-dir")
    res = run_backtest(data, cfg)
    mtx = metrics(res, cfg)
    make_outputs(res, mtx, cfg)


if __name__ == "__main__":
    main()
