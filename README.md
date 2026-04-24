# EWZ Dashboard

Dashboard diário de fluxo/opções (EWZ + SPY) com painel de OI-CHANGE e publicação no GitHub Pages.

## Atualização diária

Rode:

```bash
python3 update_daily_and_publish.py --date-dir 2026-04-23
```

Esse comando:

1. Processa os ZIPs do dia (`bot`, `dp`, `chain-oi-changes`).
2. Gera os CSVs por símbolo (`EWZ` e `SPY`) em `YYYY-MM-DD/processed`.
3. Gera dashboards de `EWZ` e `SPY` com painel de OI-CHANGE.
4. Publica um `docs/index.html` com seletor de símbolo (`EWZ/SPY`), versões latest em `docs/latest` e histórico em `docs/history`.

Depois faça push:

```bash
git add .
git commit -m "update dashboard 2026-04-23"
git push origin main
```

No push, o workflow `.github/workflows/pages.yml` faz o deploy no GitHub Pages.
