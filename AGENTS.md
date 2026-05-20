# AGENTS.md — EWZ Dashboard

> Instruções para agentes de código que operam neste repositório.
> Leia este arquivo **antes** de executar qualquer comando ou modificar código.

---

## 1. Contexto do projeto

Pipeline de processamento diário de dados de opções (fluxo, OI, dark pool) para os tickers **EWZ** e **SPY**. Gera dashboards HTML interativos e publica em **GitHub Pages** via pasta `docs/`.

- **Repo:** git, branch `main`
- **Dados de entrada:** arquivos diários em pastas `YYYY-MM-DD/`
- **Saída principal:** `docs/index.html` + `docs/latest/*.html` + `docs/history/*.html`
- **Site publicado:** GitHub Pages (source branch `main`, folder `/docs`)

---

## 2. Estrutura de diretórios

```
EWZ Dashboard/
├── YYYY-MM-DD/                    <- pasta do dia com arquivos brutos
│   ├── bot-eod-report-*.zip/csv
│   ├── dp-eod-report-*.zip/csv
│   ├── chain-oi-changes-*.zip/csv
│   └── processed/                 <- gerado automaticamente
│       ├── flow_true_..._ask/bid_side_FULL_....csv
│       ├── chain-oi-changes-{SYM}-{DATA}.csv
│       ├── dp-eod-report-{SYM}-{DATA}.csv
│       ├── {SYM}_delta_volume_dashboard_{DATA}.html
│       └── today_iv_curves_{DATA}.html   (se rodar script extra)
├── docs/                          <- GitHub Pages (NÃO edite manualmente)
│   ├── index.html
│   ├── latest.json
│   ├── latest/{SYM}.html
│   └── history/{SYM}_delta_volume_dashboard_{DATA}.html
├── ewz_runner.py                  <- SCRIPT PRINCIPAL (use este)
├── update_daily_and_publish.py    <- alternativa; chama subprocessos
├── publish_github_pages.py        <- apenas publica docs/ (sem processar)
├── process_daily_reports.py       <- extração e filtros de CSV/ZIP
├── ewz_delta_dashboard.py         <- gera o HTML do dashboard
├── build_today_iv_curves.py       <- gera painel extra de IV smile/term
└── build_*.py                     <- helpers diversos
```

---

## 3. Arquivos de entrada obrigatórios

Para cada dia, a pasta `YYYY-MM-DD/` deve conter **pelo menos um** dos formatos para cada um dos 3 stems:

| Stem               | Conteúdo                          | Formatos aceitos |
|--------------------|-----------------------------------|------------------|
| `bot-eod-report`   | Fluxo de opções (prints)          | `.zip` ou `.csv` |
| `dp-eod-report`    | Dark pool end-of-day              | `.zip` ou `.csv` |
| `chain-oi-changes` | Mudanças de open interest         | `.zip` ou `.csv` |

Exemplos válidos:
- `bot-eod-report-2026-05-19.zip`
- `dp-eod-report-2026-05-19.csv`
- `chain-oi-changes-2026-05-19.zip`

> O script `ewz_runner.py` prefere `.zip` (extrai CSV interno automaticamente) e faz fallback para `.csv`.

---

## 4. Pipeline padrão (quando o usuário pedir "rodar para o dia X")

### Passo 1 — Verifique se a pasta do dia existe
```bash
ls YYYY-MM-DD
```

### Passo 2 — Rode o script principal
Sempre use **`ewz_runner.py`**. Ele faz tudo: processamento + geração de dashboards + publicação em `docs/`.

```bash
python3 ewz_runner.py --date-dir YYYY-MM-DD
```

Flags úteis:
- `--date-dir YYYY-MM-DD` : obrigatório se quiser data específica
- `--no-publish` : processa mas NÃO atualiza `docs/` (use apenas se pedido)
- `--symbols EWZ SPY` : padrão já é EWZ + SPY

### Passo 3 — Verifique saída
Esperado no terminal:
```
EWZ runner completed.
Date: YYYY-MM-DD
...
Docs published in docs/.
```

### Passo 4 — Commit e push (se o usuário pedir para atualizar GitHub Pages)
Se `docs/` foi alterado, publique no GitHub Pages:

```bash
git add docs/
git commit -m "docs: update GitHub Pages dashboards for YYYY-MM-DD"
git push origin main
```

> Só faça commit/push se o usuário explicitamente pedir para atualizar o GitHub Pages.

---

## 5. Scripts auxiliares (uso opcional)

### `build_today_iv_curves.py`
Gera um HTML extra com **smile de volatilidade e term structure** (curvas do dia). Não é chamado automaticamente pelo runner.

```bash
python3 build_today_iv_curves.py --date-dir YYYY-MM-DD
```

Saída: `YYYY-MM-DD/processed/today_iv_curves_YYYY-MM-DD.html`

### `publish_github_pages.py`
Só copia dashboards já gerados para `docs/` e recria `index.html`. Use apenas se os HTMLs já existirem e você só precisar republicar.

```bash
python3 publish_github_pages.py --date YYYY-MM-DD --dashboard EWZ=/caminho/para/EWZ.html --dashboard SPY=/caminho/para/SPY.html
```

### `update_daily_and_publish.py`
Alternativa antiga ao `ewz_runner.py`. Não é necessário usar a menos que haja uma razão específica documentada no futuro.

---

## 6. Dependências

O ambiente deve ter:
- **Python 3**
- **pandas** (usado por `ewz_delta_dashboard.py` e `build_today_iv_curves.py`)
- **git**

Se algum script falhar com `ModuleNotFoundError: pandas`, instale:
```bash
pip install pandas
```

---

## 7. Convenções de Git

- **Branch padrão:** `main`
- **Não faça force-push, rebase ou amend** a menos que explicitamente autorizado.
- **Mensagens de commit para docs:** `docs: update GitHub Pages dashboards for YYYY-MM-DD`
- **Arquivos a commitar:** apenas o que foi alterado em `docs/` (ignore `__pycache__/`, `.DS_Store`, etc.)

---

## 8. Erros comuns e ações

| Sintoma | Causa provável | Ação |
|---------|----------------|------|
| `Missing input for bot-eod-report` | Faltam arquivos na pasta do dia | Verifique se os 3 stems existem em `YYYY-MM-DD/` |
| `Date directory not found` | Pasta com data errada ou inexistente | Confirme o nome exato (`ls`) |
| `ModuleNotFoundError: pandas` | Dependência não instalada | `pip install pandas` |
| `docs/` não atualiza no site | Esqueceu do push ou GitHub Pages demora | Verifique `git status`, faça push e aguarde 1-2 min |

---

## 9. Checklist rápido (copiar antes de rodar)

- [ ] Pasta `YYYY-MM-DD/` existe no root do repo
- [ ] Arquivos `bot-eod-report`, `dp-eod-report`, `chain-oi-changes` estão presentes (ZIP ou CSV)
- [ ] Comando: `python3 ewz_runner.py --date-dir YYYY-MM-DD`
- [ ] Terminal mostra `Docs published in docs/`
- [ ] Se pedido pelo usuário: `git add docs/ && git commit -m "..." && git push origin main`

---

## 10. Notas para agentes futuros

- **NUNCA** edite arquivos em `docs/` manualmente. Sempre use `publish_github_pages.py` ou `ewz_runner.py`.
- **NUNCA** remova pastas `docs/history/` antigas; o histórico é intencional.
- Se o usuário disser apenas "rode para hoje" sem especificar data, use a pasta de hoje (`$(date +%Y-%m-%d)`). Se não existir, `ewz_runner.py` sozinho já faz fallback para a pasta mais recente.
- Se o usuário pedir "atualizar GitHub Pages" sem mencionar processar dados, verifique se `docs/` já está atualizado antes de commitar.
