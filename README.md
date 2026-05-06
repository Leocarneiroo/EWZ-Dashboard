# EWZ Dashboard

Guia simples para **quem nunca programou** baixar este projeto e rodar na própria máquina.

## O que este projeto faz

Este projeto pega arquivos brutos do dia (ZIP ou CSV), processa os dados de `EWZ` e `SPY`, gera dashboards HTML e (por padrão) atualiza a pasta `docs/` para publicação no GitHub Pages.

Script principal usado neste guia: `ewz_runner.py`.

## Antes de começar (pré-requisitos)

Você precisa instalar:

1. **Python 3**
2. **Git**

Como testar se está tudo instalado:

### Windows (PowerShell)

```powershell
python --version
git --version
```

Se `python` não funcionar, teste:

```powershell
py --version
```

### macOS / Linux (Terminal)

```bash
python3 --version
git --version
```

Se aparecer número de versão (ex.: `Python 3.x.x`), está ok.

## Como baixar o projeto

### Opção A (mais fácil): GitHub Desktop

1. Instale e abra o GitHub Desktop.
2. Clique em **File > Clone repository**.
3. Selecione o repositório `EWZ Dashboard`.
4. Escolha uma pasta no seu computador e clique em **Clone**.
5. No final, você terá uma pasta local com todos os arquivos do projeto.

### Opção B: Terminal (`git clone`)

No terminal, rode:

```bash
git clone <URL_DO_REPOSITORIO>
```

Depois entre na pasta:

```bash
cd "EWZ Dashboard"
```

## Como abrir a pasta do projeto

Você precisa executar os comandos **dentro da pasta do projeto**.

Forma simples de checar:

- Windows (PowerShell):

```powershell
pwd
```

- macOS/Linux (Terminal):

```bash
pwd
```

O caminho exibido deve terminar com `EWZ Dashboard`.

## Como preparar os arquivos do dia

Dentro da pasta do projeto, crie uma pasta com a data no formato `AAAA-MM-DD`.

Exemplo:

`2026-05-06`

Dentro dessa pasta, coloque os 3 arquivos do dia (pode ser `.zip` ou `.csv`):

1. `bot-eod-report-*.zip` ou `bot-eod-report-*.csv`
2. `dp-eod-report-*.zip` ou `dp-eod-report-*.csv`
3. `chain-oi-changes-*.zip` ou `chain-oi-changes-*.csv`

Exemplo real de nomes válidos:

- `bot-eod-report-2026-05-06.zip`
- `dp-eod-report-2026-05-06.csv`
- `chain-oi-changes-2026-05-06.zip`

## Como rodar

### Com data específica (recomendado para iniciantes)

#### Windows (PowerShell)

```powershell
python ewz_runner.py --date-dir 2026-05-06
```

Se `python` não funcionar:

```powershell
py ewz_runner.py --date-dir 2026-05-06
```

#### macOS / Linux (Terminal)

```bash
python3 ewz_runner.py --date-dir 2026-05-06
```

### Sem informar data

Você também pode rodar sem `--date-dir`:

- o script tenta usar a pasta de **hoje** (`AAAA-MM-DD`)
- se ela não existir, usa automaticamente a pasta de data mais recente encontrada

Comando:

- Windows:

```powershell
python ewz_runner.py
```

- macOS/Linux:

```bash
python3 ewz_runner.py
```

## Como saber se deu certo

No final da execução, você deve ver no terminal:

`EWZ runner completed.`

E os resultados ficam em:

`AAAA-MM-DD/processed`

Arquivos esperados incluem dashboards HTML como:

- `EWZ_delta_volume_dashboard_AAAA-MM-DD.html`
- `SPY_delta_volume_dashboard_AAAA-MM-DD.html`

## Publicação no GitHub Pages

Por padrão, o runner **já publica** em `docs/` no final.

Se você quiser rodar sem publicar, use `--no-publish`.

### Windows

```powershell
python ewz_runner.py --date-dir 2026-05-06 --no-publish
```

### macOS/Linux

```bash
python3 ewz_runner.py --date-dir 2026-05-06 --no-publish
```

## Opções extras (opcional)

Para usuários mais avançados, o `ewz_runner.py` também aceita:

- `--symbols` (ex.: escolher tickers)
- `--timezone` (fuso horário)
- `--site-dir` (pasta de saída do site)

Exemplo:

```bash
python3 ewz_runner.py --date-dir 2026-05-06 --symbols EWZ SPY --timezone America/Sao_Paulo --site-dir docs
```

## Erros comuns e solução rápida

### 1) Erro de arquivo faltando

Mensagem parecida com “Missing input for ...”.

Como resolver:

- confirme se a pasta da data existe
- confirme se os 3 arquivos obrigatórios estão lá
- confirme se os nomes começam com:
  - `bot-eod-report`
  - `dp-eod-report`
  - `chain-oi-changes`

### 2) “python não é reconhecido” (Windows)

Tente `py` no lugar de `python`:

```powershell
py ewz_runner.py --date-dir 2026-05-06
```

### 3) “python3: command not found” (macOS/Linux)

Python 3 não está instalado (ou não está no PATH). Instale o Python 3 e teste com:

```bash
python3 --version
```

### 4) Você está na pasta errada

Rode `pwd` e confirme que está dentro da pasta do projeto `EWZ Dashboard`.

## Resumo ultra-rápido (cola e roda)

### Windows (PowerShell)

```powershell
git clone <URL_DO_REPOSITORIO>
cd "EWZ Dashboard"
python ewz_runner.py --date-dir 2026-05-06
```

Se precisar:

```powershell
py ewz_runner.py --date-dir 2026-05-06
```

### macOS (Terminal)

```bash
git clone <URL_DO_REPOSITORIO>
cd "EWZ Dashboard"
python3 ewz_runner.py --date-dir 2026-05-06
```

### Linux (Terminal)

```bash
git clone <URL_DO_REPOSITORIO>
cd "EWZ Dashboard"
python3 ewz_runner.py --date-dir 2026-05-06
```
