# Cash_Flow_Commander

A personal finance pipeline in Python. It collects bills, utility usage, solar
production and bank and card transactions from the providers' own portals,
keeps every downloaded file verbatim, parses them into a database, and answers
two questions from that data: did each expected bill and paycheck actually
happen, and what will the bank balance be over the coming weeks.

It exists to replace a hand-maintained Google Sheet where paid amounts and
dates were typed in by hand. The database is now the source of truth, and the
sheet is a report the forecast writes out.

![Pairing and forecast screen of the TUI, on sample data](docs/screenshots/tui_pairing_forecast.png)

## What it does

- **Acquires** raw artifacts per provider: portal API JSON, bill PDFs, emails,
  meter interval CSVs and bank or card CSV exports. Acquisition is done by
  Claude Code slash commands in `.claude/commands/` that drive your own Chrome
  (AppleScript or Claude in Chrome). `/cfc-update` runs every provider at once.
- **Lands** each artifact byte-for-byte in a `raw_documents` table, deduplicated
  by sha256 (`src/ingest_raw.py`).
- **Parses** raw documents into typed tables (`bills`, `bill_line_items`,
  `usage_intervals`, `plans`, `transactions`) with versioned, deterministic
  per-provider parsers (`src/parse_raw.py`, `src/providers/`). A parser fix is
  applied by reprocessing; parsed rows are never hand-edited.
- **Tracks expected money**: recurring bills, paychecks and transfers as
  series, their occurrences, and which real transactions paid them. Matching is
  suggest-then-confirm; nothing is paired without a person choosing it. See
  [`docs/EXPECTED.md`](docs/EXPECTED.md).
- **Forecasts** the balance: the anchor account's newest bank running balance
  plus every unpaid, unskipped occurrence from there forward
  (`src/expected_forecast.py`).
- **Reports quiet failures**: data holes that dashboards would render as
  plausible numbers (`src/checks.py`), and broken, net-zero, past-due, one-leg
  and drifting matches (`src/expected_checks.py`).
- **Shows it** in a terminal UI, a browser pairing board, and Grafana
  dashboards committed in `deploy/grafana/` (cash forecast, transactions,
  energy, solar net metering).

Providers with commands today:

| Kind | Provider | Command |
| --- | --- | --- |
| Electricity bills | Rhythm Energy, Gexa Energy | `/bills-rhythm`, `/bills-gexa` |
| Meter intervals | Smart Meter Texas | `/bills-smt` |
| Solar production | Enphase Enlighten | `/bills-enphase_enlighten` |
| Bank and card transactions | Chase, Citi, Elan | `/transactions-chase`, `/transactions-citi`, `/transactions-elan` |

The acquisition side is macOS only, since it drives the browser through
AppleScript. Everything after landing is plain Python.

### Goals

- Let anyone manage their finances with a Python application that can be used
  through an API, TUI, CLI, or web application.
- The initial project does not split frontend and backend, since the initial
  use case is a TUI.
- It runs on SQLite or Postgres today (see [Database](#database)); further
  backends such as Google Sheets and Excel files are a goal.

## How it works

Bills and usage follow the shared contract in
[`docs/LANDING.md`](docs/LANDING.md). A provider run is done when the data is
visible in Grafana, not when files are on disk:

```
coverage  ->  acquire  ->  ingest_raw  ->  parse_raw  ->  Grafana
(what's       (agent,      (raw_          (typed rows,    (dashboards
 missing)      only the     documents)     usage_          committed in
               gap)                        intervals)      deploy/grafana/)
```

- `src/coverage.py --provider <slug>` reports what is held and recommends the
  window to fetch. Overlap is free (raw dedups by sha256, tables upsert on
  natural keys), so windows always err wide.
- Agents acquire; scripts land and normalize; on failure the fix is to parser
  code, never to the data.

Bank and card transactions use `transaction_downloader/`, which tracks
coverage by requested window instead of by missing readings, because a card
can go ten days with no activity. See
[`transaction_downloader/README.md`](transaction_downloader/README.md).

```
plan.py  ->  agent downloads  ->  capture.py  ->  src/ingest_raw.py  ->  src/parse_raw.py
```

`bash transaction_downloader/land.sh --provider <slug>` files downloads and
runs ingest and parse in one step.

Every portal command shares one download folder and `/cfc-update` runs them at
the same time, so a run claims its own file with `src/downloads.py` (`mark`
before the click, then `claim --provider <slug>`), never by taking the newest
file.

Expected transactions live in three tables: `expected_series` (the
obligation, append-only), `expected_occurrences` (one instance) and
`expected_matches` (occurrence to real transaction, chosen by a person). Paid
status is derived every time, never stored, so a restated or reversed
transaction flips its occurrence back on its own.

## Getting started

1. Install [uv](https://docs.astral.sh/uv/), then the dependencies: `uv sync`. Details are under [Running with uv](#running-with-uv).
2. Run any script, for example `uv run python src/checks.py`. The first run on a checkout creates `providers.local.yaml` from `template_providers.yaml`, prints what to do next and stops with exit code 2. Nothing else happens on that run.
3. Add your providers. In Claude Code, `/bills-add-company` (bills and usage) and `/transactions-add-company` (bank and card exports) each fill one entry by interview and write that company's command. Or edit `providers.local.yaml` by hand; its comments explain every key. Delete the template's entries for providers you do not use.
4. Pick a database. With no `.env` the app uses a SQLite file under `data/` and says so on every run. For Postgres see [Database](#database).
5. Acquire. Run a company's command (`/bills-<slug>`, `/transactions-<slug>`), or `/cfc-update` for all of them.
6. Look at it: [the TUI](#running-the-tui), [the pairing board](#running-the-pairing-board), or the Grafana dashboards in `deploy/grafana/`.

A script that needs a value you have not filled in stops with one line naming the provider and the key, says what to do, and exits with code 2. It does not print a traceback and it does not carry on with an empty config.

`providers.local.yaml` is gitignored because this repo is public. To keep yours under version control, move it into a private repo of your own and symlink it back; see [Personal config](#personal-config).

## Setting Up

### Database

Two supported backends. Both are driven by one variable, `CFC_DATABASE_URL`.

**SQLite: the default for a fresh clone.** Clone, `uv sync`, and run something.
With no `.env` file and no `CFC_DATABASE_URL`, the app uses
`data/cash_flow_commander.db` and prints a warning saying so. Every entry point
creates its own tables on first run, so there is no setup step.

> **Back up that file yourself.** `data/` is gitignored, so nothing in this repo
> and nothing on any server has a copy of it. It is a single file: copy it
> somewhere else on a schedule, or use `sqlite3 data/cash_flow_commander.db
> ".backup 'somewhere/else.db'"`, which is safe to run while the app is open.
> Losing it loses every bill, transaction, and expected series you have entered.

**Postgres: for a shared or backed-up database.** Copy `template.env` to
`.env`, set:

| Variable | Value |
| --- | --- |
| `CFC_DATABASE_URL` | `postgresql+psycopg://cash_flow_commander_user:PASSWORD@HOST:5432/apps` |
| `CFC_DB_SCHEMA` | `cash_flow_commander` |

The role and schema are a one-time superuser step; tables are not. See
[`deploy/DEPLOY.md`](deploy/DEPLOY.md).

**SQLite is a choice, never a fallback.** If a `.env` exists but sets no
`CFC_DATABASE_URL`, the app refuses to start rather than quietly opening an
empty local file. A run against "the database" that silently finds nothing
looks exactly like a run that worked, and that has happened here before.

## Running with uv

- Install uv:

  Linux:

    ```bash
    curl -LsSf https://astral.sh/uv/install.sh | sh
    ```

  Windows:

    In powershell as admin:

    ```powershell
    powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
    ```

- Run the following command to install the dependencies from the project:

  ```bash
  uv sync
  ```

- To Activate or Source the environment and not have to prepend each command with `uv run`:

  On Linux:

  ```bash
  source ./.venv/bin/activate
  ```

  On Windows (Powershell):

  ```powershell
  .\.venv\Scripts\Activate.ps1
  ```

- To Deactivate:

  ```bash
  deactivate
  ```

- To add project dependencies:

  ```bash
  uv add <package name>
  ```

- To see a tree of dependencies:

  ```bash
  uv tree
  ```

## Running the TUI

- From the repo root:

  ```bash
  uv run python src/cfc_tui.py
  ```

- One app, three screens: pairing and forecast (`p` or `f`), every real
  transaction and the series it pays (`t`), and the series themselves (`e`).
  The screenshot at the top of this page is the pairing and forecast screen.

- On exit it rebuilds `forecast_days` once, so the Grafana forecast follows
  the session.

## Running the pairing board

- A browser view of the same pairing: expected occurrences on the left,
  transactions on the right, matches as lines. It starts a localhost server,
  opens the page, and exits when the tab closes, then rebuilds `forecast_days`
  once so the Grafana forecast follows the session.

  ```bash
  uv run python src/pairing_web.py                 # 45 days back, 45 forward
  uv run python src/pairing_web.py --days-back 90  # wider window
  ```

  ![Pairing board, on sample data](docs/screenshots/pairing_board.png)

## Other scripts

```bash
uv run python src/expected_checks.py     # broken / past-due / net-zero / drift report
uv run python src/expected_discover.py   # recurring transactions no series accounts for
uv run python src/expected_forecast.py   # cash forecast preview; --publish writes the sheet
uv run python src/checks.py              # data holes the dashboards would hide
uv run python deploy/grafana_sync.py list  # move dashboards between the repo and Grafana
```

## Bill providers: /bills-<company> commands

- Billing artifacts (portal API JSON, bill PDFs, emails) are acquired by per-company Claude Code slash commands committed in `.claude/commands/` (e.g. `/bills-rhythm`).
- Each command acquires raw-first and lands artifacts into the `raw_documents` store via `src/ingest_raw.py`. All commands follow the shared contract in `docs/LANDING.md`.

### Adding a company

- Run `/bills-add-company`. It interviews you, scaffolds `.claude/commands/bills-<slug>.md` from `templates/provider-command.md`, and stubs your entry in `providers.local.yaml`.
- For a bank or card, run `/transactions-add-company`.

### Personal config

- Personal values (account numbers, IDs, archive paths) live in `providers.local.yaml`, which is gitignored. The first script you run creates it from `template_providers.yaml` and says so; every value starts blank.
- Keep the real file under version control somewhere private: move it into a private repo of your own and symlink it back to `providers.local.yaml`. The scripts follow the link. If the link dangles they stop with an error and never write a blank file over it.
- A provider that serves one city or neighbourhood gets a generic slug (`city_utility`, `hoa`), and its real name goes in that entry's `display_name`, so nothing committed here places your home.
- Committed command files contain ONLY company knowledge and reference your values symbolically.

### Hygiene

- Before committing provider work, run the leak check and expect zero hits:

  ```bash
  git diff --cached -U0 | grep -inE "RH-[0-9]|[0-9]{15,}|/Users/|OneDrive|@(gmail|outlook)|premise_id.*[0-9]"
  ```

## Repository layout

| Path | What it holds |
| --- | --- |
| `src/` | Entry points, stores, parsers (`src/providers/`), schema (`db.py`, `bootstrap.py`) |
| `transaction_downloader/` | Bank and card window planning, capture and landing |
| `.claude/commands/` | Per-provider acquisition commands and `/cfc-update` |
| `docs/` | `LANDING.md` (acquisition contract), `EXPECTED.md` (expected transactions) |
| `deploy/` | Postgres role setup, Grafana dashboards and sync |
| `templates/` | Provider command template, pairing board page |
| `backlog/` | Known open issues and planned providers |
| `tests/` | pytest suite (`uv run pytest`) |
