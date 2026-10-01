# Cash_Flow_Commander

- The goal of this project is to allow anyone to manage their finances with a python application that can be interfaced with using an api, TUI, CLI, or Web Application.
- The initial project will not have a split frontend and backend since the initial use case will be with a TUI
- It runs on SQLite or Postgres today (see [Database](#database)); further backends such as google sheets and excel files are a goal.

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

**SQLite — the default for a fresh clone.** Clone, `uv sync`, and run something.
With no `.env` file and no `CFC_DATABASE_URL`, the app uses
`data/cash_flow_commander.db` and prints a warning saying so. Every entry point
creates its own tables on first run, so there is no setup step.

> **Back up that file yourself.** `data/` is gitignored, so nothing in this repo
> and nothing on any server has a copy of it. It is a single file — copy it
> somewhere else on a schedule, or use `sqlite3 data/cash_flow_commander.db
> ".backup 'somewhere/else.db'"`, which is safe to run while the app is open.
> Losing it loses every bill, transaction, and expected series you have entered.

**Postgres — for a shared or backed-up database.** Copy `template.env` to
`.env`, set:

| Variable | Value |
| --- | --- |
| `CFC_DATABASE_URL` | `postgresql+psycopg://cash_flow_commander_user:PASSWORD@HOST:5432/apps` |
| `CFC_DB_SCHEMA` | `cash_flow_commander` |

The role and schema are a one-time superuser step; tables are not. See
[`deploy/DEPLOY.md`](deploy/DEPLOY.md).

**SQLite is a choice, never a fallback.** If a `.env` exists but sets no
`CFC_DATABASE_URL`, the app refuses to start rather than quietly opening an
empty local file — a run against "the database" that silently finds nothing
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

## Bill providers: /bills-<company> commands

- Billing artifacts (portal API JSON, bill PDFs, emails) are acquired by per-company Claude Code slash commands committed in `.claude/commands/` (e.g. `/bills-rhythm`).
- Each command acquires raw-first and lands artifacts into the `raw_documents` store via `src/ingest_raw.py`. All commands follow the shared contract in `docs/LANDING.md`.

### Adding a company

- Run `/bills-add-company`. It interviews you, scaffolds `.claude/commands/bills-<slug>.md` from `templates/provider-command.md`, and stubs your entry in `providers.local.yaml`.

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
