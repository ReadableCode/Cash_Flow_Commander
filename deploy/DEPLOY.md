# Cash Flow Commander — Postgres Deployment

Target: the shared `apps` database on the home server Postgres instance. That
instance also hosts the `load_log` schema (fronted by PostgREST). This deploy
adds only the `cash_flow_commander` schema and its dedicated role — nothing in
`public`, `load_log`, or PostgREST config is touched.

## Run order

1. **`create_role.sql`** — run once, as the `postgres` superuser (see below).
   Creates the `cash_flow_commander_user` login role and the
   `cash_flow_commander` schema it owns. Idempotent; safe to re-run to rotate
   the password.

   This step needs a superuser because creating a schema requires `CREATE` on
   the database, which the app role deliberately does not have. It is the only
   manual step.

2. **Everything else is automatic.** Set the two env vars below and run any
   entry point — the TUI, `ingest_raw`, `parse_raw`, whatever. `src/bootstrap.py`
   creates missing tables and stamps `cash_flow_commander.deploy_meta` with its
   `SCHEMA_VERSION`. It is additive only (`CREATE TABLE` for what is absent,
   never `DROP`/`TRUNCATE`/`ALTER COLUMN`) and version-gated, so after the first
   run it costs one `SELECT`.

   If the schema from step 1 is missing, bootstrap says so and names this file
   rather than failing with `relation does not exist`.

### Changing a table definition

Edit the `Table` in `src/db.py` and bump `SCHEMA_VERSION` in `src/bootstrap.py`.
A bump makes new *tables* appear. It does **not** add a column to an existing
table, drop anything, or change a type — `create_all` is create-if-missing, so
an altered definition silently does nothing against a table that already exists
and then breaks a write later. Any change to an existing table is its own
reviewed migration against a backup, never something a boot does by surprise.

## Step 1 in detail: create_role.sql

Preferred (no psql needed, no shell on the server): run the committed runner
from the repo root on any machine that can reach Postgres. It reads the
superuser connection (`POSTGRES_URL`/`POSTGRES_PORT`/`POSTGRES_USER`/
`POSTGRES_PASSWORD`) and `CFC_DB_PASSWORD` from the private `.env`, executes
`create_role.sql` (minus the psql-only meta-commands), and verifies the
role/schema afterward:

```bash
uv run python deploy/run_create_role.py
```

Alternative, via psql inside the postgres container (the script reads the
password with `\getenv`, which requires psql 15+; the `postgres:17` image
qualifies):

```bash
PW="$(grep '^CFC_DB_PASSWORD=' /path/to/private.env | cut -d= -f2-)"
sudo docker exec -i -e CFC_DB_PASSWORD="$PW" postgres \
  psql -U postgres -d apps -v ON_ERROR_STOP=1 \
  < Cash_Flow_Commander/deploy/create_role.sql
```

## Environment settings (Postgres mode)

| Variable | Value |
| --- | --- |
| `CFC_DATABASE_URL` | `postgresql+psycopg://cash_flow_commander_user:PASSWORD@HOST:5432/apps` |
| `CFC_DB_SCHEMA` | `cash_flow_commander` |

`PASSWORD` and `HOST` are placeholders — real values live only in the private
env file.

## Placement verification (run after deploy)

All app tables must land in `cash_flow_commander` and nowhere else:

```sql
-- Must return ONLY cash_flow_commander:
SELECT table_schema FROM information_schema.tables WHERE table_name='raw_documents';
```

```text
-- \dn should show cash_flow_commander owned by cash_flow_commander_user:
\dn
```

```sql
-- Must be unchanged from before the run (load_log untouched):
SELECT count(*) FROM information_schema.tables WHERE table_schema='load_log';
```

Note: run the `load_log` count as the superuser. `information_schema` only
shows tables the current role has privileges on, so `cash_flow_commander_user`
correctly sees 0 there — that is the isolation working, not a missing schema.

## Why this app does not use PostgREST

The other self-built apps on this cluster reach their data through PostgREST
(see `dotfiles/docs/postgres_app_conventions.md`). This one deliberately does
not, decided 2026-08-28:

- **PostgREST cannot serve SQLite.** Anyone cloning this repo must be able to
  run it against their own database, including a local file, so the data-access
  seam has to span both backends. That seam is SQLAlchemy. Adding PostgREST
  would mean maintaining two access paths to the same tables.
- **There is no multi-user story.** Each person runs their own database at
  their own URL. RLS with one account filters nothing, and a `users` table would
  exist only to mint a token.
- **It is not internet-facing.** The TUI and CLIs run on the LAN or over
  Tailscale, so the `LOGIN` role is not exposed the way a web app's would be.

Consequences, so they are not mistaken for oversights: `cash_flow_commander` is
absent from `PGRST_DB_SCHEMAS` on purpose, `cash_flow_commander_user` keeps
`LOGIN` and a password on purpose, and there is no RLS on purpose. If the
schema is ever exposed via PostgREST, it must be **appended** to
`PGRST_DB_SCHEMAS` with `load_log` remaining **first**.

Grafana (below) reads the tables directly as `grafana_ro`, which stays correct
under this decision.

## Safety rules recap

- **Dedicated role only** — all app operations run as
  `cash_flow_commander_user`; never `postgres` or any superuser.
- **Additive-only DDL** — no drops or rewrites of existing objects.
- **Schema-scoped everything** — every table, grant, and query stays inside
  the `cash_flow_commander` schema; `public` and `load_log` are off-limits.

## Grafana read-only role (step 3+)

Grafana dashboards read Cash Flow Commander data through a native Postgres
datasource. Rather than reusing `cash_flow_commander_user` (which can write),
`create_grafana_ro.sql` creates a dedicated **`grafana_ro`** login role with
least privilege: `SELECT`-only, scoped to the `cash_flow_commander` schema,
with default privileges set so future tables created by the app role stay
readable. No grants on `public`, `load_log`, or any other schema; no
INSERT/UPDATE/DELETE anywhere.

Run it after step 1 (any time; idempotent, safe to re-run to rotate the
password). Preferred — the committed runner, which now takes a SQL path and
the env-var name holding the role password:

```bash
uv run python deploy/run_create_role.py deploy/create_grafana_ro.sql GRAFANA_RO_PASSWORD
```

Alternative, via psql inside the postgres container:

```bash
PW="$(grep '^GRAFANA_RO_PASSWORD=' /path/to/private.env | cut -d= -f2-)"
sudo docker exec -i -e GRAFANA_RO_PASSWORD="$PW" postgres \
  psql -U postgres -d apps -v ON_ERROR_STOP=1 \
  < Cash_Flow_Commander/deploy/create_grafana_ro.sql
```

### Grafana datasource settings

| Setting | Value |
| --- | --- |
| Host | `<HOST>:5432` |
| Database | `apps` |
| User | `grafana_ro` |
| Password | value of `GRAFANA_RO_PASSWORD` (private env only) |
| TLS/SSL mode | per your setup (`disable` on a trusted LAN, `require`+ otherwise) |

**Important:** the tables live in the `cash_flow_commander` schema, not
`public`. Set the datasource's default schema / `search_path` to
`cash_flow_commander`, or schema-qualify every table in your queries
(`cash_flow_commander.usage_intervals`, ...).

Example dashboard query:

```sql
SELECT ts AS "time", value FROM cash_flow_commander.usage_intervals
WHERE account_id = '$account' AND granularity = '15min' AND metric = 'consumption' AND $__timeFilter(ts)
ORDER BY ts;
```

### Dashboards are committed artifacts

Dashboard JSON lives in `deploy/grafana/` and is version-controlled. Grafana's
own database is not backed up by this repo, and a hand-built panel is
unreviewable and unreproducible — so the repo, not the Grafana instance, is the
source of truth.

Use `deploy/grafana_sync.py` for both directions. It reads `GRAFANA_URL`,
`GRAFANA_ADMIN_USER`, and `GRAFANA_ADMIN_PASSWORD` from the private `.env`.

```bash
uv run python deploy/grafana_sync.py list
uv run python deploy/grafana_sync.py export <uid> deploy/grafana/<name>.json
uv run python deploy/grafana_sync.py import deploy/grafana/<name>.json
uv run python deploy/grafana_sync.py verify <uid>
```

- **export** replaces the instance's concrete datasource uid with
  `${DS_CASH_FLOW_COMMANDER}` and re-adds the `__inputs` block, so the
  committed file imports into any instance.
- **import** resolves that placeholder back to this instance's postgres
  datasource (auto-detected), then re-reads the dashboard and fails loudly if
  any placeholder survived.
- **verify** runs every panel query through Grafana using each panel's **own**
  datasource reference.

The UI equivalent of import is Dashboards → New → Import → *Upload JSON file*,
which prompts for the datasource.

**Two traps, both of which have already bitten this repo:**

1. `/api/dashboards/import` resolves `__inputs` placeholders only when the
   posted dashboard **still contains its `__inputs` block**. Stripping it looks
   harmless — it reads as export-only metadata — but the import then succeeds
   while leaving every panel pointed at the literal string
   `"${DS_CASH_FLOW_COMMANDER}"`. The dashboard renders **"No data" on every
   panel**. Always import with the script.
2. When testing panel queries, do **not** substitute a known-good datasource
   uid into the query. That is what hides trap 1 and makes a completely broken
   dashboard report healthy. `verify` deliberately uses the panel's own
   reference.

**Rules for panel SQL in this repo:**

- Schema-qualify every table (`cash_flow_commander.usage_intervals`) — nothing
  lives in `public`.
- Bucket by local time (`timezone('America/Chicago', ts)`), not UTC, or every
  local day splits across two buckets. Use a **literal** timezone, not a
  dashboard constant: `$tz`-style variables are not interpolated on the
  `/api/ds/query` path and fail there while appearing fine in the UI.
- Validate with `grafana_sync.py verify` before committing. Grafana renders a
  failing query as an empty panel without explaining why.

Committed dashboards:

| File | uid | Shows |
| --- | --- | --- |
| `grafana/solar_net_metering.json` | `cfc-solar-net-metering` | Gross solar production vs metered import/export; the self-consumed energy net metering hides, per month and per billing period. |
| `grafana/energy.json` | `cfc-energy` | Usage vs solar export at 15-minute and hourly resolution, energy cost vs solar credit, monthly billed totals and category breakdown, effective all-in rate. |

### Verification checklist (connect as `grafana_ro`)

- `SELECT count(*) FROM cash_flow_commander.usage_intervals;` — works.
- Any `INSERT` into a `cash_flow_commander` table — fails with
  `permission denied`.
- `SELECT count(*) FROM load_log.anything;` — fails (no `USAGE` on
  `load_log`); that is the isolation working.
