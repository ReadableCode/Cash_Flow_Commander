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
2. **App-side `create_tables()`** — runs as `cash_flow_commander_user` via
   `CFC_DATABASE_URL`, from the repo root:

   ```bash
   export CFC_DATABASE_URL="postgresql+psycopg://cash_flow_commander_user:PASSWORD@HOST:5432/apps"
   export CFC_DB_SCHEMA="cash_flow_commander"
   uv run python -c "import sys; sys.path.insert(0,'src'); import db; db.create_tables()"
   ```

3. **Ingest CLI** — with the same two env vars set, run the normal ingest
   commands; they operate entirely inside the `cash_flow_commander` schema.

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

## NO PostgREST changes in this step

Cash Flow Commander speaks SQL directly over `CFC_DATABASE_URL`; PostgREST is
not involved and its configuration must not be modified here. If the
`cash_flow_commander` schema is ever exposed via PostgREST later, it must be
**appended** to `PGRST_DB_SCHEMAS`, and `load_log` must remain **first** in
that list.

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

### Verification checklist (connect as `grafana_ro`)

- `SELECT count(*) FROM cash_flow_commander.usage_intervals;` — works.
- Any `INSERT` into a `cash_flow_commander` table — fails with
  `permission denied`.
- `SELECT count(*) FROM load_log.anything;` — fails (no `USAGE` on
  `load_log`); that is the isolation working.
