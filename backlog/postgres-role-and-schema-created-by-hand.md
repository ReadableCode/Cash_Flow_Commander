# deploy: the Postgres role and schema are still a hand-run superuser step

    found:  2026-10-14
    status: open
    verify: grep -n "manual step" deploy/DEPLOY.md

On a Postgres deployment, `deploy/create_role.sql` (the `cash_flow_commander_user`
login role and the `cash_flow_commander` schema) and `deploy/create_grafana_ro.sql`
are applied by a person, not by code. `deploy/DEPLOY.md` says so:

    1. **`create_role.sql`** — run once, as the `postgres` superuser (see below).
    ...
       This step needs a superuser because creating a schema requires `CREATE` on
       the database, which the app role deliberately does not have. It is the only
       manual step.

`src/bootstrap.py` creates tables but only detects a missing schema and stops
with "It and the app role are a one-time superuser step: run
`uv run python deploy/run_create_role.py`". Every other app on the shared
server converges its role and schema from its own startup bootstrap with the
superuser connection; the two cluster-wide PostgREST roles are now created by
a one-shot service in the server's compose stack. This repo is the last place
on that server where database setup is a hand-run step.

**fix** — when the superuser `POSTGRES_*` settings are present, have
`src/bootstrap.py` run `deploy/create_role.sql` (and `create_grafana_ro.sql`
when `GRAFANA_RO_PASSWORD` is set) before `create_tables()`, the same
idempotent SQL `deploy/run_create_role.py` applies today, then rewrite
`deploy/DEPLOY.md` step 1 and the bootstrap's error to match. Without those
settings (anyone running against their own database or SQLite) nothing
changes.

**blast radius** — none on a database that already has the role and schema:
the SQL is idempotent. The app process would need the superuser credentials
in its environment on the server, which it does not hold today; that is the
decision to make.

**not doing yet** — found while retiring the other apps' hand-run setup on
2026-10-14; this repo is a sanctioned exception to the PostgREST conventions
(no superuser in the app's environment by design), so giving its bootstrap a
superuser connection needs a deliberate choice first.
