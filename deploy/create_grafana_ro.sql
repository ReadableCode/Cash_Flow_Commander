-- create_grafana_ro.sql — read-only Grafana role for Cash Flow Commander on the
-- shared `apps` database (same instance that hosts load_log; this file touches
-- NEITHER public NOR load_log NOR any PostgREST role).
-- Safe to re-run (idempotent).
--
-- How to run (as the postgres superuser):
--   Preferred — the committed runner (reads the superuser connection and
--   GRAFANA_RO_PASSWORD from the private .env):
--     uv run python deploy/run_create_role.py deploy/create_grafana_ro.sql GRAFANA_RO_PASSWORD
--   Alternative — psql inside the postgres container:
--     1. Extract the password from the private env (never store it in this file):
--          PW="$(grep '^GRAFANA_RO_PASSWORD=' /path/to/private.env | cut -d= -f2-)"
--     2. Pipe this file into psql inside the postgres container:
--          sudo docker exec -i -e GRAFANA_RO_PASSWORD="$PW" postgres \
--            psql -U postgres -d apps -v ON_ERROR_STOP=1 \
--            < Cash_Flow_Commander/deploy/create_grafana_ro.sql
\getenv grafana_ro_pw GRAFANA_RO_PASSWORD
\if :{?grafana_ro_pw}
\else
  \echo '>>> GRAFANA_RO_PASSWORD is not set in the environment. Aborting.'
  \quit
\endif
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'grafana_ro') THEN
    CREATE ROLE grafana_ro WITH LOGIN;
  END IF;
END
$$;
ALTER ROLE grafana_ro WITH PASSWORD :'grafana_ro_pw';

-- Read-only access, scoped to the cash_flow_commander schema only.
GRANT USAGE ON SCHEMA cash_flow_commander TO grafana_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA cash_flow_commander TO grafana_ro;

-- Future tables created by the app role stay readable without re-running this.
ALTER DEFAULT PRIVILEGES FOR ROLE cash_flow_commander_user IN SCHEMA cash_flow_commander
  GRANT SELECT ON TABLES TO grafana_ro;

-- Deliberately NO grants on public, load_log, or any other schema, and no
-- INSERT/UPDATE/DELETE anywhere: this role is read-only and scoped to the
-- cash_flow_commander schema only.
