-- create_role.sql — one-time Cash Flow Commander role + schema setup on the shared
-- `apps` database (same instance that hosts load_log; this file touches NEITHER
-- public NOR load_log NOR any PostgREST role).
-- Safe to re-run (idempotent).
--
-- How to run (as the postgres superuser, once):
--   1. Extract the password from the private env (never store it in this file):
--        PW="$(grep '^CFC_DB_PASSWORD=' /path/to/private.env | cut -d= -f2-)"
--   2. Pipe this file into psql inside the postgres container:
--        sudo docker exec -i -e CFC_DB_PASSWORD="$PW" postgres \
--          psql -U postgres -d apps -v ON_ERROR_STOP=1 \
--          < Cash_Flow_Commander/deploy/create_role.sql
\getenv cfc_pw CFC_DB_PASSWORD
\if :{?cfc_pw}
\else
  \echo '>>> CFC_DB_PASSWORD is not set in the environment. Aborting.'
  \quit
\endif
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'cash_flow_commander_user') THEN
    CREATE ROLE cash_flow_commander_user WITH LOGIN;
  END IF;
END
$$;
ALTER ROLE cash_flow_commander_user WITH PASSWORD :'cfc_pw';

-- Dedicated schema, owned by the app role. Ownership already implies full
-- rights within the schema; the explicit grant below just states the intent.
CREATE SCHEMA IF NOT EXISTS cash_flow_commander AUTHORIZATION cash_flow_commander_user;
GRANT USAGE, CREATE ON SCHEMA cash_flow_commander TO cash_flow_commander_user;

-- Deliberately NO grants on public, load_log, or anything else: this role is
-- scoped to the cash_flow_commander schema only.
