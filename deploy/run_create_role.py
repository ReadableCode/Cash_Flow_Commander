# %%
# Imports #

"""Run deploy/create_role.sql against the shared Postgres without needing psql.

Reads from the environment (via the repo .env symlink -- values never live here):
  POSTGRES_URL / POSTGRES_PORT / POSTGRES_USER / POSTGRES_PASSWORD  superuser conn
  CFC_DB_PASSWORD                                                    password for cash_flow_commander_user

It executes the committed SQL file verbatim, minus the psql-only meta-command
lines (backslash commands like \\getenv/\\if, which guard the env var when the
file is run through psql), with :'cfc_pw' replaced by the properly quoted
password from CFC_DB_PASSWORD. Idempotent, like the SQL itself.

Usage (from the repo root):
    uv run python deploy/run_create_role.py
"""

import os
import sys
from pathlib import Path

import psycopg
from dotenv import load_dotenv

# %%
# Main #

REPO_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(REPO_ROOT / ".env", override=False)

TARGET_DB = "apps"


def main() -> None:
    missing = [k for k in ("POSTGRES_URL", "POSTGRES_USER", "POSTGRES_PASSWORD", "CFC_DB_PASSWORD")
               if not os.environ.get(k)]
    if missing:
        sys.exit(f"Missing env vars: {', '.join(missing)} (expected in the private .env)")

    sql = (REPO_ROOT / "deploy" / "create_role.sql").read_text()
    # Drop psql meta-command lines; psycopg speaks plain SQL only.
    sql = "\n".join(line for line in sql.splitlines() if not line.lstrip().startswith("\\"))
    # psql would inject the password via \getenv; do the same quoting here.
    quoted_pw = "'" + os.environ["CFC_DB_PASSWORD"].replace("'", "''") + "'"
    sql = sql.replace(":'cfc_pw'", quoted_pw)

    conninfo = (
        f"host={os.environ['POSTGRES_URL']} port={os.environ.get('POSTGRES_PORT', '5432')} "
        f"dbname={TARGET_DB} user={os.environ['POSTGRES_USER']} password={os.environ['POSTGRES_PASSWORD']}"
    )
    with psycopg.connect(conninfo, autocommit=True) as conn:
        conn.execute(sql)  # type: ignore[arg-type]
        row = conn.execute(
            "SELECT r.rolname, n.nspname, pg_get_userbyid(n.nspowner) "
            "FROM pg_roles r JOIN pg_namespace n ON n.nspname = 'cash_flow_commander' "
            "WHERE r.rolname = 'cash_flow_commander_user'"
        ).fetchone()
    if row is None:
        sys.exit("Verification failed: role or schema missing after run")
    print(f"ok: role {row[0]} exists; schema {row[1]} owned by {row[2]}")


if __name__ == "__main__":
    main()
