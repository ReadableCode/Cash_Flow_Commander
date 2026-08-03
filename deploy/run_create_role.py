# %%
# Imports #

"""Run a deploy SQL file against the shared Postgres without needing psql.

Reads from the environment (via the repo .env symlink -- values never live here):
  POSTGRES_URL / POSTGRES_PORT / POSTGRES_USER / POSTGRES_PASSWORD  superuser conn
  plus the password env var the target SQL file expects (see below)

It executes the committed SQL file verbatim, minus the psql-only meta-command
lines (backslash commands like \\getenv/\\if, which guard the env var when the
file is run through psql), with the psql-style :'var' placeholder replaced by
the properly quoted password from the named env var. Idempotent, like the SQL.

Usage (from the repo root):
    uv run python deploy/run_create_role.py                                   # create_role.sql / CFC_DB_PASSWORD
    uv run python deploy/run_create_role.py deploy/create_grafana_ro.sql GRAFANA_RO_PASSWORD
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
    sql_path = Path(sys.argv[1]) if len(sys.argv) > 1 else REPO_ROOT / "deploy" / "create_role.sql"
    pw_var = sys.argv[2] if len(sys.argv) > 2 else "CFC_DB_PASSWORD"

    missing = [k for k in ("POSTGRES_URL", "POSTGRES_USER", "POSTGRES_PASSWORD", pw_var)
               if not os.environ.get(k)]
    if missing:
        sys.exit(f"Missing env vars: {', '.join(missing)} (expected in the private .env)")

    sql = sql_path.read_text()
    # Drop psql meta-command lines; psycopg speaks plain SQL only.
    kept_lines = []
    getenv_var = None
    for line in sql.splitlines():
        stripped = line.lstrip()
        if stripped.startswith("\\"):
            if stripped.startswith("\\getenv"):
                getenv_var = stripped.split()[1]
            continue
        kept_lines.append(line)
    sql = "\n".join(kept_lines)
    # psql would inject the password via \getenv; do the same quoting here.
    if getenv_var is None:
        sys.exit(f"{sql_path}: no \\getenv line found; refusing to guess the placeholder")
    quoted_pw = "'" + os.environ[pw_var].replace("'", "''") + "'"
    sql = sql.replace(f":'{getenv_var}'", quoted_pw)

    conninfo = (
        f"host={os.environ['POSTGRES_URL']} port={os.environ.get('POSTGRES_PORT', '5432')} "
        f"dbname={TARGET_DB} user={os.environ['POSTGRES_USER']} password={os.environ['POSTGRES_PASSWORD']}"
    )
    with psycopg.connect(conninfo, autocommit=True) as conn:
        conn.execute(sql)  # type: ignore[arg-type]
        if pw_var == "CFC_DB_PASSWORD":
            row = conn.execute(
                "SELECT r.rolname, n.nspname, pg_get_userbyid(n.nspowner) "
                "FROM pg_roles r JOIN pg_namespace n ON n.nspname = 'cash_flow_commander' "
                "WHERE r.rolname = 'cash_flow_commander_user'"
            ).fetchone()
            if row is None:
                sys.exit("Verification failed: role or schema missing after run")
            print(f"ok: role {row[0]} exists; schema {row[1]} owned by {row[2]}")
        else:
            print(f"ok: executed {sql_path.name}")


if __name__ == "__main__":
    main()
