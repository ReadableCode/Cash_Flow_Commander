# %%
"""App-owned schema convergence.

Every entry point calls ensure_schema() before it touches data, so a fresh
clone works with no manual step and no deploy doc to follow. Replaces the
hand-run `db.create_tables()` that only three of the eleven entry points
called - starting the TUI on a new checkout used to hit missing tables.

Two properties make it safe to call on every process start:

  - **Additive only.** It emits CREATE TABLE for objects that do not exist and
    nothing else. Never DROP, TRUNCATE, or ALTER COLUMN. A destructive change
    is a reviewed migration, not something a boot can do by surprise.
  - **Version gated.** `deploy_meta.version` is compared to SCHEMA_VERSION
    first, so the steady state is a single SELECT rather than a reflection
    round-trip per table.

Bump SCHEMA_VERSION whenever a table definition in db.py changes. Because
create_all is create-if-missing, a bump makes new *tables* appear but does NOT
add a column to an existing table - that needs its own explicit migration.
"""

# %%
# Imports #

import datetime as dt

from sqlalchemy import delete, insert, inspect, select
from sqlalchemy.engine import Engine

try:
    import db
except ImportError:  # pragma: no cover - import shim, same pattern as the stores
    from src import db  # type: ignore[no-redef]

# %%
# Version #

SCHEMA_VERSION = 1


# %%
# Convergence #


def _postgres_schema_missing(engine: Engine) -> bool:
    """True when the target Postgres schema does not exist yet.

    Creating it needs CREATE on the database, which the app role deliberately
    does not have - the schema and its owning role are a one-time superuser
    step (deploy/create_role.sql). Detecting it here turns an opaque
    "relation does not exist" into an instruction.
    """
    if engine.dialect.name != "postgresql" or db.DB_SCHEMA is None:
        return False
    return db.DB_SCHEMA not in inspect(engine).get_schema_names()


def applied_version(engine: Engine) -> int | None:
    """Schema version stamped in the database, or None if never bootstrapped."""
    if not inspect(engine).has_table(db.deploy_meta.name, schema=db.DB_SCHEMA):
        return None
    with engine.connect() as conn:
        return conn.execute(select(db.deploy_meta.c.version)).scalar()


def ensure_schema(engine: Engine | None = None, force: bool = False) -> bool:
    """Converge the schema to SCHEMA_VERSION. Returns True if it applied anything.

    Raises rather than continuing on failure: unlike a web app that should keep
    serving a login page, every caller here is about to read or write financial
    records, and a half-built schema would fail later and less clearly.
    """
    if engine is None:
        engine = db.get_engine()

    if _postgres_schema_missing(engine):
        raise RuntimeError(
            f"Postgres schema {db.DB_SCHEMA!r} does not exist. It and the app role are a "
            "one-time superuser step: run `uv run python deploy/run_create_role.py` "
            "(see deploy/DEPLOY.md). Tables are created from here, the schema is not."
        )

    if not force and applied_version(engine) == SCHEMA_VERSION:
        return False

    db.create_tables(engine)

    # One row, rewritten in place. Same transaction so a crash between the two
    # statements cannot leave the version unstamped while the tables exist.
    with engine.begin() as conn:
        conn.execute(delete(db.deploy_meta))
        conn.execute(
            insert(db.deploy_meta).values(
                version=SCHEMA_VERSION, applied_at=dt.datetime.now(dt.timezone.utc)
            )
        )
    return True


# %%
