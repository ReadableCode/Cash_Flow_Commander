# %%
"""Upserts for account_balances and account_activity.

Investment accounts: what an account held on a date, and the activity lines
that moved it. Both upsert on natural keys, so re-parsing is idempotent.
"""

# %%
# Imports #

from typing import Any

from sqlalchemy import Table
from sqlalchemy.dialects.postgresql import Insert as PgInsert
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import Insert as SqliteInsert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import Engine

try:
    import db
except ImportError:  # pragma: no cover - fallback for `import src.account_store`
    from src import db  # type: ignore[no-redef]


# %%
# Constants #

# Natural key of one balance; upserts conflict on these.
BALANCES_NATURAL_KEY = ["account_id", "as_of_date"]

# Natural key of one activity line; upserts conflict on these.
ACTIVITY_NATURAL_KEY = [
    "account_id",
    "activity_date",
    "activity",
    "fund",
    "money_source",
    "amount",
    "occurrence",
]


# %%
# Helpers #


def _dialect_insert(table: Table, engine: Engine) -> SqliteInsert | PgInsert:
    """Return a dialect-specific INSERT for table, so on_conflict_do_update is available."""
    dialect_name = engine.dialect.name
    if dialect_name == "sqlite":
        return sqlite_insert(table)
    if dialect_name == "postgresql":
        return pg_insert(table)
    raise ValueError(f"account_store only supports sqlite and postgresql, got dialect {dialect_name!r}")


def _upsert(engine: Engine, table: Table, natural_key: list[str], rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Upsert rows that share one key set, updating only the columns they carry."""
    if not rows:
        return {"upserted": 0}
    insert_stmt = _dialect_insert(table, engine)
    update_columns = [name for name in rows[0] if name not in natural_key]
    upsert_stmt = insert_stmt.on_conflict_do_update(
        index_elements=natural_key,
        set_={name: insert_stmt.excluded[name] for name in update_columns},
    )
    with engine.begin() as conn:
        conn.execute(upsert_stmt, rows)
    return {"upserted": len(rows)}


# %%
# Functions #


def upsert_balances(engine: Engine, rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Upsert balance rows on (account_id, as_of_date).

    All rows in one call must share the same key set. On conflict only the
    columns present in the rows are updated, so a balance snapshot landing on a
    statement's date refreshes the balance and leaves the statement's flows
    alone. Returns {'upserted': <rows sent>}.
    """
    return _upsert(engine, db.account_balances, BALANCES_NATURAL_KEY, rows)


def upsert_activity(engine: Engine, rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Upsert activity rows on their natural key. Returns {'upserted': <rows sent>}."""
    return _upsert(engine, db.account_activity, ACTIVITY_NATURAL_KEY, rows)
