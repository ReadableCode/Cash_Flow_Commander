# %%
# Imports #

from typing import Any

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import Insert as PgInsert
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import Insert as SqliteInsert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import Engine

try:
    import db
except ImportError:  # pragma: no cover - fallback for `import src.usage_store`
    from src import db  # type: ignore[no-redef]


# %%
# Constants #

DEFAULT_CHUNK_SIZE = 5000

# Natural key of one interval reading; upserts conflict on these.
NATURAL_KEY_COLUMNS = ["account_id", "ts", "granularity", "metric"]

# Columns refreshed when a conflicting row already exists.
UPDATE_ON_CONFLICT_COLUMNS = [
    "value",
    "unit",
    "rate",
    "cost",
    "estimated",
    "raw_document_id",
    "parser_version",
]


# %%
# Functions #


def upsert_intervals(
    engine: Engine,
    rows: list[dict[str, Any]],
    *,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> dict[str, Any]:
    """Upsert usage interval rows on their natural key (account_id, ts, granularity, metric).

    rows are dicts keyed by usage_intervals column names (except id). On
    conflict the measurement columns are updated in place, so re-parsing a
    document is idempotent. Rows are sent in chunks of chunk_size, one
    transaction per chunk, so a large document never builds one giant
    statement. Returns {'upserted': <total rows sent>}.
    """
    if not rows:
        return {"upserted": 0}

    dialect_name = engine.dialect.name
    insert_stmt: SqliteInsert | PgInsert
    if dialect_name == "sqlite":
        insert_stmt = sqlite_insert(db.usage_intervals)
    elif dialect_name == "postgresql":
        insert_stmt = pg_insert(db.usage_intervals)
    else:
        raise ValueError(
            f"usage_store.upsert_intervals only supports sqlite and postgresql, got dialect {dialect_name!r}"
        )
    upsert_stmt = insert_stmt.on_conflict_do_update(
        index_elements=NATURAL_KEY_COLUMNS,
        set_={name: insert_stmt.excluded[name] for name in UPDATE_ON_CONFLICT_COLUMNS},
    )

    for start in range(0, len(rows), chunk_size):
        chunk = rows[start:start + chunk_size]
        with engine.begin() as conn:
            conn.execute(upsert_stmt, chunk)

    return {"upserted": len(rows)}


def series_bounds(engine: Engine, account_id: str | None = None) -> dict[str, Any]:
    """Return {'min_ts', 'max_ts', 'count'} over usage_intervals for quick verification.

    Filters to one account when account_id is given; min_ts/max_ts are None
    when no rows match.
    """
    stmt = select(
        func.min(db.usage_intervals.c.ts),
        func.max(db.usage_intervals.c.ts),
        func.count(db.usage_intervals.c.id),
    )
    if account_id is not None:
        stmt = stmt.where(db.usage_intervals.c.account_id == account_id)

    with engine.connect() as conn:
        min_ts, max_ts, count = conn.execute(stmt).one()

    return {"min_ts": min_ts, "max_ts": max_ts, "count": int(count)}
