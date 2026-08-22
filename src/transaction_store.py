# %%
# Imports #

import datetime
from typing import Any

from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import Insert as PgInsert
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import Insert as SqliteInsert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import Engine

try:
    import db
    from providers import chase
except ImportError:  # pragma: no cover - fallback for `import src.transaction_store`
    from src import db  # type: ignore[no-redef]
    from src.providers import chase  # type: ignore[no-redef]


# %%
# Constants #

DEFAULT_CHUNK_SIZE = 5000

# Natural key of one transaction; upserts conflict on these.
#
# `occurrence` is in the key because Chase CSVs carry no transaction id and
# genuinely repeat identical same-day rows. Without it the second identical
# coffee upserts onto the first and disappears.
NATURAL_KEY_COLUMNS = ["account_id", "post_date", "description", "amount", "occurrence"]

# Columns refreshed when a conflicting row already exists. Chase restates recent
# activity — a pending row's description and even its amount can change once it
# posts — so a re-download of an overlapping window must be able to correct what
# is already stored.
UPDATE_ON_CONFLICT_COLUMNS = [
    "account_kind",
    "txn_date",
    "category",
    "txn_type",
    "balance",
    "check_no",
    "memo",
    "extra",
    "raw_document_id",
    "parser_version",
]


# %%
# Helpers #


def _dialect_insert(engine: Engine) -> SqliteInsert | PgInsert:
    """Return a dialect-specific INSERT so on_conflict_do_update is available."""
    dialect_name = engine.dialect.name
    if dialect_name == "sqlite":
        return sqlite_insert(db.transactions)
    if dialect_name == "postgresql":
        return pg_insert(db.transactions)
    raise ValueError(
        f"transaction_store only supports sqlite and postgresql, got dialect {dialect_name!r}"
    )


# %%
# Functions #


def upsert_transactions(
    engine: Engine,
    rows: list[dict[str, Any]],
    *,
    chunk_size: int = DEFAULT_CHUNK_SIZE,
) -> dict[str, Any]:
    """Upsert transaction rows on their natural key.

    rows are dicts keyed by transactions column names (except id). On conflict
    the detail columns are updated in place, so re-parsing a document — or
    parsing a wider re-download that overlaps one already held — is idempotent.
    Rows are sent in chunks so a large backfill never builds one giant statement.
    Returns {'upserted': <total rows sent>}.
    """
    if not rows:
        return {"upserted": 0}

    insert_stmt = _dialect_insert(engine)
    # Intersect with the row keys so a source that omits an optional column
    # (bank exports have no category; card exports have no balance) still
    # upserts cleanly instead of writing NULL over a good value.
    update_columns = [name for name in UPDATE_ON_CONFLICT_COLUMNS if name in rows[0]]
    upsert_stmt = insert_stmt.on_conflict_do_update(
        index_elements=NATURAL_KEY_COLUMNS,
        set_={name: insert_stmt.excluded[name] for name in update_columns},
    )

    total = 0
    for start in range(0, len(rows), chunk_size):
        chunk = rows[start:start + chunk_size]
        with engine.begin() as conn:
            conn.execute(upsert_stmt, chunk)
        total += len(chunk)

    return {"upserted": total}


# %%
# Capture reconciliation #

# Reconciliation only ever compares captures of this provider's documents.
_PROVIDER = "chase"


def _row_key(row: Any) -> tuple[str, str, str, str, int]:
    """Natural key of a row, normalized for cross-source comparison.

    Parser rows carry floats and date objects; rows read back from the database
    carry Decimals (and, depending on dialect, differently-typed dates). A raw
    tuple comparison would call every stored row 'not in the export' and delete
    it, so amounts are compared as fixed 2-decimal strings and dates as ISO
    strings.
    """
    return (
        str(row["account_id"]),
        row["post_date"].isoformat(),
        str(row["description"]),
        f"{float(row['amount']):.2f}",
        int(row["occurrence"]),
    )


def _authority(original_name: str | None, doc_id: int | None) -> tuple[datetime.date, int]:
    """Ordering of captures: capture date from the filename, then document id.

    A row whose provenance cannot be read (no raw_document_id, or a name that is
    not a capture) sorts oldest, so any real capture may replace it.
    """
    meta = chase.capture_meta_from_name(original_name or "")
    captured = meta["captured"] if meta else datetime.date.min
    return (captured, int(doc_id) if doc_id is not None else -1)


def _newer_capture_windows(
    engine: Engine, account: str, incoming: tuple[datetime.date, int]
) -> list[tuple[datetime.date, datetime.date]]:
    """Requested windows of this account's captures that outrank `incoming`."""
    stmt = select(db.raw_documents.c.id, db.raw_documents.c.original_name).where(
        db.raw_documents.c.provider == _PROVIDER,
        db.raw_documents.c.doc_type == "csv_export",
    )
    windows: list[tuple[datetime.date, datetime.date]] = []
    with engine.connect() as conn:
        for doc in conn.execute(stmt):
            meta = chase.capture_meta_from_name(doc.original_name)
            if meta is None or str(meta["account"]) != account:
                continue
            if (meta["captured"], int(doc.id)) > incoming:
                windows.append((meta["start"], meta["end"]))
    return windows


def sync_capture(
    engine: Engine, rows: list[dict[str, Any]], window: dict[str, Any]
) -> dict[str, Any]:
    """Upsert one capture's rows and prune rows its export proves are gone.

    Chase restates posted rows — the everyday case is a tip: a restaurant charge
    posts at the pre-tip amount and the amount then changes. A restated amount
    is a NEW natural key, so upsert alone would leave the pre-tip row behind as
    a phantom that double-counts. The export IS the truth for the window it was
    requested over, so a capture is treated as authoritative for its window:
    stored rows in the window that no longer appear in the export are deleted.

    Genuine duplicates are never collapsed by this: two identical same-day
    charges (two memberships on one account) carry distinct `occurrence` values,
    both appear in the export, and both therefore survive.

    Authority is per DAY, not per run: only the newest capture covering a given
    date (by capture date from the filename, then document id) may write or
    prune rows on that date. That makes the outcome independent of processing
    order — re-parsing an old capture after a newer overlapping one neither
    resurrects its stale rows nor deletes the newer data. `transactions` stays a
    rebuildable projection; `raw_documents` keeps every capture.

    Returns {'upserted', 'removed', 'skipped'} row counts ('skipped' = incoming
    rows on dates a newer capture owns).
    """
    account = str(window["account_id"])
    start: datetime.date = window["start"]
    end: datetime.date = window["end"]
    incoming = (window["captured"], int(window.get("raw_document_id") or -1))

    newer = _newer_capture_windows(engine, account, incoming)

    def owned_by_newer(day: datetime.date) -> bool:
        return any(w_start <= day <= w_end for w_start, w_end in newer)

    writable = [row for row in rows if not owned_by_newer(row["post_date"])]
    upserted = upsert_transactions(engine, writable)["upserted"]

    keep = {_row_key(row) for row in writable}
    stmt = (
        select(
            db.transactions.c.id,
            db.transactions.c.account_id,
            db.transactions.c.post_date,
            db.transactions.c.description,
            db.transactions.c.amount,
            db.transactions.c.occurrence,
            db.transactions.c.raw_document_id,
            db.raw_documents.c.original_name,
        )
        .select_from(
            db.transactions.outerjoin(
                db.raw_documents, db.transactions.c.raw_document_id == db.raw_documents.c.id
            )
        )
        .where(
            db.transactions.c.account_id == account,
            db.transactions.c.post_date >= start,
            db.transactions.c.post_date <= end,
        )
    )
    with engine.connect() as conn:
        existing = conn.execute(stmt).mappings().all()

    stale_ids = [
        row["id"]
        for row in existing
        if not owned_by_newer(row["post_date"])
        and _row_key(row) not in keep
        and (
            # Same document re-parsed (parser fix): its own old rows are stale.
            row["raw_document_id"] == window.get("raw_document_id")
            or _authority(row["original_name"], row["raw_document_id"]) < incoming
        )
    ]
    if stale_ids:
        with engine.begin() as conn:
            conn.execute(delete(db.transactions).where(db.transactions.c.id.in_(stale_ids)))

    return {"upserted": upserted, "removed": len(stale_ids), "skipped": len(rows) - len(writable)}


# %%
