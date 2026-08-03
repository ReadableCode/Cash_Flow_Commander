# %%
# Imports #

from datetime import timedelta
from typing import Any

from sqlalchemy import Table, delete, insert, select
from sqlalchemy.dialects.postgresql import Insert as PgInsert
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import Insert as SqliteInsert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import Engine

try:
    import db
except ImportError:  # pragma: no cover - fallback for `import src.bill_store`
    from src import db  # type: ignore[no-redef]


# %%
# Constants #

# Natural key of one bill; upserts conflict on these.
BILLS_NATURAL_KEY = ["account_id", "invoice_number"]

# Natural key of one payment; upserts conflict on these.
PAYMENTS_NATURAL_KEY = ["account_id", "paid_at", "amount"]

# Payment columns refreshed when a conflicting row already exists.
PAYMENTS_UPDATE_COLUMNS = [
    "confirmation",
    "source_message_id",
    "raw_document_id",
    "parser_version",
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
    raise ValueError(f"bill_store only supports sqlite and postgresql, got dialect {dialect_name!r}")


def _bills_upsert_stmt(engine: Engine, present_columns: list[str]) -> SqliteInsert | PgInsert:
    """Build a bills upsert that on conflict updates ONLY the columns present in the rows.

    The API invoice parser and the PDF bill parser each own a disjoint set of
    bill columns; updating only the columns a caller actually sent keeps one
    parser from clobbering the other's values with NULLs.
    """
    insert_stmt = _dialect_insert(db.bills, engine)
    update_columns = [name for name in present_columns if name not in BILLS_NATURAL_KEY]
    return insert_stmt.on_conflict_do_update(
        index_elements=BILLS_NATURAL_KEY,
        set_={name: insert_stmt.excluded[name] for name in update_columns},
    )


# %%
# Functions #


def upsert_bills(engine: Engine, rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Upsert bill rows on their natural key (account_id, invoice_number).

    rows are dicts keyed by bills column names (except id); all rows in one
    call must share the same key set. On conflict only the columns present in
    the rows (including raw_document_id and parser_version) are updated, so
    re-parsing is idempotent and each parser only touches its own columns.
    Returns {'upserted': <rows sent>}.
    """
    if not rows:
        return {"upserted": 0}

    upsert_stmt = _bills_upsert_stmt(engine, list(rows[0].keys()))
    with engine.begin() as conn:
        conn.execute(upsert_stmt, rows)

    return {"upserted": len(rows)}


def apply_pdf_bill(engine: Engine, doc_rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Apply parsed PDF bills: patch bill columns and replace that bill's line items.

    Each doc_rows entry is {'bill_patch': {...}, 'line_items': [{...}]}. Neither
    part carries invoice_number; it is resolved by (account_id, invoice_date)
    against bills, which the API invoice parser is guaranteed to have populated
    first. Entries whose date resolves to zero or multiple bills are reported in
    'unresolved' and skipped (the caller marks the document as an error). Each
    resolved entry is applied in one transaction: bill patch upserted
    (only-present-columns rule), then line items replaced wholesale. Returns
    {'bills_patched': int, 'line_items': int, 'unresolved': list[str]}.
    """
    bills_patched = 0
    line_items_inserted = 0
    unresolved: list[str] = []

    for entry in doc_rows:
        patch = entry["bill_patch"]
        items = entry["line_items"]
        account_id = patch["account_id"]
        invoice_date = patch["invoice_date"]

        resolve_stmt = select(db.bills.c.invoice_number).where(
            db.bills.c.account_id == account_id,
            db.bills.c.invoice_date == invoice_date,
        )
        # The date printed on the PDF can differ from the API's invoice_date by
        # one day (e.g. Dec 2022: PDF says the 29th, API says the 30th), so an
        # exact miss falls back to a +/-1-day window — still requiring a
        # unique match.
        fallback_stmt = select(db.bills.c.invoice_number).where(
            db.bills.c.account_id == account_id,
            db.bills.c.invoice_date.between(
                invoice_date - timedelta(days=1), invoice_date + timedelta(days=1)
            ),
        )

        with engine.begin() as conn:
            invoice_numbers = conn.execute(resolve_stmt).scalars().all()
            if not invoice_numbers:
                invoice_numbers = conn.execute(fallback_stmt).scalars().all()
            if len(invoice_numbers) != 1:
                found = "no bill" if not invoice_numbers else f"{len(invoice_numbers)} bills"
                unresolved.append(
                    f"account_id={account_id} invoice_date={invoice_date}: {found} matched; entry skipped"
                )
                continue
            invoice_number = invoice_numbers[0]

            bill_row = {**patch, "invoice_number": invoice_number}
            upsert_stmt = _bills_upsert_stmt(engine, list(bill_row.keys()))
            conn.execute(upsert_stmt, [bill_row])
            bills_patched += 1

            # The one sanctioned DELETE: scoped to the line items of this single
            # invoice, which this parser owns and immediately re-inserts below.
            # Delete-then-insert so a shrunken item list leaves no stale rows.
            conn.execute(
                delete(db.bill_line_items).where(
                    db.bill_line_items.c.account_id == account_id,
                    db.bill_line_items.c.invoice_number == invoice_number,
                )
            )
            item_rows = [
                {**item, "account_id": account_id, "invoice_number": invoice_number} for item in items
            ]
            if item_rows:
                conn.execute(insert(db.bill_line_items), item_rows)
            line_items_inserted += len(item_rows)

    return {"bills_patched": bills_patched, "line_items": line_items_inserted, "unresolved": unresolved}


def upsert_payments(engine: Engine, rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Upsert payment rows on their natural key (account_id, paid_at, amount).

    rows are dicts keyed by payments column names (except id); all rows in one
    call must share the same key set. On conflict the detail columns
    (confirmation, source_message_id, raw_document_id, parser_version) are
    updated in place, so re-parsing is idempotent. Returns
    {'upserted': <rows sent>}.
    """
    if not rows:
        return {"upserted": 0}

    insert_stmt = _dialect_insert(db.payments, engine)
    # Intersect with the row keys so a source that omits an optional detail
    # column still upserts cleanly.
    update_columns = [name for name in PAYMENTS_UPDATE_COLUMNS if name in rows[0]]
    upsert_stmt = insert_stmt.on_conflict_do_update(
        index_elements=PAYMENTS_NATURAL_KEY,
        set_={name: insert_stmt.excluded[name] for name in update_columns},
    )
    with engine.begin() as conn:
        conn.execute(upsert_stmt, rows)

    return {"upserted": len(rows)}
