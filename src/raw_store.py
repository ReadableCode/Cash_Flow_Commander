# %%
# Imports #

import hashlib
import mimetypes
from datetime import date, datetime, timezone
from typing import Any, Iterator

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import Insert as PgInsert
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import Insert as SqliteInsert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import Engine

try:
    import db
except ImportError:  # pragma: no cover - fallback for `import src.raw_store`
    from src import db  # type: ignore[no-redef]


# %%
# Constants #

DEFAULT_MIME = "application/octet-stream"
ITER_YIELD_PER = 100


# %%
# Functions #


def ingest_bytes(
    engine: Engine,
    *,
    provider: str,
    doc_type: str,
    source: str,
    content: bytes,
    original_name: str,
    mime: str | None = None,
    period_hint: date | None = None,
    fetched_at: datetime | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Insert one raw document, deduplicating on the sha256 of its content.

    Returns {'id': <int>, 'deduped': <bool>} where deduped=True means an
    identical document already existed and no new row was written.
    """
    content_sha256 = hashlib.sha256(content).hexdigest()

    if mime is None:
        mime = mimetypes.guess_type(original_name)[0] or DEFAULT_MIME

    if fetched_at is None:
        fetched_at = datetime.now(timezone.utc)
    elif fetched_at.tzinfo is None:
        fetched_at = fetched_at.replace(tzinfo=timezone.utc)

    values = {
        "provider": provider,
        "doc_type": doc_type,
        "source": source,
        "period_hint": period_hint,
        "original_name": original_name,
        "mime": mime,
        "byte_size": len(content),
        "content": content,
        "content_sha256": content_sha256,
        "fetched_at": fetched_at,
        "extra": extra,
    }

    dialect_name = engine.dialect.name
    insert_stmt: SqliteInsert | PgInsert
    if dialect_name == "sqlite":
        insert_stmt = sqlite_insert(db.raw_documents).values(**values)
    elif dialect_name == "postgresql":
        insert_stmt = pg_insert(db.raw_documents).values(**values)
    else:
        raise ValueError(
            f"raw_store.ingest_bytes only supports sqlite and postgresql, got dialect {dialect_name!r}"
        )
    # Explicit RETURNING is the only dedup signal that behaves identically on
    # both dialects: a conflicted (skipped) insert returns no row, while
    # rowcount for ON CONFLICT DO NOTHING is not reliably 0 on Postgres.
    returning_stmt = insert_stmt.on_conflict_do_nothing(
        index_elements=[db.raw_documents.c.content_sha256]
    ).returning(db.raw_documents.c.id)

    with engine.begin() as conn:
        returned_id = conn.execute(returning_stmt).scalar_one_or_none()
        deduped = returned_id is None
        if returned_id is not None:
            row_id = int(returned_id)
        else:
            row_id = int(
                conn.execute(
                    select(db.raw_documents.c.id).where(
                        db.raw_documents.c.content_sha256 == content_sha256
                    )
                ).scalar_one()
            )

    return {"id": row_id, "deduped": deduped}


def iter_documents(
    engine: Engine,
    provider: str | None = None,
    doc_type: str | None = None,
    parse_status: str | None = None,
) -> Iterator[dict[str, Any]]:
    """Stream matching raw_documents rows as dicts, ordered by id.

    Filters are applied only when not None.
    """
    stmt = select(db.raw_documents)
    if provider is not None:
        stmt = stmt.where(db.raw_documents.c.provider == provider)
    if doc_type is not None:
        stmt = stmt.where(db.raw_documents.c.doc_type == doc_type)
    if parse_status is not None:
        stmt = stmt.where(db.raw_documents.c.parse_status == parse_status)
    stmt = stmt.order_by(db.raw_documents.c.id).execution_options(
        stream_results=True, yield_per=ITER_YIELD_PER
    )

    with engine.connect() as conn:
        for row in conn.execute(stmt).mappings():
            yield dict(row)


def mark_parsed(
    engine: Engine,
    id: int,
    status: str,
    parser_version: str | None,
    error: str | None = None,
) -> None:
    """Record the parse outcome for one raw document row."""
    stmt = (
        update(db.raw_documents)
        .where(db.raw_documents.c.id == id)
        .values(
            parse_status=status,
            parsed_at=datetime.now(timezone.utc),
            parser_version=parser_version,
            parse_error=error,
        )
    )
    with engine.begin() as conn:
        conn.execute(stmt)
