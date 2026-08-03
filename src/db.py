# %%
# Imports #

import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import (
    JSON,
    Column,
    Date,
    DateTime,
    Index,
    Integer,
    LargeBinary,
    MetaData,
    Table,
    Text,
    UniqueConstraint,
    create_engine,
    text,
)
from sqlalchemy.engine import Engine, make_url
from sqlalchemy.engine.url import URL

# %%
# Environment #

# Repo root is the parent of src/. Relative sqlite paths resolve against it so
# scripts work when launched from the repo root (e.g. `uv run python src/ingest_raw.py`).
REPO_ROOT = Path(__file__).resolve().parent.parent

# .env is a symlink to a private env file; load it read-only into os.environ.
# Values must never be printed or embedded anywhere.
load_dotenv(REPO_ROOT / ".env", override=False)

DEFAULT_DATABASE_URL = "sqlite:///data/cash_flow_commander.db"

# Read at import time; tests monkeypatch env and importlib.reload this module.
DATABASE_URL: str = os.environ.get("CFC_DATABASE_URL") or DEFAULT_DATABASE_URL
DB_SCHEMA: str | None = os.environ.get("CFC_DB_SCHEMA", "").strip() or None


# %%
# Engine #


def _resolve_sqlite_url(url: URL) -> URL:
    """Resolve a relative sqlite file path against the repo root and ensure its parent dir exists."""
    if url.get_backend_name() != "sqlite":
        return url
    database = url.database
    if not database or database == ":memory:":
        return url
    db_path = Path(database)
    if not db_path.is_absolute():
        db_path = REPO_ROOT / db_path
    db_path.parent.mkdir(parents=True, exist_ok=True)
    return url.set(database=str(db_path))


def get_engine() -> Engine:
    """Build an Engine from CFC_DATABASE_URL.

    When CFC_DB_SCHEMA is set, every Postgres connection is pinned to that
    schema via search_path so nothing ever lands in `public`.
    """
    url = _resolve_sqlite_url(make_url(DATABASE_URL))
    connect_args: dict[str, str] = {}
    if DB_SCHEMA is not None and url.get_backend_name() == "postgresql":
        connect_args["options"] = f"-csearch_path={DB_SCHEMA}"
    return create_engine(url, connect_args=connect_args)


# %%
# Schema #

metadata = MetaData(schema=DB_SCHEMA)

raw_documents = Table(
    "raw_documents",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("provider", Text, nullable=False),
    Column("doc_type", Text, nullable=False),
    Column("source", Text, nullable=False),
    Column("period_hint", Date, nullable=True),
    Column("original_name", Text, nullable=False),
    Column("mime", Text, nullable=False),
    Column("byte_size", Integer, nullable=False),
    Column("content", LargeBinary, nullable=False),
    Column("content_sha256", Text, nullable=False),
    Column("fetched_at", DateTime(timezone=True), nullable=False),
    Column("extra", JSON, nullable=True),
    Column("parse_status", Text, nullable=False, server_default=text("'pending'")),
    Column("parsed_at", DateTime, nullable=True),
    Column("parser_version", Text, nullable=True),
    Column("parse_error", Text, nullable=True),
    UniqueConstraint("content_sha256", name="uq_raw_documents_content_sha256"),
    Index("ix_raw_documents_provider_doc_type_period_hint", "provider", "doc_type", "period_hint"),
)


# %%
# DDL #


def create_tables(engine: Engine | None = None) -> None:
    """Create missing tables (idempotent, additive-only).

    Only emits CREATE for objects that do not exist yet; never DROP/ALTER/TRUNCATE.
    In Postgres the target schema must already exist (created by the deploy
    script) — this function does not create schemas.
    """
    if engine is None:
        engine = get_engine()
    metadata.create_all(engine, checkfirst=True)


# %%
