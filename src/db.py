# %%
# Imports #

import os
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    Date,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    LargeBinary,
    MetaData,
    Numeric,
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

usage_intervals = Table(
    "usage_intervals",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("account_id", Text, nullable=False),
    # Interval start timestamp, always UTC.
    Column("ts", DateTime(timezone=True), nullable=False),
    Column("granularity", Text, nullable=False),  # '15min' | 'hour'
    Column("metric", Text, nullable=False),  # 'consumption' | 'generation'
    Column("value", Numeric(12, 4), nullable=False),  # kWh
    Column("unit", Text, nullable=False, server_default=text("'kwh'")),
    Column("rate", Numeric(12, 6), nullable=True),  # $/kWh
    Column("cost", Numeric(12, 4), nullable=True),  # $ (credit earned for generation)
    Column("estimated", Boolean, nullable=True),  # SMT A/E flag; NULL when source doesn't say
    Column(
        "raw_document_id",
        Integer,
        ForeignKey(raw_documents.c.id, name="fk_usage_intervals_raw_document"),
        nullable=True,
    ),
    Column("parser_version", Text, nullable=False),
    UniqueConstraint("account_id", "ts", "granularity", "metric", name="uq_usage_intervals_natural"),
    # Covers Grafana range scans: filter by series, scan by time.
    Index("ix_usage_intervals_series", "account_id", "granularity", "metric", "ts"),
)

bills = Table(
    "bills",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("account_id", Text, nullable=False),
    Column("invoice_number", Text, nullable=False),
    Column("invoice_date", Date, nullable=True),
    Column("service_start", Date, nullable=True),
    Column("service_end", Date, nullable=True),
    Column("due_date", Date, nullable=True),
    Column("total_kwh", Numeric(12, 3), nullable=True),
    Column("amount_due", Numeric(12, 2), nullable=True),
    Column("total_current_charges", Numeric(12, 2), nullable=True),
    Column("previous_balance", Numeric(12, 2), nullable=True),
    Column("forward_balance", Numeric(12, 2), nullable=True),
    Column("balance", Numeric(12, 2), nullable=True),
    Column("plan_name", Text, nullable=True),
    Column("contract_rate_cents_kwh", Numeric(8, 4), nullable=True),
    Column("late_fee_applied", Boolean, nullable=True),
    Column("invoice_type", Text, nullable=True),
    Column(
        "raw_document_id",
        Integer,
        ForeignKey(raw_documents.c.id, name="fk_bills_raw_document"),
        nullable=True,
    ),
    Column("parser_version", Text, nullable=False),
    UniqueConstraint("account_id", "invoice_number", name="uq_bills_account_invoice"),
    Index("ix_bills_account_invoice_date", "account_id", "invoice_date"),
)

bill_line_items = Table(
    "bill_line_items",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("account_id", Text, nullable=False),
    Column("invoice_number", Text, nullable=False),
    Column("line_no", Integer, nullable=False),
    Column("section", Text, nullable=False),  # 'energy' | 'non_energy' | 'current' | 'adjustment'
    Column("category", Text, nullable=False),  # 'energy' | 'delivery' | 'base' | 'tax' | 'fee' | 'credit' | 'other'
    Column("description", Text, nullable=False),
    Column("quantity_kwh", Numeric(12, 3), nullable=True),
    Column("rate_cents_kwh", Numeric(8, 4), nullable=True),
    Column("amount", Numeric(12, 2), nullable=False),  # signed; credits negative
    Column(
        "raw_document_id",
        Integer,
        ForeignKey(raw_documents.c.id, name="fk_bill_line_items_raw_document"),
        nullable=True,
    ),
    Column("parser_version", Text, nullable=False),
    UniqueConstraint("account_id", "invoice_number", "line_no", name="uq_bill_line_items_natural"),
    Index("ix_bill_line_items_invoice", "account_id", "invoice_number"),
)

payments = Table(
    "payments",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("account_id", Text, nullable=False),
    Column("paid_at", Date, nullable=False),
    Column("amount", Numeric(12, 2), nullable=False),
    Column("confirmation", Text, nullable=True),
    Column("source_message_id", Text, nullable=True),
    Column(
        "raw_document_id",
        Integer,
        ForeignKey(raw_documents.c.id, name="fk_payments_raw_document"),
        nullable=True,
    ),
    Column("parser_version", Text, nullable=False),
    UniqueConstraint("account_id", "paid_at", "amount", name="uq_payments_natural"),
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
