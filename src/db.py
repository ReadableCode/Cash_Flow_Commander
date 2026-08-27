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
#
# A dangling link is checked first: load_dotenv on a missing file is a silent
# no-op, and DATABASE_URL below would then fall back to the local dev SQLite
# file. A run against "the database" would quietly hit an empty one instead.
_ENV_PATH = REPO_ROOT / ".env"
if _ENV_PATH.is_symlink() and not _ENV_PATH.exists():
    raise RuntimeError(
        f"{_ENV_PATH} is a symlink to '{os.readlink(_ENV_PATH)}', which does not exist. "
        "The personal_credentials clone is missing or moved - without it CFC_DATABASE_URL is "
        "unset and this would silently use the local dev SQLite file."
    )
load_dotenv(_ENV_PATH, override=False)

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
    Index(
        "ix_raw_documents_provider_doc_type_period_hint",
        "provider",
        "doc_type",
        "period_hint",
    ),
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
    Column(
        "estimated", Boolean, nullable=True
    ),  # SMT A/E flag; NULL when source doesn't say
    Column(
        "raw_document_id",
        Integer,
        ForeignKey(raw_documents.c.id, name="fk_usage_intervals_raw_document"),
        nullable=True,
    ),
    Column("parser_version", Text, nullable=False),
    UniqueConstraint(
        "account_id", "ts", "granularity", "metric", name="uq_usage_intervals_natural"
    ),
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
    Column(
        "section", Text, nullable=False
    ),  # 'energy' | 'non_energy' | 'current' | 'adjustment'
    Column(
        "category", Text, nullable=False
    ),  # 'energy' | 'delivery' | 'base' | 'tax' | 'fee' | 'credit' | 'other'
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
    UniqueConstraint(
        "account_id", "invoice_number", "line_no", name="uq_bill_line_items_natural"
    ),
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


transactions = Table(
    "transactions",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    # Last 4 of the account, matching what the capture store files under.
    Column("account_id", Text, nullable=False),
    Column("account_kind", Text, nullable=False),  # 'bank' | 'credit'
    Column("txn_date", Date, nullable=False),  # when it happened
    Column(
        "post_date", Date, nullable=False
    ),  # when it settled; bank exports give only this
    Column("description", Text, nullable=False),
    Column("amount", Numeric(12, 2), nullable=False),  # signed; negative = money out
    Column("category", Text, nullable=True),  # card exports only
    Column(
        "txn_type", Text, nullable=True
    ),  # Chase 'Type', or 'Details' on bank exports
    Column(
        "balance", Numeric(12, 2), nullable=True
    ),  # running balance, bank exports only
    Column("check_no", Text, nullable=True),
    Column("memo", Text, nullable=True),
    # Chase CSVs carry NO transaction id, and genuinely identical same-day rows
    # occur (two identical coffees at one shop). Without this counter the second
    # one would upsert onto the first and silently vanish. It is assigned in
    # export order within each (account, post_date, description, amount) group,
    # which is stable across re-downloads, so reprocessing stays idempotent.
    Column("occurrence", Integer, nullable=False, server_default=text("0")),
    Column(
        "raw_document_id",
        Integer,
        ForeignKey(raw_documents.c.id, name="fk_transactions_raw_document"),
        nullable=True,
    ),
    # Every field of the source row, verbatim, keyed by its ORIGINAL header text
    # and holding the RAW unparsed string — including columns that are also
    # projected into the typed columns above. The typed columns are a
    # convenience projection; this is the record. A column Chase adds later
    # lands here automatically instead of being silently dropped.
    Column("extra", JSON, nullable=True),
    Column("parser_version", Text, nullable=False),
    UniqueConstraint(
        "account_id",
        "post_date",
        "description",
        "amount",
        "occurrence",
        name="uq_transactions_natural",
    ),
    # Covers the dashboard's range scans: filter by account, scan by date.
    Index("ix_transactions_account_post_date", "account_id", "post_date"),
)


# Expected transactions — the bills, paychecks, and transfers we KNOW are
# coming, and which real transactions turned out to be them.
#
# These three tables are different in kind from `transactions`: they are
# human-authored source of truth (like `raw_documents`), not a rebuildable
# projection. Nothing automated may edit or delete rows a person wrote.

expected_series = Table(
    "expected_series",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("name", Text, nullable=False),  # 'Mortgage', 'FiberFirst'
    Column(
        "category", Text, nullable=False
    ),  # 'Expense' | 'Income' | 'Debt' | 'Leisure' | 'Credit Card'
    Column("sub_category", Text, nullable=True),
    # 'monthly' | 'yearly' | 'biweekly' | 'every_x_days' | 'every_x_months' | 'once'
    # 'once' series get no generated occurrences; their dates are entered directly.
    Column("schedule_type", Text, nullable=False),
    Column("day_of_month", Integer, nullable=True),  # monthly and yearly
    Column("month_of_year", Integer, nullable=True),  # yearly only
    Column(
        "anchor_date", Date, nullable=True
    ),  # biweekly, every_x_days, every_x_months
    Column(
        "interval_days", Integer, nullable=True
    ),  # every_x_days (biweekly is always 14)
    Column("interval_months", Integer, nullable=True),  # every_x_months
    Column("amount", Numeric(12, 2), nullable=False),  # signed; negative = money out
    # How far an actual can differ from `amount` and still count as this bill.
    # NULL means use the default in expected_store.
    Column("amount_tolerance", Numeric(12, 2), nullable=True),
    # Account the money leaves (or arrives in), as transactions.account_id (last 4).
    # NULL when the account has no export in this system (e.g. an external card).
    Column("auto_pay_account_id", Text, nullable=True),
    # A credit card payoff moves money between our own accounts: -X from the
    # bank AND +X on the card. Transfers are excluded from spend reporting
    # (the real expenses are the card purchases) but still forecast each leg.
    Column("is_transfer", Boolean, nullable=False, server_default=text("FALSE")),
    Column("transfer_account_id", Text, nullable=True),  # the other leg's account
    # Substring the payment's bank description usually contains; used only to
    # rank suggestions, never to auto-match.
    Column("match_pattern", Text, nullable=True),
    # A series is never edited in place. To change a bill, close this row
    # (set active_until) and insert a new one — old matches keep pointing at
    # the old series, so history stays true. Overlapping series are fine.
    Column("active_from", Date, nullable=False),
    Column("active_until", Date, nullable=True),  # NULL = still active
    Column(
        "replaces_series_id",
        Integer,
        ForeignKey("expected_series.id", name="fk_expected_series_replaces"),
        nullable=True,
    ),
    Column("notes", Text, nullable=True),
    UniqueConstraint("name", "active_from", name="uq_expected_series_name_active_from"),
)

expected_occurrences = Table(
    "expected_occurrences",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column(
        "series_id",
        Integer,
        ForeignKey(expected_series.c.id, name="fk_expected_occurrences_series"),
        nullable=False,
    ),
    Column("due_date", Date, nullable=False),
    # Copied from the series when generated; edit per-occurrence when the real
    # amount is known ahead of time (a statement balance, a known tax bill).
    Column("amount", Numeric(12, 2), nullable=False),
    # There is NO stored paid flag. Paid status is derived by joining matches
    # to live transactions (see expected_store.get_occurrence_status_df), so a
    # deleted or restated transaction flips its occurrence back to unpaid on
    # its own. The only stored override is this manual skip.
    Column("skipped_at", DateTime, nullable=True),
    Column("skip_note", Text, nullable=True),
    Column("source", Text, nullable=False),  # 'generated' | 'import_sheet' | 'manual'
    UniqueConstraint("series_id", "due_date", name="uq_expected_occurrences_natural"),
    Index("ix_expected_occurrences_due_date", "due_date"),
)

expected_matches = Table(
    "expected_matches",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column(
        "occurrence_id",
        Integer,
        ForeignKey(expected_occurrences.c.id, name="fk_expected_matches_occurrence"),
        nullable=False,
    ),
    # The matched transaction's NATURAL key, not transactions.id — sync_capture
    # deletes and re-inserts rows when Chase restates them, so an id would go
    # stale silently. A key that stops resolving is VISIBLE: the occurrence
    # shows 'broken' and comes back to the pairing queue.
    Column("txn_account_id", Text, nullable=False),
    Column("txn_post_date", Date, nullable=False),
    Column("txn_description", Text, nullable=False),
    Column("txn_amount", Numeric(12, 2), nullable=False),
    Column("txn_occurrence", Integer, nullable=False, server_default=text("0")),
    # Portion of the transaction that belongs to THIS occurrence; NULL means
    # the whole transaction. Set when one bill pays several expected items —
    # the mortgage payment that also covers the PMI line gets one match per
    # occurrence, each carrying its share.
    Column("matched_amount", Numeric(12, 2), nullable=True),
    Column(
        "source", Text, nullable=False
    ),  # 'manual' | 'confirmed_suggestion' | 'import_sheet'
    Column("matched_at", DateTime, nullable=False),
    # Matches are never deleted, only voided — re-pairing keeps its history.
    Column("voided_at", DateTime, nullable=True),
    Column("note", Text, nullable=True),
    Index("ix_expected_matches_occurrence", "occurrence_id"),
    Index("ix_expected_matches_txn", "txn_account_id", "txn_post_date"),
)


# Daily cash forecast, one row per future day — a machine-built projection
# (rebuilt wholesale by expected_forecast.rebuild_forecast_days) that exists
# so Grafana can chart the forecast without re-deriving it in SQL.
forecast_days = Table(
    "forecast_days",
    metadata,
    Column("day", Date, primary_key=True),
    Column("start_balance", Numeric(12, 2), nullable=False),
    Column("outflows", Numeric(12, 2), nullable=False),  # signed; <= 0
    Column("inflows", Numeric(12, 2), nullable=False),  # >= 0
    # The day's worst moment: all money OUT is assumed to leave before any
    # money IN arrives, so a same-day paycheck can never hide an overdraft.
    Column("trough_balance", Numeric(12, 2), nullable=False),
    Column("end_balance", Numeric(12, 2), nullable=False),
    # Six months of essential expenses (from local config), repeated on every
    # row so the dashboard can draw the always-stay-above-this line without
    # the personal number living in the committed dashboard JSON.
    Column("emergency_fund", Numeric(12, 2), nullable=False, server_default=text("0")),
    Column("generated_at", DateTime, nullable=False),
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
