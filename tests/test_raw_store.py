# %%
# Imports #

import datetime as dt
import hashlib
import importlib
import os
import uuid
from collections.abc import Iterator
from typing import Any

import pytest
from sqlalchemy import create_engine, delete, func, select
from sqlalchemy.engine import Engine, RowMapping

import db
import ingest_raw
import raw_store

# %%
# Synthetic fixtures (never real bills or personal data) #

FAKE_PDF = b"%PDF-1.4 fake synthetic bill\n%%EOF"
FAKE_PDF_OTHER = b"%PDF-1.4 fake synthetic bill number two\n%%EOF"
FAKE_CSV = b"month,total\n2025-01,123.45\n"
FAKE_JSON = b'{"synthetic": true, "rows": []}'

# Opt-in Postgres URL for the integration test; never a hardcoded connection string.
POSTGRES_TEST_URL = os.environ.get("CFC_TEST_DATABASE_URL")


# %%
# Helpers #


def _restore_env(name: str, value: str | None) -> None:
    if value is None:
        os.environ.pop(name, None)
    else:
        os.environ[name] = value


def _reload_modules() -> None:
    """Reload db (env is read at import time) and raw_store (rebind its db imports)."""
    importlib.reload(db)
    importlib.reload(raw_store)


def _count_rows(engine: Engine) -> int:
    with engine.connect() as conn:
        return conn.execute(select(func.count()).select_from(db.raw_documents)).scalar_one()


def _fetch_row(engine: Engine, doc_id: int) -> RowMapping:
    with engine.connect() as conn:
        return conn.execute(
            select(db.raw_documents).where(db.raw_documents.c.id == doc_id)
        ).mappings().one()


# %%
# Fixtures #


@pytest.fixture()
def sqlite_engine(tmp_path: Any) -> Iterator[Engine]:
    """Engine on a throwaway SQLite file, with CFC_DB_SCHEMA forced off.

    CFC_DB_SCHEMA is set to "" (not popped) so db's load_dotenv(override=False)
    cannot re-populate it from .env on reload; db treats "" as no schema.
    Env is restored and modules re-reloaded in the finally so other tests and
    the ambient environment are unaffected.
    """
    saved_url = os.environ.get("CFC_DATABASE_URL")
    saved_schema = os.environ.get("CFC_DB_SCHEMA")
    os.environ["CFC_DATABASE_URL"] = f"sqlite:///{tmp_path / 'raw_store_test.db'}"
    os.environ["CFC_DB_SCHEMA"] = ""
    engine: Engine | None = None
    try:
        _reload_modules()
        engine = db.get_engine()
        db.create_tables(engine)
        yield engine
    finally:
        if engine is not None:
            engine.dispose()
        _restore_env("CFC_DATABASE_URL", saved_url)
        _restore_env("CFC_DB_SCHEMA", saved_schema)
        _reload_modules()


# %%
# Ingest round-trip #


def test_ingest_round_trip(sqlite_engine: Engine) -> None:
    result = raw_store.ingest_bytes(
        sqlite_engine,
        provider="rhythm",
        doc_type="bill_pdf",
        source="manual",
        content=FAKE_PDF,
        original_name="Rythm 2025-01.pdf",
        mime="application/pdf",
        period_hint=dt.date(2025, 1, 1),
        extra={"note": "synthetic"},
    )

    assert result["deduped"] is False
    assert isinstance(result["id"], int)

    row = _fetch_row(sqlite_engine, result["id"])
    assert row["provider"] == "rhythm"
    assert row["doc_type"] == "bill_pdf"
    assert row["source"] == "manual"
    assert row["original_name"] == "Rythm 2025-01.pdf"
    assert row["mime"] == "application/pdf"
    assert row["period_hint"] == dt.date(2025, 1, 1)
    assert bytes(row["content"]) == FAKE_PDF
    assert row["byte_size"] == len(FAKE_PDF)
    assert row["content_sha256"] == hashlib.sha256(FAKE_PDF).hexdigest()
    assert row["extra"] == {"note": "synthetic"}
    # Defaults the store must fill in.
    assert row["fetched_at"] is not None
    assert row["parse_status"] == "pending"
    assert row["parsed_at"] is None
    assert row["parser_version"] is None
    assert row["parse_error"] is None


def test_ingest_optional_args_defaulted(sqlite_engine: Engine) -> None:
    """mime/period_hint/fetched_at/extra are optional; the NOT NULL columns must still be satisfied."""
    result = raw_store.ingest_bytes(
        sqlite_engine,
        provider="rhythm",
        doc_type="other",
        source="manual",
        content=b"synthetic payload with no mime",
        original_name="mystery.bin",
    )
    row = _fetch_row(sqlite_engine, result["id"])
    assert row["mime"] is not None  # column is NOT NULL; store must default it
    assert row["fetched_at"] is not None
    assert row["period_hint"] is None
    assert row["extra"] is None


# %%
# Dedup #


def test_dedup_identical_bytes(sqlite_engine: Engine) -> None:
    first = raw_store.ingest_bytes(
        sqlite_engine,
        provider="rhythm",
        doc_type="bill_pdf",
        source="manual",
        content=FAKE_PDF,
        original_name="Rythm 2025-01.pdf",
        mime="application/pdf",
    )
    assert first["deduped"] is False
    assert _count_rows(sqlite_engine) == 1

    # Same bytes under a different name still dedups (keyed on content_sha256).
    again = raw_store.ingest_bytes(
        sqlite_engine,
        provider="rhythm",
        doc_type="bill_pdf",
        source="email",
        content=FAKE_PDF,
        original_name="renamed copy of same bill.pdf",
        mime="application/pdf",
    )
    assert again["deduped"] is True
    assert again["id"] == first["id"]
    assert _count_rows(sqlite_engine) == 1

    other = raw_store.ingest_bytes(
        sqlite_engine,
        provider="rhythm",
        doc_type="bill_pdf",
        source="manual",
        content=FAKE_PDF_OTHER,
        original_name="Rythm 2025-02.pdf",
        mime="application/pdf",
    )
    assert other["deduped"] is False
    assert other["id"] != first["id"]
    assert _count_rows(sqlite_engine) == 2


# %%
# Classification #


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        # Rythm bill PDFs: 'Rythm YYYY-MM.pdf' -> first of month.
        ("Rythm 2025-01.pdf", ("bill_pdf", dt.date(2025, 1, 1))),
        ("Rythm 2024-11.pdf", ("bill_pdf", dt.date(2024, 11, 1))),
        # 'rhythm_bill_*_YYYY-MM-DD.pdf' -> that exact date.
        ("rhythm_bill_synthetic_2025-02-14.pdf", ("bill_pdf", dt.date(2025, 2, 14))),
        ("rhythm_bill_fake_download_2024-12-31.pdf", ("bill_pdf", dt.date(2024, 12, 31))),
        # Exact-name CSV exports.
        ("monthly_bills.csv", ("csv_export", None)),
        ("hourly_usage.csv", ("csv_export", None)),
        ("weekly_usage.csv", ("csv_export", None)),
        ("payments.csv", ("csv_export", None)),
        ("bill_line_items.csv", ("csv_export", None)),
        ("rhythm_api_invoices.csv", ("csv_export", None)),
        ("rhythm_api_orders.csv", ("csv_export", None)),
        # JSON payloads keyed by name substring.
        ("invoice-history_2025.json", ("api_invoice_json", None)),
        ("rhythm_usage_2025.json", ("api_usage_json", None)),
        ("rhythm_orders_2025.json", ("api_orders_json", None)),
        # Smart Meter Texas exports.
        ("smt_export_synthetic.csv", ("smt_export", None)),
        ("My_SMT_Data.csv", ("smt_export", None)),
        ("IntervalData_synthetic_meter.csv", ("smt_export", None)),
        # Anything else.
        ("random_notes.txt", ("other", None)),
        ("mystery.json", ("other", None)),
    ],
)
def test_classify(name: str, expected: tuple[str, dt.date | None]) -> None:
    assert ingest_raw.classify(name) == expected


def test_gather_files_skips_hidden_and_underscore_dirs(tmp_path: Any) -> None:
    """Staging dirs like _to_delete and hidden dirs must be excluded from the walk."""
    (tmp_path / "keep").mkdir()
    (tmp_path / "_to_delete").mkdir()
    (tmp_path / ".hidden").mkdir()
    (tmp_path / "top.pdf").write_bytes(b"synthetic")
    (tmp_path / "keep" / "a.csv").write_bytes(b"synthetic")
    (tmp_path / "_to_delete" / "b.csv").write_bytes(b"synthetic")
    (tmp_path / ".hidden" / "c.csv").write_bytes(b"synthetic")

    files, skips = ingest_raw.gather_files([str(tmp_path)])

    names = [(os.path.basename(f), hint) for f, hint in files]
    assert names == [("top.pdf", None), ("a.csv", "keep")]
    assert skips == []


@pytest.mark.parametrize(
    ("folder", "expected"),
    [
        ("Rythm", "rhythm"),  # alias: archive folder keeps the provider's spelling
        ("City of Georgetown", "city_of_georgetown"),
        ("Just Energy", "just_energy"),
        ("Atmos", "atmos"),
        ("---", None),
    ],
)
def test_slugify_provider(folder: str, expected: str | None) -> None:
    assert ingest_raw.slugify_provider(folder) == expected


def test_gather_files_provider_hint_from_first_level_dir(tmp_path: Any) -> None:
    """Provider comes from the first-level folder, even for nested files."""
    (tmp_path / "Rythm" / "data").mkdir(parents=True)
    (tmp_path / "Rythm" / "bill.pdf").write_bytes(b"synthetic")
    (tmp_path / "Rythm" / "data" / "monthly_bills.csv").write_bytes(b"synthetic")
    (tmp_path / "loose.pdf").write_bytes(b"synthetic")

    files, _ = ingest_raw.gather_files([str(tmp_path)])

    assert [(os.path.basename(f), hint) for f, hint in files] == [
        ("loose.pdf", None),
        ("bill.pdf", "rhythm"),
        ("monthly_bills.csv", "rhythm"),
    ]


# %%
# iter_documents / mark_parsed #


def _ingest_three(engine: Engine) -> list[int]:
    specs = [
        ("rhythm", "bill_pdf", FAKE_PDF),
        ("rhythm", "api_usage_json", FAKE_JSON),
        ("smt", "smt_export", FAKE_CSV),
    ]
    ids: list[int] = []
    for provider, doc_type, content in specs:
        result = raw_store.ingest_bytes(
            engine,
            provider=provider,
            doc_type=doc_type,
            source="manual",
            content=content,
            original_name=f"synthetic_{doc_type}.bin",
            mime="application/octet-stream",
        )
        ids.append(result["id"])
    return ids


def test_iter_documents_filters_and_ordering(sqlite_engine: Engine) -> None:
    ids = _ingest_three(sqlite_engine)

    all_rows = list(raw_store.iter_documents(sqlite_engine))
    assert [row["id"] for row in all_rows] == sorted(ids)

    rhythm_rows = list(raw_store.iter_documents(sqlite_engine, provider="rhythm"))
    assert [row["id"] for row in rhythm_rows] == sorted(ids[:2])
    assert all(row["provider"] == "rhythm" for row in rhythm_rows)

    pdf_rows = list(raw_store.iter_documents(sqlite_engine, doc_type="bill_pdf"))
    assert [row["id"] for row in pdf_rows] == [ids[0]]

    pending_rows = list(raw_store.iter_documents(sqlite_engine, parse_status="pending"))
    assert [row["id"] for row in pending_rows] == sorted(ids)

    combined = list(raw_store.iter_documents(sqlite_engine, provider="rhythm", doc_type="api_usage_json"))
    assert [row["id"] for row in combined] == [ids[1]]


def test_mark_parsed_round_trip(sqlite_engine: Engine) -> None:
    ids = _ingest_three(sqlite_engine)
    parsed_id, error_id, pending_id = ids

    raw_store.mark_parsed(sqlite_engine, parsed_id, "parsed", "v1")
    row = _fetch_row(sqlite_engine, parsed_id)
    assert row["parse_status"] == "parsed"
    assert row["parsed_at"] is not None
    assert row["parser_version"] == "v1"
    assert row["parse_error"] is None

    raw_store.mark_parsed(sqlite_engine, error_id, "error", "v1", error="synthetic parse failure")
    row = _fetch_row(sqlite_engine, error_id)
    assert row["parse_status"] == "error"
    assert row["parsed_at"] is not None
    assert row["parser_version"] == "v1"
    assert row["parse_error"] == "synthetic parse failure"

    # Filtering by status reflects the transitions; the untouched doc stays pending.
    assert [r["id"] for r in raw_store.iter_documents(sqlite_engine, parse_status="pending")] == [pending_id]
    assert [r["id"] for r in raw_store.iter_documents(sqlite_engine, parse_status="parsed")] == [parsed_id]
    assert [r["id"] for r in raw_store.iter_documents(sqlite_engine, parse_status="error")] == [error_id]


# %%
# Schema placement #


def test_metadata_schema_from_env() -> None:
    """CFC_DB_SCHEMA must land on every table in db.metadata. No connection is made."""
    saved_url = os.environ.get("CFC_DATABASE_URL")
    saved_schema = os.environ.get("CFC_DB_SCHEMA")
    try:
        # Synthetic URL string only -- never connected to.
        os.environ["CFC_DATABASE_URL"] = "postgresql+psycopg2://synthetic_user:synthetic_pw@localhost:5432/fake_db"
        os.environ["CFC_DB_SCHEMA"] = "cash_flow_commander"
        importlib.reload(db)
        assert db.metadata.tables, "expected at least one table on db.metadata"
        for table in db.metadata.tables.values():
            assert table.schema == "cash_flow_commander", f"table {table.name} not in schema"
    finally:
        _restore_env("CFC_DATABASE_URL", saved_url)
        _restore_env("CFC_DB_SCHEMA", saved_schema)
        _reload_modules()


# %%
# Postgres opt-in integration #


@pytest.mark.skipif(not POSTGRES_TEST_URL, reason="CFC_TEST_DATABASE_URL not set; Postgres test is opt-in")
def test_postgres_ingest_and_dedup() -> None:
    """Runs ingest+dedup against a real Postgres when CFC_TEST_DATABASE_URL is set.

    Cleans up only the rows it created, deleting by the exact content_sha256
    values it inserted -- never a bare DELETE.
    """
    assert POSTGRES_TEST_URL is not None
    if POSTGRES_TEST_URL.startswith("postgresql") and not os.environ.get("CFC_DB_SCHEMA"):
        pytest.skip("CFC_DB_SCHEMA must be set for the Postgres opt-in test (schema-scoped safety)")
    engine = create_engine(POSTGRES_TEST_URL)
    # Per-run unique payloads so this test never collides with existing rows.
    unique = uuid.uuid4().hex.encode()
    content_a = b"%PDF-1.4 fake pg doc A " + unique
    content_b = b"%PDF-1.4 fake pg doc B " + unique
    created_shas = [hashlib.sha256(c).hexdigest() for c in (content_a, content_b)]
    try:
        db.create_tables(engine)

        first = raw_store.ingest_bytes(
            engine,
            provider="rhythm",
            doc_type="bill_pdf",
            source="pytest",
            content=content_a,
            original_name="synthetic_pg_bill_a.pdf",
            mime="application/pdf",
        )
        assert first["deduped"] is False

        again = raw_store.ingest_bytes(
            engine,
            provider="rhythm",
            doc_type="bill_pdf",
            source="pytest",
            content=content_a,
            original_name="synthetic_pg_bill_a_copy.pdf",
            mime="application/pdf",
        )
        assert again["deduped"] is True
        assert again["id"] == first["id"]

        other = raw_store.ingest_bytes(
            engine,
            provider="rhythm",
            doc_type="bill_pdf",
            source="pytest",
            content=content_b,
            original_name="synthetic_pg_bill_b.pdf",
            mime="application/pdf",
        )
        assert other["deduped"] is False
        assert other["id"] != first["id"]

        row = _fetch_row(engine, first["id"])
        assert row["content_sha256"] == hashlib.sha256(content_a).hexdigest()
        assert row["byte_size"] == len(content_a)

        with engine.connect() as conn:
            count = conn.execute(
                select(func.count())
                .select_from(db.raw_documents)
                .where(db.raw_documents.c.content_sha256.in_(created_shas))
            ).scalar_one()
        assert count == 2  # dedup left exactly one row per distinct payload
    finally:
        with engine.begin() as conn:
            conn.execute(
                delete(db.raw_documents).where(db.raw_documents.c.content_sha256.in_(created_shas))
            )
        engine.dispose()


# %%
