# %%
# Imports #

import datetime as dt
import importlib
import json
import os
from collections.abc import Iterator
from decimal import Decimal
from typing import Any

import pytest
from sqlalchemy import func, select
from sqlalchemy.engine import Engine, RowMapping

import db
import parse_raw
import raw_store
import usage_store
from providers import get_parser, rhythm, smt

# %%
# Synthetic fixtures (never real accounts, ESI IDs, or personal data) #

ACCOUNT_ID = "ACCT-TEST-1"
CTX: dict[str, Any] = {"account_id": ACCOUNT_ID}

# Two hourly items: one on a CST date (UTC-6) and one on a CDT date (UTC-5).
RHYTHM_USAGE_ITEMS = [
    {
        "datetime": "2022-02-26T00:00:00",
        "consumption_kwh": "1.235",
        "consumption_rate": "0.112",
        "consumption_cost": "0.138",
        "generation_kwh": "0.500",
        "generation_rate": "0.100",
        "generation_earned": "0.050",
    },
    {
        "datetime": "2025-07-01T00:00:00",
        "consumption_kwh": "2.750",
        "consumption_rate": "0.112",
        "consumption_cost": "0.308",
        "generation_kwh": "0.000",
        "generation_rate": "0.100",
        "generation_earned": "0.000",
    },
]
RHYTHM_USAGE_JSON = json.dumps(RHYTHM_USAGE_ITEMS).encode()

RHYTHM_HOURLY_CSV = (
    b"invoice_number,datetime,consumption_kwh,consumption_rate,consumption_cost,"
    b"generation_kwh,generation_rate,generation_earned\n"
    b"INV-TEST-1,2022-02-26T00:00:00,1.235,0.112,0.138,0.500,0.100,0.050\n"
    b"INV-TEST-1,2022-02-26T01:00:00,2.750,0.112,0.308,0.000,0.100,0.000\n"
)

# Leading-apostrophe ESIID (synthetic), one Surplus Generation line, one estimated
# line, and one empty-kWh DST spring-forward gap line (real SMT files emit these
# for the nonexistent local hour) that must be skipped without error.
SMT_CSV = (
    b"ESIID,USAGE_DATE,REVISION_DATE,USAGE_START_TIME,USAGE_END_TIME,"
    b"USAGE_KWH,ESTIMATED_ACTUAL,CONSUMPTION_SURPLUSGENERATION\n"
    b"'10000000000000001,02/26/2022,,00:00,00:15,0.250,A,Consumption\n"
    b"'10000000000000001,02/26/2022,,00:15,00:30,0.300,E,Consumption\n"
    b"'10000000000000001,02/26/2022,,00:30,00:45,0.100,A,Surplus Generation\n"
    b"'10000000000000001,02/26/2022,,00:45,01:00,0.275,A,Consumption\n"
    b"'10000000000000001,03/09/2025,,02:00,02:15,,,Consumption\n"
)

CST_0226_UTC = dt.datetime(2022, 2, 26, 6, 0, tzinfo=dt.timezone.utc)  # CST is UTC-6
CDT_0701_UTC = dt.datetime(2025, 7, 1, 5, 0, tzinfo=dt.timezone.utc)  # CDT is UTC-5

MALFORMED_JSON = b'{"synthetic": "not valid json'  # truncated on purpose
NO_PARSER_PAYLOAD = b"synthetic weekly email body, no parser registered"


# %%
# Helpers #


def _restore_env(name: str, value: str | None) -> None:
    if value is None:
        os.environ.pop(name, None)
    else:
        os.environ[name] = value


def _reload_modules() -> None:
    """Reload db (env is read at import time) and every module that binds db objects.

    raw_store, usage_store, and parse_raw all reference tables created at db
    import time, so they are reloaded alongside db, dependents last.
    """
    importlib.reload(db)
    importlib.reload(raw_store)
    importlib.reload(usage_store)
    importlib.reload(parse_raw)


def _utc_naive(value: dt.datetime) -> dt.datetime:
    """Normalize a timestamp read back from the DB to naive UTC (sqlite drops tzinfo)."""
    if value.tzinfo is not None:
        return value.astimezone(dt.timezone.utc).replace(tzinfo=None)
    return value


def _fetch_doc(engine: Engine, doc_id: int) -> RowMapping:
    with engine.connect() as conn:
        return conn.execute(
            select(db.raw_documents).where(db.raw_documents.c.id == doc_id)
        ).mappings().one()


def _fetch_intervals(engine: Engine, account_id: str = ACCOUNT_ID) -> list[RowMapping]:
    stmt = (
        select(db.usage_intervals)
        .where(db.usage_intervals.c.account_id == account_id)
        .order_by(db.usage_intervals.c.ts, db.usage_intervals.c.metric)
    )
    with engine.connect() as conn:
        return list(conn.execute(stmt).mappings())


def _count_intervals(engine: Engine) -> int:
    with engine.connect() as conn:
        return conn.execute(select(func.count()).select_from(db.usage_intervals)).scalar_one()


def _interval_row(ts: dt.datetime, metric: str, value: Decimal) -> dict[str, Any]:
    return {
        "account_id": ACCOUNT_ID,
        "ts": ts,
        "granularity": "hour",
        "metric": metric,
        "value": value,
        "unit": "kwh",
        "rate": None,
        "cost": None,
        "estimated": None,
        "raw_document_id": None,
        "parser_version": "test-parser/0",
    }


# %%
# Fixtures #


@pytest.fixture()
def sqlite_engine(tmp_path: Any) -> Iterator[Engine]:
    """Engine on a throwaway SQLite file, with CFC_DB_SCHEMA forced off.

    Same pattern as test_raw_store: CFC_DB_SCHEMA is set to "" (not popped) so
    db's load_dotenv(override=False) cannot re-populate it from .env on reload.
    Env is restored and modules re-reloaded in the finally.
    """
    saved_url = os.environ.get("CFC_DATABASE_URL")
    saved_schema = os.environ.get("CFC_DB_SCHEMA")
    os.environ["CFC_DATABASE_URL"] = f"sqlite:///{tmp_path / 'usage_intervals_test.db'}"
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
# Timezone correctness #


def test_rhythm_json_central_to_utc() -> None:
    """Naive US/Central inputs land as UTC: CST offset -6, CDT offset -5."""
    rows = rhythm.parse_api_usage_json(RHYTHM_USAGE_JSON, CTX)
    ts_by_item = sorted({row["ts"] for row in rows})
    assert ts_by_item == [CST_0226_UTC, CDT_0701_UTC]
    for ts in ts_by_item:
        assert ts.utcoffset() == dt.timedelta(0)


def test_smt_csv_central_to_utc() -> None:
    rows = smt.parse_interval_csv(SMT_CSV, CTX)
    first = rows[0]
    assert first["ts"] == CST_0226_UTC
    assert first["ts"].utcoffset() == dt.timedelta(0)


# %%
# rhythm.parse_api_usage_json #


def test_rhythm_api_usage_json_rows() -> None:
    rows = rhythm.parse_api_usage_json(RHYTHM_USAGE_JSON, CTX)
    assert len(rows) == 4  # consumption + generation per item

    by_key = {(row["ts"], row["metric"]): row for row in rows}
    assert set(by_key) == {
        (CST_0226_UTC, "consumption"),
        (CST_0226_UTC, "generation"),
        (CDT_0701_UTC, "consumption"),
        (CDT_0701_UTC, "generation"),
    }

    consumption = by_key[(CST_0226_UTC, "consumption")]
    assert isinstance(consumption["value"], Decimal)
    assert consumption["value"] == Decimal("1.235")  # exact, no float round-trip
    assert consumption["rate"] == Decimal("0.112")
    assert consumption["cost"] == Decimal("0.138")

    generation = by_key[(CST_0226_UTC, "generation")]
    assert generation["value"] == Decimal("0.500")
    assert generation["cost"] == Decimal("0.050")  # taken from generation_earned

    for row in rows:
        assert row["account_id"] == ACCOUNT_ID
        assert row["granularity"] == "hour"
        assert row["unit"] == "kwh"


# %%
# rhythm.parse_hourly_usage_csv #


def test_rhythm_hourly_usage_csv_rows() -> None:
    rows = rhythm.parse_hourly_usage_csv(RHYTHM_HOURLY_CSV, CTX)
    assert len(rows) == 4  # consumption + generation per line

    for row in rows:
        assert row["account_id"] == ACCOUNT_ID
        assert row["granularity"] == "hour"
        assert isinstance(row["value"], Decimal)

    by_key = {(row["ts"], row["metric"]): row for row in rows}
    hour_two = CST_0226_UTC + dt.timedelta(hours=1)
    assert set(by_key) == {
        (CST_0226_UTC, "consumption"),
        (CST_0226_UTC, "generation"),
        (hour_two, "consumption"),
        (hour_two, "generation"),
    }
    assert by_key[(CST_0226_UTC, "consumption")]["value"] == Decimal("1.235")
    assert by_key[(CST_0226_UTC, "generation")]["cost"] == Decimal("0.050")


# %%
# smt.parse_interval_csv #


def test_smt_interval_csv_rows() -> None:
    rows = smt.parse_interval_csv(SMT_CSV, CTX)
    assert len(rows) == 4  # one row per data line

    assert [row["metric"] for row in rows] == [
        "consumption",
        "consumption",
        "generation",  # 'Surplus Generation' label
        "consumption",
    ]
    assert [row["estimated"] for row in rows] == [False, True, False, False]  # A/E flags
    assert [row["value"] for row in rows] == [
        Decimal("0.250"),
        Decimal("0.300"),
        Decimal("0.100"),
        Decimal("0.275"),
    ]
    assert [row["ts"] for row in rows] == [
        CST_0226_UTC + dt.timedelta(minutes=15 * i) for i in range(4)
    ]
    for row in rows:
        # The series is keyed on the configured account, never the ESIID column.
        assert row["account_id"] == ACCOUNT_ID
        assert "10000000000000001" not in str(row["account_id"])
        assert row["granularity"] == "15min"
        assert row["unit"] == "kwh"


# %%
# get_parser dispatch #


def test_get_parser_dispatch() -> None:
    assert get_parser("rhythm", "api_usage_json", "rhythm_usage_synthetic.json") == (
        rhythm.parse_api_usage_json,
        rhythm.PARSER_VERSION,
    )
    assert get_parser("rhythm", "csv_export", "hourly_usage.csv") == (
        rhythm.parse_hourly_usage_csv,
        rhythm.PARSER_VERSION,
    )
    # csv_export is name-gated: only hourly_usage.csv has a parser.
    assert get_parser("rhythm", "csv_export", "monthly_bills.csv") is None
    assert get_parser("rhythm", "smt_export", "smt_export_synthetic.csv") == (
        smt.parse_interval_csv,
        smt.PARSER_VERSION,
    )
    assert get_parser("rhythm", "weekly_email", "synthetic_weekly.txt") is None
    assert get_parser("unknown_provider", "api_usage_json", "anything.json") is None


# %%
# Upsert idempotency #


def test_upsert_idempotent_and_series_bounds(sqlite_engine: Engine) -> None:
    t0 = dt.datetime(2025, 3, 1, 6, 0, tzinfo=dt.timezone.utc)
    t1 = t0 + dt.timedelta(hours=1)
    rows = [
        _interval_row(t0, "consumption", Decimal("1.25")),
        _interval_row(t0, "generation", Decimal("0.5")),
        _interval_row(t1, "consumption", Decimal("2.5")),
    ]

    result = usage_store.upsert_intervals(sqlite_engine, rows)
    assert result == {"upserted": 3}
    assert _count_intervals(sqlite_engine) == 3

    # Same natural keys with a changed value: no new rows, value refreshed.
    changed = [dict(row, value=row["value"] + Decimal("10")) for row in rows]
    result = usage_store.upsert_intervals(sqlite_engine, changed)
    assert result == {"upserted": 3}
    assert _count_intervals(sqlite_engine) == 3

    stored = _fetch_intervals(sqlite_engine)
    values = {(_utc_naive(row["ts"]), row["metric"]): row["value"] for row in stored}
    assert values == {
        (_utc_naive(t0), "consumption"): Decimal("11.25"),
        (_utc_naive(t0), "generation"): Decimal("10.5"),
        (_utc_naive(t1), "consumption"): Decimal("12.5"),
    }

    bounds = usage_store.series_bounds(sqlite_engine, account_id=ACCOUNT_ID)
    assert bounds["count"] == 3
    assert _utc_naive(bounds["min_ts"]) == _utc_naive(t0)
    assert _utc_naive(bounds["max_ts"]) == _utc_naive(t1)

    # Unfiltered bounds cover the same rows; a different account matches nothing.
    assert usage_store.series_bounds(sqlite_engine)["count"] == 3
    empty = usage_store.series_bounds(sqlite_engine, account_id="ACCT-TEST-OTHER")
    assert empty == {"min_ts": None, "max_ts": None, "count": 0}


# %%
# End-to-end: ingest -> parse_raw CLI -> usage_intervals #


def _ingest_doc(engine: Engine, doc_type: str, content: bytes, original_name: str) -> int:
    result = raw_store.ingest_bytes(
        engine,
        provider="rhythm",
        doc_type=doc_type,
        source="manual",
        content=content,
        original_name=original_name,
        mime="application/octet-stream",
    )
    return result["id"]


def test_parse_raw_end_to_end(sqlite_engine: Engine) -> None:
    # 1. A parseable doc: exit 0, rows landed, provenance stamped, doc marked ok.
    doc_id = _ingest_doc(
        sqlite_engine, "api_usage_json", RHYTHM_USAGE_JSON, "rhythm_usage_synthetic.json"
    )
    assert parse_raw.main(["--account-id", ACCOUNT_ID]) == 0

    intervals = _fetch_intervals(sqlite_engine)
    assert len(intervals) == 4
    for row in intervals:
        assert row["raw_document_id"] == doc_id
        assert row["parser_version"] == rhythm.PARSER_VERSION

    doc = _fetch_doc(sqlite_engine, doc_id)
    assert doc["parse_status"] == "ok"
    assert doc["parser_version"] == rhythm.PARSER_VERSION
    assert doc["parse_error"] is None

    # 2. A malformed doc: exit 1, marked error with parse_error, first doc untouched.
    bad_id = _ingest_doc(
        sqlite_engine, "api_usage_json", MALFORMED_JSON, "rhythm_usage_malformed.json"
    )
    assert parse_raw.main(["--account-id", ACCOUNT_ID]) == 1

    bad_doc = _fetch_doc(sqlite_engine, bad_id)
    assert bad_doc["parse_status"] == "error"
    assert bad_doc["parse_error"]

    doc = _fetch_doc(sqlite_engine, doc_id)
    assert doc["parse_status"] == "ok"
    assert len(_fetch_intervals(sqlite_engine)) == 4

    # 3. A doc with no registered parser stays pending.
    email_id = _ingest_doc(
        sqlite_engine, "weekly_email", NO_PARSER_PAYLOAD, "rhythm_email_weekly_synthetic.txt"
    )
    parse_raw.main(["--account-id", ACCOUNT_ID])

    email_doc = _fetch_doc(sqlite_engine, email_id)
    assert email_doc["parse_status"] == "pending"
    assert email_doc["parser_version"] is None
    # The error doc was not retried (default scope is pending docs) and rows are unchanged.
    assert _fetch_doc(sqlite_engine, bad_id)["parse_status"] == "error"
    assert len(_fetch_intervals(sqlite_engine)) == 4


# %%
