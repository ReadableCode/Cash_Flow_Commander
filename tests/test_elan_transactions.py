# %%
# Imports #

import datetime as dt
import importlib
import os
import sys
from typing import Any

import pytest
from sqlalchemy import func, select

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
for _path in (os.path.join(_ROOT, "src"), _ROOT):
    if _path not in sys.path:
        sys.path.insert(0, _path)
_PKG = os.path.join(_ROOT, "transaction_downloader")
if _PKG not in sys.path:
    sys.path.insert(0, _PKG)

import db  # noqa: E402
import providers  # noqa: E402
import store  # noqa: E402
import transaction_store  # noqa: E402
from providers import capture_names, elan  # noqa: E402

# %%
# Fixtures #

CAPTURE = "elan_csv_export_0001_20260801_20260824_captured20260824.csv"

# The one observed layout: "Date","Transaction","Name","Memo","Amount" —
# ISO dates and a single signed Amount (negative = money out; a waived fee
# prints "-0.00"). Memo is semicolon-slotted reference data on purchases and
# separator-only ("; ; ; ; ;") on fees.
CARD = (
    '"Date","Transaction","Name","Memo","Amount"\n'
    '"2026-08-01","CREDIT","PAYMENT   THANK YOU","WEB AUTOMTC; ; ; ; ;","250.00"\n'
    '"2026-08-13","DEBIT","BOOKSTORE           SEATTLE       WA","24000000000000000000000; 05942; ; ; ;","-12.34"\n'
    '"2026-08-17","DEBIT","ANNUAL MEMBERSHIP FEE","; ; ; ; ;","-0.00"\n'
)

IDENTICAL_ROWS = (
    '"Date","Transaction","Name","Memo","Amount"\n'
    '"2026-08-18","DEBIT","COFFEE SHOP","24000000000000000000001; 05814; ; ; ;","-4.75"\n'
    '"2026-08-18","DEBIT","COFFEE SHOP","24000000000000000000001; 05814; ; ; ;","-4.75"\n'
)


def _parse(text: str, name: str = CAPTURE) -> list[dict[str, Any]]:
    """Run the parser the way parse_raw does, returning the transactions sink."""
    return elan.parse_transactions_csv(text.encode("utf-8"), {"original_name": name})[
        "transactions"
    ]


def _reload_modules() -> None:
    """Reload db and everything holding references to its Table objects."""
    importlib.reload(db)
    importlib.reload(transaction_store)


def _restore_env(name: str, value: str | None) -> None:
    """Put an environment variable back the way it was."""
    if value is None:
        os.environ.pop(name, None)
    else:
        os.environ[name] = value


@pytest.fixture()
def engine(tmp_path: Any) -> Any:
    """Engine on a throwaway SQLite file, with CFC_DB_SCHEMA forced off."""
    saved_url = os.environ.get("CFC_DATABASE_URL")
    saved_schema = os.environ.get("CFC_DB_SCHEMA")
    os.environ["CFC_DATABASE_URL"] = f"sqlite:///{tmp_path / 'elan_test.db'}"
    os.environ["CFC_DB_SCHEMA"] = ""
    try:
        _reload_modules()
        eng = db.get_engine()
        db.create_tables(eng)
        yield eng
    finally:
        _restore_env("CFC_DATABASE_URL", saved_url)
        _restore_env("CFC_DB_SCHEMA", saved_schema)
        _reload_modules()


def _count(eng: Any) -> int:
    with eng.connect() as conn:
        return int(
            conn.execute(select(func.count()).select_from(db.transactions)).scalar_one()
        )


def _stamp(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Add the provenance columns parse_raw stamps on before upsert."""
    for row in rows:
        row.setdefault("raw_document_id", None)
        row.setdefault("parser_version", elan.PARSER_VERSION)
    return rows


# %%
# Layout and projection #


def test_parses_the_card_layout() -> None:
    """All three observed row shapes: payment credit, purchase, waived fee."""
    rows = _parse(CARD)

    assert len(rows) == 3
    first = rows[0]
    assert first["account_id"] == "0001"
    assert first["account_kind"] == "credit"
    assert first["description"] == "PAYMENT THANK YOU"


def test_amount_is_kept_as_signed_no_flip() -> None:
    """Elan already signs Amount with negative = money out — parse as-is."""
    rows = _parse(CARD)

    assert rows[0]["amount"] == 250.00  # payment: money in
    assert rows[1]["amount"] == -12.34  # purchase: money out

    assert rows[0]["txn_type"] == "CREDIT"
    assert rows[1]["txn_type"] == "DEBIT"


def test_signed_zero_normalizes_to_plain_zero() -> None:
    """A waived fee prints "-0.00"; the stored amount must never be -0.0."""
    rows = _parse(CARD)

    fee = rows[2]
    assert fee["amount"] == 0.0
    assert str(fee["amount"]) == "0.0"


def test_single_date_column_feeds_both_dates() -> None:
    """Elan exports one ISO Date column; inventing a second would be fabrication."""
    rows = _parse(CARD)

    assert rows[0]["txn_date"] == rows[0]["post_date"] == dt.date(2026, 8, 1)


def test_memo_keeps_reference_data_and_drops_separator_only_values() -> None:
    rows = _parse(CARD)

    assert rows[1]["memo"] == "24000000000000000000000; 05942; ; ; ;"
    assert rows[2]["memo"] is None  # "; ; ; ; ;" carries no information


def test_description_whitespace_is_squashed() -> None:
    """Elan pads Name with column-aligned runs of spaces; comparison needs stability."""
    rows = _parse(CARD)

    assert rows[1]["description"] == "BOOKSTORE SEATTLE WA"


def test_every_source_field_survives_verbatim_in_extra() -> None:
    """The typed columns are a projection; extra is the record."""
    rows = _parse(CARD)

    extra = rows[1]["extra"]
    assert extra["layout"] == "elan_card"
    assert extra["columns"] == {
        "Date": "2026-08-13",
        "Transaction": "DEBIT",
        "Name": "BOOKSTORE           SEATTLE       WA",
        "Memo": "24000000000000000000000; 05942; ; ; ;",
        "Amount": "-12.34",
    }


def test_values_past_the_last_header_are_kept() -> None:
    """A future Elan column must land in extra, not vanish."""
    text = (
        '"Date","Transaction","Name","Memo","Amount"\n'
        '"2026-08-17","DEBIT","SHOP","; ; ; ; ;","-1.00","SURPRISE"\n'
    )
    rows = _parse(text)

    assert rows[0]["extra"]["unnamed"] == ["SURPRISE"]


def test_footer_and_blank_rows_are_skipped() -> None:
    text = CARD + "\nTotals,,,,\n"
    rows = _parse(text)

    assert len(rows) == 3


# %%
# Occurrence counter #


def test_identical_same_day_rows_stay_distinct() -> None:
    """Two identical coffees must not collapse into one."""
    rows = _parse(IDENTICAL_ROWS)

    assert [row["occurrence"] for row in rows] == [0, 1]
    assert rows[0]["amount"] == rows[1]["amount"] == -4.75


# %%
# Guards #


def test_rejects_a_name_that_is_not_an_elan_capture() -> None:
    with pytest.raises(ValueError, match="cannot determine account"):
        _parse(CARD, name="Credit Card - 0001_01-01-2026_08-28-2026.csv")


def test_rejects_a_citi_capture_name() -> None:
    """A citi capture routed here by mistake must fail loudly, not misfile."""
    with pytest.raises(ValueError, match="cannot determine account"):
        _parse(
            CARD, name="citi_csv_export_0001_20260801_20260824_captured20260824.csv"
        )


def test_rejects_unrecognized_columns() -> None:
    with pytest.raises(ValueError, match="unrecognized Elan CSV columns"):
        _parse("date,total\n2026-08-01,5\n")


def test_rejects_an_empty_file() -> None:
    with pytest.raises(ValueError, match="empty"):
        _parse("")


def test_empty_window_parser_returns_nothing() -> None:
    """Coverage evidence only — and no window sink that could prune real rows."""
    assert elan.parse_empty_window(b"whatever", {}) == {}


# %%
# Capture-name plumbing #


def test_capture_meta_round_trip() -> None:
    meta = elan.capture_meta_from_name(CAPTURE)

    assert meta == {
        "provider": "elan",
        "account": "0001",
        "start": dt.date(2026, 8, 1),
        "end": dt.date(2026, 8, 24),
        "captured": dt.date(2026, 8, 24),
    }


def test_generic_parser_scopes_by_provider() -> None:
    citi_name = "citi_csv_export_9999_20260801_20260824_captured20260824.csv"

    assert capture_names.capture_meta_from_name(CAPTURE)["provider"] == "elan"
    assert capture_names.capture_meta_from_name(citi_name, "elan") is None
    assert capture_names.capture_meta_from_name("Credit Card - 0001_01-01-2026_08-28-2026.csv") is None


# %%
# Downloader store wiring #


def test_store_detects_the_elan_layout() -> None:
    described = store.read_export(CARD, provider="elan")

    assert described["layout"] == "elan_card"
    assert described["account_type"] == "credit"
    assert described["rows"] == 3
    assert described["min_date"] == "2026-08-01"
    assert described["max_date"] == "2026-08-17"


def test_store_rejects_a_citi_shaped_file_for_elan() -> None:
    citi_csv = "Status,Date,Description,Debit,Credit,Member Name\nCleared,08/17/2026,SHOP,1.00,,X\n"
    with pytest.raises(store.UnrecognizedExport):
        store.read_export(citi_csv, provider="elan")


def test_download_filename_hint_pulls_the_last_4() -> None:
    """Elan names downloads '<label> - <last4>_<start>_<end>.csv'."""
    name = "Credit Card - 0001_01-01-2026_08-28-2026.csv"
    assert store.account_hint_from_name(name, provider="elan") == "0001"
    assert store.account_hint_from_name("Date range.CSV", provider="elan") is None


# %%
# Registry wiring #


def test_registry_routes_elan_documents() -> None:
    parser = providers.get_parser("elan", "csv_export", CAPTURE)
    assert parser is not None
    assert parser[0] is elan.parse_transactions_csv

    empty = providers.get_parser(
        "elan",
        "empty_window",
        "elan_empty_window_0001_20260801_20260824_captured20260824.txt",
    )
    assert empty is not None
    assert empty[0] is elan.parse_empty_window


def test_registry_does_not_route_citi_names_to_elan() -> None:
    citi_name = "citi_csv_export_0001_20260801_20260824_captured20260824.csv"
    assert providers.get_parser("elan", "csv_export", citi_name) is None


# %%
# Database round trips #


def _sync(eng: Any, text: str, name: str, doc_id: int) -> dict[str, Any]:
    """Parse and sync one capture the way parse_raw does."""
    sinks = elan.parse_transactions_csv(text.encode("utf-8"), {"original_name": name})
    rows = _stamp(sinks["transactions"])
    window = {**sinks["transactions_window"], "raw_document_id": doc_id}
    return transaction_store.sync_capture(eng, rows, window)


def _insert_doc(eng: Any, name: str, provider: str = "elan") -> int:
    """Insert a raw_documents row so reconciliation can read capture provenance."""
    with eng.begin() as conn:
        result = conn.execute(
            db.raw_documents.insert().values(
                provider=provider,
                doc_type="csv_export",
                source="manual",
                original_name=name,
                mime="text/csv",
                byte_size=0,
                content=b"",
                content_sha256=name,  # uniqueness is all that matters here
                fetched_at=dt.datetime(2026, 8, 24, tzinfo=dt.timezone.utc),
            )
        )
        return int(result.inserted_primary_key[0])


def test_reprocessing_is_idempotent(engine: Any) -> None:
    doc = _insert_doc(engine, CAPTURE)
    first = _sync(engine, CARD, CAPTURE, doc)
    again = _sync(engine, CARD, CAPTURE, doc)

    assert first["upserted"] == 3
    assert again["removed"] == 0
    assert _count(engine) == 3


def test_an_overlapping_redownload_adds_only_what_is_new(engine: Any) -> None:
    doc1 = _insert_doc(engine, CAPTURE)
    _sync(engine, CARD, CAPTURE, doc1)

    wider_name = "elan_csv_export_0001_20260801_20260824_captured20260825.csv"
    wider = CARD + '"2026-08-20","DEBIT","NEW SHOP","; ; ; ; ;","-9.99"\n'
    doc2 = _insert_doc(engine, wider_name)
    result = _sync(engine, wider, wider_name, doc2)

    assert result["removed"] == 0
    assert _count(engine) == 4


def test_a_restated_row_is_corrected_in_place(engine: Any) -> None:
    """A changed amount is a new natural key; the stale pre-restatement row must go."""
    doc1 = _insert_doc(engine, CAPTURE)
    _sync(engine, CARD, CAPTURE, doc1)

    restated_name = "elan_csv_export_0001_20260801_20260824_captured20260825.csv"
    restated = CARD.replace("-12.34", "-15.34")
    doc2 = _insert_doc(engine, restated_name)
    result = _sync(engine, restated, restated_name, doc2)

    assert result["removed"] == 1
    assert _count(engine) == 3
    with engine.connect() as conn:
        amounts = sorted(
            float(row[0]) for row in conn.execute(select(db.transactions.c.amount))
        )
    assert -15.34 in amounts
    assert -12.34 not in amounts


def test_an_elan_capture_never_prunes_another_providers_rows(engine: Any) -> None:
    """transactions has no provider column, so a colliding last-4 needs this guard."""
    from providers import citi

    citi_name = "citi_csv_export_0001_20260801_20260824_captured20260820.csv"
    citi_csv = (
        "Status,Date,Description,Debit,Credit,Member Name\n"
        'Cleared,08/10/2026,"CITI ONLY ROW",5.00,,JANE Q MEMBER\n'
    )
    citi_doc = _insert_doc(engine, citi_name, provider="citi")
    citi_sinks = citi.parse_transactions_csv(
        citi_csv.encode(), {"original_name": citi_name}
    )
    citi_rows = citi_sinks["transactions"]
    for row in citi_rows:
        row.setdefault("raw_document_id", citi_doc)
        row.setdefault("parser_version", citi.PARSER_VERSION)
    transaction_store.sync_capture(
        engine,
        citi_rows,
        {**citi_sinks["transactions_window"], "raw_document_id": citi_doc},
    )
    assert _count(engine) == 1

    # A newer elan capture of the SAME account_id and window, without the
    # citi row in it, must not treat that row as stale.
    doc = _insert_doc(engine, CAPTURE)
    result = _sync(engine, CARD, CAPTURE, doc)

    assert result["removed"] == 0
    assert _count(engine) == 4


# %%
