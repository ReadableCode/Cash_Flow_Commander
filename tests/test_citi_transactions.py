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

import db  # noqa: E402
import providers  # noqa: E402
import transaction_store  # noqa: E402
from providers import capture_names, citi  # noqa: E402

# %%
# Fixtures #

CAPTURE = "citi_csv_export_0001_20260801_20260824_captured20260824.csv"

# The one observed layout: Status,Date,Description,Debit,Credit,Member Name.
# A charge is Debit (unsigned positive); a payment/refund is Credit, which
# Citi prints as a NEGATIVE number (verified across every live credit row).
CARD = (
    "Status,Date,Description,Debit,Credit,Member Name\n"
    'Cleared,08/17/2026,"BOOKSTORE SEATTLE WA",12.34,,JANE Q MEMBER\n'
    'Cleared,08/13/2026,"RESORT CHICAGO IL",1500.00,,JANE Q MEMBER\n'
    "Cleared,08/01/2026,AUTOPAY AUTO-PMT,,-250.00,JANE Q MEMBER\n"
)

IDENTICAL_ROWS = (
    "Status,Date,Description,Debit,Credit,Member Name\n"
    'Cleared,08/18/2026,"COFFEE SHOP",4.75,,JANE Q MEMBER\n'
    'Cleared,08/18/2026,"COFFEE SHOP",4.75,,JANE Q MEMBER\n'
)


def _parse(text: str, name: str = CAPTURE) -> list[dict[str, Any]]:
    """Run the parser the way parse_raw does, returning the transactions sink."""
    return citi.parse_transactions_csv(text.encode("utf-8"), {"original_name": name})[
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
    os.environ["CFC_DATABASE_URL"] = f"sqlite:///{tmp_path / 'citi_test.db'}"
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
        row.setdefault("parser_version", citi.PARSER_VERSION)
    return rows


# %%
# Layout and projection #


def test_parses_the_card_layout() -> None:
    """All three observed row shapes: charge, big charge, autopay credit."""
    rows = _parse(CARD)

    assert len(rows) == 3
    first = rows[0]
    assert first["account_id"] == "0001"
    assert first["account_kind"] == "credit"
    assert first["description"] == "BOOKSTORE SEATTLE WA"


def test_debit_is_money_out_and_credit_is_money_in() -> None:
    """Citi's unsigned Debit/Credit columns project onto the signed convention."""
    rows = _parse(CARD)

    assert rows[0]["amount"] == -12.34  # charge
    assert rows[1]["amount"] == -1500.00  # charge
    assert rows[2]["amount"] == 250.00  # payment: money IN despite Citi's minus sign

    assert rows[0]["txn_type"] == "DEBIT"
    assert rows[2]["txn_type"] == "CREDIT"


def test_single_date_column_feeds_both_dates() -> None:
    """Citi exports one Date column; inventing a second would be fabrication."""
    rows = _parse(CARD)

    assert rows[0]["txn_date"] == rows[0]["post_date"] == dt.date(2026, 8, 17)


def test_member_name_lands_in_memo() -> None:
    rows = _parse(CARD)

    assert rows[0]["memo"] == "JANE Q MEMBER"


def test_every_source_field_survives_verbatim_in_extra() -> None:
    """The typed columns are a projection; extra is the record."""
    rows = _parse(CARD)

    extra = rows[0]["extra"]
    assert extra["layout"] == "citi_card"
    assert extra["columns"] == {
        "Status": "Cleared",
        "Date": "08/17/2026",
        "Description": "BOOKSTORE SEATTLE WA",
        "Debit": "12.34",
        "Credit": "",
        "Member Name": "JANE Q MEMBER",
    }


def test_values_past_the_last_header_are_kept() -> None:
    """A future Citi column must land in extra, not vanish."""
    text = (
        "Status,Date,Description,Debit,Credit,Member Name\n"
        'Cleared,08/17/2026,"SHOP",1.00,,JANE Q MEMBER,SURPRISE\n'
    )
    rows = _parse(text)

    assert rows[0]["extra"]["unnamed"] == ["SURPRISE"]


def test_footer_and_blank_rows_are_skipped() -> None:
    text = CARD + "\nTotals,,,,,\n"
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


def test_rejects_a_name_that_is_not_a_citi_capture() -> None:
    with pytest.raises(ValueError, match="cannot determine account"):
        _parse(CARD, name="Date range.CSV")


def test_rejects_a_chase_capture_name() -> None:
    """A chase capture routed here by mistake must fail loudly, not misfile."""
    with pytest.raises(ValueError, match="cannot determine account"):
        _parse(
            CARD, name="chase_csv_export_0001_20260801_20260824_captured20260824.csv"
        )


def test_rejects_unrecognized_columns() -> None:
    with pytest.raises(ValueError, match="unrecognized Citi CSV columns"):
        _parse("date,total\n08/01/2026,5\n")


def test_rejects_an_empty_file() -> None:
    with pytest.raises(ValueError, match="empty"):
        _parse("")


def test_empty_window_parser_returns_nothing() -> None:
    """Coverage evidence only — and no window sink that could prune real rows."""
    assert citi.parse_empty_window(b"whatever", {}) == {}


# %%
# Capture-name plumbing #


def test_capture_meta_round_trip() -> None:
    meta = citi.capture_meta_from_name(CAPTURE)

    assert meta == {
        "provider": "citi",
        "account": "0001",
        "start": dt.date(2026, 8, 1),
        "end": dt.date(2026, 8, 24),
        "captured": dt.date(2026, 8, 24),
    }


def test_generic_parser_reads_both_providers() -> None:
    chase_name = "chase_csv_export_9999_20260801_20260822_captured20260822.csv"

    assert capture_names.capture_meta_from_name(chase_name)["provider"] == "chase"
    assert capture_names.capture_meta_from_name(CAPTURE)["provider"] == "citi"
    assert capture_names.capture_meta_from_name(chase_name, "citi") is None
    assert capture_names.capture_meta_from_name("Date range.CSV") is None


# %%
# Registry wiring #


def test_registry_routes_citi_documents() -> None:
    parser = providers.get_parser("citi", "csv_export", CAPTURE)
    assert parser is not None
    assert parser[0] is citi.parse_transactions_csv

    empty = providers.get_parser(
        "citi",
        "empty_window",
        "citi_empty_window_0001_20260801_20260824_captured20260824.txt",
    )
    assert empty is not None
    assert empty[0] is citi.parse_empty_window


def test_registry_does_not_route_chase_names_to_citi() -> None:
    chase_name = "chase_csv_export_0001_20260801_20260822_captured20260822.csv"
    assert providers.get_parser("citi", "csv_export", chase_name) is None


# %%
# Database round trips #


def _sync(eng: Any, text: str, name: str, doc_id: int) -> dict[str, Any]:
    """Parse and sync one capture the way parse_raw does."""
    sinks = citi.parse_transactions_csv(text.encode("utf-8"), {"original_name": name})
    rows = _stamp(sinks["transactions"])
    window = {**sinks["transactions_window"], "raw_document_id": doc_id}
    return transaction_store.sync_capture(eng, rows, window)


def _insert_doc(eng: Any, name: str, provider: str = "citi") -> int:
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

    wider_name = "citi_csv_export_0001_20260801_20260824_captured20260825.csv"
    wider = CARD + 'Cleared,08/20/2026,"NEW SHOP",9.99,,JANE Q MEMBER\n'
    doc2 = _insert_doc(engine, wider_name)
    result = _sync(engine, wider, wider_name, doc2)

    assert result["removed"] == 0
    assert _count(engine) == 4


def test_a_restated_row_is_corrected_in_place(engine: Any) -> None:
    """A changed amount is a new natural key; the stale pre-restatement row must go."""
    doc1 = _insert_doc(engine, CAPTURE)
    _sync(engine, CARD, CAPTURE, doc1)

    restated_name = "citi_csv_export_0001_20260801_20260824_captured20260825.csv"
    restated = CARD.replace("1500.00", "1550.00")
    doc2 = _insert_doc(engine, restated_name)
    result = _sync(engine, restated, restated_name, doc2)

    assert result["removed"] == 1
    assert _count(engine) == 3
    with engine.connect() as conn:
        amounts = sorted(
            float(row[0]) for row in conn.execute(select(db.transactions.c.amount))
        )
    assert -1550.00 in amounts
    assert -1500.00 not in amounts


def test_a_citi_capture_never_prunes_another_providers_rows(engine: Any) -> None:
    """transactions has no provider column, so a colliding last-4 needs this guard."""
    from providers import chase

    chase_name = "chase_csv_export_0001_20260801_20260824_captured20260820.csv"
    chase_csv = (
        "Transaction Date,Post Date,Description,Category,Type,Amount,Memo\n"
        "08/10/2026,08/11/2026,CHASE ONLY ROW,Shopping,Sale,-5.00,\n"
    )
    chase_doc = _insert_doc(engine, chase_name, provider="chase")
    chase_sinks = chase.parse_transactions_csv(
        chase_csv.encode(), {"original_name": chase_name}
    )
    chase_rows = chase_sinks["transactions"]
    for row in chase_rows:
        row.setdefault("raw_document_id", chase_doc)
        row.setdefault("parser_version", chase.PARSER_VERSION)
    transaction_store.sync_capture(
        engine,
        chase_rows,
        {**chase_sinks["transactions_window"], "raw_document_id": chase_doc},
    )
    assert _count(engine) == 1

    # A newer citi capture of the SAME account_id and window, without the
    # chase row in it, must not treat that row as stale.
    doc = _insert_doc(engine, CAPTURE)
    result = _sync(engine, CARD, CAPTURE, doc)

    assert result["removed"] == 0
    assert _count(engine) == 4


# %%
