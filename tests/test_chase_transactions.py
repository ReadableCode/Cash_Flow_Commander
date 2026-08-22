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
from providers import chase  # noqa: E402

# %%
# Fixtures #

CAPTURE = "chase_csv_export_0001_20260801_20260822_captured20260822.csv"
CARD_CAPTURE = "chase_csv_export_0002_20260801_20260822_captured20260822.csv"

CHECKING = (
    "Details,Posting Date,Description,Amount,Type,Balance,Check or Slip #,\n"
    'DEBIT,08/19/2026,"HEB #0567  AUSTIN TX",-96.31,DEBIT_CARD,5210.09,,\n'
    'DEBIT,08/18/2026,"STARBUCKS 04412",-4.75,DEBIT_CARD,5306.40,,\n'
    'DEBIT,08/18/2026,"STARBUCKS 04412",-4.75,DEBIT_CARD,5311.15,,\n'
    'CHECK,08/12/2026,"CHECK #1042",-250.00,CHECK_PAID,5315.90,1042,\n'
    "Totals,,,,,,,\n"
)

CARD = (
    "Transaction Date,Post Date,Description,Category,Type,Amount,Memo\n"
    "08/11/2026,08/12/2026,DELTA AIR LINES,Travel,Sale,-388.20,\n"
    "08/06/2026,08/07/2026,Payment Thank You - Web,,Payment,900.00,\n"
)

CARD_LEGACY = (
    "Type,Trans Date,Post Date,Description,Amount\n"
    "SALE,12/22/2024,12/23/2024,APPLE.COM/BILL,-9.99\n"
)


def _parse(text: str, name: str = CAPTURE) -> list[dict[str, Any]]:
    """Run the parser the way parse_raw does, returning the transactions sink."""
    return chase.parse_transactions_csv(text.encode("utf-8"), {"original_name": name})["transactions"]


def _reload_modules() -> None:
    """Reload db and everything holding references to its Table objects.

    Table objects are created at db import time, so a module that imported them
    keeps the old ones after a reload — which is how a schema-mismatch error
    shows up only when the whole suite runs together.
    """
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
    """Engine on a throwaway SQLite file, with CFC_DB_SCHEMA forced off.

    CFC_DB_SCHEMA is set to "" rather than popped so db's
    load_dotenv(override=False) cannot re-populate it from a developer's real
    .env on reload; db treats "" as no schema. Without this the suite passes
    alone and fails alongside the others.
    """
    saved_url = os.environ.get("CFC_DATABASE_URL")
    saved_schema = os.environ.get("CFC_DB_SCHEMA")
    os.environ["CFC_DATABASE_URL"] = f"sqlite:///{tmp_path / 'chase_test.db'}"
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
        return int(conn.execute(select(func.count()).select_from(db.transactions)).scalar_one())


def _stamp(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Add the provenance columns parse_raw stamps on before upsert."""
    for row in rows:
        row.setdefault("raw_document_id", None)
        row.setdefault("parser_version", chase.PARSER_VERSION)
    return rows


# %%
# Layout handling #


def test_parses_the_checking_layout() -> None:
    """Bank exports give a posting date only; txn_date falls back rather than inventing one."""
    rows = _parse(CHECKING)

    assert len(rows) == 4
    first = rows[0]
    assert first["account_id"] == "0001"
    assert first["account_kind"] == "bank"
    assert first["post_date"] == dt.date(2026, 8, 19)
    assert first["txn_date"] == first["post_date"]
    assert first["amount"] == -96.31
    assert first["description"] == "HEB #0567 AUSTIN TX"  # inner whitespace collapsed
    assert first["balance"] == 5210.09


def test_parses_both_credit_card_layouts() -> None:
    """Card exports carry a separate transaction date, a category, and no balance."""
    current = _parse(CARD, CARD_CAPTURE)
    legacy = _parse(CARD_LEGACY, CARD_CAPTURE)

    assert current[0]["account_kind"] == "credit"
    assert current[0]["txn_date"] == dt.date(2026, 8, 11)
    assert current[0]["post_date"] == dt.date(2026, 8, 12)
    assert current[0]["category"] == "Travel"
    assert current[0]["balance"] is None
    assert legacy[0]["txn_type"] == "SALE"


def test_details_column_stands_in_for_a_blank_type() -> None:
    """Bank exports put DEBIT/CREDIT/CHECK in Details, not Type."""
    rows = _parse(CHECKING)

    assert rows[3]["txn_type"] == "CHECK_PAID"
    assert rows[3]["check_no"] == "1042"


def test_footer_and_blank_rows_are_skipped() -> None:
    """A totals line is not a transaction; skipping beats guessing."""
    assert all(row["description"] != "Totals" for row in _parse(CHECKING))


def test_unknown_layout_raises() -> None:
    """An unrecognized export must fail loudly, not import zero rows quietly."""
    with pytest.raises(ValueError, match="unrecognized"):
        _parse("date,total\n2026-08-01,5\n")


def test_a_file_at_the_row_cap_is_rejected_as_truncated() -> None:
    """Chase caps a report at 1,000 rows and truncates silently — refuse to accept it."""
    header = "Transaction Date,Post Date,Description,Category,Type,Amount,Memo\n"
    body = "".join(f"08/0{i % 9 + 1}/2026,08/0{i % 9 + 1}/2026,TXN {i},Shopping,Sale,-1.00,\n"
                   for i in range(chase.ROW_CAP))

    with pytest.raises(ValueError, match="truncated"):
        _parse(header + body, CARD_CAPTURE)


def test_account_must_come_from_a_capture_filename() -> None:
    """A raw Chase download has no recorded window, so it is not a capture."""
    with pytest.raises(ValueError, match="cannot determine account"):
        _parse(CHECKING, "Chase0001_Activity_20260822.CSV")


# %%
# Identical same-day transactions #


def test_identical_same_day_rows_get_distinct_occurrences() -> None:
    """Two identical coffees are two transactions, not one."""
    rows = _parse(CHECKING)
    coffees = [row for row in rows if row["description"] == "STARBUCKS 04412"]

    assert len(coffees) == 2
    assert sorted(row["occurrence"] for row in coffees) == [0, 1]


def test_occurrence_numbering_is_stable_across_reparses() -> None:
    """Reprocessing must not renumber, or every reprocess would duplicate rows."""
    assert [row["occurrence"] for row in _parse(CHECKING)] == [row["occurrence"] for row in _parse(CHECKING)]


# %%
# Upsert behavior #


def test_upsert_is_idempotent(engine: Any) -> None:
    """Re-parsing the same document writes no new rows."""
    transaction_store.upsert_transactions(engine, _stamp(_parse(CHECKING)))
    transaction_store.upsert_transactions(engine, _stamp(_parse(CHECKING)))

    assert _count(engine) == 4


def test_both_identical_coffees_survive_the_upsert(engine: Any) -> None:
    """The occurrence counter is in the natural key precisely so this holds."""
    transaction_store.upsert_transactions(engine, _stamp(_parse(CHECKING)))

    with engine.connect() as conn:
        n = conn.execute(
            select(func.count())
            .select_from(db.transactions)
            .where(db.transactions.c.description == "STARBUCKS 04412")
        ).scalar_one()
    assert n == 2


def test_an_overlapping_redownload_adds_only_what_is_new(engine: Any) -> None:
    """The whole point of erring wide: overlap costs bandwidth, not duplicate rows."""
    transaction_store.upsert_transactions(engine, _stamp(_parse(CHECKING)))
    wider = CHECKING.replace(
        "Totals,,,,,,,\n",
        'DEBIT,08/21/2026,"NEW ROW",-1.00,DEBIT_CARD,5209.09,,\nTotals,,,,,,,\n',
    )

    transaction_store.upsert_transactions(engine, _stamp(_parse(wider)))

    assert _count(engine) == 5


def test_a_restated_transaction_is_corrected_in_place(engine: Any) -> None:
    """Chase revises pending activity; a re-download must update, not duplicate."""
    transaction_store.upsert_transactions(engine, _stamp(_parse(CARD, CARD_CAPTURE)))
    restated = CARD.replace("DELTA AIR LINES,Travel", "DELTA AIR LINES,Airlines")

    transaction_store.upsert_transactions(engine, _stamp(_parse(restated, CARD_CAPTURE)))

    with engine.connect() as conn:
        category = conn.execute(
            select(db.transactions.c.category).where(db.transactions.c.description == "DELTA AIR LINES")
        ).scalar_one()
    assert category == "Airlines"
    assert _count(engine) == 2


def test_bank_and_card_accounts_coexist(engine: Any) -> None:
    """One login, several accounts, one table."""
    transaction_store.upsert_transactions(engine, _stamp(_parse(CHECKING)))
    transaction_store.upsert_transactions(engine, _stamp(_parse(CARD, CARD_CAPTURE)))

    with engine.connect() as conn:
        kinds = dict(
            conn.execute(
                select(db.transactions.c.account_id, db.transactions.c.account_kind).distinct()
            ).all()
        )
    assert kinds == {"0001": "bank", "0002": "credit"}


# %%
# Registry wiring #


def test_the_parser_is_registered_for_chase_captures() -> None:
    """parse_raw finds this parser by (provider, doc_type, filename)."""
    found = providers.get_parser("chase", "csv_export", CAPTURE)

    assert found is not None
    parse_fn, version = found
    assert parse_fn is chase.parse_transactions_csv
    assert version == chase.PARSER_VERSION


def test_a_non_chase_csv_export_does_not_match_the_chase_parser() -> None:
    """The predicate must not steal another provider's csv_export documents."""
    assert providers.get_parser("chase", "csv_export", "hourly_usage.csv") is None


# %%
# Lossless capture #


def test_every_source_column_is_kept_verbatim() -> None:
    """Typed columns are a projection; `extra` is the record."""
    row = _parse(CHECKING)[0]

    assert row["extra"]["layout"] == "checking"
    assert row["extra"]["columns"] == {
        "Details": "DEBIT",
        "Posting Date": "08/19/2026",
        "Description": "HEB #0567  AUSTIN TX",
        "Amount": "-96.31",
        "Type": "DEBIT_CARD",
        "Balance": "5210.09",
        "Check or Slip #": "",
    }
    # The header line's trailing comma yields a blank header; its value is kept
    # as an unnamed position rather than becoming a "" key.
    assert row["extra"]["unnamed"] == [""]


def test_raw_strings_are_stored_unparsed_and_untrimmed() -> None:
    """The stored copy must round-trip to the file, not to the parsed value."""
    row = _parse(CHECKING)[0]

    # Typed column is normalized; the verbatim copy keeps the double space.
    assert row["description"] == "HEB #0567 AUSTIN TX"
    assert row["extra"]["columns"]["Description"] == "HEB #0567  AUSTIN TX"
    assert row["extra"]["columns"]["Amount"] == "-96.31"      # string, not float


def test_values_past_the_last_header_are_not_dropped() -> None:
    """Real Chase bank exports emit 8 fields against 7 headers."""
    extended = (
        "Details,Posting Date,Description,Amount,Type,Balance,Check or Slip #\n"
        'DEBIT,08/19/2026,"HEB",-96.31,DEBIT_CARD,5210.09,,TRAILING\n'
    )

    row = _parse(extended)[0]

    assert row["extra"]["unnamed"] == ["TRAILING"]


def test_a_column_chase_adds_later_survives() -> None:
    """An unmapped column must land in extra rather than vanishing."""
    with_new = (
        "Transaction Date,Post Date,Description,Category,Type,Amount,Memo,Merchant ID\n"
        "08/11/2026,08/12/2026,DELTA,Travel,Sale,-388.20,,MID-99\n"
    )

    row = _parse(with_new, CARD_CAPTURE)[0]

    assert row["extra"]["columns"]["Merchant ID"] == "MID-99"


def test_capture_shape_is_deterministic() -> None:
    """Same input, same JSON — nothing is left to run-to-run judgement."""
    import json

    first = json.dumps(_parse(CHECKING)[0]["extra"], sort_keys=True)
    second = json.dumps(_parse(CHECKING)[0]["extra"], sort_keys=True)

    assert first == second


def test_extra_survives_the_round_trip_to_the_database(engine: Any) -> None:
    """A JSON column is only useful if it comes back out intact."""
    transaction_store.upsert_transactions(engine, _stamp(_parse(CHECKING)))

    with engine.connect() as conn:
        stored = conn.execute(
            select(db.transactions.c.extra).where(db.transactions.c.description == "HEB #0567 AUSTIN TX")
        ).scalar_one()
    assert stored["columns"]["Description"] == "HEB #0567  AUSTIN TX"
    assert stored["layout"] == "checking"


# %%
# Capture reconciliation #
#
# Chase restates posted amounts — a tip posts on top of the pre-tip amount —
# which changes the natural key, so a plain upsert would keep the pre-tip row
# as a phantom duplicate. sync_capture treats each capture as authoritative for
# the dates of its window that no newer capture covers: rows gone from the
# newest export are pruned, rows still present are kept — including genuinely
# identical same-day rows, which share every column except `occurrence`.

_DOC_SEQ = iter(range(10_000))


def _cap_name(account: str, start: str, end: str, captured: str) -> str:
    return f"chase_csv_export_{account}_{start}_{end}_captured{captured}.csv"


def _ingest_doc(eng: Any, name: str) -> int:
    """Insert a minimal raw_documents row so authority can be read from its name."""
    with eng.begin() as conn:
        result = conn.execute(
            db.raw_documents.insert().values(
                provider="chase", doc_type="csv_export", source="test",
                original_name=name, mime="text/csv", byte_size=0, content=b"",
                content_sha256=f"sha-{next(_DOC_SEQ)}", fetched_at=dt.datetime(2026, 8, 22),
            )
        )
        return int(result.inserted_primary_key[0])


def _sync(eng: Any, text: str, name: str, doc_id: int | None = None) -> dict[str, Any]:
    """Run one capture through parse + sync the way parse_raw does."""
    sinks = chase.parse_transactions_csv(text.encode("utf-8"), {"original_name": name})
    if doc_id is None:
        doc_id = _ingest_doc(eng, name)
    rows = _stamp(sinks["transactions"])
    for row in rows:
        row["raw_document_id"] = doc_id
    window = sinks["transactions_window"]
    window["raw_document_id"] = doc_id
    return transaction_store.sync_capture(eng, rows, window)


def _amount_of(eng: Any, description: str) -> float:
    with eng.connect() as conn:
        return float(
            conn.execute(
                select(db.transactions.c.amount).where(db.transactions.c.description == description)
            ).scalar_one()
        )


def test_a_restated_tip_replaces_the_pre_tip_row(engine: Any) -> None:
    """The pre-tip amount must not survive as a phantom second transaction."""
    window = ("20260801", "20260822")
    _sync(engine, CARD, _cap_name("0002", *window, "20260813"))
    with_tip = CARD.replace("-388.20", "-410.20")

    result = _sync(engine, with_tip, _cap_name("0002", *window, "20260815"))

    assert result["removed"] == 1
    assert _count(engine) == 2
    assert _amount_of(engine, "DELTA AIR LINES") == -410.20


def test_reprocessing_an_older_capture_cannot_resurrect_stale_rows(engine: Any) -> None:
    """Authority is per day, so processing order does not matter."""
    window = ("20260801", "20260822")
    old_name = _cap_name("0002", *window, "20260813")
    old_doc = _ingest_doc(engine, old_name)
    _sync(engine, CARD, old_name, doc_id=old_doc)
    _sync(engine, CARD.replace("-388.20", "-410.20"), _cap_name("0002", *window, "20260815"))

    result = _sync(engine, CARD, old_name, doc_id=old_doc)  # re-parse the old capture

    assert result["upserted"] == 0
    assert result["removed"] == 0
    assert _count(engine) == 2
    assert _amount_of(engine, "DELTA AIR LINES") == -410.20


def test_identical_same_day_rows_survive_reconciliation(engine: Any) -> None:
    """Two identical memberships are two real transactions; never dedupe them."""
    window = ("20260801", "20260822")
    _sync(engine, CHECKING, _cap_name("0001", *window, "20260819"))

    result = _sync(engine, CHECKING, _cap_name("0001", *window, "20260822"))

    assert result["removed"] == 0
    with engine.connect() as conn:
        n = conn.execute(
            select(func.count())
            .select_from(db.transactions)
            .where(db.transactions.c.description == "STARBUCKS 04412")
        ).scalar_one()
    assert n == 2


def test_a_row_gone_from_the_newest_export_is_pruned(engine: Any) -> None:
    """When the export itself drops one of two identical rows, so do we."""
    window = ("20260801", "20260822")
    _sync(engine, CHECKING, _cap_name("0001", *window, "20260819"))
    one_membership = CHECKING.replace(
        'DEBIT,08/18/2026,"STARBUCKS 04412",-4.75,DEBIT_CARD,5311.15,,\n', "", 1
    )

    result = _sync(engine, one_membership, _cap_name("0001", *window, "20260822"))

    assert result["removed"] == 1
    assert _count(engine) == 3


def test_reconciliation_only_prunes_inside_its_own_window(engine: Any) -> None:
    """A narrower re-download says nothing about dates it did not request."""
    _sync(engine, CHECKING, _cap_name("0001", "20260801", "20260822", "20260819"))
    # Newer but narrower: covers Aug 15-22 only, so the Aug 12 check is out of scope.
    tail_only = (
        "Details,Posting Date,Description,Amount,Type,Balance,Check or Slip #,\n"
        'DEBIT,08/19/2026,"HEB #0567  AUSTIN TX",-96.31,DEBIT_CARD,5210.09,,\n'
        'DEBIT,08/18/2026,"STARBUCKS 04412",-4.75,DEBIT_CARD,5306.40,,\n'
        'DEBIT,08/18/2026,"STARBUCKS 04412",-4.75,DEBIT_CARD,5311.15,,\n'
    )

    result = _sync(engine, tail_only, _cap_name("0001", "20260815", "20260822", "20260822"))

    assert result["removed"] == 0
    assert _count(engine) == 4  # the Aug 12 check survives


def test_a_legacy_archive_never_outranks_a_live_redownload(engine: Any) -> None:
    """import-legacy stamps the file's newest txn date, so live captures win."""
    window = ("20260801", "20260822")
    _sync(engine, CARD.replace("-388.20", "-410.20"), _cap_name("0002", *window, "20260815"))
    # As import-legacy files it: window inferred, captured = newest txn date.
    legacy = _cap_name("0002", "20260801", "20260831", "20260812")

    result = _sync(engine, CARD, legacy)

    assert result["upserted"] == 0
    assert result["skipped"] == 2
    assert result["removed"] == 0
    assert _amount_of(engine, "DELTA AIR LINES") == -410.20


def test_a_reparse_of_the_same_document_prunes_its_own_stale_keys(engine: Any) -> None:
    """A parser fix that changes keys must not leave the old keys behind."""
    name = _cap_name("0002", "20260801", "20260822", "20260815")
    doc = _ingest_doc(engine, name)
    _sync(engine, CARD, name, doc_id=doc)

    result = _sync(engine, CARD.replace("DELTA AIR LINES", "DELTA AIR LINES INC"), name, doc_id=doc)

    assert result["removed"] == 1
    assert _count(engine) == 2
    assert _amount_of(engine, "DELTA AIR LINES INC") == -388.20


# %%
