"""Voya parsers, classification and the account_balances / account_activity upserts."""

import datetime as dt
import importlib
import json
import os
from decimal import Decimal
from typing import Any

import pytest
from sqlalchemy import select

import account_store
import checks
import db
import ingest_raw
from providers import get_parser, voya


# %%
# Synthetic fixtures (never real balances, plan numbers, names or addresses) #

CTX: dict[str, Any] = {"provider": "voya", "account_id": None}

STATEMENT_TEXT = """\
Your Account Summary
Account Balance on 07/01/2026 $1,000.00
Your Contributions $200.00
Employer Contributions $100.00
Withdrawals -$5.00
Investment Earnings $55.00
Account Balance on 09/30/2026 $1,350.00
Vested Balance $1,300.00
July 01, 2026 - September 30, 2026
About Your Plan
Plan Number Plan Type
900001 401K
Fee Detail for Plan 900001
Asset-Based Fee 07/10/2026 Employee PreTax Synthetic Fund -0.1000 $10.0000 -$3.00
Asset-Based Fee 07/10/2026 Employer Match Synthetic Fund -0.0500 $10.0000 -$2.00
Total Asset-Based Fee -$5.00
"""

ACTIVITY_CSV = (
    b"\nSYNTHETIC SPONSOR  900001\n"
    b"Activity Date,Activity,Fund,Money Source,# of Units,Unit Price,Amount\n\n"
    b'"01/06/2026","Contribution","1 Synthetic Fund","Employee PreTax","4.0000","10.000000","$40.00","114","001"\n'
    b'"01/06/2026","Contribution","1 Synthetic Fund","Employee PreTax","4.0000","10.000000","$40.00","114","001"\n'
    b'"01/10/2026","Fee","1 Synthetic Fund","Employee PreTax","(0.1000)","10.000000","($1.00)","245","026"\n'
)

ACCOUNTS_JSON = json.dumps(
    {
        "categorizedAccounts": [
            {
                "accType": "Investment",
                "accounts": [
                    {"planId": "900001", "accountBalance": "1400.25", "accountBalanceAsOf": "10/20/2026"},
                ],
            }
        ]
    }
).encode()


class _FakePage:
    def __init__(self, text: str) -> None:
        self._text = text

    def extract_text(self) -> str:
        return self._text


def _patch_pdf_text(monkeypatch: pytest.MonkeyPatch, text: str) -> None:
    class _FakeReader:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self.pages = [_FakePage(text)]

    monkeypatch.setattr(voya, "PdfReader", _FakeReader)


# %%
# Classification and routing #


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("voya_statement_2026-09-30.pdf", ("statement_pdf", dt.date(2026, 9, 30))),
        ("voya_activity_20000101_20261021_captured20261021.csv", ("csv_export", dt.date(2000, 1, 1))),
        ("voya_api_balance_2026-10-21.json", ("api_balance_json", None)),
    ],
)
def test_captures_classify(name: str, expected: tuple[str, Any]) -> None:
    assert ingest_raw.classify(name) == expected


def test_registry_routes_voya_documents() -> None:
    assert get_parser("voya", "statement_pdf", "voya_statement_2026-09-30.pdf")[0] is voya.parse_statement_pdf
    assert get_parser("voya", "api_balance_json", "voya_api_balance_2026-10-21.json")[0] is voya.parse_accounts_json
    assert get_parser("voya", "csv_export", "voya_activity_x.csv")[0] is voya.parse_activity_csv


# %%
# Parsers #


def test_statement_row(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_pdf_text(monkeypatch, STATEMENT_TEXT)
    [row] = voya.parse_statement_pdf(b"%PDF-1.4 synthetic", CTX)["account_balances"]
    assert row == {
        "provider": "voya",
        "account_id": "900001",
        "account_kind": "retirement",
        "as_of_date": dt.date(2026, 9, 30),
        "balance": Decimal("1350.00"),
        "vested_balance": Decimal("1300.00"),
        "period_start": dt.date(2026, 7, 1),
        "beginning_balance": Decimal("1000.00"),
        "contributions": Decimal("300.00"),
        "employer_contributions": Decimal("100.00"),
        "withdrawals": Decimal("-5.00"),
        "earnings": Decimal("55.00"),
        "fees": Decimal("-5.00"),
    }


def test_statement_without_employer_line_or_fees(monkeypatch: pytest.MonkeyPatch) -> None:
    text = (
        STATEMENT_TEXT.replace("Employer Contributions $100.00\n", "")
        .replace("Account Balance on 09/30/2026 $1,350.00", "Account Balance on 09/30/2026 $1,250.00")
        .replace("Total Asset-Based Fee -$5.00\n", "")
    )
    _patch_pdf_text(monkeypatch, text)
    [row] = voya.parse_statement_pdf(b"%PDF-1.4 synthetic", CTX)["account_balances"]
    assert row["contributions"] == Decimal("200.00")
    assert row["employer_contributions"] == Decimal("0.00")
    assert row["fees"] is None


def test_statement_that_does_not_add_up_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_pdf_text(monkeypatch, STATEMENT_TEXT.replace("Investment Earnings $55.00", "Investment Earnings $56.00"))
    with pytest.raises(ValueError, match="does not add up"):
        voya.parse_statement_pdf(b"%PDF-1.4 synthetic", CTX)


def test_capture_for_another_plan_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_pdf_text(monkeypatch, STATEMENT_TEXT)
    with pytest.raises(ValueError, match="plan 900001"):
        voya.parse_statement_pdf(b"%PDF-1.4 synthetic", {**CTX, "account_id": "900002"})


def test_accounts_json_row() -> None:
    assert voya.parse_accounts_json(ACCOUNTS_JSON, CTX) == {
        "account_balances": [
            {
                "provider": "voya",
                "account_id": "900001",
                "account_kind": "retirement",
                "as_of_date": dt.date(2026, 10, 20),
                "balance": Decimal("1400.25"),
            }
        ]
    }


def test_activity_rows_number_identical_lines() -> None:
    rows = voya.parse_activity_csv(ACTIVITY_CSV, CTX)["account_activity"]
    assert [(r["activity"], r["amount"], r["units"], r["occurrence"]) for r in rows] == [
        ("Contribution", Decimal("40.00"), Decimal("4.0000"), 0),
        ("Contribution", Decimal("40.00"), Decimal("4.0000"), 1),
        ("Fee", Decimal("-1.00"), Decimal("-0.1000"), 0),
    ]
    assert {r["account_id"] for r in rows} == {"900001"}


def test_empty_activity_window_yields_no_rows() -> None:
    content = b"\nSYNTHETIC SPONSOR  900001\nYou have no transaction history.\n\n\n"
    assert voya.parse_activity_csv(content, CTX) == {"account_activity": []}


# %%
# Store #


@pytest.fixture()
def engine(tmp_path: Any) -> Any:
    """Engine on a throwaway SQLite file, with CFC_DB_SCHEMA forced off."""
    saved = {name: os.environ.get(name) for name in ("CFC_DATABASE_URL", "CFC_DB_SCHEMA")}
    os.environ["CFC_DATABASE_URL"] = f"sqlite:///{tmp_path / 'voya_test.db'}"
    os.environ["CFC_DB_SCHEMA"] = ""
    try:
        importlib.reload(db)
        eng = db.get_engine()
        db.create_tables(eng)
        yield eng
    finally:
        for name, value in saved.items():
            if value is None:
                os.environ.pop(name, None)
            else:
                os.environ[name] = value
        importlib.reload(db)


def _stamped(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{**row, "raw_document_id": None, "parser_version": voya.PARSER_VERSION} for row in rows]


def test_snapshot_on_a_statement_date_keeps_the_flows(engine: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_pdf_text(monkeypatch, STATEMENT_TEXT)
    statement = voya.parse_statement_pdf(b"%PDF-1.4 synthetic", CTX)["account_balances"]
    account_store.upsert_balances(engine, _stamped(statement))
    snapshot = {**statement[0], "balance": Decimal("1350.01")}
    snapshot = {key: snapshot[key] for key in ("provider", "account_id", "account_kind", "as_of_date", "balance")}
    account_store.upsert_balances(engine, _stamped([snapshot]))
    with engine.connect() as conn:
        rows = conn.execute(select(db.account_balances)).mappings().all()
    assert len(rows) == 1
    assert rows[0]["balance"] == Decimal("1350.01")
    assert rows[0]["contributions"] == Decimal("300.00")


def test_reparsing_activity_is_idempotent(engine: Any) -> None:
    rows = _stamped(voya.parse_activity_csv(ACTIVITY_CSV, CTX)["account_activity"])
    account_store.upsert_activity(engine, rows)
    account_store.upsert_activity(engine, rows)
    with engine.connect() as conn:
        assert len(conn.execute(select(db.account_activity)).all()) == 3


def test_statement_chain_check_finds_a_missing_quarter(engine: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_pdf_text(monkeypatch, STATEMENT_TEXT)
    [q3] = voya.parse_statement_pdf(b"%PDF-1.4 synthetic", CTX)["account_balances"]
    q1 = {**q3, "period_start": dt.date(2026, 1, 1), "as_of_date": dt.date(2026, 3, 31), "balance": Decimal("1000.00")}
    account_store.upsert_balances(engine, _stamped([q1, q3]))
    [finding] = checks.statements_unchained(engine, None)
    assert finding["problem"] == "no statement held for 2026-04-01 .. 2026-06-30"
    q2 = {**q3, "period_start": dt.date(2026, 4, 1), "as_of_date": dt.date(2026, 6, 30), "balance": Decimal("999.00")}
    account_store.upsert_balances(engine, _stamped([q2]))
    [finding] = checks.statements_unchained(engine, None)
    assert finding["problem"] == "opens at 1000.00, the previous statement closed at 999.00"
