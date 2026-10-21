"""KeyBank Solar Lending statement parser and the loan_transactions upsert."""

import datetime as dt
import importlib
import os
from decimal import Decimal
from typing import Any

import pytest
from sqlalchemy import select

import account_store
import db
import ingest_raw
from providers import get_parser, keybank_solar_loan


# %%
# Synthetic fixtures (never real loan ids, balances, names or addresses) #

CTX: dict[str, Any] = {"provider": "keybank_solar_loan", "account_id": None}

HEADER = """\
Payment Notice
Statement Date:
10/09/2026
Account Number:
L00-XX-0000001
Outstanding Principal:
{outstanding}
Transactions:
Trans
Date
Eff
Date
Transaction
Amount
Description
Principal
Interest
Other
Principal
Balance
09/09/2026
Beginning Balance
1,000.00
"""

FOOTER = """\
10/09/2026
Ending Balance
{ending}
Payments Due:
"""


def _statement(body: str, ending: str) -> str:
    return HEADER.format(outstanding=ending) + body + FOOTER.format(ending=ending)


PAYMENT = """\
09/22/2026
09/22/2026
-100.00
Payment
-90.00
-10.00
910.00
"""

INTEREST_ONLY = """\
09/22/2026
-10.00
P+I Interest Payment
-10.00
1,000.00
"""


class _FakePage:
    def __init__(self, text: str) -> None:
        self._text = text

    def extract_text(self) -> str:
        return self._text


def _patch_pdf_text(monkeypatch: pytest.MonkeyPatch, text: str) -> None:
    class _FakeReader:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self.pages = [_FakePage(text)]

    monkeypatch.setattr(keybank_solar_loan, "PdfReader", _FakeReader)


# %%
# Classification and routing #


def test_capture_classifies_and_routes() -> None:
    name = "keybank_solar_loan_statement_2026-10-09.pdf"
    assert ingest_raw.classify(name) == ("statement_pdf", dt.date(2026, 10, 9))
    assert get_parser("keybank_solar_loan", "statement_pdf", name)[0] is keybank_solar_loan.parse_statement_pdf


# %%
# Parser #


def test_payment_statement(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_pdf_text(monkeypatch, _statement(PAYMENT, "910.00"))
    result = keybank_solar_loan.parse_statement_pdf(b"%PDF-1.4 synthetic", CTX)
    assert result["account_balances"] == [
        {
            "provider": "keybank_solar_loan",
            "account_id": "L00-XX-0000001",
            "account_kind": "loan",
            "as_of_date": dt.date(2026, 10, 9),
            "balance": Decimal("910.00"),
            "period_start": dt.date(2026, 9, 10),
            "beginning_balance": Decimal("1000.00"),
        }
    ]
    assert result["loan_transactions"] == [
        {
            "provider": "keybank_solar_loan",
            "account_id": "L00-XX-0000001",
            "trans_date": dt.date(2026, 9, 22),
            "description": "Payment",
            "amount": Decimal("-100.00"),
            "principal": Decimal("-90.00"),
            "interest": Decimal("-10.00"),
            "principal_balance": Decimal("910.00"),
            "occurrence": 0,
        }
    ]


def test_interest_only_payment_leaves_principal(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_pdf_text(monkeypatch, _statement(INTEREST_ONLY, "1,000.00"))
    [row] = keybank_solar_loan.parse_statement_pdf(b"%PDF-1.4 synthetic", CTX)["loan_transactions"]
    assert (row["principal"], row["interest"]) == (Decimal("0.00"), Decimal("-10.00"))


def test_statement_without_transactions(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_pdf_text(monkeypatch, _statement("", "1,000.00"))
    result = keybank_solar_loan.parse_statement_pdf(b"%PDF-1.4 synthetic", CTX)
    assert result["loan_transactions"] == []
    assert result["account_balances"][0]["balance"] == Decimal("1000.00")


def test_balances_that_disagree_raise(monkeypatch: pytest.MonkeyPatch) -> None:
    text = _statement(PAYMENT, "910.00").replace("Outstanding Principal:\n910.00", "Outstanding Principal:\n911.00")
    _patch_pdf_text(monkeypatch, text)
    with pytest.raises(ValueError, match="balances disagree"):
        keybank_solar_loan.parse_statement_pdf(b"%PDF-1.4 synthetic", CTX)


def test_split_that_does_not_sum_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_pdf_text(monkeypatch, _statement(PAYMENT.replace("-10.00", "-11.00"), "910.00"))
    with pytest.raises(ValueError, match="do not sum"):
        keybank_solar_loan.parse_statement_pdf(b"%PDF-1.4 synthetic", CTX)


def test_capture_for_another_loan_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_pdf_text(monkeypatch, _statement(PAYMENT, "910.00"))
    with pytest.raises(ValueError, match="L00-XX-0000001"):
        keybank_solar_loan.parse_statement_pdf(b"%PDF-1.4 synthetic", {**CTX, "account_id": "L00-XX-0000002"})


# %%
# Store #


@pytest.fixture()
def engine(tmp_path: Any) -> Any:
    """Engine on a throwaway SQLite file, with CFC_DB_SCHEMA forced off."""
    saved = {name: os.environ.get(name) for name in ("CFC_DATABASE_URL", "CFC_DB_SCHEMA")}
    os.environ["CFC_DATABASE_URL"] = f"sqlite:///{tmp_path / 'keybank_test.db'}"
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


def test_reparsing_loan_transactions_is_idempotent(engine: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_pdf_text(monkeypatch, _statement(PAYMENT, "910.00"))
    rows = keybank_solar_loan.parse_statement_pdf(b"%PDF-1.4 synthetic", CTX)["loan_transactions"]
    rows = [{**row, "raw_document_id": None, "parser_version": keybank_solar_loan.PARSER_VERSION} for row in rows]
    account_store.upsert_loan_transactions(engine, rows)
    account_store.upsert_loan_transactions(engine, rows)
    with engine.connect() as conn:
        assert len(conn.execute(select(db.loan_transactions)).all()) == 1
