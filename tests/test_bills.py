# %%
# Imports #

import datetime as dt
import importlib
import json
import os
from collections.abc import Iterator
from decimal import Decimal
from typing import Any

import pypdf
import pytest
from sqlalchemy import func, select
from sqlalchemy.engine import Engine, RowMapping

import bill_store
import db
import parse_raw
import raw_store
import usage_store
from providers import rhythm

# %%
# Synthetic fixtures (never real invoices, amounts, addresses, or personal data) #

ACCOUNT_ID = "ACCT-TEST-1"
CTX: dict[str, Any] = {"account_id": ACCOUNT_ID}

# One invoice-history API page: DRF-style pagination wrapper, ISO date strings,
# numeric fields as strings — matching the portal payload shape.
API_INVOICE_PAGE = {
    "count": 2,
    "next": None,
    "previous": None,
    "results": [
        {
            "id": "synthetic-invoice-id-0001",
            "amount": "223.50",
            "balance": "223.50",
            "due_date": "2026-08-17",
            "invoice_date": "2026-07-30",
            "invoice_number": "INV-TEST-0001",
            "late_fee_applied": False,
            "reward_credit_applied": False,
            "service_end_date": "2026-07-28",
            "service_start_date": "2026-06-28",
            "total_kwh": "1100.000",
            "invoice_type": "ENERGY",
        },
        {
            "id": "synthetic-invoice-id-0002",
            "amount": "114.50",
            "balance": "0.00",
            "due_date": "2022-02-16",
            "invoice_date": "2022-01-30",
            "invoice_number": "INV-TEST-0002",
            "late_fee_applied": True,
            "reward_credit_applied": False,
            "service_end_date": "2022-01-28",
            "service_start_date": "2021-12-29",
            "total_kwh": "1000.000",
            "invoice_type": "ENERGY",
        },
    ],
}
API_INVOICE_JSON = json.dumps(API_INVOICE_PAGE).encode()

# payments.csv export: three payments, the middle one with a blank confirmation.
PAYMENTS_CSV = (
    b"payment_email_date,amount_paid,confirmation_number,gmail_message_id\n"
    b"2026-06-16,120.00,CONF-TEST-0001,msg-test-0001\n"
    b"2026-07-16,121.00,,msg-test-0002\n"
    b"2026-08-17,223.50,CONF-TEST-0003,msg-test-0003\n"
)

# 2026-era sectioned layout: labels above values, Energy/Non Energy blocks,
# Previous Balance Summary block, trailing Solar Buyback Credit Summary.
# All numbers invented; internally consistent to the cent:
#   energy 15.50 + 155.00 - 5.00 = 165.50; non-energy 9.95 + 40.00 + 4.05
#   + 2.00 + 0.25 + 1.75 = 58.00; current = 223.50.
BILL_PDF_TEXT_2026 = """\
Account Overview SERVICE ADDRESS
100 SYNTHETIC TEST LN
TESTVILLE, TX 00000
ACCOUNT NUMBER ACCT-TEST-1
ESI ID 10000000000000001
Usage History SERVICE ADDRESS USAGE
1,100.000 kWh

INVOICE DATE:
Jul 30, 2026
SERVICE PERIOD:
Jun 28, 2026 -- Jul 28, 2026
Total Amount Due
on Aug 17, 2026
$223.50
For more information about residential electric service please visit www.powertochoose.com
Agreement Details
Contract Term
06/28/2026 to 07/28/2026
Plan Name
Synthetic Buyback Flex
Contract Rate
15.500 (¢/kWh)
The average price you paid for electric service this month
20.3 (¢/kWh) Contract Rate + TDU Delivery Charges
Invoice Breakdown
Previous Balance Summary
Previous Balance $120.00
Energy Charges $100.00
Non Energy Charges $20.00
Payment -- Jul 16, 2026 -$120.00
Total Previous Balance Summary $0.00
Current Charges
Energy Charges
Rhythm Energy Charge 100.000 kWh x 15.500 ¢/kWh $15.50
Rhythm Energy Charge 1,000.000 kWh x 15.500 ¢/kWh $155.00
Solar Buyback Credit - Applied Towards Energy -$5.00
Total Energy Charges $165.50
Non Energy Charges
Rhythm Base Charge $9.95
Oncor - Delivery charge per kWh $40.00
Oncor - Delivery charge per month $4.05
Sales & Gross Receipt Taxes
City Sales Tax $2.00
PUC Assessment $0.25
Misc Gross Receipts Tax Reimbursement $1.75
Total Non-Energy Charges $58.00
Total Current Charges $223.50
Total Amount Due $223.50
Forward Balance $0.00
Bill credits that carry-over to your next invoice
(Excludes Solar Buyback Credits)

INVOICE DATE:
Jul 30, 2026
SERVICE PERIOD:
Jun 28, 2026 -- Jul 28, 2026
Solar Buyback Credit Summary
Previous Solar Buyback Credit Balance $0.00
Solar Buyback Credit 200.000 kWh x 2.50 ¢/kWh -$5.00
Solar Buyback Credits Earned $5.00
Solar Buyback Credits Applied -$5.00
Solar Buyback Credit Balance $0.00
Solar Buyback Credits that carry-over to your next invoice
"""

# 2022-era flat layout: labels below values in the agreement block, one flat
# Current Charges list, solar credit under Payments and Adjustments.
# Consistent to the cent: 82.50 + 3.50 + 40.00 + 2.50 + 0.20 + 2.30 = 131.00;
# amount due = 131.00 - 16.50 = 114.50.
BILL_PDF_TEXT_2022 = """\
Account Overview SERVICE ADDRESS
100 SYNTHETIC TEST LN
TESTVILLE, TX 00000
ESI ID 10000000000000001
ACCOUNT NUMBER ACCT-TEST-1

INVOICE DATE:
Jan 30, 2022
SERVICE PERIOD:
Dec 29, 2021 -- Jan 28, 2022
Total Amount Due
$114.50
Automatic payment scheduled
for Feb 16, 2022
For more information about residential electric service please visit www.powertochoose.com
Agreement Details
12/29/2021 to 12/29/2024
Contract Valid
Synthetic Green 36
Product Type
8.25 (¢/kWh)
Contract Rate
12.0 (¢/kWh) Rhythm + TDU Charges
The average price you paid for electric service this month
Payment Breakdown
Current Charges
ELECTRICAL SERVICE
Rhythm Energy Charge 1,000 kWh x 8.25¢/kWh $82.50
TDU Delivery Charge - Base $3.50
TDU Delivery Charge - Energy $40.00
SALES & GROSS RECEIPT TAXES
City Sales Tax $2.50
PUC Assessment $0.20
Misc Gross Receipts Tax Reimbursement $2.30
Total Current Charges $131.00
Payments and Adjustments
Credit -- Jan 30, 2022 -- Solar Buyback Credit 200 kWh generated -$16.50
Total Payments and Adjustments -$16.50
Previous Balance $0.00
Total Amount Due $114.50
Forward Balance $0.00
Bill credits that carry-over to your next invoice
"""

# (section, category, description label, quantity_kwh, rate_cents_kwh, amount)
# in document order; line_no is 1-based document order.
EXPECTED_2026_ITEMS: list[tuple[str, str, str, Decimal | None, Decimal | None, Decimal]] = [
    ("energy", "energy", "Rhythm Energy Charge", Decimal("100.000"), Decimal("15.500"), Decimal("15.50")),
    ("energy", "energy", "Rhythm Energy Charge", Decimal("1000.000"), Decimal("15.500"), Decimal("155.00")),
    ("energy", "credit", "Solar Buyback Credit - Applied Towards Energy", None, None, Decimal("-5.00")),
    ("non_energy", "base", "Rhythm Base Charge", None, None, Decimal("9.95")),
    ("non_energy", "delivery", "Oncor - Delivery charge per kWh", None, None, Decimal("40.00")),
    ("non_energy", "delivery", "Oncor - Delivery charge per month", None, None, Decimal("4.05")),
    ("non_energy", "tax", "City Sales Tax", None, None, Decimal("2.00")),
    ("non_energy", "tax", "PUC Assessment", None, None, Decimal("0.25")),
    ("non_energy", "tax", "Misc Gross Receipts Tax Reimbursement", None, None, Decimal("1.75")),
]

EXPECTED_2022_ITEMS: list[tuple[str, str, str, Decimal | None, Decimal | None, Decimal]] = [
    ("current", "energy", "Rhythm Energy Charge", Decimal("1000"), Decimal("8.25"), Decimal("82.50")),
    ("current", "delivery", "TDU Delivery Charge - Base", None, None, Decimal("3.50")),
    ("current", "delivery", "TDU Delivery Charge - Energy", None, None, Decimal("40.00")),
    ("current", "tax", "City Sales Tax", None, None, Decimal("2.50")),
    ("current", "tax", "PUC Assessment", None, None, Decimal("0.20")),
    ("current", "tax", "Misc Gross Receipts Tax Reimbursement", None, None, Decimal("2.30")),
]


# %%
# Helpers #


def _restore_env(name: str, value: str | None) -> None:
    if value is None:
        os.environ.pop(name, None)
    else:
        os.environ[name] = value


def _reload_modules() -> None:
    """Reload db (env is read at import time) and every module that binds db objects.

    raw_store, usage_store, bill_store, and parse_raw all reference tables
    created at db import time, so they are reloaded alongside db, dependents
    last.
    """
    importlib.reload(db)
    importlib.reload(raw_store)
    importlib.reload(usage_store)
    importlib.reload(bill_store)
    importlib.reload(parse_raw)


class _FakePage:
    def __init__(self, text: str) -> None:
        self._text = text

    def extract_text(self) -> str:
        return self._text


def _patch_pdf_text(monkeypatch: pytest.MonkeyPatch, text: str) -> None:
    """Make pypdf text extraction return `text` regardless of the PDF bytes.

    Building a text-bearing PDF in-test is impractical, and the parser
    contract fixes extraction to pypdf.PdfReader — so that factory is
    replaced. Both import styles are covered: the module attribute
    (`pypdf.PdfReader`) and a from-import bound on the rhythm module.
    """

    class _FakeReader:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self.pages = [_FakePage(text)]

    monkeypatch.setattr(pypdf, "PdfReader", _FakeReader)
    if hasattr(rhythm, "PdfReader"):
        monkeypatch.setattr(rhythm, "PdfReader", _FakeReader)


def _assert_line_items(
    items: list[dict[str, Any]],
    expected: list[tuple[str, str, str, Decimal | None, Decimal | None, Decimal]],
) -> None:
    """Assert parsed line items match (section, category, label, qty, rate, amount) in order."""
    assert len(items) >= len(expected)
    for line_no, (item, exp) in enumerate(zip(items, expected), start=1):
        section, category, label, quantity, rate, amount = exp
        assert item["line_no"] == line_no
        assert item["section"] == section, (line_no, item)
        assert item["category"] == category, (line_no, item)
        assert item["description"].startswith(label), (line_no, item)
        assert item["quantity_kwh"] == quantity, (line_no, item)
        assert item["rate_cents_kwh"] == rate, (line_no, item)
        assert item["amount"] == amount, (line_no, item)
        assert isinstance(item["amount"], Decimal)


def _assert_sections_sum_to_total(entry: dict[str, Any]) -> None:
    """The roadmap's to-the-cent rule: charge sections sum to total_current_charges."""
    charge_sum = sum(
        item["amount"]
        for item in entry["line_items"]
        if item["section"] in ("energy", "non_energy", "current")
    )
    assert charge_sum == entry["bill_patch"]["total_current_charges"]


def _fetch_doc(engine: Engine, doc_id: int) -> RowMapping:
    with engine.connect() as conn:
        return conn.execute(
            select(db.raw_documents).where(db.raw_documents.c.id == doc_id)
        ).mappings().one()


def _fetch_bill(engine: Engine, invoice_number: str) -> RowMapping:
    stmt = select(db.bills).where(
        db.bills.c.account_id == ACCOUNT_ID,
        db.bills.c.invoice_number == invoice_number,
    )
    with engine.connect() as conn:
        return conn.execute(stmt).mappings().one()


def _fetch_line_items(engine: Engine, invoice_number: str) -> list[RowMapping]:
    stmt = (
        select(db.bill_line_items)
        .where(
            db.bill_line_items.c.account_id == ACCOUNT_ID,
            db.bill_line_items.c.invoice_number == invoice_number,
        )
        .order_by(db.bill_line_items.c.line_no)
    )
    with engine.connect() as conn:
        return list(conn.execute(stmt).mappings())


def _count_rows(engine: Engine, table: Any) -> int:
    with engine.connect() as conn:
        return conn.execute(select(func.count()).select_from(table)).scalar_one()


def _api_bill_row(invoice_number: str, invoice_date: dt.date) -> dict[str, Any]:
    """A bills row shaped like the API invoice parser's output (its column set)."""
    return {
        "account_id": ACCOUNT_ID,
        "invoice_number": invoice_number,
        "invoice_date": invoice_date,
        "service_start": invoice_date - dt.timedelta(days=32),
        "service_end": invoice_date - dt.timedelta(days=2),
        "due_date": invoice_date + dt.timedelta(days=18),
        "total_kwh": Decimal("1100.000"),
        "amount_due": Decimal("223.50"),
        "balance": Decimal("223.50"),
        "late_fee_applied": False,
        "invoice_type": "ENERGY",
        "raw_document_id": None,
        "parser_version": "test-bills/0",
    }


def _pdf_patch_row(invoice_date: dt.date) -> dict[str, Any]:
    """A bill patch shaped like the PDF parser's output (disjoint column set, no invoice_number)."""
    return {
        "account_id": ACCOUNT_ID,
        "invoice_date": invoice_date,
        "plan_name": "Synthetic Buyback Flex",
        "contract_rate_cents_kwh": Decimal("15.5000"),
        "previous_balance": Decimal("120.00"),
        "total_current_charges": Decimal("223.50"),
        "forward_balance": Decimal("0.00"),
        "raw_document_id": None,
        "parser_version": "test-bills/0",
    }


def _pdf_line_item(line_no: int, description: str, amount: Decimal) -> dict[str, Any]:
    return {
        "line_no": line_no,
        "section": "energy",
        "category": "energy",
        "description": description,
        "quantity_kwh": None,
        "rate_cents_kwh": None,
        "amount": amount,
        "raw_document_id": None,
        "parser_version": "test-bills/0",
    }


# %%
# Fixtures #


@pytest.fixture()
def sqlite_engine(tmp_path: Any) -> Iterator[Engine]:
    """Engine on a throwaway SQLite file, with CFC_DB_SCHEMA forced off.

    Same pattern as test_usage_intervals: CFC_DB_SCHEMA is set to "" (not
    popped) so db's load_dotenv(override=False) cannot re-populate it from
    .env on reload. Env is restored and modules re-reloaded in the finally.
    """
    saved_url = os.environ.get("CFC_DATABASE_URL")
    saved_schema = os.environ.get("CFC_DB_SCHEMA")
    os.environ["CFC_DATABASE_URL"] = f"sqlite:///{tmp_path / 'bills_test.db'}"
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
# rhythm.parse_api_invoice_json #


def test_parse_api_invoice_json_rows() -> None:
    result = rhythm.parse_api_invoice_json(API_INVOICE_JSON, CTX)
    assert set(result) == {"bills"}
    bills = result["bills"]
    assert len(bills) == 2

    by_invoice = {row["invoice_number"]: row for row in bills}
    assert set(by_invoice) == {"INV-TEST-0001", "INV-TEST-0002"}

    first = by_invoice["INV-TEST-0001"]
    assert first["account_id"] == ACCOUNT_ID
    # ISO strings become dates; service_start/_end map from service_*_date.
    assert first["invoice_date"] == dt.date(2026, 7, 30)
    assert first["service_start"] == dt.date(2026, 6, 28)
    assert first["service_end"] == dt.date(2026, 7, 28)
    assert first["due_date"] == dt.date(2026, 8, 17)
    # String numerics become Decimal, exactly; API 'amount' lands as amount_due.
    assert isinstance(first["amount_due"], Decimal)
    assert first["amount_due"] == Decimal("223.50")
    assert "amount" not in first
    assert first["balance"] == Decimal("223.50")
    assert isinstance(first["total_kwh"], Decimal)
    assert first["total_kwh"] == Decimal("1100.000")
    assert first["late_fee_applied"] is False
    assert first["invoice_type"] == "ENERGY"

    second = by_invoice["INV-TEST-0002"]
    assert second["invoice_date"] == dt.date(2022, 1, 30)
    assert second["amount_due"] == Decimal("114.50")
    assert second["balance"] == Decimal("0.00")
    assert second["late_fee_applied"] is True


# %%
# rhythm.parse_payments_csv #


def test_parse_payments_csv_rows() -> None:
    result = rhythm.parse_payments_csv(PAYMENTS_CSV, CTX)
    assert set(result) == {"payments"}
    payments = result["payments"]
    assert len(payments) == 3

    for row in payments:
        assert row["account_id"] == ACCOUNT_ID
        assert isinstance(row["paid_at"], dt.date)
        assert isinstance(row["amount"], Decimal)

    by_date = {row["paid_at"]: row for row in payments}
    assert set(by_date) == {dt.date(2026, 6, 16), dt.date(2026, 7, 16), dt.date(2026, 8, 17)}
    assert by_date[dt.date(2026, 6, 16)]["amount"] == Decimal("120.00")
    assert by_date[dt.date(2026, 6, 16)]["confirmation"] == "CONF-TEST-0001"
    assert by_date[dt.date(2026, 6, 16)]["source_message_id"] == "msg-test-0001"
    # Blank confirmation_number lands as None, not "".
    assert by_date[dt.date(2026, 7, 16)]["confirmation"] is None
    assert by_date[dt.date(2026, 7, 16)]["source_message_id"] == "msg-test-0002"
    assert by_date[dt.date(2026, 8, 17)]["amount"] == Decimal("223.50")


# %%
# rhythm.parse_bill_pdf #


def test_parse_bill_pdf_2026_sectioned_layout(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_pdf_text(monkeypatch, BILL_PDF_TEXT_2026)
    result = rhythm.parse_bill_pdf(b"%PDF-1.4 synthetic bytes", CTX)
    assert set(result) == {"pdf_bills"}
    assert len(result["pdf_bills"]) == 1
    entry = result["pdf_bills"][0]

    patch = entry["bill_patch"]
    assert patch["account_id"] == ACCOUNT_ID
    assert patch["invoice_date"] == dt.date(2026, 7, 30)
    assert patch["service_start"] == dt.date(2026, 6, 28)
    assert patch["service_end"] == dt.date(2026, 7, 28)
    assert patch["plan_name"] == "Synthetic Buyback Flex"
    assert patch["contract_rate_cents_kwh"] == Decimal("15.500")
    assert patch["previous_balance"] == Decimal("120.00")
    assert patch["total_current_charges"] == Decimal("223.50")
    assert patch["forward_balance"] == Decimal("0.00")
    # The PDF patch never carries the API parser's columns.
    assert "invoice_number" not in patch
    assert "amount_due" not in patch

    items = entry["line_items"]
    assert len(items) == 9
    _assert_line_items(items, EXPECTED_2026_ITEMS)
    _assert_sections_sum_to_total(entry)


def test_parse_bill_pdf_2022_flat_layout(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_pdf_text(monkeypatch, BILL_PDF_TEXT_2022)
    result = rhythm.parse_bill_pdf(b"%PDF-1.4 synthetic bytes", CTX)
    entry = result["pdf_bills"][0]

    patch = entry["bill_patch"]
    assert patch["invoice_date"] == dt.date(2022, 1, 30)
    assert patch["service_start"] == dt.date(2021, 12, 29)
    assert patch["service_end"] == dt.date(2022, 1, 28)
    assert patch["plan_name"] == "Synthetic Green 36"
    assert patch["contract_rate_cents_kwh"] == Decimal("8.25")
    assert patch["previous_balance"] == Decimal("0.00")
    assert patch["total_current_charges"] == Decimal("131.00")
    assert patch["forward_balance"] == Decimal("0.00")

    items = entry["line_items"]
    assert len(items) == 7
    _assert_line_items(items[:6], EXPECTED_2022_ITEMS)
    # The solar credit sits under Payments and Adjustments: signed negative,
    # excluded from the charge-section sum.
    credit = items[6]
    assert credit["line_no"] == 7
    assert credit["section"] == "adjustment"
    assert credit["category"] == "credit"
    assert "Solar Buyback Credit" in credit["description"]
    assert credit["amount"] == Decimal("-16.50")
    _assert_sections_sum_to_total(entry)


def test_parse_bill_pdf_total_mismatch_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    mismatched = BILL_PDF_TEXT_2026.replace(
        "Total Current Charges $223.50", "Total Current Charges $999.99"
    )
    _patch_pdf_text(monkeypatch, mismatched)
    # A failed to-the-cent validation must fail the doc, never emit wrong data.
    with pytest.raises(ValueError):
        rhythm.parse_bill_pdf(b"%PDF-1.4 synthetic bytes", CTX)


# %%
# bill_store.upsert_bills #


def test_upsert_bills_partial_column_semantics(sqlite_engine: Engine) -> None:
    invoice_date = dt.date(2026, 7, 30)
    api_row = _api_bill_row("INV-TEST-0001", invoice_date)
    assert bill_store.upsert_bills(sqlite_engine, [api_row]) == {"upserted": 1}

    # A patch-style row with a different column set (plus the natural key):
    # only its columns update; the API columns survive untouched.
    patch_row = {**_pdf_patch_row(invoice_date), "invoice_number": "INV-TEST-0001"}
    assert bill_store.upsert_bills(sqlite_engine, [patch_row]) == {"upserted": 1}

    assert _count_rows(sqlite_engine, db.bills) == 1
    stored = _fetch_bill(sqlite_engine, "INV-TEST-0001")
    # API-owned columns still present after the patch upsert.
    assert stored["amount_due"] == Decimal("223.50")
    assert stored["balance"] == Decimal("223.50")
    assert stored["due_date"] == api_row["due_date"]
    assert not stored["late_fee_applied"]
    # Patch-owned columns landed.
    assert stored["plan_name"] == "Synthetic Buyback Flex"
    assert stored["contract_rate_cents_kwh"] == Decimal("15.5000")
    assert stored["previous_balance"] == Decimal("120.00")
    assert stored["total_current_charges"] == Decimal("223.50")
    assert stored["forward_balance"] == Decimal("0.00")

    # Re-sending the API row with a changed value refreshes it without
    # clobbering the patch columns (only-present-columns rule, both ways).
    assert bill_store.upsert_bills(
        sqlite_engine, [dict(api_row, amount_due=Decimal("230.00"))]
    ) == {"upserted": 1}
    stored = _fetch_bill(sqlite_engine, "INV-TEST-0001")
    assert stored["amount_due"] == Decimal("230.00")
    assert stored["plan_name"] == "Synthetic Buyback Flex"
    assert _count_rows(sqlite_engine, db.bills) == 1


# %%
# bill_store.apply_pdf_bill #


def test_apply_pdf_bill_happy_and_shrink(sqlite_engine: Engine) -> None:
    invoice_date = dt.date(2026, 7, 30)
    bill_store.upsert_bills(sqlite_engine, [_api_bill_row("INV-TEST-0001", invoice_date)])

    doc = {
        "bill_patch": _pdf_patch_row(invoice_date),
        "line_items": [
            _pdf_line_item(1, "Rhythm Energy Charge", Decimal("170.50")),
            _pdf_line_item(2, "Solar Buyback Credit - Applied Towards Energy", Decimal("-5.00")),
            _pdf_line_item(3, "Rhythm Base Charge", Decimal("9.95")),
        ],
    }
    result = bill_store.apply_pdf_bill(sqlite_engine, [doc])
    assert result == {"bills_patched": 1, "line_items": 3, "unresolved": []}

    stored = _fetch_bill(sqlite_engine, "INV-TEST-0001")
    assert stored["plan_name"] == "Synthetic Buyback Flex"
    assert stored["amount_due"] == Decimal("223.50")  # API column untouched

    # invoice_number was resolved via (account_id, invoice_date).
    items = _fetch_line_items(sqlite_engine, "INV-TEST-0001")
    assert [row["line_no"] for row in items] == [1, 2, 3]
    assert items[1]["amount"] == Decimal("-5.00")

    # Re-apply with fewer items: delete-then-insert leaves no stale rows.
    shrunk = {
        "bill_patch": _pdf_patch_row(invoice_date),
        "line_items": [_pdf_line_item(1, "Rhythm Energy Charge", Decimal("165.50"))],
    }
    result = bill_store.apply_pdf_bill(sqlite_engine, [shrunk])
    assert result == {"bills_patched": 1, "line_items": 1, "unresolved": []}
    items = _fetch_line_items(sqlite_engine, "INV-TEST-0001")
    assert len(items) == 1
    assert items[0]["amount"] == Decimal("165.50")


def test_apply_pdf_bill_unresolved(sqlite_engine: Engine) -> None:
    bill_store.upsert_bills(sqlite_engine, [_api_bill_row("INV-TEST-0001", dt.date(2026, 7, 30))])

    # No bill exists at this invoice_date: reported unresolved, nothing written.
    doc = {
        "bill_patch": _pdf_patch_row(dt.date(2026, 1, 15)),
        "line_items": [_pdf_line_item(1, "Rhythm Energy Charge", Decimal("165.50"))],
    }
    result = bill_store.apply_pdf_bill(sqlite_engine, [doc])
    assert result["bills_patched"] == 0
    assert result["line_items"] == 0
    assert len(result["unresolved"]) == 1

    assert _count_rows(sqlite_engine, db.bill_line_items) == 0
    stored = _fetch_bill(sqlite_engine, "INV-TEST-0001")
    assert stored["plan_name"] is None  # the patch never landed anywhere


# %%
# bill_store.upsert_payments #


def test_upsert_payments_idempotent(sqlite_engine: Engine) -> None:
    rows = [
        {
            "account_id": ACCOUNT_ID,
            "paid_at": dt.date(2026, 6, 16),
            "amount": Decimal("120.00"),
            "confirmation": None,
            "source_message_id": "msg-test-0001",
            "raw_document_id": None,
            "parser_version": "test-bills/0",
        },
        {
            "account_id": ACCOUNT_ID,
            "paid_at": dt.date(2026, 7, 16),
            "amount": Decimal("121.00"),
            "confirmation": "CONF-TEST-0002",
            "source_message_id": "msg-test-0002",
            "raw_document_id": None,
            "parser_version": "test-bills/0",
        },
    ]
    assert bill_store.upsert_payments(sqlite_engine, rows) == {"upserted": 2}
    assert _count_rows(sqlite_engine, db.payments) == 2

    # Same natural key with a filled confirmation: no new row, detail refreshed.
    assert bill_store.upsert_payments(
        sqlite_engine, [dict(rows[0], confirmation="CONF-TEST-0001")]
    ) == {"upserted": 1}
    assert _count_rows(sqlite_engine, db.payments) == 2
    with sqlite_engine.connect() as conn:
        stored = conn.execute(
            select(db.payments).order_by(db.payments.c.paid_at)
        ).mappings().all()
    assert stored[0]["confirmation"] == "CONF-TEST-0001"
    assert stored[1]["confirmation"] == "CONF-TEST-0002"


# %%
# End-to-end: ingest -> parse_raw CLI -> bills + line items #


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


def test_parse_raw_bills_end_to_end(
    sqlite_engine: Engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The PDF is ingested FIRST (lower id) — it must still resolve, because
    # parse_raw processes api_invoice_json before bill_pdf within one run.
    _patch_pdf_text(monkeypatch, BILL_PDF_TEXT_2026)
    pdf_id = _ingest_doc(
        sqlite_engine, "bill_pdf", b"%PDF-1.4 synthetic bytes", "rhythm_bill_synthetic_2026-07.pdf"
    )
    api_id = _ingest_doc(
        sqlite_engine, "api_invoice_json", API_INVOICE_JSON, "rhythm_api_invoice-history_p1_synthetic.json"
    )
    assert pdf_id < api_id

    assert parse_raw.main(["--account-id", ACCOUNT_ID]) == 0

    # Both docs parsed ok with the bill parser version.
    for doc_id in (pdf_id, api_id):
        doc = _fetch_doc(sqlite_engine, doc_id)
        assert doc["parse_status"] == "ok", doc["parse_error"]
        assert doc["parser_version"] == rhythm.BILL_PARSER_VERSION
        assert doc["parse_error"] is None

    # One merged bill row: API-owned columns AND PDF patch columns together.
    assert _count_rows(sqlite_engine, db.bills) == 2  # INV-TEST-0001 + INV-TEST-0002
    bill = _fetch_bill(sqlite_engine, "INV-TEST-0001")
    assert bill["amount_due"] == Decimal("223.50")
    assert bill["balance"] == Decimal("223.50")
    assert bill["due_date"] == dt.date(2026, 8, 17)
    assert bill["total_kwh"] == Decimal("1100.000")
    assert bill["plan_name"] == "Synthetic Buyback Flex"
    assert bill["contract_rate_cents_kwh"] == Decimal("15.500")
    assert bill["previous_balance"] == Decimal("120.00")
    assert bill["total_current_charges"] == Decimal("223.50")
    assert bill["forward_balance"] == Decimal("0.00")

    # Line items carry the PDF document's provenance.
    items = _fetch_line_items(sqlite_engine, "INV-TEST-0001")
    assert len(items) == 9
    for row in items:
        assert row["raw_document_id"] == pdf_id
        assert row["parser_version"] == rhythm.BILL_PARSER_VERSION
    assert [row["line_no"] for row in items] == list(range(1, 10))
    assert items[2]["amount"] == Decimal("-5.00")  # signed credit survived the round-trip

    # The API-only bill landed too, with no line items.
    other = _fetch_bill(sqlite_engine, "INV-TEST-0002")
    assert other["amount_due"] == Decimal("114.50")
    assert _fetch_line_items(sqlite_engine, "INV-TEST-0002") == []


# %%
