# %%
# Imports #

import datetime as dt
import json
from decimal import Decimal
from typing import Any

import pypdf
import pytest

from providers import gexa, get_parser

# %%
# Synthetic fixtures (never real invoices, amounts, addresses, or personal data) #

ACCOUNT_ID = "ACCT-TEST-GEXA"

# The delivery utility's fixed charge per billing cycle, as providers.local.yaml
# carries it: effective date -> dollars. The bill below is read on 2026-02-26,
# so the 2026-01-01 charge applies and the later one must be ignored.
CONFIG: dict[str, Any] = {
    "tdu_fixed_monthly_charges": {"2025-06-01": "3.00", "2026-01-01": "4.00", "2026-03-01": "9.00"}
}
CTX: dict[str, Any] = {"account_id": ACCOUNT_ID, "config": CONFIG}

# The invoice-list response: a bare list, MM/DD/YYYY dates, the amount as a
# JSON number (so 140.5, not "140.50").
API_INVOICE_LIST = [
    {
        "InvoiceNumber": "90000002",
        "InvoiceDate": "02/27/2026",
        "InvoiceDueDate": "03/16/2026",
        "TotalAmount": 140.5,
        "TotalAmontDisplayString": "$140.50",
        "InvoicePaidDate": None,
        "Invoice_Number_Base36": "synth2",
        "IsMigrated": False,
        "IsPaid": "notpaid",
    },
    {
        "InvoiceNumber": "90000001",
        "InvoiceDate": "01/28/2026",
        "InvoiceDueDate": "02/13/2026",
        "TotalAmount": 98,
        "TotalAmontDisplayString": "$98.00",
        "InvoicePaidDate": "02/13/2026",
        "Invoice_Number_Base36": "synth1",
        "IsMigrated": False,
        "IsPaid": "paid",
    },
]
API_INVOICE_JSON = json.dumps(API_INVOICE_LIST).encode()

# Two pages joined: the account summary repeats the block totals, then the
# detail page carries the meter table and both charge blocks. All numbers
# invented; internally consistent to the cent:
#   electricity 50.00 + 0.10 + 1.00 + 1.40 = 52.50
#   tdu         80.00 + 5.00 + 0.20 + 0.15 + 1.25 + 1.40 = 88.00
#   total current charges = 140.50
BILL_PDF_TEXT = "\n".join(
    [
        "www.example.com",
        "Account summary (see second page for details)",
        "Invoice date: Feb 27 2026, Invoice No: 90000002",
        "Opening Balance $98.00",
        "Balance Forward $0.00",
        "Electricity Charges and Taxes (see second page for details) $52.50",
        "TDU Charges and Taxes (see second page for details) $88.00",
        "Total Current Charges $140.50",
        "Total Amount Due $140.50",
        "5.0% Late Payment Penalty (if paid after 03/16/2026) $7.03",
        "Total Amount Due with Late Payment Penalty (if paid after due date) $147.53",
        "Due Date 03/16/2026",
        "This period billed usage: 1,000 kWh",
        "Electricity Usage Details",
        "SYNTHMETER1 01/28/2026 - 02/26/2026 Actual 1000 2000 1 1000",
        "Total Usage 1000",
        "Electricity Charges and Taxes Billing Period: 01/28/2026 - 02/26/2026 Units Rate $ Total $",
        "*Energy Charge 1000 0.050000 $50.00",
        "PUC Assessment $0.10",
        "Gross Receipts Reimb $1.00",
        "Sales Tax - City $1.40",
        "Total Electricity Charges and Taxes $52.50",
        "TDU Charges and Taxes Billing Period: 01/28/2026 - 02/26/2026 Units Rate $ Total $",
        "*TDU Delivery Charges $80.00",
        "Base Charge $5.00",
        "Out of Cycle Meter Reading Regular Hours $0.20",
        "PUC Assessment $0.15",
        "Gross Receipts Reimb $1.25",
        "Sales Tax - City $1.40",
        "Total TDU Charges and Taxes $88.00",
        "The estimated contract end date is 01/28/2027",
        "Page 2 of 2",
    ]
)

# (section, category, description, quantity_kwh, rate_cents_kwh, amount)
EXPECTED_ITEMS: list[tuple[str, str, str, Decimal | None, Decimal | None, Decimal]] = [
    ("energy", "energy", "Energy Charge", Decimal("1000"), Decimal("5"), Decimal("50.00")),
    ("energy", "tax", "PUC Assessment", None, None, Decimal("0.10")),
    ("energy", "tax", "Gross Receipts Reimb", None, None, Decimal("1.00")),
    ("energy", "tax", "Sales Tax - City", None, None, Decimal("1.40")),
    # One printed line, split: a 29-day cycle pays the whole 4.00 fixed charge,
    # and the remaining 76.00 over 1000 kWh is 7.6 cents per kWh.
    (
        "non_energy",
        "delivery_variable",
        "TDU Delivery Charges - per kWh part (derived)",
        Decimal("1000"),
        Decimal("7.6"),
        Decimal("76.00"),
    ),
    ("non_energy", "delivery_fixed", "TDU Delivery Charges - fixed part (derived)", None, None, Decimal("4.00")),
    ("non_energy", "base", "Base Charge", None, None, Decimal("5.00")),
    (
        "non_energy",
        "fee",
        "Out of Cycle Meter Reading Regular Hours",
        None,
        None,
        Decimal("0.20"),
    ),
    ("non_energy", "tax", "PUC Assessment", None, None, Decimal("0.15")),
    ("non_energy", "tax", "Gross Receipts Reimb", None, None, Decimal("1.25")),
    ("non_energy", "tax", "Sales Tax - City", None, None, Decimal("1.40")),
]


# %%
# Helpers #


class _FakePage:
    def __init__(self, text: str) -> None:
        self._text = text

    def extract_text(self) -> str:
        return self._text


def _patch_pdf_text(monkeypatch: pytest.MonkeyPatch, text: str) -> None:
    """Make pypdf text extraction return `text` regardless of the PDF bytes."""

    class _FakeReader:
        def __init__(self, *args: Any, **kwargs: Any) -> None:
            self.pages = [_FakePage(text)]

    monkeypatch.setattr(pypdf, "PdfReader", _FakeReader)
    monkeypatch.setattr(gexa, "PdfReader", _FakeReader)


# %%
# Registry #


def test_registry_routes_gexa_documents() -> None:
    assert get_parser("gexa", "api_invoice_json", "gexa_api_invoice-history_20260227.json") == (
        gexa.parse_api_invoice_json,
        gexa.BILL_PARSER_VERSION,
    )
    assert get_parser("gexa", "bill_pdf", "gexa_bill_90000002_2026-02-27.pdf") == (
        gexa.parse_bill_pdf,
        gexa.BILL_PARSER_VERSION,
    )
    # Portal usage is consumption plus exported generation summed together, so
    # no usage parser may ever be registered for it.
    assert get_parser("gexa", "api_usage_json", "gexa_api_usage_2026-02-27.json") is None


# %%
# gexa.parse_api_invoice_json #


def test_parse_api_invoice_json_rows() -> None:
    result = gexa.parse_api_invoice_json(API_INVOICE_JSON, CTX)
    assert set(result) == {"bills"}
    assert result["bills"] == [
        {
            "account_id": ACCOUNT_ID,
            "invoice_number": "90000002",
            "invoice_date": dt.date(2026, 2, 27),
            "due_date": dt.date(2026, 3, 16),
            "amount_due": Decimal("140.50"),
        },
        {
            "account_id": ACCOUNT_ID,
            "invoice_number": "90000001",
            "invoice_date": dt.date(2026, 1, 28),
            "due_date": dt.date(2026, 2, 13),
            "amount_due": Decimal("98.00"),
        },
    ]
    # A JSON number loses its trailing zero; the stored amount must not.
    assert str(result["bills"][0]["amount_due"]) == "140.50"


# %%
# gexa.parse_bill_pdf #


def test_parse_bill_pdf(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_pdf_text(monkeypatch, BILL_PDF_TEXT)
    result = gexa.parse_bill_pdf(b"%PDF-1.4 synthetic bytes", CTX)
    assert set(result) == {"pdf_bills"}
    assert len(result["pdf_bills"]) == 1
    entry = result["pdf_bills"][0]

    patch = entry["bill_patch"]
    assert patch["account_id"] == ACCOUNT_ID
    assert patch["invoice_date"] == dt.date(2026, 2, 27)
    assert patch["service_start"] == dt.date(2026, 1, 28)
    # The bill prints the closing meter read, 02/26, which opens the next
    # period; the last day this bill covers is the day before.
    assert patch["service_end"] == dt.date(2026, 2, 25)
    assert patch["total_kwh"] == Decimal("1000")
    assert patch["contract_rate_cents_kwh"] == Decimal("5")
    assert patch["previous_balance"] == Decimal("98.00")
    assert patch["total_current_charges"] == Decimal("140.50")
    assert patch["forward_balance"] == Decimal("0.00")
    # The PDF patch never carries the invoice-list parser's columns, and the
    # plan name is not printed on the bill.
    assert "invoice_number" not in patch
    assert "due_date" not in patch
    assert "amount_due" not in patch
    assert "plan_name" not in patch

    items = entry["line_items"]
    assert len(items) == len(EXPECTED_ITEMS)
    for line_no, (item, expected) in enumerate(zip(items, EXPECTED_ITEMS), start=1):
        section, category, description, quantity, rate, amount = expected
        assert item["line_no"] == line_no
        assert item["section"] == section, (line_no, item)
        assert item["category"] == category, (line_no, item)
        assert item["description"] == description, (line_no, item)
        assert item["quantity_kwh"] == quantity, (line_no, item)
        assert item["rate_cents_kwh"] == rate, (line_no, item)
        assert item["amount"] == amount, (line_no, item)
        assert isinstance(item["amount"], Decimal)
    assert sum(item["amount"] for item in items) == patch["total_current_charges"]


def test_parse_bill_pdf_summary_totals_are_not_line_items(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_pdf_text(monkeypatch, BILL_PDF_TEXT)
    entry = gexa.parse_bill_pdf(b"%PDF-1.4 synthetic bytes", CTX)["pdf_bills"][0]
    descriptions = [item["description"] for item in entry["line_items"]]
    # The first page repeats both block totals and prints the late penalty;
    # none of those are charges on this bill.
    assert not any("see second page" in text for text in descriptions)
    assert not any("Late Payment" in text for text in descriptions)
    assert not any(text.startswith("Total") for text in descriptions)


def test_parse_bill_pdf_total_mismatch_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    mismatched = BILL_PDF_TEXT.replace(
        "Total Current Charges $140.50", "Total Current Charges $999.99"
    )
    _patch_pdf_text(monkeypatch, mismatched)
    # A failed to-the-cent validation must fail the doc, never emit wrong data.
    with pytest.raises(ValueError):
        gexa.parse_bill_pdf(b"%PDF-1.4 synthetic bytes", CTX)


def test_parse_bill_pdf_missing_charge_blocks_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    summary_only = BILL_PDF_TEXT.split("Electricity Usage Details")[0]
    _patch_pdf_text(monkeypatch, summary_only)
    with pytest.raises(ValueError):
        gexa.parse_bill_pdf(b"%PDF-1.4 synthetic bytes", CTX)


# %%
# The lump-sum delivery split #


def _delivery_parts(entry: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    """Return the (per-kWh, fixed) line items of a parsed bill."""
    by_category = {item["category"]: item for item in entry["line_items"]}
    return by_category["delivery_variable"], by_category["delivery_fixed"]


def test_short_period_prorates_the_fixed_charge(monkeypatch: pytest.MonkeyPatch) -> None:
    # A first bill after a switch: 15 days between reads is half of a 30-day
    # month, so half of the 4.00 fixed charge.
    short = BILL_PDF_TEXT.replace("01/28/2026 - 02/26/2026", "02/11/2026 - 02/26/2026")
    _patch_pdf_text(monkeypatch, short)
    entry = gexa.parse_bill_pdf(b"%PDF-1.4 synthetic bytes", CTX)["pdf_bills"][0]

    variable, fixed = _delivery_parts(entry)
    assert fixed["amount"] == Decimal("2.00")
    assert variable["amount"] == Decimal("78.00")
    assert variable["amount"] + fixed["amount"] == Decimal("80.00")
    assert entry["bill_patch"]["service_start"] == dt.date(2026, 2, 11)
    assert entry["bill_patch"]["service_end"] == dt.date(2026, 2, 25)


def test_long_period_pays_the_fixed_charge_once(monkeypatch: pytest.MonkeyPatch) -> None:
    # 35 days between reads is still one billing cycle, not 35/30 of one.
    long = BILL_PDF_TEXT.replace("01/28/2026 - 02/26/2026", "01/22/2026 - 02/26/2026")
    _patch_pdf_text(monkeypatch, long)
    entry = gexa.parse_bill_pdf(b"%PDF-1.4 synthetic bytes", CTX)["pdf_bills"][0]

    variable, fixed = _delivery_parts(entry)
    assert fixed["amount"] == Decimal("4.00")
    assert variable["amount"] == Decimal("76.00")


def test_bill_without_a_fixed_charge_in_effect_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_pdf_text(monkeypatch, BILL_PDF_TEXT)
    # Splitting with an invented charge would misprice every kWh on the bill.
    with pytest.raises(ValueError, match="tdu_fixed_monthly_charges"):
        gexa.parse_bill_pdf(b"%PDF-1.4 synthetic bytes", {"account_id": ACCOUNT_ID})
    too_late = {"tdu_fixed_monthly_charges": {"2026-03-01": "9.00"}}
    with pytest.raises(ValueError, match="tdu_fixed_monthly_charges"):
        gexa.parse_bill_pdf(b"%PDF-1.4 synthetic bytes", {"account_id": ACCOUNT_ID, "config": too_late})


def test_unrecognized_delivery_wording_is_not_priced_as_delivery(monkeypatch: pytest.MonkeyPatch) -> None:
    reworded = BILL_PDF_TEXT.replace("*TDU Delivery Charges $80.00", "*Wires Company Charges $80.00")
    _patch_pdf_text(monkeypatch, reworded)
    entry = gexa.parse_bill_pdf(b"%PDF-1.4 synthetic bytes", CTX)["pdf_bills"][0]

    categories = [item["category"] for item in entry["line_items"]]
    # It lands in 'other', which checks.py reports as a bill with no per-kWh
    # delivery, instead of being guessed at here.
    assert "delivery_variable" not in categories
    assert "other" in categories
