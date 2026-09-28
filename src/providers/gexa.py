# %%
# Imports #

import io
import json
import re
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from pypdf import PdfReader


# %%
# Constants #

BILL_PARSER_VERSION = "gexa-bills/1.0.0"

CENT = Decimal("0.01")

# Trailing dollar amount on a bill line; a credit would print `-$46.51`.
_AMOUNT_RE = re.compile(r"(-?)\$([\d,]+\.\d{2})$")

# `*Energy Charge 1000 0.050000`: label, kWh units, then the rate in DOLLARS
# per kWh (six decimals), left after the trailing amount is split off.
_UNITS_RATE_RE = re.compile(r"^(.*\S)\s+(\d[\d,]*(?:\.\d+)?)\s+(\d+\.\d+)$")

# `Invoice date: Feb 27 2026, Invoice No: ...` — no comma inside the date.
_INVOICE_DATE_RE = re.compile(r"Invoice date:\s*([A-Z][a-z]{2} \d{1,2} \d{4})")

_PORTAL_DATE = r"\d{2}/\d{2}/\d{4}"

# Charge-block headers; both carry the billing period on the same line.
_BLOCK_HEADER_RE = re.compile(
    rf"^(Electricity|TDU) Charges and Taxes Billing Period:\s*({_PORTAL_DATE})\s*-\s*({_PORTAL_DATE})"
)
_BLOCK_SECTIONS = {"Electricity": "energy", "TDU": "non_energy"}
_BLOCK_TOTAL_PREFIXES = ("Total Electricity Charges and Taxes", "Total TDU Charges and Taxes")

_TOTAL_USAGE_RE = re.compile(r"^Total Usage\s+(\d[\d,]*(?:\.\d+)?)$")

# (lowercased label substring, category) — first match wins.
_CATEGORY_RULES: tuple[tuple[str, str], ...] = (
    ("energy charge", "energy"),
    ("base charge", "base"),
    ("tdu delivery", "delivery"),
    ("sales tax", "tax"),
    ("puc assessment", "tax"),
    ("gross receipts", "tax"),
    ("meter reading", "fee"),
    ("late payment", "fee"),
    ("credit", "credit"),
)


# %%
# Bill Helpers #


def _pdf_text(content: bytes) -> str:
    """Extract text from every page of a bill PDF, joined with newlines."""
    reader = PdfReader(io.BytesIO(content))
    return "\n".join(page.extract_text() for page in reader.pages)


def _portal_date(text: str) -> date:
    """Parse a `MM/DD/YYYY` date, the format of the portal and the bill body."""
    return datetime.strptime(text, "%m/%d/%Y").date()


def _line_amount(line: str) -> tuple[str, Decimal] | None:
    """Split a bill line into (label text, signed amount); None when no trailing amount."""
    match = _AMOUNT_RE.search(line)
    if match is None:
        return None
    amount = Decimal(match.group(1) + match.group(2).replace(",", ""))
    return line[: match.start()].strip(), amount


def _required_amount(line: str) -> Decimal:
    """Return the trailing amount of a total line, failing loudly when absent."""
    split = _line_amount(line)
    if split is None:
        raise ValueError(f"expected a dollar amount on bill line: {line!r}")
    return split[1]


def _categorize(description: str) -> str:
    """Map a charge-line label to its line-item category."""
    lowered = description.lower()
    for needle, category in _CATEGORY_RULES:
        if needle in lowered:
            return category
    return "other"


def _charge_item(line: str, section: str) -> dict[str, Any] | None:
    """Build one line-item dict from a bill line; None when it has no amount.

    The leading `*` marks lines that count toward the average price per kWh
    and is dropped. A `<units> <rate>` tail fills quantity_kwh and
    rate_cents_kwh; the bill prints the rate in dollars, stored here in cents.
    """
    split = _line_amount(line)
    if split is None:
        return None
    description, amount = split
    description = description.lstrip("*").strip()
    quantity: Decimal | None = None
    rate: Decimal | None = None
    units_rate = _UNITS_RATE_RE.match(description)
    if units_rate is not None:
        description = units_rate.group(1)
        quantity = Decimal(units_rate.group(2).replace(",", ""))
        rate = Decimal(units_rate.group(3)) * 100
    return {
        "section": section,
        "category": _categorize(description),
        "description": description,
        "quantity_kwh": quantity,
        "rate_cents_kwh": rate,
        "amount": amount,
    }


def _block_items(lines: list[str]) -> tuple[list[dict[str, Any]], date, date]:
    """Collect the line items of both charge blocks plus the billing period.

    Items are gathered only between a block header and its total line, so the
    first-page account summary, which repeats the block totals, never leaks in.
    """
    items: list[dict[str, Any]] = []
    period: tuple[date, date] | None = None
    section: str | None = None
    for line in lines:
        header = _BLOCK_HEADER_RE.match(line)
        if header is not None:
            section = _BLOCK_SECTIONS[header.group(1)]
            if period is None:
                period = (_portal_date(header.group(2)), _portal_date(header.group(3)))
        elif section is not None and line.startswith(_BLOCK_TOTAL_PREFIXES):
            section = None
        elif section is not None:
            item = _charge_item(line, section)
            if item is not None:
                items.append(item)
    if period is None:
        raise ValueError("bill PDF has no `Charges and Taxes Billing Period` block")
    return items, period[0], period[1]


def _labeled_amount(lines: list[str], label: str) -> Decimal | None:
    """Find the first `<label> $X` line (exact label) and return its amount."""
    for line in lines:
        if line.startswith(label):
            split = _line_amount(line)
            if split is not None and split[0] == label:
                return split[1]
    return None


def _total_usage(lines: list[str]) -> Decimal | None:
    """Return the billed kWh from the `Total Usage` line of the meter table."""
    for line in lines:
        match = _TOTAL_USAGE_RE.match(line)
        if match is not None:
            return Decimal(match.group(1).replace(",", ""))
    return None


def _energy_rate(items: list[dict[str, Any]]) -> Decimal | None:
    """Return the rate of the first energy line item, in cents per kWh."""
    for item in items:
        if item["category"] == "energy" and item["rate_cents_kwh"] is not None:
            return item["rate_cents_kwh"]
    return None


def _validate_line_items(items: list[dict[str, Any]], total_current: Decimal) -> None:
    """Enforce the to-the-cent rule: line-item amounts must sum to the total."""
    charge_sum = sum((item["amount"] for item in items), Decimal("0"))
    if charge_sum != total_current:
        raise ValueError(
            f"bill line items sum to {charge_sum} but total current charges is {total_current}"
        )


# %%
# Bill Parsers #


def parse_api_invoice_json(content: bytes, ctx: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    """Parse the portal invoice-list response into bills rows.

    The response is a bare list with `MM/DD/YYYY` dates and the amount as a
    JSON number. It carries no service period and no kWh; the bill PDF parser
    owns those. account_id always comes from ctx.
    """
    bills = [
        {
            "account_id": ctx["account_id"],
            "invoice_number": str(item["InvoiceNumber"]),
            "invoice_date": _portal_date(item["InvoiceDate"]),
            "due_date": _portal_date(item["InvoiceDueDate"]),
            "amount_due": Decimal(str(item["TotalAmount"])).quantize(CENT),
        }
        for item in json.loads(content)
    ]
    return {"bills": bills}


def parse_bill_pdf(content: bytes, ctx: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    """Parse one Gexa bill PDF into a bill patch plus categorized line items.

    The bill patch carries no invoice_number, due_date or amount_due — the
    invoice-list parser owns those. The plan name is not printed on the bill,
    so the patch leaves that column alone. Raises ValueError when the line
    items do not sum exactly to the printed total current charges, so a bad
    parse fails the document instead of emitting wrong data.
    """
    text = _pdf_text(content)
    lines = [line.strip() for line in text.splitlines()]
    invoice = _INVOICE_DATE_RE.search(text)
    if invoice is None:
        raise ValueError("bill PDF is missing its `Invoice date:` header")
    items, service_start, service_end = _block_items(lines)
    total_current = _labeled_amount(lines, "Total Current Charges")
    if total_current is None:
        raise ValueError("no Total Current Charges line found in bill PDF")
    _validate_line_items(items, total_current)
    for line_no, item in enumerate(items, start=1):
        item["line_no"] = line_no
    bill_patch = {
        "account_id": ctx["account_id"],
        "invoice_date": datetime.strptime(invoice.group(1), "%b %d %Y").date(),
        "service_start": service_start,
        "service_end": service_end,
        "total_kwh": _total_usage(lines),
        "contract_rate_cents_kwh": _energy_rate(items),
        "previous_balance": _labeled_amount(lines, "Opening Balance"),
        "total_current_charges": total_current,
        "forward_balance": _labeled_amount(lines, "Balance Forward"),
    }
    return {"pdf_bills": [{"bill_patch": bill_patch, "line_items": items}]}
