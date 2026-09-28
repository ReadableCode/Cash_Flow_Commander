# %%
# Imports #

import io
import json
import re
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any

from pypdf import PdfReader


# %%
# Constants #

BILL_PARSER_VERSION = "gexa-bills/1.1.0"

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

# The bill prints delivery as one lump sum. It is split into a per-kWh part and
# a fixed part (see _split_delivery), because the value of a kWh that was never
# bought includes the first and not the second.
_DELIVERY_LUMP_LABEL = "TDU Delivery Charges"

# The delivery utility charges its fixed part per billing cycle. A cycle this
# long or longer pays it in full; a shorter first or last period pays a share
# of a 30-day month.
_FULL_CYCLE_DAYS = 27
_PRORATION_DAYS = 30

# (lowercased label substring, category) — first match wins.
_CATEGORY_RULES: tuple[tuple[str, str], ...] = (
    ("energy charge", "energy"),
    ("base charge", "base"),
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


def _fixed_monthly_charge(config: dict[str, Any], read_end: date) -> Decimal:
    """Return the delivery utility's fixed monthly charge in effect on read_end.

    It comes from `tdu_fixed_monthly_charges` in the provider's
    providers.local.yaml entry, a map of effective date to dollars, because
    the bill prints only the lump sum. A missing map, or one with no entry on
    or before read_end, fails the document: splitting with an invented charge
    would misprice every kWh on the bill without any sign of it.
    """
    charges = config.get("tdu_fixed_monthly_charges") or {}
    in_effect = [
        (date.fromisoformat(str(effective)), Decimal(str(amount)))
        for effective, amount in charges.items()
        if date.fromisoformat(str(effective)) <= read_end
    ]
    if not in_effect:
        raise ValueError(
            f"no tdu_fixed_monthly_charges entry in effect on {read_end} in providers.local.yaml; "
            "the bill prints delivery as one lump sum and cannot be split without it"
        )
    return max(in_effect)[1]


def _split_delivery(
    item: dict[str, Any], monthly_charge: Decimal, read_days: int, total_kwh: Decimal | None
) -> list[dict[str, Any]]:
    """Split the lump-sum delivery line into its fixed and per-kWh parts.

    The fixed part is the monthly charge, prorated when the period is a short
    first or last one; the per-kWh part is the rest. Both are marked derived in
    their description, and together they still sum to the printed line, so the
    to-the-cent rule holds. The fixed part is small next to the whole, which is
    why subtracting it tracks a change in the per-kWh rates by itself.
    """
    lump = item["amount"]
    fixed = monthly_charge
    if read_days < _FULL_CYCLE_DAYS:
        fixed = (monthly_charge * read_days / _PRORATION_DAYS).quantize(CENT)
    fixed = min(fixed, lump)
    variable = lump - fixed
    rate: Decimal | None = None
    if total_kwh:
        rate = (variable * 100 / total_kwh).quantize(Decimal("0.0001"))
    return [
        {
            **item,
            "category": "delivery_variable",
            "description": f"{item['description']} - per kWh part (derived)",
            "quantity_kwh": total_kwh,
            "rate_cents_kwh": rate,
            "amount": variable,
        },
        {
            **item,
            "category": "delivery_fixed",
            "description": f"{item['description']} - fixed part (derived)",
            "amount": fixed,
        },
    ]


def _block_items(lines: list[str]) -> tuple[list[dict[str, Any]], date, date]:
    """Collect the line items of both charge blocks plus the two meter read dates.

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

    The billing period is printed as two meter read dates, and the second read
    opens the next period. service_end is stored as the day before it, the
    last day this bill covers, so consecutive bills never share a day.
    """
    text = _pdf_text(content)
    lines = [line.strip() for line in text.splitlines()]
    invoice = _INVOICE_DATE_RE.search(text)
    if invoice is None:
        raise ValueError("bill PDF is missing its `Invoice date:` header")
    printed_items, read_start, read_end = _block_items(lines)
    total_kwh = _total_usage(lines)
    monthly_charge = _fixed_monthly_charge(ctx.get("config") or {}, read_end)
    items: list[dict[str, Any]] = []
    for item in printed_items:
        if item["description"] == _DELIVERY_LUMP_LABEL:
            items.extend(_split_delivery(item, monthly_charge, (read_end - read_start).days, total_kwh))
        else:
            items.append(item)
    total_current = _labeled_amount(lines, "Total Current Charges")
    if total_current is None:
        raise ValueError("no Total Current Charges line found in bill PDF")
    _validate_line_items(items, total_current)
    for line_no, item in enumerate(items, start=1):
        item["line_no"] = line_no
    bill_patch = {
        "account_id": ctx["account_id"],
        "invoice_date": datetime.strptime(invoice.group(1), "%b %d %Y").date(),
        "service_start": read_start,
        "service_end": read_end - timedelta(days=1),
        "total_kwh": total_kwh,
        "contract_rate_cents_kwh": _energy_rate(items),
        "previous_balance": _labeled_amount(lines, "Opening Balance"),
        "total_current_charges": total_current,
        "forward_balance": _labeled_amount(lines, "Balance Forward"),
    }
    return {"pdf_bills": [{"bill_patch": bill_patch, "line_items": items}]}
