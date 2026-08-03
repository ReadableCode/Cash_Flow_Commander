# %%
# Imports #

import csv
import io
import json
import re
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

from pypdf import PdfReader


# %%
# Constants #

PARSER_VERSION = "rhythm-usage/1.0.0"

# Source timestamps are naive US/Central local times; they are localized with
# zoneinfo and converted to UTC. DST handling: spring-forward nonexistent
# local times resolve forward (zoneinfo default); fall-back ambiguous local
# times use fold=0 (the first occurrence). Both readings of a repeated local
# hour therefore map to the same UTC instant, and the natural-key upsert means
# the second overwrites the first — acceptable, documented here.
CENTRAL = ZoneInfo("America/Chicago")

# (metric, kwh field, rate field, cost field) — field names are shared by the
# API JSON items and the hourly-usage CSV header.
_METRIC_FIELDS: tuple[tuple[str, str, str, str], ...] = (
    ("consumption", "consumption_kwh", "consumption_rate", "consumption_cost"),
    ("generation", "generation_kwh", "generation_rate", "generation_earned"),
)


# %%
# Functions #


def _to_decimal(value: Any) -> Decimal | None:
    """Convert a numeric string to Decimal, mapping missing/null/empty to None."""
    if value is None or value == "":
        return None
    return Decimal(str(value))


def _central_to_utc(naive_iso: str) -> datetime:
    """Localize a naive ISO-8601 Central timestamp and convert it to UTC."""
    naive = datetime.fromisoformat(naive_iso)
    return naive.replace(tzinfo=CENTRAL, fold=0).astimezone(timezone.utc)


def _hour_row(
    account_id: str,
    ts: datetime,
    metric: str,
    value: Decimal,
    rate: Decimal | None,
    cost: Decimal | None,
) -> dict[str, Any]:
    """Build one hourly usage row dict."""
    return {
        "account_id": account_id,
        "ts": ts,
        "granularity": "hour",
        "metric": metric,
        "value": value,
        "unit": "kwh",
        "rate": rate,
        "cost": cost,
        "estimated": None,
    }


def _emit_metric_rows(
    account_id: str, ts: datetime, item: dict[str, Any]
) -> list[dict[str, Any]]:
    """Emit up to two rows (consumption, generation) for one hourly record.

    A metric whose kwh field is missing/null is skipped entirely; missing
    rate/cost fields are allowed and stored as None.
    """
    rows: list[dict[str, Any]] = []
    for metric, kwh_field, rate_field, cost_field in _METRIC_FIELDS:
        value = _to_decimal(item.get(kwh_field))
        if value is None:
            continue
        rows.append(
            _hour_row(
                account_id,
                ts,
                metric,
                value,
                _to_decimal(item.get(rate_field)),
                _to_decimal(item.get(cost_field)),
            )
        )
    return rows


def parse_api_usage_json(content: bytes, ctx: dict[str, Any]) -> list[dict[str, Any]]:
    """Parse a Rhythm API hourly-usage JSON array into usage row dicts.

    Each item yields up to two rows (consumption and generation), granularity
    'hour'. Tier fields are ignored. account_id always comes from ctx.
    """
    account_id = ctx["account_id"]
    rows: list[dict[str, Any]] = []
    for item in json.loads(content):
        ts = _central_to_utc(item["datetime"])
        rows.extend(_emit_metric_rows(account_id, ts, item))
    return rows


def parse_hourly_usage_csv(content: bytes, ctx: dict[str, Any]) -> list[dict[str, Any]]:
    """Parse a Rhythm hourly-usage CSV export into usage row dicts.

    Header: invoice_number,datetime,consumption_kwh,consumption_rate,
    consumption_cost,generation_kwh,generation_rate,generation_earned.
    invoice_number is ignored (provenance comes from raw_document_id).
    """
    account_id = ctx["account_id"]
    reader = csv.DictReader(io.StringIO(content.decode("utf-8-sig")))
    rows: list[dict[str, Any]] = []
    for record in reader:
        ts = _central_to_utc(record["datetime"])
        rows.extend(_emit_metric_rows(account_id, ts, record))
    return rows


# %%
# Bill Constants #

BILL_PARSER_VERSION = "rhythm-bills/1.0.0"

# Trailing dollar amount on a bill line; credits are written `-$46.51`.
_AMOUNT_RE = re.compile(r"(-?)\$([\d,]+\.\d{2})$")

# `1,062 kWh x 7.175¢/kWh` (2022 era, no space before ¢) and
# `60.799 kWh x 17.471 ¢/kWh` (later eras).
_QTY_RATE_RE = re.compile(r"(\d[\d,]*(?:\.\d+)?)\s*kWh x\s*(\d+(?:\.\d+)?)\s*¢/kWh")

# Bare kWh quantity, e.g. `648 kWh generated` on 2022 solar-credit lines.
_QTY_RE = re.compile(r"(\d[\d,]*(?:\.\d+)?)\s*kWh")

# A contract-rate value line, e.g. `7.175 (¢/kWh)`; anchored so the
# `... (¢/kWh) Contract Rate + TDU Delivery Charges` average-price line
# never matches.
_RATE_LINE_RE = re.compile(r"^(\d+(?:\.\d+)?) \(¢/kWh\)$")

# Header dates like `Mar 30, 2022`; the service-period separator has appeared
# as a double hyphen and could plausibly render as an en/em dash.
_BILL_DATE = r"[A-Z][a-z]{2} \d{1,2}, \d{4}"
_INVOICE_DATE_RE = re.compile(rf"INVOICE DATE:\s*({_BILL_DATE})")
_SERVICE_PERIOD_RE = re.compile(rf"SERVICE PERIOD:\s*({_BILL_DATE})\s*(?:--|–|—)\s*({_BILL_DATE})")

# Flat-layout grand-total line: `Total Current Charges` (2022) was renamed
# `Total Electrical Charges` mid-era.
_FLAT_TOTAL_RE = re.compile(r"^Total (?:Current|Electrical) Charges ")

# Sectioned-layout (2025+) block headers and their line-item section labels.
_SECTION_HEADERS = {"Energy Charges": "energy", "Non Energy Charges": "non_energy"}
_SECTION_TOTAL_PREFIXES = ("Total Energy Charges", "Total Non-Energy Charges")

# Line-item sections whose amounts must sum exactly to total current charges.
_CHARGE_SECTIONS = ("energy", "non_energy", "current")

# (lowercased label substring, category) — first match wins, so the solar
# credit rule precedes the energy-charge rule.
_CATEGORY_RULES: tuple[tuple[str, str], ...] = (
    ("solar buyback credit", "credit"),
    ("rhythm energy charge", "energy"),
    ("rhythm base charge", "base"),
    ("tdu delivery", "delivery"),
    ("oncor - delivery", "delivery"),
    ("city sales tax", "tax"),
    ("puc assessment", "tax"),
    ("misc gross receipts", "tax"),
    ("late fee", "fee"),
)


# %%
# Bill Helpers #


def _pdf_text(content: bytes) -> str:
    """Extract text from every page of a bill PDF, joined with newlines."""
    reader = PdfReader(io.BytesIO(content))
    return "\n".join(page.extract_text() for page in reader.pages)


def _parse_bill_date(text: str) -> date:
    """Parse a `Mon DD, YYYY` bill date."""
    return datetime.strptime(text, "%b %d, %Y").date()


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

    A `N kWh x R ¢/kWh` segment fills quantity_kwh and rate_cents_kwh and is
    dropped from the description; a bare `N kWh` (2022 solar credits) fills
    quantity_kwh only and stays in the description.
    """
    split = _line_amount(line)
    if split is None:
        return None
    description, amount = split
    quantity: Decimal | None = None
    rate: Decimal | None = None
    qty_rate = _QTY_RATE_RE.search(description)
    if qty_rate is not None:
        quantity = Decimal(qty_rate.group(1).replace(",", ""))
        rate = Decimal(qty_rate.group(2))
        description = (description[: qty_rate.start()] + description[qty_rate.end():]).strip()
    else:
        bare_qty = _QTY_RE.search(description)
        if bare_qty is not None:
            quantity = Decimal(bare_qty.group(1).replace(",", ""))
    return {
        "section": section,
        "category": _categorize(description),
        "description": description,
        "quantity_kwh": quantity,
        "rate_cents_kwh": rate,
        "amount": amount,
    }


def _flat_items(lines: list[str]) -> tuple[list[dict[str, Any]], Decimal]:
    """Collect charge items and the printed total from a flat-layout bill.

    Charges run from the `Payment Breakdown` header to the grand-total line;
    subsection headers (no amount) and intermediate `Total $X` subtotal lines
    are skipped. Every flat charge line is section 'current', including the
    inline solar credit used by later flat bills.
    """
    items: list[dict[str, Any]] = []
    started = False
    for line in lines:
        if not started:
            started = line == "Payment Breakdown"
        elif _FLAT_TOTAL_RE.match(line):
            return items, _required_amount(line)
        elif not line.startswith("Total"):
            item = _charge_item(line, "current")
            if item is not None:
                items.append(item)
    raise ValueError("no Total Current/Electrical Charges line found in flat-layout bill")


def _sectioned_total(remaining: list[str], section_totals: list[Decimal]) -> Decimal:
    """Resolve total current charges after the Non-Energy block closes.

    Later sectioned bills print a `Total Current Charges` line; early ones do
    not, so the total falls back to the sum of the printed section totals.
    """
    for line in remaining:
        if line.startswith("Total Current Charges"):
            return _required_amount(line)
    return sum(section_totals, Decimal("0"))


def _sectioned_items(lines: list[str]) -> tuple[list[dict[str, Any]], Decimal]:
    """Collect charge items and the total from a sectioned-layout (2025+) bill.

    Items are gathered only inside the exact `Energy Charges` / `Non Energy
    Charges` header blocks, so previous-balance breakdown lines that reuse
    those labels with amounts never leak in.
    """
    items: list[dict[str, Any]] = []
    section_totals: list[Decimal] = []
    section: str | None = None
    for index, line in enumerate(lines):
        if line in _SECTION_HEADERS:
            section = _SECTION_HEADERS[line]
        elif section is not None and line.startswith(_SECTION_TOTAL_PREFIXES):
            section_totals.append(_required_amount(line))
            if line.startswith("Total Non-Energy Charges"):
                return items, _sectioned_total(lines[index + 1:], section_totals)
            section = None
        elif section is not None:
            item = _charge_item(line, section)
            if item is not None:
                items.append(item)
    raise ValueError("no Total Non-Energy Charges line found in sectioned-layout bill")


def _adjustment_items(lines: list[str]) -> list[dict[str, Any]]:
    """Collect solar-credit line items from the `Payments and Adjustments` block.

    Payment lines and previous-balance breakdown lines are not bill line
    items; only credit lines qualify (2022 `Credit -- <date> -- Solar Buyback
    Credit ...` and early-sectioned `Solar Buyback Credit - Applied Towards
    Energy`). Bills without the block yield no adjustments.
    """
    items: list[dict[str, Any]] = []
    in_block = False
    for line in lines:
        if not in_block:
            in_block = line == "Payments and Adjustments"
        elif line.startswith("Total Payments and Adjustments"):
            break
        elif line.startswith("Credit -- ") or "Solar Buyback Credit" in line:
            item = _charge_item(line, "adjustment")
            if item is not None:
                items.append(item)
    return items


def _header_dates(text: str) -> tuple[date, date, date]:
    """Extract (invoice_date, service_start, service_end) from the bill header."""
    invoice = _INVOICE_DATE_RE.search(text)
    period = _SERVICE_PERIOD_RE.search(text)
    if invoice is None or period is None:
        raise ValueError("bill PDF is missing its INVOICE DATE / SERVICE PERIOD header")
    return (
        _parse_bill_date(invoice.group(1)),
        _parse_bill_date(period.group(1)),
        _parse_bill_date(period.group(2)),
    )


def _adjacent_rate(lines: list[str], label_index: int) -> Decimal | None:
    """Return the `N (¢/kWh)` value on the line next to a `Contract Rate` label.

    2022-era bills print the value on the line before the label; 2025+ bills
    print it on the line after.
    """
    for neighbor in (label_index + 1, label_index - 1):
        if 0 <= neighbor < len(lines):
            match = _RATE_LINE_RE.match(lines[neighbor])
            if match is not None:
                return Decimal(match.group(1))
    return None


def _agreement_fields(lines: list[str]) -> tuple[str | None, Decimal | None]:
    """Extract (plan_name, contract_rate_cents_kwh) from the Agreement Details block.

    The plan name is the line after the `Plan Name` label (2025+) or after the
    `Contract Valid` label (earlier bills, where the plan sits between
    `Contract Valid` and `Product Type`).
    """
    plan: str | None = None
    rate: Decimal | None = None
    for index, line in enumerate(lines):
        if plan is None and line in ("Plan Name", "Contract Valid") and index + 1 < len(lines):
            plan = lines[index + 1]
        if rate is None and line == "Contract Rate":
            rate = _adjacent_rate(lines, index)
        if plan is not None and rate is not None:
            break
    return plan, rate


def _labeled_amount(lines: list[str], label: str) -> Decimal | None:
    """Find the first `<label> $X` line (exact label) and return its amount."""
    for line in lines:
        if line.startswith(label):
            split = _line_amount(line)
            if split is not None and split[0] == label:
                return split[1]
    return None


def _validate_line_items(items: list[dict[str, Any]], total_current: Decimal) -> None:
    """Enforce the to-the-cent rule: charge-section amounts must sum to the total."""
    charge_sum = sum(
        (item["amount"] for item in items if item["section"] in _CHARGE_SECTIONS),
        Decimal("0"),
    )
    if charge_sum != total_current:
        raise ValueError(
            f"bill line items sum to {charge_sum} but total current charges is {total_current}"
        )


# %%
# Bill Parsers #


def parse_api_invoice_json(content: bytes, ctx: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    """Parse one page of the Rhythm invoice-history API response into bills rows.

    The page is an object whose `results` key holds the invoice list (a bare
    list is accepted). account_id always comes from ctx.
    """
    payload = json.loads(content)
    results = payload.get("results", []) if isinstance(payload, dict) else payload
    bills = [
        {
            "account_id": ctx["account_id"],
            "invoice_number": item["invoice_number"],
            "invoice_date": date.fromisoformat(item["invoice_date"]),
            "service_start": date.fromisoformat(item["service_start_date"]),
            "service_end": date.fromisoformat(item["service_end_date"]),
            "due_date": date.fromisoformat(item["due_date"]),
            "total_kwh": _to_decimal(item.get("total_kwh")),
            "amount_due": _to_decimal(item.get("amount")),
            "balance": _to_decimal(item.get("balance")),
            "late_fee_applied": bool(item.get("late_fee_applied")),
            "invoice_type": item.get("invoice_type"),
        }
        for item in results
    ]
    return {"bills": bills}


def parse_bill_pdf(content: bytes, ctx: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    """Parse one Rhythm bill PDF into a bill patch plus categorized line items.

    Handles both layout generations: the flat charge list (with its
    `Total Electrical Charges` and subtotal-line variants) and the sectioned
    Energy/Non-Energy layout. The bill patch carries no invoice_number or
    amount_due — the invoice-history API parser owns those. Raises ValueError
    when the charge-section line items do not sum exactly to the printed total
    current charges, so a bad parse fails the document instead of emitting
    wrong data.
    """
    text = _pdf_text(content)
    lines = [line.strip() for line in text.splitlines()]
    invoice_date, service_start, service_end = _header_dates(text)
    plan_name, contract_rate = _agreement_fields(lines)
    if any(line.startswith("Total Energy Charges") for line in lines):
        items, total_current = _sectioned_items(lines)
    else:
        items, total_current = _flat_items(lines)
    items.extend(_adjustment_items(lines))
    _validate_line_items(items, total_current)
    for line_no, item in enumerate(items, start=1):
        item["line_no"] = line_no
    bill_patch = {
        "account_id": ctx["account_id"],
        "invoice_date": invoice_date,
        "service_start": service_start,
        "service_end": service_end,
        "plan_name": plan_name,
        "contract_rate_cents_kwh": contract_rate,
        "previous_balance": _labeled_amount(lines, "Previous Balance"),
        "total_current_charges": total_current,
        "forward_balance": _labeled_amount(lines, "Forward Balance"),
    }
    return {"pdf_bills": [{"bill_patch": bill_patch, "line_items": items}]}


def parse_payments_csv(content: bytes, ctx: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    """Parse the payments.csv export into payments rows.

    Header: payment_email_date,amount_paid,confirmation_number,
    gmail_message_id. confirmation is None when the column is blank.
    """
    reader = csv.DictReader(io.StringIO(content.decode("utf-8-sig")))
    payments = [
        {
            "account_id": ctx["account_id"],
            "paid_at": date.fromisoformat(record["payment_email_date"]),
            "amount": Decimal(record["amount_paid"].replace(",", "")),
            "confirmation": (record["confirmation_number"] or "").strip() or None,
            "source_message_id": record["gmail_message_id"],
        }
        for record in reader
    ]
    return {"payments": payments}
