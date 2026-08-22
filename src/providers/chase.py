"""Parse Chase transaction CSV exports into `transactions` rows.

Chase emits a different column layout per product and none of them carry a
transaction identifier, which drives the two design choices here:

- **Layout is detected from the header row**, not from the filename or the
  account. Three layouts are known: `checking` (bank), `credit_card`, and
  `credit_card_legacy` (older archived exports).
- **Every field is kept verbatim** in the `extra` JSON column, keyed by the
  original header text and holding the raw unparsed string — including columns
  that are also projected into typed columns, and including values in positions
  past the last header (real Chase bank exports emit 8 fields against 7 headers).
  The typed columns are a projection; `extra` is the record. This is done here,
  in deterministic parser code, so no run gets to decide what is worth keeping.
- **Rows get an `occurrence` counter** within each
  (account, post_date, description, amount) group, assigned in export order.
  Chase repeats genuinely identical same-day rows — two identical coffees at the
  same shop — and without the counter the second would upsert onto the first and
  silently vanish. Export order is stable across re-downloads, so the same
  transaction keeps the same occurrence and reprocessing stays idempotent.

The account is taken from the capture filename rather than from ctx, because one
Chase login covers several accounts and `providers.local.yaml` has no single
`account_number` for the provider. See `transaction_downloader/store.py` for the
filename contract.
"""

# %%
# Imports #

import csv
import datetime
import io
import re
from collections import defaultdict
from typing import Any

# %%
# Constants #

PARSER_VERSION = "chase-transactions-2"

# Full capture-name contract (see transaction_downloader/store.py, which owns
# the writing side): account, requested window, and the capture date all live
# in the filename, so the parser can reconcile a re-downloaded window against
# what an older download of the same window stored.
_CAPTURE_META_RE = re.compile(
    r"^chase_csv_export_([A-Za-z0-9\-]+)_(\d{8})_(\d{8})_captured(\d{8})(?:\(\d+\))?\.csv$",
    re.IGNORECASE,
)

_DATE_FORMATS = ("%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d", "%m-%d-%Y")

# Header sets that identify each layout, and where each field lives. `txn_date`
# falls back to `post_date` when the layout has only one date column: a bank
# export gives posting date only, and inventing a transaction date would be
# fabrication.
LAYOUTS: tuple[dict[str, Any], ...] = (
    {
        "name": "checking",
        "account_kind": "bank",
        "required": frozenset({"details", "posting_date", "description", "amount"}),
        "fields": {
            "txn_date": "posting_date",
            "post_date": "posting_date",
            "description": "description",
            "amount": "amount",
            "txn_type": "type",
            "balance": "balance",
            "check_no": "check_or_slip",
            "category": None,
            "memo": None,
        },
        # 'Details' (DEBIT/CREDIT/CHECK/DSLIP) stands in when 'Type' is blank.
        "type_fallback": "details",
    },
    {
        "name": "credit_card",
        "account_kind": "credit",
        "required": frozenset({"transaction_date", "post_date", "description", "amount"}),
        "fields": {
            "txn_date": "transaction_date",
            "post_date": "post_date",
            "description": "description",
            "amount": "amount",
            "txn_type": "type",
            "category": "category",
            "memo": "memo",
            "balance": None,
            "check_no": None,
        },
    },
    {
        "name": "credit_card_legacy",
        "account_kind": "credit",
        "required": frozenset({"trans_date", "post_date", "description", "amount"}),
        "fields": {
            "txn_date": "trans_date",
            "post_date": "post_date",
            "description": "description",
            "amount": "amount",
            "txn_type": "type",
            "category": None,
            "memo": None,
            "balance": None,
            "check_no": None,
        },
    },
)

# Chase warns that a report is capped at 1,000 rows and must be split. A file
# landing exactly on the cap is almost certainly truncated, and accepting it
# would mark a window covered while silently dropping data.
ROW_CAP = 1000


# %%
# Helpers #


def _norm_header(cell: str) -> str:
    """Normalize a header cell to a comparable token: 'Posting Date' -> 'posting_date'."""
    return re.sub(r"[^a-z0-9]+", "_", cell.strip().lower()).strip("_")


def _detect_layout(headers: list[str]) -> dict[str, Any]:
    """Return the layout whose required columns are all present, or raise."""
    have = {_norm_header(cell) for cell in headers if cell.strip()}
    for layout in LAYOUTS:
        if layout["required"] <= have:
            return layout
    raise ValueError(f"unrecognized Chase CSV columns: {', '.join(sorted(have)) or '(none)'}")


def _parse_date(raw: str) -> datetime.date | None:
    """Parse any date format Chase has used; None when blank or unparseable."""
    raw = (raw or "").strip()
    if not raw:
        return None
    for fmt in _DATE_FORMATS:
        try:
            return datetime.datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    return None


def _parse_amount(raw: str) -> float | None:
    """Signed amount. Chase signs debits negative; archived exports use parentheses."""
    raw = (raw or "").strip()
    if not raw:
        return None
    negative = raw.startswith("(") and raw.endswith(")")
    cleaned = re.sub(r"[^0-9.\-]", "", raw)
    if not cleaned or cleaned in {"-", "."}:
        return None
    value = float(cleaned)
    return -abs(value) if negative else value


def _squash(raw: str) -> str:
    """Collapse whitespace; Chase pads descriptions inconsistently between exports."""
    return re.sub(r"\s+", " ", (raw or "").strip())


def capture_meta_from_name(name: str) -> dict[str, Any] | None:
    """Parse account, requested window, and capture date from a capture filename.

    Returns {'account', 'start', 'end', 'captured'} with real date objects, or
    None when the name is not a capture. The capture date is the authority
    order between overlapping downloads of the same window — a later download
    reflects Chase's restatements (tip adjustments being the everyday case) and
    supersedes an earlier one.
    """
    match = _CAPTURE_META_RE.match(name or "")
    if match is None:
        return None
    try:
        return {
            "account": match.group(1),
            "start": datetime.datetime.strptime(match.group(2), "%Y%m%d").date(),
            "end": datetime.datetime.strptime(match.group(3), "%Y%m%d").date(),
            "captured": datetime.datetime.strptime(match.group(4), "%Y%m%d").date(),
        }
    except ValueError:
        return None


def account_from_capture_name(name: str) -> str | None:
    """Pull the account last-4 out of a capture filename; None when it is not one."""
    meta = capture_meta_from_name(name)
    return meta["account"] if meta else None


# %%
# Parsers #


def parse_transactions_csv(content: bytes, ctx: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    """Parse one Chase CSV capture into {'transactions': rows}.

    Raises ValueError when the account cannot be determined, when the layout is
    unrecognized, or when the file looks truncated at Chase's 1,000-row cap —
    all cases where accepting the document would record partial data as complete.
    """
    original_name = str(ctx.get("original_name") or "")
    meta = capture_meta_from_name(original_name)
    if meta is None:
        raise ValueError(
            f"cannot determine account from {original_name!r}; expected a "
            "chase_csv_export_{account}_{start}_{end}_captured{date}.csv capture"
        )
    account_id = str(meta["account"])

    text = content.decode("utf-8-sig", errors="replace")
    reader = csv.reader(io.StringIO(text))

    headers = None
    for candidate in reader:
        if candidate and any(cell.strip() for cell in candidate):
            headers = candidate
            break
    if headers is None:
        raise ValueError("empty Chase CSV export")

    layout = _detect_layout(headers)
    keys = [_norm_header(cell) for cell in headers]
    fields = layout["fields"]

    def cell(record: dict[str, str], field: str | None) -> str:
        return record.get(fields.get(field) or "", "") if field else ""

    rows: list[dict[str, Any]] = []
    for raw in reader:
        if not raw or not any(value.strip() for value in raw):
            continue
        record = {key: (raw[i] if i < len(raw) else "") for i, key in enumerate(keys)}
        extra = _verbatim_row(headers, raw, layout["name"])

        post_date = _parse_date(cell(record, "post_date"))
        txn_date = _parse_date(cell(record, "txn_date")) or post_date
        amount = _parse_amount(cell(record, "amount"))
        # A row without a date or an amount is a footer, a totals line, or a
        # blank — not a transaction. Skipping beats guessing.
        if post_date is None or amount is None:
            continue

        txn_type = _squash(cell(record, "txn_type"))
        if not txn_type and layout.get("type_fallback"):
            txn_type = _squash(record.get(layout["type_fallback"], ""))

        rows.append(
            {
                "account_id": account_id,
                "account_kind": layout["account_kind"],
                "txn_date": txn_date,
                "post_date": post_date,
                "description": _squash(cell(record, "description")),
                "amount": round(amount, 2),
                "category": _squash(cell(record, "category")) or None,
                "txn_type": txn_type or None,
                "balance": _parse_amount(cell(record, "balance")),
                "check_no": _squash(cell(record, "check_no")) or None,
                "memo": _squash(cell(record, "memo")) or None,
                "extra": extra,
            }
        )

    if len(rows) >= ROW_CAP:
        raise ValueError(
            f"{len(rows)} rows hits Chase's {ROW_CAP}-row report cap — the export is "
            "almost certainly truncated. Re-download this window in smaller pieces."
        )

    _assign_occurrences(rows)
    # The window sink is what lets transaction_store treat this capture as
    # authoritative for the dates it covers: Chase restates posted rows (a tip
    # posts at the pre-tip amount, then the amount changes), and a restated
    # amount is a NEW natural key — upsert alone would leave the pre-tip row
    # behind as a phantom duplicate. Genuinely identical rows are NOT collapsed
    # by this: both carry distinct `occurrence` values and both appear in the
    # export, so both survive reconciliation.
    return {
        "transactions": rows,
        "transactions_window": {
            "account_id": account_id,
            "start": meta["start"],
            "end": meta["end"],
            "captured": meta["captured"],
        },
    }


def _verbatim_row(headers: list[str], values: list[str], layout: str) -> dict[str, Any]:
    """Capture one source row losslessly, deterministically.

    - `columns` keys on the ORIGINAL header text (not the normalized token) and
      holds raw, untrimmed strings, so the stored copy round-trips to the file.
    - `unnamed` holds values in positions past the last header. Chase bank
      exports really do emit a trailing empty field, and a future column would
      otherwise vanish silently.
    - A blank or duplicated header name cannot key a dict unambiguously, so
      those positions go to `unnamed` too. Chase's bank export ends its header
      line with a trailing comma, which would otherwise produce a "" key.

    No ordering or content decisions are left to the caller; the shape is fixed.
    """
    columns: dict[str, str] = {}
    unnamed: list[str] = []
    for index, value in enumerate(values):
        name = headers[index].strip() if index < len(headers) else ""
        if name and name not in columns:
            columns[name] = value
        else:
            unnamed.append(value)
    out: dict[str, Any] = {"layout": layout, "columns": columns}
    if unnamed:
        out["unnamed"] = unnamed
    return out


def _assign_occurrences(rows: list[dict[str, Any]]) -> None:
    """Number identical rows within their natural-key group, in export order."""
    counters: dict[tuple[Any, ...], int] = defaultdict(int)
    for row in rows:
        group = (row["account_id"], row["post_date"], row["description"], row["amount"])
        row["occurrence"] = counters[group]
        counters[group] += 1


# %%
