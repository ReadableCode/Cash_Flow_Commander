"""Parse Citi credit-card CSV exports into `transactions` rows.

One layout has been observed (live discovery 2026-08-24):

    Status,Date,Description,Debit,Credit,Member Name

Differences from Chase that shape this parser:

- **No signed amount.** Debit and Credit are separate unsigned columns: a
  charge lands in Debit, a payment or refund in Credit. The pipeline's
  convention (negative = money out, matching Chase) makes the projection
  `amount = credit - debit`.
- **One date column.** `txn_date` and `post_date` are both the Date column —
  inventing a second date would be fabrication.
- **No transaction id**, same as Chase, so rows carry an `occurrence` counter
  assigned in export order within each (account, post_date, description,
  amount) group. Export order was verified stable live: two independent
  downloads of overlapping windows, minutes apart, returned the 748-row
  overlap region byte-identical and identically ordered (newest first).
- **Status column.** Every observed row is `Cleared`; whether pending activity
  ever exports is unverified. Restatement handling therefore mirrors Chase's:
  each capture is authoritative for its requested window, and rows that vanish
  from a newer capture of the same window are pruned by
  transaction_store.sync_capture. If Citi restates less aggressively than
  Chase, the prune is simply a no-op.
- **No row cap** has been observed (a 773-row export of the entire searchable
  window came back complete), so unlike Chase there is no truncation guard —
  there is no known cap to guard against.

Every field is kept verbatim in the `extra` JSON column, keyed by the original
header text — the typed columns are a projection; `extra` is the record. The
account comes from the capture filename: one Citi login can cover several
cards, so `citi` is listed in parse_raw.DERIVES_OWN_ACCOUNT_ID.
"""

# %%
# Imports #

import csv
import datetime
import io
import re
from collections import defaultdict
from typing import Any

try:
    from providers import capture_names
except ImportError:  # pragma: no cover - fallback for `import src.providers.citi`
    from src.providers import capture_names  # type: ignore[no-redef]

# %%
# Constants #

PARSER_VERSION = "citi-transactions-1"

PROVIDER = "citi"

_DATE_FORMATS = ("%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d", "%m-%d-%Y")

# The one observed layout. Member Name is not required so a future export that
# drops it still parses; Debit/Credit are what make it recognizably Citi.
_REQUIRED = frozenset({"status", "date", "description", "debit", "credit"})

_ACCOUNT_KIND = "credit"


# %%
# Helpers #


def _norm_header(cell: str) -> str:
    """Normalize a header cell to a comparable token: 'Member Name' -> 'member_name'."""
    return re.sub(r"[^a-z0-9]+", "_", cell.strip().lower()).strip("_")


def _parse_date(raw: str) -> datetime.date | None:
    """Parse any date format Citi has used; None when blank or unparseable."""
    raw = (raw or "").strip()
    if not raw:
        return None
    for fmt in _DATE_FORMATS:
        try:
            return datetime.datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    return None


def _parse_money(raw: str) -> float | None:
    """Unsigned money cell -> float; None when blank. Tolerates $ , and parentheses."""
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
    """Collapse whitespace so a description compares stably across exports."""
    return re.sub(r"\s+", " ", (raw or "").strip())


def capture_meta_from_name(name: str) -> dict[str, Any] | None:
    """Meta for a citi capture filename; None for anything else (including chase)."""
    return capture_names.capture_meta_from_name(name, PROVIDER)


def account_from_capture_name(name: str) -> str | None:
    """Pull the account last-4 out of a citi capture filename; None when it is not one."""
    meta = capture_meta_from_name(name)
    return meta["account"] if meta else None


# %%
# Parsers #


def parse_transactions_csv(content: bytes, ctx: dict[str, Any]) -> dict[str, Any]:
    """Parse one Citi CSV capture into {'transactions': rows, 'transactions_window': ...}.

    Raises ValueError when the account cannot be determined or the layout is
    unrecognized — cases where accepting the document would record data against
    the wrong account or silently misread columns.
    """
    original_name = str(ctx.get("original_name") or "")
    meta = capture_meta_from_name(original_name)
    if meta is None:
        raise ValueError(
            f"cannot determine account from {original_name!r}; expected a "
            "citi_csv_export_{account}_{start}_{end}_captured{date}.csv capture"
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
        raise ValueError("empty Citi CSV export")

    keys = [_norm_header(cell) for cell in headers]
    have = {key for key, cell in zip(keys, headers) if cell.strip()}
    if not _REQUIRED <= have:
        raise ValueError(
            f"unrecognized Citi CSV columns: {', '.join(sorted(have)) or '(none)'}"
        )

    rows: list[dict[str, Any]] = []
    for raw in reader:
        if not raw or not any(value.strip() for value in raw):
            continue
        record = {key: (raw[i] if i < len(raw) else "") for i, key in enumerate(keys)}
        extra = _verbatim_row(headers, raw)

        date = _parse_date(record.get("date", ""))
        debit = _parse_money(record.get("debit", ""))
        credit = _parse_money(record.get("credit", ""))
        # A row without a date or with neither money column is a footer or a
        # blank — not a transaction. Skipping beats guessing.
        if date is None or (debit is None and credit is None):
            continue

        # Negative = money out, matching Chase and the transactions table
        # contract. A charge (Debit) is money out; a payment/refund (Credit)
        # is money in. Both present would be contradictory but still resolves
        # to the net rather than dropping the row.
        amount = (credit or 0.0) - (debit or 0.0)

        rows.append(
            {
                "account_id": account_id,
                "account_kind": _ACCOUNT_KIND,
                "txn_date": date,
                "post_date": date,
                "description": _squash(record.get("description", "")),
                "amount": round(amount, 2),
                "category": None,
                # Direction of the money, in the source's own vocabulary.
                "txn_type": "DEBIT" if debit is not None else "CREDIT",
                "balance": None,
                "check_no": None,
                # Member Name distinguishes cardholders on one account; memo is
                # the closest typed column, and the raw value is in extra too.
                "memo": _squash(record.get("member_name", "")) or None,
                "extra": extra,
            }
        )

    _assign_occurrences(rows)
    # The window sink makes this capture authoritative for the dates of its
    # requested window (see transaction_store.sync_capture): if a future
    # export restates or drops a row, the newer capture's view wins for the
    # days it covers. `provider` scopes reconciliation to citi captures so a
    # chase account with a colliding last-4 could never be pruned by one.
    return {
        "transactions": rows,
        "transactions_window": {
            "provider": PROVIDER,
            "account_id": account_id,
            "start": meta["start"],
            "end": meta["end"],
            "captured": meta["captured"],
        },
    }


def parse_empty_window(content: bytes, ctx: dict[str, Any]) -> dict[str, Any]:
    """'Parse' an empty-window marker: there is nothing to extract, by design.

    The document exists purely as coverage evidence — Citi's export icon
    silently serves nothing for a window with no activity, and the marker
    records that observation. It deliberately does NOT emit a
    transactions_window sink: an empty result must never prune rows a real
    export previously proved existed.
    """
    return {}


def _verbatim_row(headers: list[str], values: list[str]) -> dict[str, Any]:
    """Capture one source row losslessly, deterministically.

    Same contract as the Chase parser: `columns` keys on the ORIGINAL header
    text and holds raw untrimmed strings; `unnamed` holds values in positions
    past the last header or under blank/duplicate header names, so a future
    Citi column change lands here instead of vanishing.
    """
    columns: dict[str, str] = {}
    unnamed: list[str] = []
    for index, value in enumerate(values):
        name = headers[index].strip() if index < len(headers) else ""
        if name and name not in columns:
            columns[name] = value
        else:
            unnamed.append(value)
    out: dict[str, Any] = {"layout": "citi_card", "columns": columns}
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
