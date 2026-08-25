"""Parse Elan Financial Services (myaccountaccess.com) card CSV exports.

One layout has been observed (live discovery 2026-08-24):

    "Date","Transaction","Name","Memo","Amount"

Differences from Chase and Citi that shape this parser:

- **Amount is already signed the way the transactions table wants it**:
  DEBIT rows (purchases, interest, fees) print negative, CREDIT rows
  (payments, refunds) print positive, so negative = money out with no
  projection needed. Verified against live rows 2026-08-24. A waived fee
  prints as signed zero ("-0.00"), which is normalized to plain 0.0 so the
  natural key never depends on the sign of nothing.
- **The Transaction column states the direction** (DEBIT/CREDIT) rather than
  it being inferred from which money column is populated.
- **One date column**, ISO formatted. `txn_date` and `post_date` are both the
  Date column — inventing a second date would be fabrication. The portal's
  list labels rows POSTED, so treat it as the posting date.
- **No transaction id**, same as Chase and Citi, so rows carry an
  `occurrence` counter assigned in export order within each (account,
  post_date, description, amount) group. Export order is oldest-first and was
  verified stable live: two overlapping downloads minutes apart returned the
  25-row overlap region byte-identical and identically ordered. (Purchases
  carry a network reference number inside Memo, but payments and fees do not,
  so Memo cannot serve as a key.)
- **Memo is semicolon-slotted reference data** ("ref; category; ; ; ;");
  empty slots print as "; ; ; ; ;". A memo that is nothing but separators is
  stored as None — the verbatim value is in `extra` regardless.
- **No row cap** has been observed, though the discovery account's volume
  (~2.6 rows/month) could not have surfaced one; there is no truncation
  guard because there is no known cap to guard against.
- Restatement handling mirrors Chase/Citi: each capture is authoritative for
  its requested window, and rows that vanish from a newer capture of the same
  window are pruned by transaction_store.sync_capture. Whether Elan restates
  is unverified — if it never does, the prune is a no-op.

Every field is kept verbatim in the `extra` JSON column, keyed by the original
header text — the typed columns are a projection; `extra` is the record. The
account comes from the capture filename: one Elan login can cover several
cards, so `elan` is listed in parse_raw.DERIVES_OWN_ACCOUNT_ID.
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
except ImportError:  # pragma: no cover - fallback for `import src.providers.elan`
    from src.providers import capture_names  # type: ignore[no-redef]

# %%
# Constants #

PARSER_VERSION = "elan-transactions-1"

PROVIDER = "elan"

_DATE_FORMATS = ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y", "%m-%d-%Y")

# The one observed layout. Memo is not required so a future export that drops
# it still parses; Transaction + Name + a signed Amount are what make it
# recognizably Elan.
_REQUIRED = frozenset({"date", "transaction", "name", "amount"})

_ACCOUNT_KIND = "credit"


# %%
# Helpers #


def _norm_header(cell: str) -> str:
    """Normalize a header cell to a comparable token: 'Transaction' -> 'transaction'."""
    return re.sub(r"[^a-z0-9]+", "_", cell.strip().lower()).strip("_")


def _parse_date(raw: str) -> datetime.date | None:
    """Parse any date format Elan has used; None when blank or unparseable."""
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
    """Signed money cell -> float; None when blank. Tolerates $ , and parentheses."""
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
    """Meta for an elan capture filename; None for anything else (including chase/citi)."""
    return capture_names.capture_meta_from_name(name, PROVIDER)


def account_from_capture_name(name: str) -> str | None:
    """Pull the account last-4 out of an elan capture filename; None when it is not one."""
    meta = capture_meta_from_name(name)
    return meta["account"] if meta else None


# %%
# Parsers #


def parse_transactions_csv(content: bytes, ctx: dict[str, Any]) -> dict[str, Any]:
    """Parse one Elan CSV capture into {'transactions': rows, 'transactions_window': ...}.

    Raises ValueError when the account cannot be determined or the layout is
    unrecognized — cases where accepting the document would record data against
    the wrong account or silently misread columns.
    """
    original_name = str(ctx.get("original_name") or "")
    meta = capture_meta_from_name(original_name)
    if meta is None:
        raise ValueError(
            f"cannot determine account from {original_name!r}; expected an "
            "elan_csv_export_{account}_{start}_{end}_captured{date}.csv capture"
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
        raise ValueError("empty Elan CSV export")

    keys = [_norm_header(cell) for cell in headers]
    have = {key for key, cell in zip(keys, headers) if cell.strip()}
    if not _REQUIRED <= have:
        raise ValueError(
            f"unrecognized Elan CSV columns: {', '.join(sorted(have)) or '(none)'}"
        )

    rows: list[dict[str, Any]] = []
    for raw in reader:
        if not raw or not any(value.strip() for value in raw):
            continue
        record = {key: (raw[i] if i < len(raw) else "") for i, key in enumerate(keys)}
        extra = _verbatim_row(headers, raw)

        date = _parse_date(record.get("date", ""))
        amount = _parse_money(record.get("amount", ""))
        # A row without a date or an amount is a footer or a blank — not a
        # transaction. Skipping beats guessing.
        if date is None or amount is None:
            continue

        # The Amount column is already signed with negative = money out,
        # matching Chase, Citi, and the transactions table contract — parse
        # as-is, do not flip. A waived fee prints "-0.00"; fold signed zero
        # to plain 0.0 so equality and display never see -0.0.
        amount = round(amount, 2)
        if amount == 0:
            amount = 0.0

        # Memo is semicolon-slotted reference data; a value that is nothing
        # but separators carries no information. Verbatim copy is in extra.
        memo = _squash(record.get("memo", ""))
        if not re.sub(r"[;\s]", "", memo):
            memo = ""

        rows.append(
            {
                "account_id": account_id,
                "account_kind": _ACCOUNT_KIND,
                "txn_date": date,
                "post_date": date,
                "description": _squash(record.get("name", "")),
                "amount": amount,
                "category": None,
                # Direction of the money, in the source's own vocabulary
                # (the Transaction column: DEBIT or CREDIT).
                "txn_type": _squash(record.get("transaction", "")).upper() or None,
                "balance": None,
                "check_no": None,
                "memo": memo or None,
                "extra": extra,
            }
        )

    _assign_occurrences(rows)
    # The window sink makes this capture authoritative for the dates of its
    # requested window (see transaction_store.sync_capture): if a future
    # export restates or drops a row, the newer capture's view wins for the
    # days it covers. `provider` scopes reconciliation to elan captures so a
    # chase/citi account with a colliding last-4 could never be pruned by one.
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

    The document exists purely as coverage evidence — Elan shows "There aren't
    any transactions for that date range." and downloads nothing for a window
    with no activity, and the marker records that observation. It deliberately
    does NOT emit a transactions_window sink: an empty result must never prune
    rows a real export previously proved existed.
    """
    return {}


def _verbatim_row(headers: list[str], values: list[str]) -> dict[str, Any]:
    """Capture one source row losslessly, deterministically.

    Same contract as the Chase and Citi parsers: `columns` keys on the
    ORIGINAL header text and holds raw untrimmed strings; `unnamed` holds
    values in positions past the last header or under blank/duplicate header
    names, so a future Elan column change lands here instead of vanishing.
    """
    columns: dict[str, str] = {}
    unnamed: list[str] = []
    for index, value in enumerate(values):
        name = headers[index].strip() if index < len(headers) else ""
        if name and name not in columns:
            columns[name] = value
        else:
            unnamed.append(value)
    out: dict[str, Any] = {"layout": "elan_card", "columns": columns}
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
