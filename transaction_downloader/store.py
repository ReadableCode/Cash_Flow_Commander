"""Shared plumbing for the transaction downloader: naming, CSV reading, manifest.

Built against Chase first and later generalized: each source (Chase, Citi, …)
is one entry in PROVIDERS, carrying its CSV layouts, its row cap, and its
download-filename hint. Two facts common to every source shape this module:

- Exports carry no record of the window you asked for, so `read_export` detects
  the layout from the header row and reports the date span the file actually
  contains — coverage never has to trust a filename.
- A capture records the window that was *requested*, which the file itself
  cannot tell you. A month with genuinely zero transactions produces an empty
  export (or, on some sources, no file at all); without the requested window
  that month looks identical to one never fetched, and the planner would ask
  for it forever.

Capture filename shape (all captures are kept; nothing is ever overwritten):

    {provider}_csv_export_{account}_{YYYYMMDD}_{YYYYMMDD}_captured{YYYYMMDD}.csv
                          account   req.start  req.end    capture date
"""

# %%
# Imports #

import csv
import datetime
import hashlib
import io
import json
import os
import re
from typing import Any

# %%
# Constants #

DEFAULT_PROVIDER = "chase"

# The capture/empty regexes accept any provider slug prefix so one scan can
# read a mixed directory; the `_csv_export_` / `_empty_window_` literal is the
# unambiguous delimiter between slug and account.
CAPTURE_RE = re.compile(
    r"^(?P<provider>[a-z0-9_]+?)_csv_export_(?P<account>[A-Za-z0-9\-]+)"
    r"_(?P<start>\d{8})_(?P<end>\d{8})_captured(?P<captured>\d{8})"
    # Same window captured twice on one day gets a "(1)" suffix rather than
    # clobbering the earlier file; it is still a capture.
    r"(?:\(\d+\))?\.csv$"
)

EMPTY_RE = re.compile(
    r"^(?P<provider>[a-z0-9_]+?)_empty_window_(?P<account>[A-Za-z0-9\-]+)"
    r"_(?P<start>\d{8})_(?P<end>\d{8})_captured(?P<captured>\d{8})"
    r"(?:\(\d+\))?\.txt$"
)

_DATE_FORMATS = ("%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d", "%m-%d-%Y")


# %%
# Providers #


def _norm_header(cell: str) -> str:
    """Normalize a header cell to a comparable token: 'Posting Date' -> 'posting_date'."""
    return re.sub(r"[^a-z0-9]+", "_", cell.strip().lower()).strip("_")


# Every layout Chase has been observed to emit. `date_fields` are all date
# columns in that layout: coverage takes the min and max across all of them so
# the reported span errs wide, which is the safe direction (see LANDING 0.1).
CHASE_LAYOUTS: tuple[dict[str, Any], ...] = (
    {
        "name": "checking",
        "account_type": "bank",
        "required": frozenset({"details", "posting_date", "description", "amount"}),
        "date_fields": ("posting_date",),
    },
    {
        "name": "credit_card",
        "account_type": "credit",
        "required": frozenset({"transaction_date", "post_date", "description", "amount"}),
        "date_fields": ("transaction_date", "post_date"),
    },
    {
        "name": "credit_card_legacy",
        "account_type": "credit",
        "required": frozenset({"trans_date", "post_date", "description", "amount"}),
        "date_fields": ("trans_date", "post_date"),
    },
)

# Citi card exports carry one layout (observed live 2026-08-24):
# Status,Date,Description,Debit,Credit,Member Name — a single date column and
# unsigned Debit/Credit columns instead of a signed amount.
CITI_LAYOUTS: tuple[dict[str, Any], ...] = (
    {
        "name": "citi_card",
        "account_type": "credit",
        "required": frozenset({"status", "date", "description", "debit", "credit"}),
        "date_fields": ("date",),
    },
)

# Everything provider-specific the downloader tooling needs, in one place.
#
# - layouts: header-detected CSV shapes for this source
# - row_cap: rows at which an export is presumed silently truncated and refused
#   at capture time (None = no cap has ever been observed for this source)
# - download_hint_re: pulls the account last-4 out of the source's own download
#   filename, when it carries one (Citi's do not — the filename is the scope
#   label, e.g. "Date range.CSV")
# - empty_message: the portal's observed no-activity message, quoted in
#   record-empty markers so the marker documents what was actually seen
# - retention_months: how far back the portal's export reaches; plan.py clips
#   the oldest window to this floor rather than requesting days the portal
#   refuses
PROVIDERS: dict[str, dict[str, Any]] = {
    "chase": {
        "layouts": CHASE_LAYOUTS,
        "row_cap": 1000,
        "download_hint_re": re.compile(r"chase[_\- ]?(\d{4})", re.IGNORECASE),
        "empty_message": "We couldn't find any activity that matched the date range you chose.",
        # Rolling DAILY floor, verified live 2026-08-22: From 2024-08-22
        # refused, 2024-08-23 accepted, applied to each endpoint independently.
        "retention_months": 24,
    },
    "citi": {
        "layouts": CITI_LAYOUTS,
        # No cap found: a single export of the entire ~24-month searchable
        # window came back complete (773 rows), with no cap warning anywhere
        # on the form. Revisit if an export ever lands on a suspiciously
        # round row count.
        "row_cap": None,
        "download_hint_re": re.compile(r"citi[_\- ]?(\d{4})", re.IGNORECASE),
        "empty_message": "no transactions for this time period.",
        # Citi's real floor is the statement-close date ~24 months back (a
        # cycle boundary that moves with the statement calendar, verified live
        # 2026-08-24: "You can only search from 8/5/2024 to 8/24/2026").
        # 24 rolling months is the conservative approximation: every date at
        # or after it is guaranteed servable; the sliver between the cycle
        # boundary and the rolling floor is reachable manually if ever needed.
        "retention_months": 24,
    },
}


def provider_def(provider: str) -> dict[str, Any]:
    """The PROVIDERS entry for a slug, or raise with the known slugs listed."""
    try:
        return PROVIDERS[provider]
    except KeyError:
        known = ", ".join(sorted(PROVIDERS))
        raise KeyError(f"unknown transaction provider {provider!r}; known: {known}") from None


class UnrecognizedExport(Exception):
    """Raised when a file does not match any known CSV layout for its provider."""


# Original name, kept as an alias so existing imports and except-clauses hold.
NotAChaseExport = UnrecognizedExport


def detect_layout(headers: list[str], provider: str = DEFAULT_PROVIDER) -> dict[str, Any]:
    """Return the provider layout whose required columns are all present, or raise."""
    have = {_norm_header(cell) for cell in headers if cell.strip()}
    for layout in provider_def(provider)["layouts"]:
        if layout["required"] <= have:
            return layout
    raise UnrecognizedExport(
        f"unrecognized {provider} columns: {', '.join(sorted(have)) or '(none)'}"
    )


# %%
# Reading #


def parse_date(raw: str) -> datetime.date | None:
    """Parse any date format Chase has used; None when the cell is blank or junk."""
    raw = (raw or "").strip()
    if not raw:
        return None
    for fmt in _DATE_FORMATS:
        try:
            return datetime.datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    return None


def read_export(content: str, provider: str = DEFAULT_PROVIDER) -> dict[str, Any]:
    """Describe one CSV export: layout, row count, and the date span it holds.

    Rows whose date cells are all unparseable are skipped rather than fatal —
    Chase appends the occasional blank or totals line, and one junk row should
    not disqualify an otherwise good capture.
    """
    reader = csv.reader(io.StringIO(content))
    headers: list[str] | None = None
    for candidate in reader:
        if candidate and any(cell.strip() for cell in candidate):
            headers = candidate
            break
    if headers is None:
        raise UnrecognizedExport("file is empty")

    layout = detect_layout(headers, provider)
    keys = [_norm_header(cell) for cell in headers]
    date_indexes = [keys.index(field) for field in layout["date_fields"] if field in keys]

    rows = 0
    dates: list[datetime.date] = []
    for raw in reader:
        if not raw or not any(cell.strip() for cell in raw):
            continue
        found = [parse_date(raw[i]) for i in date_indexes if i < len(raw)]
        found = [day for day in found if day is not None]
        if not found:
            continue
        rows += 1
        dates.extend(found)

    return {
        "layout": layout["name"],
        "account_type": layout["account_type"],
        "rows": rows,
        "min_date": min(dates).isoformat() if dates else None,
        "max_date": max(dates).isoformat() if dates else None,
    }


def read_export_file(path: str, provider: str = DEFAULT_PROVIDER) -> dict[str, Any]:
    """read_export for a file on disk, tolerating the occasional BOM."""
    with open(path, "r", encoding="utf-8-sig", errors="replace") as handle:
        return read_export(handle.read(), provider)


def sha256_file(path: str) -> str:
    """Hex sha256 of a file, streamed so large exports do not load into memory."""
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def account_hint_from_name(name: str, provider: str = DEFAULT_PROVIDER) -> str | None:
    """Pull the last-4 out of a source-supplied download filename; None when absent.

    Citi never trips this — its downloads are named by scope label ("Date
    range.CSV"), so the agent must always pass --account for that source.
    """
    match = provider_def(provider)["download_hint_re"].search(os.path.basename(name))
    return match.group(1) if match else None


# %%
# Naming #


def capture_name(
    account: str,
    start: datetime.date,
    end: datetime.date,
    captured: datetime.date,
    provider: str = DEFAULT_PROVIDER,
) -> str:
    """Build the canonical capture filename for a requested window."""
    return (
        f"{provider}_csv_export_{account}"
        f"_{start.strftime('%Y%m%d')}_{end.strftime('%Y%m%d')}"
        f"_captured{captured.strftime('%Y%m%d')}.csv"
    )


def empty_name(
    account: str,
    start: datetime.date,
    end: datetime.date,
    captured: datetime.date,
    provider: str = DEFAULT_PROVIDER,
) -> str:
    """Canonical marker filename for a window the source reported as having no activity."""
    return (
        f"{provider}_empty_window_{account}"
        f"_{start.strftime('%Y%m%d')}_{end.strftime('%Y%m%d')}"
        f"_captured{captured.strftime('%Y%m%d')}.txt"
    )


def parse_capture_name(name: str, provider: str | None = None) -> dict[str, Any] | None:
    """Inverse of capture_name/empty_name; None when the filename is neither.

    With `provider` given, a capture belonging to a different source also
    returns None — a raw_dir is per-provider, but a stray file must not count
    toward the wrong source's coverage.

    Empty-window markers come back with `"empty": True` and represent a
    requested window that held zero transactions — covered, with nothing in it.
    """
    base = os.path.basename(name)
    match = CAPTURE_RE.match(base)
    empty = False
    if match is None:
        match = EMPTY_RE.match(base)
        empty = True
    if match is None:
        return None
    if provider is not None and match.group("provider") != provider:
        return None
    parsed = {
        "provider": match.group("provider"),
        "account": match.group("account"),
        "requested_start": datetime.datetime.strptime(match.group("start"), "%Y%m%d").date().isoformat(),
        "requested_end": datetime.datetime.strptime(match.group("end"), "%Y%m%d").date().isoformat(),
        "captured_at": datetime.datetime.strptime(match.group("captured"), "%Y%m%d").date().isoformat(),
    }
    if empty:
        parsed["empty"] = True
    return parsed


# %%
# Manifest #


# Append-only sidecar next to the captures, one per provider. Rebuildable from
# the files themselves via `capture.py reindex`, so losing it is never fatal.
#
# The leading dot is load-bearing: src/ingest_raw.py walks raw_dir and skips
# hidden files, but only skips underscore-prefixed DIRECTORIES. Named
# `_chase_captures.jsonl` this bookkeeping file gets ingested as an `other`
# raw document on every run.
def manifest_path(raw_dir: str, provider: str = DEFAULT_PROVIDER) -> str:
    """Absolute path to the capture manifest inside a raw_dir."""
    return os.path.join(raw_dir, f".{provider}_captures.jsonl")


def append_manifest(raw_dir: str, entry: dict[str, Any], provider: str = DEFAULT_PROVIDER) -> None:
    """Append one capture record. Append-only: history of captures is never rewritten."""
    os.makedirs(raw_dir, exist_ok=True)
    with open(manifest_path(raw_dir, provider), "a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry, sort_keys=True) + "\n")


def write_manifest(
    raw_dir: str, entries: list[dict[str, Any]], provider: str = DEFAULT_PROVIDER
) -> None:
    """Replace the manifest wholesale. Only `capture.py reindex` should call this."""
    os.makedirs(raw_dir, exist_ok=True)
    tmp = manifest_path(raw_dir, provider) + ".tmp"
    with open(tmp, "w", encoding="utf-8") as handle:
        for entry in entries:
            handle.write(json.dumps(entry, sort_keys=True) + "\n")
    os.replace(tmp, manifest_path(raw_dir, provider))


def read_manifest(raw_dir: str, provider: str = DEFAULT_PROVIDER) -> list[dict[str, Any]]:
    """Read capture records, skipping any corrupt line rather than failing the run."""
    path = manifest_path(raw_dir, provider)
    if not os.path.isfile(path):
        return []
    entries: list[dict[str, Any]] = []
    with open(path, "r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                loaded = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(loaded, dict):
                entries.append(loaded)
    return entries


def scan_captures(raw_dir: str, provider: str = DEFAULT_PROVIDER) -> list[dict[str, Any]]:
    """Rebuild capture records by reading every capture file in raw_dir.

    This is the authoritative recovery path: the manifest is a cache, the files
    on disk are the truth. Only this provider's captures count — a raw_dir is
    per-provider, but a stray file must not pollute coverage.
    """
    if not os.path.isdir(raw_dir):
        return []
    entries: list[dict[str, Any]] = []
    for name in sorted(os.listdir(raw_dir)):
        meta = parse_capture_name(name, provider)
        if meta is None:
            continue
        path = os.path.join(raw_dir, name)
        entry = dict(meta)
        entry["file"] = name
        entry["sha256"] = sha256_file(path)
        if entry.get("empty"):
            # An empty-window marker holds a refusal note, not a CSV; the
            # window in its name is the whole record.
            entry.update({"rows": 0, "min_date": None, "max_date": None})
        else:
            try:
                entry.update(read_export_file(path, provider))
            except UnrecognizedExport as exc:
                entry["error"] = str(exc)
                entry["rows"] = 0
                entry["min_date"] = None
                entry["max_date"] = None
        entries.append(entry)
    return entries


# %%
