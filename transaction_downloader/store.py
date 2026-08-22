"""Shared plumbing for the Chase transaction downloader: naming, CSV reading, manifest.

Chase hands out a different column layout per product, and its CSV exports carry
no transaction identifier and no record of the window you asked for. Both facts
shape this module:

- `read_export` detects the layout from the header row and reports the date span
  the file actually contains, so coverage never has to trust a filename.
- `Capture` records the window that was *requested*, which the file itself cannot
  tell you. A month with genuinely zero transactions produces an empty export;
  without the requested window that month looks identical to one never fetched,
  and the planner would ask for it forever.

Capture filename shape (all captures are kept; nothing is ever overwritten):

    chase_csv_export_{account}_{YYYYMMDD}_{YYYYMMDD}_captured{YYYYMMDD}.csv
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

CAPTURE_PREFIX = "chase_csv_export"

CAPTURE_RE = re.compile(
    r"^chase_csv_export_(?P<account>[A-Za-z0-9\-]+)"
    r"_(?P<start>\d{8})_(?P<end>\d{8})_captured(?P<captured>\d{8})"
    # Same window captured twice on one day gets a "(1)" suffix rather than
    # clobbering the earlier file; it is still a capture.
    r"(?:\(\d+\))?\.csv$"
)

# Append-only sidecar next to the captures. Rebuildable from the files
# themselves via `capture.py reindex`, so losing it is never fatal.
#
# The leading dot is load-bearing: src/ingest_raw.py walks raw_dir and skips
# hidden files, but only skips underscore-prefixed DIRECTORIES. Named
# `_chase_captures.jsonl` this bookkeeping file gets ingested as an `other`
# raw document on every run.
MANIFEST_NAME = ".chase_captures.jsonl"

# Chase names its own downloads like Chase7676_Activity_20260801.CSV. Used only
# to guess the account when filing a file the agent did not label.
_CHASE_DOWNLOAD_RE = re.compile(r"chase[_\- ]?(\d{4})", re.IGNORECASE)

_DATE_FORMATS = ("%m/%d/%Y", "%m/%d/%y", "%Y-%m-%d", "%m-%d-%Y")


# %%
# Layouts #


def _norm_header(cell: str) -> str:
    """Normalize a header cell to a comparable token: 'Posting Date' -> 'posting_date'."""
    return re.sub(r"[^a-z0-9]+", "_", cell.strip().lower()).strip("_")


# Every layout Chase has been observed to emit. `date_fields` are all date
# columns in that layout: coverage takes the min and max across all of them so
# the reported span errs wide, which is the safe direction (see LANDING 0.1).
LAYOUTS: tuple[dict[str, Any], ...] = (
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


class NotAChaseExport(Exception):
    """Raised when a file does not match any known Chase CSV layout."""


def detect_layout(headers: list[str]) -> dict[str, Any]:
    """Return the layout whose required columns are all present, or raise."""
    have = {_norm_header(cell) for cell in headers if cell.strip()}
    for layout in LAYOUTS:
        if layout["required"] <= have:
            return layout
    raise NotAChaseExport(f"unrecognized columns: {', '.join(sorted(have)) or '(none)'}")


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


def read_export(content: str) -> dict[str, Any]:
    """Describe one Chase CSV export: layout, row count, and the date span it holds.

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
        raise NotAChaseExport("file is empty")

    layout = detect_layout(headers)
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


def read_export_file(path: str) -> dict[str, Any]:
    """read_export for a file on disk, tolerating Chase's occasional BOM."""
    with open(path, "r", encoding="utf-8-sig", errors="replace") as handle:
        return read_export(handle.read())


def sha256_file(path: str) -> str:
    """Hex sha256 of a file, streamed so large exports do not load into memory."""
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def account_hint_from_name(name: str) -> str | None:
    """Pull the last-4 out of a Chase-supplied download filename; None when absent."""
    match = _CHASE_DOWNLOAD_RE.search(os.path.basename(name))
    return match.group(1) if match else None


# %%
# Naming #


def capture_name(account: str, start: datetime.date, end: datetime.date, captured: datetime.date) -> str:
    """Build the canonical capture filename for a requested window."""
    return (
        f"{CAPTURE_PREFIX}_{account}"
        f"_{start.strftime('%Y%m%d')}_{end.strftime('%Y%m%d')}"
        f"_captured{captured.strftime('%Y%m%d')}.csv"
    )


def parse_capture_name(name: str) -> dict[str, Any] | None:
    """Inverse of capture_name; None when the filename is not a capture."""
    match = CAPTURE_RE.match(os.path.basename(name))
    if match is None:
        return None
    return {
        "account": match.group("account"),
        "requested_start": datetime.datetime.strptime(match.group("start"), "%Y%m%d").date().isoformat(),
        "requested_end": datetime.datetime.strptime(match.group("end"), "%Y%m%d").date().isoformat(),
        "captured_at": datetime.datetime.strptime(match.group("captured"), "%Y%m%d").date().isoformat(),
    }


# %%
# Manifest #


def manifest_path(raw_dir: str) -> str:
    """Absolute path to the capture manifest inside a raw_dir."""
    return os.path.join(raw_dir, MANIFEST_NAME)


def append_manifest(raw_dir: str, entry: dict[str, Any]) -> None:
    """Append one capture record. Append-only: history of captures is never rewritten."""
    os.makedirs(raw_dir, exist_ok=True)
    with open(manifest_path(raw_dir), "a", encoding="utf-8") as handle:
        handle.write(json.dumps(entry, sort_keys=True) + "\n")


def write_manifest(raw_dir: str, entries: list[dict[str, Any]]) -> None:
    """Replace the manifest wholesale. Only `capture.py reindex` should call this."""
    os.makedirs(raw_dir, exist_ok=True)
    tmp = manifest_path(raw_dir) + ".tmp"
    with open(tmp, "w", encoding="utf-8") as handle:
        for entry in entries:
            handle.write(json.dumps(entry, sort_keys=True) + "\n")
    os.replace(tmp, manifest_path(raw_dir))


def read_manifest(raw_dir: str) -> list[dict[str, Any]]:
    """Read capture records, skipping any corrupt line rather than failing the run."""
    path = manifest_path(raw_dir)
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


def scan_captures(raw_dir: str) -> list[dict[str, Any]]:
    """Rebuild capture records by reading every capture file in raw_dir.

    This is the authoritative recovery path: the manifest is a cache, the files
    on disk are the truth.
    """
    if not os.path.isdir(raw_dir):
        return []
    entries: list[dict[str, Any]] = []
    for name in sorted(os.listdir(raw_dir)):
        meta = parse_capture_name(name)
        if meta is None:
            continue
        path = os.path.join(raw_dir, name)
        entry = dict(meta)
        entry["file"] = name
        entry["sha256"] = sha256_file(path)
        try:
            entry.update(read_export_file(path))
        except NotAChaseExport as exc:
            entry["error"] = str(exc)
            entry["rows"] = 0
            entry["min_date"] = None
            entry["max_date"] = None
        entries.append(entry)
    return entries


# %%
