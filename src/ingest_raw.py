# %%
# Imports #

import argparse
import datetime
import os
import re
import sys
from collections import Counter, defaultdict

# Make db/raw_store importable both from `uv run python src/ingest_raw.py` at the
# repo root and from pytest (which adds src/ via conftest.py).
_SRC_DIR = os.path.dirname(os.path.abspath(__file__))
if _SRC_DIR not in sys.path:
    sys.path.insert(0, _SRC_DIR)

import db  # noqa: E402
import raw_store  # noqa: E402
import user_paths  # noqa: E402
from sqlalchemy.engine import Engine  # noqa: E402

# %%
# Constants #

VALID_DOC_TYPES = (
    "bill_pdf",
    "api_invoice_json",
    "api_usage_json",
    "api_orders_json",
    "weekly_email",
    "payment_email",
    "csv_export",
    "smt_export",
    "other",
)

SOURCES = ("portal_api", "gmail", "manual")

CSV_EXPORT_NAMES = frozenset(
    {
        "monthly_bills.csv",
        "hourly_usage.csv",
        "weekly_usage.csv",
        "payments.csv",
        "bill_line_items.csv",
        "rhythm_api_invoices.csv",
        "rhythm_api_orders.csv",
    }
)

# 'Rythm' spelling is intentional: it matches the provider's own bill filenames.
_RYTHM_MONTH_RE = re.compile(r"^Rythm (\d{4})-(\d{2})\.pdf$")
_RHYTHM_BILL_RE = re.compile(r"^rhythm_bill_.*_(\d{4})-(\d{2})-(\d{2})\.pdf$")

# Transaction exports, as filed by transaction_downloader/capture.py for any
# provider (chase, citi, ...):
# {provider}_csv_export_{account}_{YYYYMMDD}_{YYYYMMDD}_captured{YYYYMMDD}.csv
_TXN_EXPORT_RE = re.compile(
    r"^[a-z0-9_]+?_csv_export_[A-Za-z0-9\-]+_(\d{4})(\d{2})\d{2}_\d{8}_captured\d{8}(?:\(\d+\))?\.csv$"
)

# Empty-window markers from transaction_downloader/capture.py record-empty:
# both known sources serve no file for a window with no activity, so the
# refusal is recorded as its own document — coverage evidence, not data.
_TXN_EMPTY_RE = re.compile(
    r"^[a-z0-9_]+?_empty_window_[A-Za-z0-9\-]+_(\d{4})(\d{2})\d{2}_\d{8}_captured\d{8}(?:\(\d+\))?\.txt$"
)

# Folder-name slugs that should map onto an established provider slug (the
# archive folder keeps the provider's own 'Rythm' spelling; rows use 'rhythm').
PROVIDER_ALIASES = {"rythm": "rhythm"}

_MIME_BY_EXT = {
    ".pdf": "application/pdf",
    ".csv": "text/csv",
    ".json": "application/json",
    ".txt": "text/plain",
}


# %%
# Classification #


def _classify_bill_pdf(name: str) -> tuple[str, datetime.date] | None:
    """Match the two known bill PDF filename shapes; None when not a bill PDF."""
    match = _RYTHM_MONTH_RE.match(name)
    if match is not None:
        parts = (int(match.group(1)), int(match.group(2)), 1)
    else:
        match = _RHYTHM_BILL_RE.match(name)
        if match is None:
            return None
        parts = (int(match.group(1)), int(match.group(2)), int(match.group(3)))
    try:
        return ("bill_pdf", datetime.date(*parts))
    except ValueError:
        return None


def classify(name: str) -> tuple[str, datetime.date | None]:
    """Classify a raw document filename (basename) into (doc_type, period_hint)."""
    bill = _classify_bill_pdf(name)
    if bill is not None:
        return bill
    txn = _TXN_EXPORT_RE.match(name)
    if txn is not None:
        # period_hint is the first of the requested window's start month, which
        # is what makes the raw_documents provider/doc_type/period_hint index
        # useful for "which months have we landed".
        return ("csv_export", datetime.date(int(txn.group(1)), int(txn.group(2)), 1))
    txn_empty = _TXN_EMPTY_RE.match(name)
    if txn_empty is not None:
        return ("empty_window", datetime.date(int(txn_empty.group(1)), int(txn_empty.group(2)), 1))
    if name in CSV_EXPORT_NAMES:
        return ("csv_export", None)
    lowered = name.lower()
    if lowered.endswith(".json"):
        if "invoice-history" in lowered:
            return ("api_invoice_json", None)
        if "usage" in lowered:
            return ("api_usage_json", None)
        if "orders" in lowered:
            return ("api_orders_json", None)
        return ("other", None)
    if "smt" in lowered or name.startswith("IntervalData"):
        return ("smt_export", None)
    return ("other", None)


def guess_mime(name: str) -> str | None:
    """Guess a mime type from the file extension; None when unknown."""
    return _MIME_BY_EXT.get(os.path.splitext(name)[1].lower())


def slugify_provider(name: str) -> str | None:
    """Turn a provider folder name into a provider slug, e.g. 'City of Georgetown'
    -> 'city_of_georgetown'. Applies PROVIDER_ALIASES; None when nothing usable."""
    slug = re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")
    return PROVIDER_ALIASES.get(slug, slug) or None


# %%
# File Discovery #


def gather_files(paths: list[str]) -> tuple[list[tuple[str, str | None]], list[tuple[str, str]]]:
    """Expand paths into (file, provider_hint) pairs plus (basename, reason) skips.

    provider_hint is the slug of the first-level folder under a walked root
    (e.g. ROOT/Rythm/bill.pdf -> 'rhythm'); None for files directly in the root
    or passed as explicit file arguments. Directories are walked recursively in
    sorted order; hidden files and hidden or underscore-prefixed directories
    (e.g. staging dirs like _to_delete) are skipped silently. Missing paths
    become reported skips.
    """
    files: list[tuple[str, str | None]] = []
    skips: list[tuple[str, str]] = []
    for path in paths:
        if os.path.isdir(path):
            base = os.path.abspath(path)
            for root, dirnames, filenames in os.walk(base):
                dirnames[:] = sorted(d for d in dirnames if not d.startswith((".", "_")))
                rel = os.path.relpath(root, base)
                hint = None if rel == "." else slugify_provider(rel.split(os.sep)[0])
                for filename in sorted(filenames):
                    if not filename.startswith("."):
                        files.append((os.path.join(root, filename), hint))
        elif os.path.isfile(path):
            if os.path.basename(path).startswith("."):
                skips.append((os.path.basename(path), "hidden file"))
            else:
                files.append((path, None))
        else:
            skips.append((os.path.basename(path) or path, "path does not exist"))
    return files, skips


def _read_file(path: str) -> tuple[bytes | None, datetime.datetime | None, str | None]:
    """Read one file; returns (content, mtime_utc, skip_reason).

    skip_reason is set (and content is None) for zero-byte files, cloud
    placeholders (OneDrive-style: non-empty stat size but no local blocks, or a
    read that fails despite a non-zero size), and unreadable files.
    """
    try:
        stat_result = os.stat(path)
    except OSError as exc:
        return None, None, f"unreadable ({exc.strerror or exc})"
    if stat_result.st_size == 0:
        return None, None, "zero-byte file"
    if hasattr(stat_result, "st_blocks") and stat_result.st_blocks == 0:
        return None, None, "cloud placeholder (no local blocks)"
    fetched_at = datetime.datetime.fromtimestamp(stat_result.st_mtime, tz=datetime.timezone.utc)
    try:
        with open(path, "rb") as handle:
            content = handle.read()
    except OSError as exc:
        return None, None, f"cloud placeholder or unreadable ({exc.strerror or exc})"
    return content, fetched_at, None


# %%
# Ingest #


def _process_file(
    engine: Engine | None, path: str, provider: str, args: argparse.Namespace
) -> tuple[str, str, str | None]:
    """Process one file; returns (status, doc_type, skip_reason).

    status is one of 'ingested', 'deduped', 'skipped'. Never stores absolute
    paths — only the basename goes to the database.
    """
    name = os.path.basename(path)
    classified_doc_type, period_hint = classify(name)
    doc_type = args.doc_type or classified_doc_type
    classified_by = "cli_override" if args.doc_type else "filename"
    if args.doc_type is None and classified_doc_type == "other":
        print(f"WARNING: classified as 'other': {name}")
    content, fetched_at, reason = _read_file(path)
    if reason is not None or content is None:
        return ("skipped", doc_type, reason or "unreadable")
    if args.dry_run:
        hint = f" (period_hint {period_hint})" if period_hint else ""
        print(f"[dry-run] {name} -> {provider}/{doc_type}{hint}")
        return ("ingested", doc_type, None)
    assert engine is not None
    result = raw_store.ingest_bytes(
        engine,
        provider=provider,
        doc_type=doc_type,
        source=args.source,
        content=content,
        original_name=name,
        mime=guess_mime(name),
        period_hint=period_hint,
        fetched_at=fetched_at,
        extra={
            "classified_by": classified_by,
            "provider_from": "cli" if args.provider else "folder",
        },
    )
    return ("deduped" if result["deduped"] else "ingested", doc_type, None)


def _print_summary(counts: dict[str, Counter[str]], skips: list[tuple[str, str]], dry_run: bool) -> None:
    """Print per-doc_type and total ingested/deduped/skipped counts."""
    verb = "would ingest" if dry_run else "ingested"
    totals: Counter[str] = Counter()
    print()
    print("Summary by provider/doc_type:")
    for key in sorted(counts):
        counter = counts[key]
        totals.update(counter)
        print(f"  {key}: {verb} {counter['ingested']}, deduped {counter['deduped']}, skipped {counter['skipped']}")
    if not counts:
        print("  (no files found)")
    print(f"Totals: {verb} {totals['ingested']}, deduped {totals['deduped']}, skipped {totals['skipped']}")
    if skips:
        print("Skipped files:")
        for name, reason in skips:
            print(f"  {name}: {reason}")


# %%
# CLI #


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest raw provider documents (bills, exports, API dumps) into the raw_documents table.",
    )
    parser.add_argument(
        "paths",
        nargs="*",
        metavar="DIR_OR_FILE",
        help="files or directories to ingest; defaults to CFC_RAW_INGEST_DIRS (colon-separated) when omitted",
    )
    parser.add_argument(
        "--provider",
        default=None,
        help="lowercase provider slug, e.g. rhythm; when omitted, inferred per file from its "
        "first-level folder under each ingested directory (e.g. Utilities/Rythm/... -> rhythm)",
    )
    parser.add_argument(
        "--doc-type",
        choices=VALID_DOC_TYPES,
        default=None,
        help="override doc_type for every file instead of classifying by filename",
    )
    parser.add_argument("--source", choices=SOURCES, default="manual", help="how the files were obtained")
    parser.add_argument("--dry-run", action="store_true", help="classify and report only; no database writes")
    args = parser.parse_args(argv)
    if args.provider is not None and not re.fullmatch(r"[a-z0-9][a-z0-9_-]*", args.provider):
        parser.error("--provider must be a lowercase slug (a-z, 0-9, '_', '-')")
    if not args.paths:
        # Entries go through expand_config_path so $ONEDRIVE_DOCS resolves; an
        # unexpanded one raises rather than becoming a literal "$..." directory.
        raw_dirs = os.environ.get("CFC_RAW_INGEST_DIRS", "").split(":")
        args.paths = [user_paths.expand_config_path(p) for p in raw_dirs if p]
        if not args.paths:
            parser.error("no paths given and CFC_RAW_INGEST_DIRS is not set")
    return args


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    files, path_skips = gather_files(args.paths)
    engine: Engine | None = None
    if not args.dry_run:
        engine = db.get_engine()
        db.create_tables(engine)
    counts: dict[str, Counter[str]] = defaultdict(Counter)
    skips: list[tuple[str, str]] = []
    for skipped_name, skip_reason in path_skips:
        counts[f"unknown/{args.doc_type or classify(skipped_name)[0]}"]["skipped"] += 1
        skips.append((skipped_name, skip_reason))
    for path, hint in files:
        name = os.path.basename(path)
        provider = args.provider or hint
        if provider is None:
            counts[f"unknown/{args.doc_type or classify(name)[0]}"]["skipped"] += 1
            skips.append((name, "no provider folder (file it under a provider dir or pass --provider)"))
            continue
        try:
            status, doc_type, reason = _process_file(engine, path, provider, args)
        except Exception as exc:
            status, doc_type, reason = "skipped", args.doc_type or classify(name)[0], f"error: {exc}"
        counts[f"{provider}/{doc_type}"][status] += 1
        if reason is not None:
            skips.append((name, reason))
    _print_summary(counts, skips, args.dry_run)
    return 0


# %%
# Main #

if __name__ == "__main__":
    sys.exit(main())
