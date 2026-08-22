"""Parse ingested raw_documents into typed rows via provider parsers.

Streams documents from the raw_documents table (filtered by --provider,
--doc-type, and --status), looks up a parser via providers.get_parser, and
routes the parsed rows to their sink:

  'usage_intervals' -> usage_store.upsert_intervals
  'bills'           -> bill_store.upsert_bills
  'payments'        -> bill_store.upsert_payments
  'pdf_bills'       -> bill_store.apply_pdf_bill

Parsers either return a bare list (usage interval rows) or a dict mapping
sink name -> rows. Within a run, api_invoice_json documents are parsed before
bill_pdf documents so PDF bill patches can resolve against the API bill rows.

Reprocessing: to mass-reprocess after a parser fix, run with --status ok (or
--status all) — upserts overwrite on each sink's natural key, so re-parsing is
idempotent. Bump PARSER_VERSION in the provider module so the refreshed rows
carry the new version.
"""

# %%
# Imports #

import argparse
import os
import sys
from collections import Counter, defaultdict
from typing import Any

import yaml

# Make db/raw_store/usage_store/bill_store/providers importable both from
# `uv run python src/parse_raw.py` at the repo root and from pytest
# (which adds src/ via conftest.py).
_SRC_DIR = os.path.dirname(os.path.abspath(__file__))
if _SRC_DIR not in sys.path:
    sys.path.insert(0, _SRC_DIR)

import bill_store  # noqa: E402
import db  # noqa: E402
import providers  # noqa: E402
import raw_store  # noqa: E402
import transaction_store  # noqa: E402
import usage_store  # noqa: E402
from sqlalchemy import select  # noqa: E402
from sqlalchemy.engine import Engine  # noqa: E402

# %%
# Constants #

STATUS_CHOICES = ("pending", "error", "ok", "all")

# parse_error column stores at most this many characters of the exception text.
ERROR_MAX_LEN = 500

# providers.local.yaml lives at the repo root (parent of src/); it maps
# provider slug -> {'account_number': ...} and is never committed.
_REPO_ROOT = os.path.dirname(_SRC_DIR)
PROVIDERS_YAML_PATH = os.path.join(_REPO_ROOT, "providers.local.yaml")

# Processing order within a run: api_invoice_json first so bills rows exist
# before bill_pdf patches try to resolve against them; everything else in
# between; ties broken by document id.
_DOC_TYPE_PRIORITY = {"api_invoice_json": 0, "bill_pdf": 2}
_DEFAULT_PRIORITY = 1

# Fixed display order for per-sink counts in the summary.
_SINK_LABEL_ORDER = (
    "bills", "bills_patched", "line_items", "payments", "usage rows",
    "transactions", "stale transactions removed",
)

# Providers whose documents carry their own account identity, so the single
# `account_number` in providers.local.yaml does not apply. One Chase login
# covers several accounts; each capture names the account it came from, and the
# parser reads it from the filename.
DERIVES_OWN_ACCOUNT_ID = frozenset({"chase"})


# %%
# Account Context #


def _load_account_config(path: str) -> dict[str, Any]:
    """Load providers.local.yaml; returns {} when the file is missing or not a mapping."""
    if not os.path.isfile(path):
        return {}
    with open(path, "r", encoding="utf-8") as handle:
        loaded = yaml.safe_load(handle)
    return loaded if isinstance(loaded, dict) else {}


def _account_id_for(provider: str, config: dict[str, Any], override: str | None) -> str | None:
    """Resolve the account id for one provider; None means no usable value.

    --account-id wins outright; otherwise the value comes from
    config[provider]['account_number']. Missing or empty values are never
    substituted with an invented id.
    """
    if override:
        return override
    entry = config.get(provider)
    if not isinstance(entry, dict):
        return None
    account = entry.get("account_number")
    if account is None or not str(account).strip():
        return None
    return str(account)


# %%
# Document Stream #


def _collect_doc_refs(engine: Engine, args: argparse.Namespace, status_filter: str | None) -> list[dict[str, Any]]:
    """Collect lightweight refs (id + routing metadata) for every matching document.

    Content is dropped immediately — only ids and metadata are held — then the
    refs are ordered so api_invoice_json parses before bill_pdf within the run.
    """
    refs = [
        {"id": doc["id"], "provider": doc["provider"], "doc_type": doc["doc_type"]}
        for doc in raw_store.iter_documents(
            engine, provider=args.provider, doc_type=args.doc_type, parse_status=status_filter
        )
    ]
    refs.sort(key=lambda ref: (_DOC_TYPE_PRIORITY.get(ref["doc_type"], _DEFAULT_PRIORITY), ref["id"]))
    return refs


def _fetch_document(engine: Engine, doc_id: int) -> dict[str, Any] | None:
    """Load one raw_documents row (including content) by id; None if it vanished."""
    stmt = select(db.raw_documents).where(db.raw_documents.c.id == doc_id)
    with engine.connect() as conn:
        row = conn.execute(stmt).mappings().one_or_none()
    return dict(row) if row is not None else None


# %%
# Sinks #


def _stamp_rows(rows: list[dict[str, Any]], doc_id: int, parser_version: str) -> None:
    """Stamp provenance (raw_document_id + parser_version) onto every row in place."""
    for row in rows:
        row["raw_document_id"] = doc_id
        row["parser_version"] = parser_version


def _stamp_sinks(sinks: dict[str, Any], doc_id: int, parser_version: str) -> None:
    """Stamp provenance onto every row of every sink.

    For 'pdf_bills' entries both the bill_patch and each line_item are stamped.
    """
    for sink, payload in sinks.items():
        if sink == "transactions_window":
            # Capture-window metadata, not row data; the document id is what
            # breaks authority ties between two captures made the same day.
            payload["raw_document_id"] = doc_id
        elif sink == "pdf_bills":
            for entry in payload:
                _stamp_rows([entry["bill_patch"]], doc_id, parser_version)
                _stamp_rows(entry["line_items"], doc_id, parser_version)
        else:
            _stamp_rows(payload, doc_id, parser_version)


def _dry_run_counts(sinks: dict[str, Any]) -> Counter[str]:
    """Per-sink would-upsert counts for --dry-run; nothing is written."""
    counts: Counter[str] = Counter()
    for sink, payload in sinks.items():
        if sink == "transactions_window":
            continue  # metadata; a dry run cannot know what it would prune
        if sink == "usage_intervals":
            counts["usage rows"] += len(payload)
        elif sink in ("bills", "payments", "transactions"):
            counts[sink] += len(payload)
        elif sink == "pdf_bills":
            counts["bills_patched"] += len(payload)
            counts["line_items"] += sum(len(entry["line_items"]) for entry in payload)
        else:
            raise ValueError(f"parser returned unknown sink {sink!r}")
    return counts


def _upsert_sinks(engine: Engine, sinks: dict[str, Any]) -> tuple[Counter[str], str | None]:
    """Write every sink's rows to its store; returns (per-sink counts, unresolved reason).

    The reason is non-None only when apply_pdf_bill reports unresolved bill
    patches — the matching api_invoice_json bill row is missing — which marks
    the document as errored.
    """
    counts: Counter[str] = Counter()
    unresolved_reason: str | None = None
    # The window rides alongside the transactions sink; it is consumed here,
    # not written anywhere itself.
    window = sinks.pop("transactions_window", None)
    for sink, payload in sinks.items():
        if sink == "usage_intervals":
            counts["usage rows"] += int(usage_store.upsert_intervals(engine, payload)["upserted"])
        elif sink == "bills":
            counts["bills"] += int(bill_store.upsert_bills(engine, payload)["upserted"])
        elif sink == "payments":
            counts["payments"] += int(bill_store.upsert_payments(engine, payload)["upserted"])
        elif sink == "transactions":
            if window is not None:
                result = transaction_store.sync_capture(engine, payload, window)
                counts["transactions"] += int(result["upserted"])
                counts["stale transactions removed"] += int(result["removed"])
            else:
                counts["transactions"] += int(
                    transaction_store.upsert_transactions(engine, payload)["upserted"]
                )
        elif sink == "pdf_bills":
            result = bill_store.apply_pdf_bill(engine, payload)
            counts["bills_patched"] += int(result["bills_patched"])
            counts["line_items"] += int(result["line_items"])
            unresolved = result.get("unresolved") or []
            if unresolved:
                unresolved_reason = (
                    "unresolved PDF bill patches (no matching api_invoice_json bill row — "
                    "parse api_invoice_json first): " + "; ".join(str(item) for item in unresolved)
                )
        else:
            raise ValueError(f"parser returned unknown sink {sink!r}")
    return counts, unresolved_reason


# %%
# Parse #


def _process_document(
    engine: Engine,
    doc: dict[str, Any],
    config: dict[str, Any],
    args: argparse.Namespace,
) -> tuple[str, Counter[str], str | None]:
    """Parse one raw document; returns (status, per-sink upsert counts, error_reason).

    status is one of 'ok', 'error', 'no_parser'. Documents without a parser
    are left untouched (parse_status unchanged). In dry-run mode nothing is
    written — the counts are what would have been upserted per sink and
    parse_status is never updated.
    """
    parser = providers.get_parser(doc["provider"], doc["doc_type"], doc["original_name"])
    if parser is None:
        return ("no_parser", Counter(), None)
    parse_fn, parser_version = parser

    account_id = _account_id_for(doc["provider"], config, args.account_id)
    if account_id is None and doc["provider"] not in DERIVES_OWN_ACCOUNT_ID:
        reason = (
            f"no account_number for provider '{doc['provider']}' in providers.local.yaml "
            "(add it there or pass --account-id)"
        )
        if not args.dry_run:
            raw_store.mark_parsed(engine, doc["id"], "error", parser_version, error=reason[:ERROR_MAX_LEN])
        return ("error", Counter(), reason)

    ctx: dict[str, Any] = {
        "account_id": account_id,
        "provider": doc["provider"],
        "doc_type": doc["doc_type"],
        "original_name": doc["original_name"],
        "raw_document_id": doc["id"],
    }
    try:
        result: Any = parse_fn(doc["content"], ctx)
        # Old usage parsers return a bare list of usage_intervals rows; newer
        # parsers return a dict mapping sink -> rows. Normalize to the dict.
        sinks: dict[str, Any] = result if isinstance(result, dict) else {"usage_intervals": result}
        _stamp_sinks(sinks, doc["id"], parser_version)
        if args.dry_run:
            return ("ok", _dry_run_counts(sinks), None)
        counts, unresolved_reason = _upsert_sinks(engine, sinks)
    except Exception as exc:
        if not args.dry_run:
            raw_store.mark_parsed(engine, doc["id"], "error", parser_version, error=str(exc)[:ERROR_MAX_LEN])
        return ("error", Counter(), str(exc))
    if unresolved_reason is not None:
        raw_store.mark_parsed(engine, doc["id"], "error", parser_version, error=unresolved_reason[:ERROR_MAX_LEN])
        return ("error", counts, unresolved_reason)
    raw_store.mark_parsed(engine, doc["id"], "ok", parser_version)
    return ("ok", counts, None)


# %%
# Summary #


def _format_sink_counts(counter: Counter[str]) -> str:
    """Render non-zero per-sink counts, e.g. '55 bills, 610 line_items, 20208 usage rows'."""
    parts = [f"{counter[label]} {label}" for label in _SINK_LABEL_ORDER if counter[label]]
    return ", ".join(parts) if parts else "0 rows"


def _print_summary(
    counts: dict[str, Counter[str]],
    rows_by_key: dict[str, Counter[str]],
    errors: list[tuple[str, str]],
    dry_run: bool,
) -> None:
    """Print per provider/doc_type parse outcomes and per-sink rows upserted."""
    verb = "would upsert" if dry_run else "upserted"
    totals: Counter[str] = Counter()
    total_rows: Counter[str] = Counter()
    print()
    print("Summary by provider/doc_type:")
    for key in sorted(counts):
        counter = counts[key]
        totals.update(counter)
        total_rows.update(rows_by_key[key])
        print(
            f"  {key}: parsed ok {counter['ok']}, errored {counter['error']}, "
            f"no_parser {counter['no_parser']}, {verb} {_format_sink_counts(rows_by_key[key])}"
        )
    if not counts:
        print("  (no documents matched)")
    print(
        f"Totals: parsed ok {totals['ok']}, errored {totals['error']}, "
        f"no_parser {totals['no_parser']}, {verb} {_format_sink_counts(total_rows)}"
    )
    if errors:
        print("Errored documents:")
        for name, reason in errors:
            print(f"  {name}: {reason}")


# %%
# CLI #


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Parse ingested raw_documents into usage/bill/payment rows via provider parsers.",
    )
    parser.add_argument("--provider", default=None, help="only parse documents from this provider slug, e.g. rhythm")
    parser.add_argument("--doc-type", default=None, help="only parse documents of this doc_type, e.g. smt_export")
    parser.add_argument(
        "--status",
        choices=STATUS_CHOICES,
        default="pending",
        help="parse_status filter; 'all' parses regardless of current status (default: pending)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="parse and report per-sink row counts only; no upserts and no parse_status updates",
    )
    parser.add_argument(
        "--account-id",
        default=None,
        help="override the account id for every parsed document instead of reading providers.local.yaml",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    engine = db.get_engine()
    db.create_tables(engine)
    config = {} if args.account_id else _load_account_config(PROVIDERS_YAML_PATH)
    status_filter = None if args.status == "all" else args.status
    counts: dict[str, Counter[str]] = defaultdict(Counter)
    rows_by_key: dict[str, Counter[str]] = defaultdict(Counter)
    errors: list[tuple[str, str]] = []
    # Two passes keep memory bounded: first collect only ids + routing metadata
    # (small) and order them, then load one document's content at a time — a
    # single doc can be ~140k rows and rows are never accumulated across docs.
    for ref in _collect_doc_refs(engine, args, status_filter):
        doc = _fetch_document(engine, ref["id"])
        if doc is None:
            continue
        key = f"{doc['provider']}/{doc['doc_type']}"
        status, sink_counts, reason = _process_document(engine, doc, config, args)
        counts[key][status] += 1
        rows_by_key[key].update(sink_counts)
        if reason is not None:
            errors.append((doc["original_name"], reason))
    _print_summary(counts, rows_by_key, errors, args.dry_run)
    return 1 if any(counter["error"] for counter in counts.values()) else 0


# %%
# Main #

if __name__ == "__main__":
    sys.exit(main())
