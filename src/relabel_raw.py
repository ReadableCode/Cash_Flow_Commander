"""Correct the provider a raw document is filed under.

The same bytes never belong to two providers, and ingest dedups on the bytes
alone. A document that landed under the wrong provider therefore cannot be
fixed by ingesting it again: the second ingest reports a provider_conflict and
writes nothing. This moves the held row instead.

Like purge_raw it writes nothing by default and requires an explicit filter.
A relabelled document goes back to `pending`, so the next parse_raw run parses
it as the new provider.

Rows already parsed from a document were written under the old provider's
account. They are a projection of the document, so --rebuild deletes them in
the same transaction as the relabel and the next parse writes them again under
the new provider. Without --rebuild a document with parsed rows is refused.

Usage:

    uv run python src/relabel_raw.py --from elan --to rhythm --doc-type bill_pdf
    uv run python src/relabel_raw.py --from elan --to rhythm --doc-type bill_pdf --yes
    uv run python src/relabel_raw.py --from rhythm --to smt --doc-type smt_export --rebuild --yes
"""

# %%
# Imports #

import argparse
import os
import re
import sys

from sqlalchemy import delete, select, update
from sqlalchemy.engine import Engine

_SRC_DIR = os.path.dirname(os.path.abspath(__file__))
if _SRC_DIR not in sys.path:
    sys.path.insert(0, _SRC_DIR)

import bootstrap  # noqa: E402
import db  # noqa: E402
import providers  # noqa: E402
import purge_raw  # noqa: E402

# %%
# Constants #

_SLUG_RE = re.compile(r"[a-z0-9][a-z0-9_-]*")


# %%
# Functions #


def relabel(engine: Engine, doc_ids: list[int], from_provider: str, to_provider: str, rebuild: bool) -> dict[str, int]:
    """Move the documents to to_provider and reset them to pending.

    With rebuild, rows parsed from them are deleted first, in the same
    transaction. Returns {table: rows deleted}. Caller must have checked
    dependents first.
    """
    removed: dict[str, int] = {}
    if not doc_ids:
        return removed
    with engine.begin() as conn:
        if rebuild:
            for name in purge_raw.DEPENDENT_TABLES:
                table = getattr(db, name, None)
                if table is None or "raw_document_id" not in table.c:
                    continue
                result = conn.execute(delete(table).where(table.c.raw_document_id.in_(doc_ids)))
                if result.rowcount:
                    removed[name] = int(result.rowcount)
        extras = conn.execute(
            select(db.raw_documents.c.id, db.raw_documents.c.extra).where(db.raw_documents.c.id.in_(doc_ids))
        ).all()
        for doc_id, extra in extras:
            conn.execute(
                update(db.raw_documents)
                .where(db.raw_documents.c.id == doc_id)
                .values(
                    provider=to_provider,
                    # Where the label came from, next to how it was first set.
                    extra={**(extra or {}), "relabelled_from": from_provider},
                    parse_status="pending",
                    parsed_at=None,
                    parser_version=None,
                    parse_error=None,
                )
            )
    return removed


# %%
# CLI #


def build_arg_parser() -> argparse.ArgumentParser:
    """Build the relabel CLI argument parser."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--from", dest="from_provider", required=True, help="provider the documents are held under")
    parser.add_argument("--to", dest="to_provider", required=True, help="provider they belong to")
    parser.add_argument("--doc-type", required=True, help="doc_type to relabel, e.g. smt_export")
    parser.add_argument(
        "--name",
        action="append",
        default=None,
        help="only documents with this exact original_name; repeat for several",
    )
    parser.add_argument(
        "--rebuild",
        action="store_true",
        help="also delete the rows parsed from these documents, so the next parse rewrites them",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="actually write; without this nothing is written",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Entry point; returns a process exit code."""
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    if not _SLUG_RE.fullmatch(args.to_provider):
        parser.error("--to must be a lowercase slug (a-z, 0-9, '_', '-')")
    if args.to_provider == args.from_provider:
        parser.error("--from and --to are the same provider")
    engine = db.get_engine()
    bootstrap.ensure_schema(engine)

    docs = purge_raw.find(engine, args.from_provider, args.doc_type, args.name)
    if not docs:
        print(f"Nothing matches provider={args.from_provider!r} doc_type={args.doc_type!r}.")
        return 0

    # A name that matched nothing is a typo, and a relabel that quietly did
    # less than it was asked reads as done.
    unmatched = sorted(set(args.name or []) - {doc["original_name"] for doc in docs})
    if unmatched:
        print(f"REFUSING: no document named {', '.join(unmatched)} under that provider and doc_type.")
        return 2

    print(
        f"{len(docs)} document(s) held under provider={args.from_provider!r} "
        f"doc_type={args.doc_type!r} would move to provider={args.to_provider!r}:"
    )
    for doc in docs:
        print(f"  #{doc['id']:<6} {doc['parse_status']:<8} {doc['byte_size']:>9,} B  {doc['original_name']}")

    unparseable = [
        doc["original_name"]
        for doc in docs
        if providers.get_parser(args.to_provider, args.doc_type, doc["original_name"]) is None
    ]
    if unparseable:
        print(
            f"\nNOTE: no parser is registered for {args.to_provider}/{args.doc_type} "
            f"for {len(unparseable)} of these; they would report as no_parser."
        )

    doc_ids = [doc["id"] for doc in docs]
    parsed = purge_raw.dependents(engine, doc_ids)
    if parsed:
        print(f"\nRows parsed from these documents, written under {args.from_provider!r}:")
        for table, count in sorted(parsed.items()):
            print(f"  {count} row(s) in {table}")
        if not args.rebuild:
            print(
                "REFUSING: those rows would keep the old provider's account. Pass --rebuild to "
                "delete them with the relabel; the next parse writes them under the new provider."
            )
            return 2
        print("--rebuild: they are deleted with the relabel and rewritten by the next parse.")

    next_parse = f"uv run python src/parse_raw.py --provider {args.to_provider} --doc-type {args.doc_type}"
    if not args.yes:
        print(f"\nNothing written. Re-run with --yes to relabel them, then:\n  {next_parse}")
        return 0

    removed = relabel(engine, doc_ids, args.from_provider, args.to_provider, args.rebuild)
    print(f"\nRelabelled {len(docs)} document(s) {args.from_provider} -> {args.to_provider}.")
    for table, count in sorted(removed.items()):
        print(f"Deleted {count} row(s) from {table}.")
    print(f"They are pending again. Parse them now:\n  {next_parse}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


# %%
