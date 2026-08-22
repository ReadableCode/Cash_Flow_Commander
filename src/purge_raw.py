"""Remove raw documents that should never have been ingested.

`raw_documents` is the source of truth, so deleting from it is deliberately
awkward: this refuses by default, requires an explicit filter, and will not
touch a document that anything downstream was parsed from.

It exists because a mis-pointed directory lands the wrong files. Chase's
`archive_dir` briefly pointed at a folder of unrelated correspondence, which
ingested a letter as `doc_type='other'` — harmless in itself, but it reports as
`no_parser` on every parse run forever, and a permanent false alarm trains you
to ignore the one that matters.

Usage:

    uv run python src/purge_raw.py --provider chase --doc-type other
    uv run python src/purge_raw.py --provider chase --doc-type other --yes
"""

# %%
# Imports #

import argparse
import os
import sys

from sqlalchemy import delete, func, select
from sqlalchemy.engine import Engine

_SRC_DIR = os.path.dirname(os.path.abspath(__file__))
if _SRC_DIR not in sys.path:
    sys.path.insert(0, _SRC_DIR)

import db  # noqa: E402

# %%
# Constants #

# Tables that carry a raw_document_id. A document any of these were parsed from
# is provenance for real rows and is never purged.
DEPENDENT_TABLES = ("usage_intervals", "bills", "bill_line_items", "payments", "transactions")


# %%
# Functions #


def dependents(engine: Engine, doc_ids: list[int]) -> dict[str, int]:
    """Count rows in each dependent table that point at these documents."""
    if not doc_ids:
        return {}
    counts: dict[str, int] = {}
    with engine.connect() as conn:
        for name in DEPENDENT_TABLES:
            table = getattr(db, name, None)
            if table is None or "raw_document_id" not in table.c:
                continue
            n = conn.execute(
                select(func.count()).select_from(table).where(table.c.raw_document_id.in_(doc_ids))
            ).scalar_one()
            if n:
                counts[name] = int(n)
    return counts


def find(engine: Engine, provider: str, doc_type: str) -> list[dict]:
    """List the documents matching provider + doc_type."""
    stmt = select(
        db.raw_documents.c.id,
        db.raw_documents.c.original_name,
        db.raw_documents.c.byte_size,
        db.raw_documents.c.parse_status,
    ).where(
        db.raw_documents.c.provider == provider,
        db.raw_documents.c.doc_type == doc_type,
    ).order_by(db.raw_documents.c.id)
    with engine.connect() as conn:
        return [dict(row) for row in conn.execute(stmt).mappings()]


def purge(engine: Engine, doc_ids: list[int]) -> int:
    """Delete the given documents. Caller must have checked dependents first."""
    if not doc_ids:
        return 0
    with engine.begin() as conn:
        result = conn.execute(delete(db.raw_documents).where(db.raw_documents.c.id.in_(doc_ids)))
    return int(result.rowcount or 0)


# %%
# CLI #


def build_arg_parser() -> argparse.ArgumentParser:
    """Build the purge CLI argument parser."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--provider", required=True, help="provider slug, e.g. chase")
    parser.add_argument("--doc-type", required=True, help="doc_type to remove, e.g. other")
    parser.add_argument(
        "--yes",
        action="store_true",
        help="actually delete; without this nothing is written",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Entry point; returns a process exit code."""
    args = build_arg_parser().parse_args(argv)
    engine = db.get_engine()

    docs = find(engine, args.provider, args.doc_type)
    if not docs:
        print(f"Nothing matches provider={args.provider!r} doc_type={args.doc_type!r}.")
        return 0

    print(f"{len(docs)} document(s) match provider={args.provider!r} doc_type={args.doc_type!r}:")
    for doc in docs:
        print(f"  #{doc['id']:<6} {doc['parse_status']:<8} {doc['byte_size']:>9,} B  {doc['original_name']}")

    blocking = dependents(engine, [doc["id"] for doc in docs])
    if blocking:
        print("\nREFUSING: rows were parsed from these documents —")
        for table, count in sorted(blocking.items()):
            print(f"  {count} row(s) in {table}")
        print("Deleting would orphan real data. Remove those rows first if you truly mean it.")
        return 2

    if not args.yes:
        print("\nNothing deleted. Re-run with --yes to remove them.")
        return 0

    removed = purge(engine, [doc["id"] for doc in docs])
    print(f"\nDeleted {removed} document(s).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


# %%
