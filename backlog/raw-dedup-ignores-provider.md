# ingest: a mislabelled raw document cannot be corrected by re-ingesting it

    found:  2026-09-04
    status: open
    verify: grep -n "on_conflict_do_nothing" -A2 src/raw_store.py

`src/raw_store.py:89` resolves ingest conflicts on the content hash alone:

    returning_stmt = insert_stmt.on_conflict_do_nothing(
        index_elements=[db.raw_documents.c.content_sha256]
    ).returning(db.raw_documents.c.id)

`provider` is not in the conflict key, so whichever provider ingests a file's
bytes first owns that row permanently. A later `ingest_raw.py --provider X` over
the same bytes conflicts, inserts nothing, and counts the document as `deduped`
— a success line. No code path corrects `provider` afterwards; `raw_store` only
ever UPDATEs `parse_status` (`mark_parsed`, line 135).

**Scope note.** The bug that originally *caused* mislabelling — `--provider`
riding the shared `CFC_RAW_INGEST_DIRS` fallback — was fixed separately on
2026-09-04 in commit `6d7930b` ("refuse --provider without an explicit path so
the ingest fallback cannot mislabel"): `_parse_args` now refuses `--provider`
without an explicit `DIR_OR_FILE`. That closes the path that produced the
incident below. What remains open, and what this entry is about, is narrower:
**when a row does end up mislabelled, re-ingesting under the correct provider is
a silent no-op rather than a repair.** Recovery requires `purge_raw.py` plus a
re-ingest, and nothing tells you that is needed.

## Evidence

During a `/bills-rhythm` run on 2026-09-04, a concurrent `/transactions-elan`
run ingested five Rhythm captures under `provider='elan'`:

    (1498, 'elan', 'bill_pdf',         'Rythm 2026-08.pdf',                          'pending')
    (1499, 'elan', 'api_invoice_json', 'rhythm_api_invoice-history_p1_20260904.json', 'pending')
    (1500, 'elan', 'api_invoice_json', 'rhythm_api_invoice-history_p2_20260904.json', 'pending')
    (1501, 'elan', 'api_orders_json',  'rhythm_api_orders_INV05157699.json',          'pending')
    (1502, 'elan', 'api_usage_json',   'rhythm_api_usage_INV05157699.json',           'pending')

The rhythm run then re-ingested those same files with an explicit, correct
`--provider rhythm <archive_dir> <raw_dir> <data_dir>` and got:

    Totals: ingested 0, deduped 11, skipped 225

Nothing inserted, nothing errored, no mention that 5 of those "dedupes" resolved
to rows belonging to a different provider. The artifacts stayed invisible to
every `provider='rhythm'` query and to `coverage.py`. They were repaired
out-of-band by the other session.

Impact: after any mislabelling — from a future bug, a mis-set `--provider`, or a
hand-run command — the obvious corrective action (re-run ingest properly) looks
like it worked and does nothing. For retention-limited sources such as Rhythm's
~12-month hourly window, a month of that is a permanent gap.

## fix

Make a cross-provider hash collision loud instead of silent. In
`raw_store.ingest_bytes`, the conflict branch already re-selects the existing
row to get its id; also read its `provider` and compare:

    row = conn.execute(
        select(db.raw_documents.c.id, db.raw_documents.c.provider).where(
            db.raw_documents.c.content_sha256 == content_sha256
        )
    ).one()
    if row.provider != provider:
        return {"id": int(row.id), "deduped": True, "provider_conflict": row.provider}

then surface a `provider_conflict` count in the `ingest_raw` summary so it
cannot be read as a clean dedup.

The stronger alternative is to put provider in the conflict key
(`UniqueConstraint("provider", "content_sha256")` plus matching
`index_elements`), which lets the same bytes exist under two providers and makes
re-ingest self-healing. That needs a migration dropping the existing unique
index on `content_sha256`.

## blast radius

The reporting fix is contained: one branch in `raw_store.ingest_bytes` and a
counter in the `ingest_raw` summary. No schema change, no reprocessing, existing
rows untouched. `tests/test_raw_store.py` already covers the dedup path and is
where the new case belongs.

The schema alternative is wider. The unique index on `content_sha256` is what
makes re-ingest idempotent, so the migration must land before the next ingest of
any provider. Row ids are preserved and nothing downstream keys off the hash —
`usage_intervals`, `bills`, `bill_line_items`, `payments` and `transactions` all
reference `raw_document_id` — so no reparse is required.

## not doing yet

Needs a data-model decision before the code: may the same bytes legitimately
belong to two providers (schema fix), or are they global with collisions treated
as errors (reporting fix)? Worth settling together with the `ingest_raw.py`
change already made on 2026-09-04, since both are about the same failure.

Recovery today, if it recurs: `src/purge_raw.py --provider <wrong> --doc-type
<type>` (dry run first, then `--yes`), then re-ingest. It refuses to touch a
document anything was parsed from, so do it before parsing.
