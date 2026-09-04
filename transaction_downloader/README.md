# transaction_downloader

Bank and credit-card transaction acquisition — Chase, Citi, and Elan today,
one `--provider` flag apart. Feeds the same pipeline as the bill providers — see
`docs/LANDING.md` — but tracks coverage differently, for a reason worth
understanding before changing anything here.

```
plan.py          →  agent downloads   →  capture.py     →  src/ingest_raw.py →  src/parse_raw.py
what's missing      (Claude in Chrome)   file verbatim     raw_documents        transactions table
```

The agent half lives in `.claude/commands/transactions-<slug>.md`. Run it with
`/transactions-chase`, `/transactions-citi`, or `/transactions-elan`. There is no schedule and no
cron: the planner derives what is missing from what is already captured, so the
command is safe and cheap to run whenever you want, at whatever interval you
feel like.

Everything provider-specific — CSV layouts, the row cap, the retention months,
the download-filename hint, the portal's empty-window message — lives in
`store.PROVIDERS`. `plan.py` and `capture.py` take `--provider <slug>`
(default `chase`); a new source is one PROVIDERS entry, one parser in
`src/providers/<slug>.py`, and one registry line.

## Why not `src/coverage.py`?

`coverage.py` finds gaps by looking for *missing readings*. That works for an
interval series because a meter emits a fixed number of readings per day, so a
day with none is unambiguously a hole.

Transactions have no cadence. A credit card can legitimately go ten days with no
activity. "No rows on that date" tells you nothing about whether you fetched it.

So coverage here is tracked by **requested window**, recorded at capture time:

- A month you asked Chase for and got nothing back is **covered**. It was an
  empty month.
- A month you never asked for is **not covered**, however much data surrounds it.

Two fetches leave no `csv_export` behind and so record their window as a marker
file instead — both are coverage evidence, and both are what stop the planner
re-asking forever:

- **`empty_window`** — the source served no file at all (written by
  `capture.py record-empty`).
- **`refetched_window`** — the source served an export whose bytes were already
  filed, which `raw_documents` then dedups away. Written automatically by
  `capture.py file`. It matters because that table's uniqueness is on
  `content_sha256` ALONE: identical bytes can never produce a second row, so a
  re-download reaching further than any recorded window has no other way to say
  so. A dormant account re-pulled on the overlap rule hits this every run.

That distinction is why `capture.py file` requires `--start` and `--end` — the
window you asked for, not the dates inside the file. Without it, every
genuinely-empty month gets re-downloaded forever.

## Chase's export limits — verified live 2026-08-22

These are not guesses; they were checked against the real portal.

| | Bank (checking, savings) | Credit card |
| --- | --- | --- |
| Oldest date the form accepts | **24 months, to the day** | **same — 24 months** |
| Activity presets | `last24monthsOption`, `DATE_RANGE` | year-to-date, last year, since last statement, 24 statement cycles, `ALL`, `DATE_RANGE` |
| "All transactions" means | **24 months** — the label is a misnomer | genuinely all (still capped at 24 months of history) |
| Range filters on | posting date | posting date |
| Rows per report | 1,000, then silently truncated | same |

The floor is a **rolling daily** one, not a month boundary: with today
2026-08-22, a From of 2024-08-22 was refused and 2024-08-23 accepted. Widening
the range does not evade it — a 4-years-back-to-2-years-back window is rejected
on both endpoints, because the rule applies to each date independently rather
than to the span. And the form does not validate at all until both date fields
are filled, so a lone out-of-range date can look accepted when it is not.

Three consequences baked into the code:

1. `RETENTION_MONTHS` in `plan.py` treats the floor as **hard**, and clips the
   oldest window's start to it so no request asks for a day Chase refuses.
   Months past it are reported as `BEYOND CHASE RETENTION`, not retried forever
   — they can only come from `capture.py import-legacy`.
2. Windows are month-aligned partly because of the **1,000-row cap**. A month
   rarely exceeds it; `ALL` on a busy card easily does, and the truncation is
   silent. `src/providers/chase.py` raises on any file with exactly 1,000 rows
   rather than recording a truncated month as complete.
3. Only accounts Chase actually offers a CSV export for are configurable.
   Investment and loan accounts show on the dashboard but are absent from the
   export form.

## Citi's export limits — verified live 2026-08-24

| | Citi credit card |
| --- | --- |
| Oldest date the search accepts | the statement-close date ~24 months back (a cycle boundary, moves with the statement calendar; refused with "You can only search from … For older transactions, you can request statements for download.") |
| Rows per report | **no cap found** — the entire searchable window exported complete in one file |
| Layout | `Status,Date,Description,Debit,Credit,Member Name` — one date column, unsigned Debit/Credit |
| Empty window | shows "no transactions for this time period." and the export icon silently does nothing — record with `record-empty` |
| Download filename | the scope label ("Date range.CSV"), no account, collisions get " (1)" — always pass `--account`, detect downloads by marker timestamp |

The planner approximates Citi's cycle-boundary floor with the same rolling
24-month computation as Chase; that is conservative (every date at or after it
is guaranteed servable). Export order is newest-first and was verified stable
across re-downloads — two overlapping captures minutes apart returned the
748-row overlap region byte-identical and identically ordered — which is what
makes the `occurrence` counter safe for Citi too. Sign convention:
`amount = credit − debit`, so negative is money out, same as Chase.

## Elan's export limits — verified live 2026-08-24

| | Elan (myaccountaccess.com) credit card |
| --- | --- |
| Oldest date the export serves | **18 months** ("All available dates" is literally `d_545` in the portal's search select) — and the portal never refuses an out-of-range date, it **silently clamps**: a request from 2024-01-01 returned rows starting at the 545-day boundary with no error |
| Rows per report | no cap found — but the discovery account ran ~2.6 rows/month, far too little volume to surface one |
| Layout | `"Date","Transaction","Name","Memo","Amount"` — ISO dates, a single **signed** Amount (negative = money out, no projection needed; a waived fee prints `-0.00`), direction stated in the Transaction column (DEBIT/CREDIT) |
| Empty window | inline "There aren't any transactions for that date range." and no file — record with `record-empty` |
| Download filename | `<label> - <last4>_<start>_<end>.csv`, but the end segment is NOT the requested end (observed ~4 days past it) — the last-4 hint works, the window does not; detect downloads by marker timestamp |

Export order is oldest-first and was verified stable across re-downloads (two
overlapping downloads minutes apart, 25-row overlap byte-identical and
identically ordered), which is what makes the `occurrence` counter safe for
Elan too. Because the portal clamps instead of refusing, the planner's
retention clip is what keeps requests honest — and a floor-straddling request
(only possible manually) must be *filed* with its start clipped to the floor,
or the pre-floor months would be falsely recorded as fetched-and-empty.

## Importing archives older than the retention floor

```sh
uv run python transaction_downloader/capture.py import-legacy <old files>
```

Exports downloaded before the window lapsed have no record of the window that
was *requested*, which is the one thing the coverage model needs. One is
inferred from the transaction dates inside and stamped `window_source:
inferred`; `plan.py` reports those months separately, because coverage resting
on a guess should be visible as such. `--exact` claims only what the file
literally proves, at the cost of leaving partial edge months looking uncovered.

## What always gets re-fetched

Two windows, every run, regardless of what is already stored:

1. **The current month** — incomplete by definition.
2. **Back to `newest transaction − 5 days`** (`--overlap-days`) — Chase posts
   pending activity late and revises descriptions and amounts after the fact, so
   the newest days already stored are the least trustworthy ones. This is the
   transaction-shaped version of `RESTATEMENT_LOOKBACK_DAYS` in `coverage.py`.

Because that second window is measured from the newest transaction rather than
the calendar, it reaches back into the previous month exactly when it should —
if the newest transaction you hold is the 2nd, the previous month is still
settling and gets re-fetched too.

Overlap is free (sha256 dedup at ingest), so both windows err wide on purpose.

## Storage layout

Everything lands in the `chase` entry's `raw_dir` from `providers.local.yaml`:

```
raw_dir/
├── chase_csv_export_{account}_{start}_{end}_captured{date}.csv
├── chase_csv_export_...
└── .chase_captures.jsonl        # manifest: one record per capture
```

Captures are month-aligned, verbatim, and **never overwritten** — re-pulling a
month adds a file rather than replacing one, so a restated transaction stays
traceable to the day it appeared. A download is consumed exactly once either
way: filing moves it out of the browser's folder, and a re-download whose bytes
are already filed is discarded rather than left behind, so a successful run
leaves nothing to clean up by hand. The manifest is a cache; the files are the
truth, and `capture.py reindex` rebuilds it from them. `plan.py` falls back to
scanning the files automatically if the manifest is missing, so losing it never
triggers a full re-download.

## Commands

`land.sh` wraps the file/ingest/parse/plan tail of the pipeline for one
provider, reading `raw_dir` from `providers.local.yaml` so the paths are never
retyped — prefer it over hand-typing `ingest_raw.py`, which is the step that
goes wrong. It defaults to chase and finds the repo from its own path, so it
runs from anywhere:

```sh
bash transaction_downloader/land.sh                       # chase: ingest + parse + plan
bash transaction_downloader/land.sh --provider elan
bash transaction_downloader/land.sh --provider citi \
    <last4>:2026-08-01:2026-08-24:"Date range.CSV"        # file first, then land
bash transaction_downloader/land.sh --provider chase --legacy <archived files>
bash transaction_downloader/land.sh --provider elan --dry-run    # show what it would touch
```

The individual steps, when you need one on its own:

```sh
# what's missing (default provider is chase; add --provider citi for Citi)
uv run python transaction_downloader/plan.py
uv run python transaction_downloader/plan.py --provider citi
uv run python transaction_downloader/plan.py --json          # for the agent
uv run python transaction_downloader/plan.py --full          # include deferred backfill
uv run python transaction_downloader/plan.py --overlap-days 10

# file what was downloaded (--provider before the subcommand)
uv run python transaction_downloader/capture.py file \
    --account <last4> --start 2026-08-01 --end 2026-08-22 <download_dir>/<file>.CSV
uv run python transaction_downloader/capture.py --provider citi file \
    --account <last4> --start 2026-08-01 --end 2026-08-24 "Date range.CSV"
uv run python transaction_downloader/capture.py status
uv run python transaction_downloader/capture.py reindex

# archives older than the retention floor
uv run python transaction_downloader/capture.py import-legacy <old files>

# land it, then normalize it
uv run python src/ingest_raw.py --provider chase <raw_dir>
uv run python src/parse_raw.py --provider chase
```

## Configuration

The `chase` block in `providers.local.yaml` (gitignored; shape in
`template_providers.yaml`). `raw_dir` and `external_ids.accounts` are required;
`backfill_start` is optional and defaults to 24 months.

## Normalizing into `transactions`

```sh
uv run python src/parse_raw.py --provider chase
```

`src/providers/chase.py` handles all three layouts and upserts into the
`transactions` table via `src/transaction_store.py`.

The natural key is
`(account_id, post_date, description, amount, occurrence)`. That last column is
the interesting one: Chase CSVs carry **no transaction id**, and genuinely
identical same-day rows occur — two identical coffees at the same shop. Rows are
numbered in export order within each group, which is stable across re-downloads,
so re-parsing an overlapping window updates in place instead of duplicating, and
mass reprocessing stays idempotent. Without the counter the second coffee would
upsert onto the first and vanish.

### Nothing is discarded

Every field of every source row is stored verbatim in the `transactions.extra`
JSON column: keyed by the original header text, holding the raw unparsed string,
including columns that are also projected into typed columns, values in
positions past the last header, and any column Chase adds later. The typed
columns are a projection for querying; `extra` is the record.

The shape is fixed by parser code — `{layout, columns, unnamed?}` — so it is
identical run to run and no agent decides what is worth keeping.

One Chase login covers several accounts, so there is no single `account_number`
for the provider. The parser reads the account from the capture filename, and
`chase` is listed in `parse_raw.DERIVES_OWN_ACCOUNT_ID` so the usual
account-resolution guard is skipped.

## Still open: no dashboard

Per `docs/LANDING.md` §0 a provider run is not done until the data is visible in
Grafana, and `transactions` currently has no committed dashboard. That is the
remaining gap. It was left undone deliberately rather than hand-rolled: §9 of
the landing contract says dashboards are built in the UI and round-tripped
through `deploy/grafana_sync.py`, never hand-authored as JSON.

## Tests

```sh
uv run pytest tests/test_transaction_downloader.py tests/test_chase_transactions.py tests/test_citi_transactions.py tests/test_elan_transactions.py
```

`test_transaction_downloader.py` covers planning and filing: layout detection,
the overlap window crossing a month boundary, gap detection,
empty-month-is-covered, the retention floor, backfill chunking, verbatim filing,
duplicate suppression, manifest recovery, legacy import, and the ingest
classification hand-off.

`test_chase_transactions.py` covers parsing and storage against a real sqlite
database: all three layouts, identical same-day rows staying distinct,
reprocessing idempotency, an overlapping re-download adding only what is new, a
restated transaction being corrected in place, the 1,000-row truncation guard,
and registry wiring.

`test_citi_transactions.py` mirrors that for Citi: the Debit/Credit sign
convention, the single-date fallback, occurrence stability, restatement
pruning, registry wiring, and the guard that a Citi capture can never prune
another provider's rows when two cards share a last-4 (`transactions` has no
provider column, so the capture filename is what proves provenance).

`test_elan_transactions.py` mirrors that for Elan: the signed Amount kept
as-is (no flip), signed-zero normalization, separator-only memos dropping to
None, layout detection and the download-filename last-4 hint in `store`,
occurrence stability, restatement pruning, registry wiring, and the
cross-provider prune guard.
