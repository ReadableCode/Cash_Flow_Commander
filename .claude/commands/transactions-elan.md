---
description: Elan Financial Services credit-card transaction acquisition — plan the windows still needed, download each via the user's real Chrome, file verbatim, land into raw_documents
argument-hint: [optional: "full" for deferred backfill months, or "YYYY-MM" for a single month]
---

# /transactions-elan — Elan transaction run

Pull Elan Financial Services credit-card transaction exports (the
myaccountaccess.com cardmember portal — Elan issues white-label cards for many
banks and credit unions) and land them in Cash Flow Commander's raw store.
**Raw-first rule:** every export is captured VERBATIM before any parsing.
`raw_documents` is the source of truth; anything derived is a rebuildable
projection.

Run this whenever you want fresh transactions. There is deliberately no
schedule — the planner works out what is still missing from what is already on
disk, so running it twice in a day is cheap and running it once a quarter still
gets everything.

## 0. Orient

**This command is deployed globally, so the session may not start in the repo.**
Every path below is relative to the Cash Flow Commander clone. Before anything
else, change into it — conventionally `~/GitHub/Cash_Flow_Commander`. If it is
somewhere else on this host, find it and use that; do not run these commands
from an unrelated directory, and do not hardcode a machine-specific path here.

- Read `docs/LANDING.md` for the landing architecture and ingest conventions.
- Read `transaction_downloader/README.md` for how coverage is tracked here and
  why it differs from `src/coverage.py`: **by requested window, not by data
  cadence**. A month you asked Elan for and got nothing back is covered; a
  month you never asked for is not, however much data surrounds it.
- Load the `elan` entry from `providers.local.yaml` (repo root, gitignored):
  `external_ids.accounts` (last-4 → label), `raw_dir`, `archive_dir`,
  `backfill_start`, `notes`.
- **STOP if the entry is absent.** Do not guess paths or account numbers — copy
  the `elan` block from `template_providers.yaml` into `providers.local.yaml`,
  have the user fill it in, then re-run.

## 0.1 Plan — decide what to download

```sh
uv run python transaction_downloader/plan.py --provider elan
```

Every `FETCH` line is one download to perform. Use those windows; do not invent
your own and do not re-pull everything. **One download per planner window** —
never merge windows, because the requested window is what marks a month
covered.

Overlap is free — sha256 dedup at ingest — so a window that overlaps held data
is correct and a window that misses days is not.

Argument handling for `$ARGUMENTS`:

- empty → run the plan as-is
- `full` → add `--full`, which includes months the planner deferred (a long
  backfill is chunked newest-first across runs)
- `YYYY-MM` → ignore the plan and fetch just that month, for a targeted repair

**Set `backfill_start` explicitly when the goal is "all of it".** Without it
the planner anchors history to the oldest existing capture — reasonable after a
completed first run, but a single stray capture (e.g. one filed during a live
verification) silently suppresses the whole backfill for that account. That
happened once on Chase (2026-08-22): one stray July capture made a 24-month
backfill plan look like a 2-month refresh.

## 1. Browser session — the user's REAL Chrome, one way or another

- Portal: `https://www.myaccountaccess.com/` (partner-branded card servicing;
  the signed-in dashboard lives under
  `/digital/servicing/pcsp-dashboard/partner-account-dashboard`).
- **Before opening the portal, sweep Chrome for existing tabs on the same
  domain and close all but one (or all, then open fresh).** Do this FIRST,
  not after noticing trouble: AppleScript finds tabs by URL match, and
  duplicate same-site tabs make every subsequent call ambiguous — plus two
  tabs sharing a session can log both out when one idles into timeout.
  Verify exactly one tab remains by counting, not by assuming the close
  landed.
- **It must be the user's real Chrome profile.** The password manager lives
  there; embedded/collaborative browser panels and fresh automation profiles
  don't autofill, which forces manual credential entry — the user will reject
  that. Two pathways that work:
  - **Claude in Chrome**, when its tools are available in the session.
  - **AppleScript on macOS**, when they are not: open the page with
    `osascript -e 'tell app "Google Chrome" to open location ...'`, then drive
    it with `execute <tab> javascript "..."`. Requires the one-time toggle
    **View → Developer → Allow JavaScript from Apple Events** in Chrome (have
    the user click it) plus macOS Automation consent on first use. Find the tab
    by URL match on every call — never by index, tabs move.
  - **Off macOS** (Windows or Linux), neither AppleScript nor any osascript
    equivalent exists. If Claude in Chrome is not in the session, stop and say
    so rather than improvising an automation profile — a fresh profile has no
    password manager, which puts the user back to typing credentials. Either
    run this from a Mac, or bring Claude in Chrome into the session first.
- The user signs in themselves — **never type, store, or echo credentials**,
  and never read them out of the password manager. Hand control over for MFA
  and wait. If Elan offers "remember this device," accept it.
- **Keep exactly ONE signed-in Elan tab.** Two tabs sharing a session can log
  both out when one idles into timeout. Close extras before starting.
- **Chrome silently blocks every download after the first** until the user
  clicks Allow on the "wants to download multiple files" prompt (near the
  address bar). The tell: the portal confirms the export but no file lands.
  The permission then persists for the profile.

## 2. Popups, interstitials, and other things in the way

Bank portals insert these unpredictably, which is why this command drives a
real browser with judgment instead of a fixed selector script. Read the page
before acting on each step.

**Dismiss, never accept.** Close, "Not now", "Remind me later", "No thanks".
Never enroll in anything, never change an account setting, never accept an
offer, never dismiss a genuine security prompt — if a modal looks like it is
about security or account settings rather than marketing, stop and ask the
user. If a modal has no visible dismissal, re-navigate to the dashboard
rather than clicking through it.

No marketing interstitials or nag modals were seen on the 2026-08-24
discovery session or the 2026-09-04 run — login landed straight on the
dashboard both times. Expect them anyway; when one appears, add it here with
the date first seen.

## 3. The export flow — as last observed 2026-09-04

Verified live against the real portal on that date. Elan will move this page,
so if what you see disagrees, believe the page and then update this section
(§8).

**Everything lives on the account dashboard**
(`/digital/servicing/pcsp-dashboard/partner-account-dashboard`) — there is no
separate download page. It is U.S. Bank's design system (`usb-*` classes) in
the **light DOM with plain HTML controls and stable element ids**: native
`<select>` elements and ordinary `<input>` fields. No shadow roots anywhere
(unlike Chase). Navigating to `https://www.myaccountaccess.com/` while a
session is alive redirects to the dashboard; a dead session bounces deep
links to a logout-confirmation page — re-login from the root URL.

### The Transaction List controls

Three buttons above the list: `#transaction-search-button`,
`#transaction-download-button`, `#transaction-print-button`.

- **Search** opens an on-page filter panel: keyword input, a Transaction
  type select, a native date-range select `#select_transaction-date-range`
  (values `d_30`, `d_60`, `d_90`, `d_180`, `ytd`, `m_12`, `d_545` labelled
  "All available dates", and `custom`), and amount radios. Selecting
  `custom` reveals `#input_startDate` / `#input_endDate` (`MM/DD/YYYY`).
  Panel buttons: `#transactionSearch__searchButton`, `...__clearFormButton`,
  `...__cancelButton`. The search filters the on-page list only — **the
  download panel takes its own dates, so a run never needs the search
  panel**; it is described here because it shares input ids with the
  download panel (only one is visible at a time — scope queries carefully).
- **Download** (`#transaction-download-button`) opens the export panel:
  its own `#input_startDate` / `#input_endDate` (`MM/DD/YYYY`, prefilled to
  the last 30 days), a native format select `#select_download-types`
  (`CSV` default, `QBO`, `QFX` — keep CSV), and
  `#transactionsDownloadOptionsDownload` / `...Dismiss` buttons. The panel
  stays open after a download, so consecutive windows need no re-opening.
  Drive the date inputs with the native value setter, then dispatch
  `input`/`change` with `{bubbles:true}` and blur; verify by reading
  `.value` back.

The panel warns *"your download may not include recent or pending
transactions"* — which is exactly why the planner's always-refetch overlap
window exists.

### Retention — the soft limit

Claimed and observed: **18 months** (the search select's "All available
dates" is literally `d_545` — 545 days ≈ 18 months — and the helper text
says "Search for transactions in last 18 months").

Unlike Chase and Citi, **out-of-range dates are not refused — they are
silently clamped**. A download requested from 2024-01-01 (2026-08-24 run)
returned rows starting 2025-02-27, right at the 545-day boundary — and the
first row was a payment followed by interest charges, proving older activity
existed and was clamped away, not absent. A window lying entirely before the
floor behaves like an empty window (below). So the floor never errors; it
just quietly returns less than you asked for. The planner's
`retention_months` clip is what keeps requests honest.

### Row cap

Unknown. The observed account ran ~2.6 rows/month (46 rows over the full 18
months), far too little volume to surface any cap. No cap warning appears on
the form. Watch for suspiciously round row counts on busier cards.

### The CSV

Header: `"Date","Transaction","Name","Memo","Amount"` — all fields quoted.

- **Dates are ISO** (`YYYY-MM-DD`), one date column only. The on-page list
  labels rows POSTED, so treat it as the posting date; there is no second
  date to project.
- **Amount is a single signed column and negative is money out**: `DEBIT`
  rows (purchases, interest, fees) print negative, `CREDIT` rows (payments,
  refunds) print positive. Verified against live rows 2026-08-24. This
  matches the `transactions` table contract directly — no sign flip needed.
  A waived fee prints as signed zero (`-0.00`).
- The `Transaction` column carries `DEBIT`/`CREDIT`; `Name` is the merchant
  or description; `Memo` holds semicolon-separated reference data (network
  reference number and category code on purchases, channel markers like
  `WEB AUTOMTC` on payments — empty slots print as `; ; ; ; ;`).
- **No transaction id column** — the occurrence-counter design applies.
  (Purchases carry a network reference number inside Memo, but payments and
  fees do not, so it cannot serve as a key.)
- **Export order is oldest-first and stable across re-downloads**: two
  overlapping downloads minutes apart (2026-08-24) returned the 25-row
  overlap region byte-identical and identically ordered — which is what
  makes the occurrence counter safe for Elan.

### Empty windows produce a visible error and NO file

A window with no activity (including one entirely before the retention
floor) shows an inline notification — *"There aren't any transactions for
that date range. Try expanding your search."*
(`#transactionsDownloadErrorNotification`) — and downloads nothing. This is
a normal outcome — record it with `capture.py record-empty` (§4) so the
planner stops re-asking.

### Where the file lands, and what it is called

Wherever the browser profile says — record the real location in
`providers.local.yaml` notes. The filename pattern is
`<account label> - <last4>_<MM-DD-YYYY start>_<MM-DD-YYYY end>.csv`, with
` (1)` appended on collision — **but the end segment is NOT the requested
end date**: it has run exactly four days past the requested end in every
download so far (both 2026-08-24 downloads, requested end 08/24, were named
`..._08-28-2026.csv`; the 2026-09-04 download, requested end 09/04, came back
`..._09-08-2026.csv`). Do not lean on that offset — it is an observation, not
a documented contract. Detect a completed download ONLY by a marker-timestamp
watch on the download folder; never trust the name.

Chrome's multiple-download block applies: the second download of a session
silently produces no file until the user clicks Allow on the
"www.myaccountaccess.com wants to download multiple files" prompt — and the
blocked download is then released retroactively, so expect a duplicate
(sha256 dedup absorbs it).

## 4. File each download verbatim

For every download, immediately (`--provider` comes before the subcommand):

```sh
uv run python transaction_downloader/capture.py --provider elan file \
    --account <last4> --start <YYYY-MM-DD> --end <YYYY-MM-DD> <downloaded file>
```

`--start`/`--end` must be the window you actually requested from Elan, not the
dates you see inside the file. That distinction is the whole coverage model: a
month fetched and found empty is covered, a month never fetched is not.

One Elan-specific wrinkle: because the portal silently clamps rather than
refuses (§3), a request that straddles the retention floor gets answered only
from the floor onward. If that ever happens (routine runs never hit it — the
planner clips requests to the floor first), file with `--start` clipped to the
floor, not the nominal request: claiming the pre-floor months would record
them as fetched-and-empty when in truth the portal declined to serve them.

If a window produces no file because there was no activity, record it
explicitly so the planner stops re-asking — an empty window is a normal
outcome, not a failure:

```sh
uv run python transaction_downloader/capture.py --provider elan record-empty \
    --account <last4> --start <YYYY-MM-DD> --end <YYYY-MM-DD>
```

Check the user's `notes` in `providers.local.yaml` for their browser download
location before hunting for files. Identical bytes filed twice are a no-op.
Every download is consumed exactly once — a new one is moved into the repo,
an identical re-download is discarded once its bytes are confirmed already
filed — so a clean run leaves the download folder empty and anything left
there is real unfiled work. A discarded re-download still records the window it
was requested for, as a `refetched_window` marker, because coverage is tracked
by requested window and identical bytes can never record it themselves.
Nothing is ever overwritten.

## 4.1 Months Elan will not serve

Months older than the 18-month floor (§3) cannot come from the CSV export —
the portal silently clamps to the floor and a fully pre-floor window returns
the no-transactions notification. `plan.py` reports them as beyond retention.
Recovery paths, both feeding `capture.py --provider elan import-legacy`
(which infers the request window from the dates inside and stamps it
`window_source: inferred`):

- CSV exports downloaded before the window lapsed, if any exist in archives.
- Statement PDFs from the portal's Documents area — parser work of a
  different order (PDF, not CSV), out of scope until someone decides those
  months are worth it.

## 5. Land into raw_documents

```sh
uv run python src/ingest_raw.py --provider elan data/elan/incoming
```

That path is this provider's `raw_dir`. Unlike the bills commands, `archive_dir`
is deliberately empty here and `data_dir` is a work directory holding no
captures — `raw_dir` is the only directory with anything to ingest.

**Never omit the directory arguments.** With `--provider` set and no paths,
`ingest_raw.py` falls back to `$CFC_RAW_INGEST_DIRS` and stamps that provider
onto every file it finds there — including other providers' documents, whose
`provider` column is then simply wrong. It happened on 2026-09-04: an
argument-less `--provider elan` filed five Rhythm documents under `elan`, and
they had to be reassigned by hand afterwards. Always pass the directory
explicitly, even when you think the default is set to something harmless.

Captures classify as `csv_export` with `period_hint` set to the first of the
requested window's month. sha256 dedup makes re-runs free. Report ingested vs
deduped counts.

## 6. Normalize

```sh
uv run python src/parse_raw.py --provider elan
```

`src/providers/elan.py` handles the layout and upserts into `transactions`.
Report parsed / errored / no_parser counts and rows upserted.

Facts the parser encodes, established live 2026-08-24, repeated here because
they are the ones that bite:

- **No transaction id**, so rows carry an `occurrence` counter in export
  order within each (account, post_date, description, amount) group, part
  of the natural key. Export order was verified stable (§3), which is what
  makes the counter safe.
- **Sign convention: none needed.** Amount is already signed with negative
  as money out, matching the table contract. Parse it as-is; do not flip.
  Handle signed zero (`-0.00`). `txn_type` comes from the `Transaction`
  column (DEBIT/CREDIT).
- **One date column** (ISO format) feeds both `txn_date` and `post_date` —
  inventing a second would be fabrication.
- **Restatement handling mirrors Chase/Citi**: each capture is
  authoritative for its requested window; rows that vanish from a newer
  capture of the same window are pruned. Whether Elan restates is
  unverified — if it never does, the prune is a no-op.
- **No known row cap**, so no truncation guard constant exists yet — watch
  for suspiciously round row counts and add one the day a cap is observed.

Every source field goes verbatim into `transactions.extra`, keyed by its
original header text, with the shape fixed by parser code — no run decides
what is worth keeping. The typed columns are a convenience projection; `extra`
is the record.

The account comes from the capture filename, not from `account_number` — an
Elan login can cover several cards, so `elan` is listed in
`parse_raw.DERIVES_OWN_ACCOUNT_ID`.

On failure: fix parser code, bump `PARSER_VERSION`, reprocess. Never hand-edit
parsed output. Upserts are keyed naturally, so mass-reprocessing is idempotent.

## 7. Report and verify

Report: windows planned vs downloaded, per-account row counts, anything Elan
refused to export, captures filed vs deduped, ingest ingested/deduped counts,
transactions upserted, and any popup or flow change you had to work around.

- [ ] every planned window has a capture, a `record-empty` marker, a
      `refetched_window` marker (written automatically when a re-download comes
      back byte-identical), or an explicit reason it has none of those
- [ ] the planner re-run shows the fetched months covered
- [ ] no capture came back at the export row cap (silent truncation)
- [ ] re-run `ingest_raw.py` → 100% dedup, zero new rows
- [ ] `parse_raw.py` reports zero errored and zero no_parser
- [ ] re-run `parse_raw.py --status all` → same row count in `transactions`
      (reprocessing must be idempotent)
- [ ] a spot-checked month's transaction count and net total match what the
      Elan UI shows for that account and period
- [ ] sign spot-check: payments/credits land POSITIVE in `transactions`
      (money in), charges negative — an account with zero rows in one
      direction means the sign projection is wrong (Citi shipped that way
      once)
- [ ] months reported as inferred are ones you actually imported from an
      archive

## 8. Keeping this command current

Elan will change this flow. When it does, the fix belongs here, not in a
one-off workaround you forget by next run. Before finishing, if the portal did
not match §3:

1. Update §3 to what you actually saw, and change the "as last observed" date.
2. Add any new popup to §2.
3. Put user-specific quirks (download location, which cards export how far
   back) in the `notes` field of `providers.local.yaml` — never in this file.
4. Tell the user what you changed. **Do not commit** — they review and commit.

---

## Pre-commit hygiene checklist (this repo is PUBLIC)

- [ ] No account numbers or last-4 digits.
- [ ] No paths containing a username (no `/Users/<name>/...`).
- [ ] No email addresses, credentials, tokens, or session cookies.
- [ ] No balances, transaction descriptions, or merchant names.
- [ ] Personal values appear only symbolically, referenced from
      `providers.local.yaml`.
