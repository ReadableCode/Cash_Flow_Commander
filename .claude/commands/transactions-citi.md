---
description: Citi credit-card transaction acquisition — plan the windows still needed, download each via the user's real Chrome, file verbatim, land into raw_documents
argument-hint: [optional: "full" for deferred backfill months, or "YYYY-MM" for a single month]
---

# /transactions-citi — Citi transaction run

Pull Citi credit-card transaction exports and land them in Cash Flow
Commander's raw store. **Raw-first rule:** every export is captured VERBATIM
before any parsing. `raw_documents` is the source of truth; anything derived is
a rebuildable projection.

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
  cadence**. A month you asked Citi for and got nothing back is covered; a
  month you never asked for is not, however much data surrounds it.
- Load the `citi` entry from `providers.local.yaml` (repo root, gitignored):
  `external_ids.accounts` (last-4 → label), `raw_dir`, `archive_dir`,
  `backfill_start`, `notes`.
- **STOP if the entry is absent.** Do not guess paths or account numbers — copy
  the `citi` block from `template_providers.yaml` into `providers.local.yaml`,
  have the user fill it in, then re-run.

## 0.1 Plan — decide what to download

```sh
uv run python transaction_downloader/plan.py --provider citi
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

- Portal: `https://www.citi.com/` (post-login SPA lives on `online.citi.com`).
  There is no separate download page — the export lives in the Transactions
  tile of the card dashboard (§3), and deep-linking mostly 404s, so navigate
  by clicking the SPA's own links.
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
  and wait. If Citi offers "remember this device," accept it.
- **Keep exactly ONE signed-in Citi tab.** Two tabs sharing a session can log
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
user. If a modal has no visible dismissal, re-navigate to the card dashboard
rather than clicking through it.

Observed at least once (2026-08-24):

- **Post-login marketing interstitial** — login lands on a "Special Offers For
  You" page (`/US/nga/offerintr-nga`), not the dashboard. Do not click
  through it; navigate to the card dashboard instead.
- **A card-conversion notice modal** ("We're Converting Your Card") exists in
  the page's modal stack. It is an account-lifecycle notice, not marketing —
  never click through it, and tell the user it exists: a conversion changes
  the account's last 4, which affects `providers.local.yaml` and capture
  continuity.
- The dashboard pre-renders MANY invisible modal templates (app upsells,
  screen-share-with-rep, cookie settings). A `[role=dialog]` existing in the
  DOM does not mean a dialog is open — check visibility.

## 3. The export flow — as last observed 2026-08-24

Verified live against the real portal on that date. Citi will move this page,
so if what you see disagrees, believe the page and then update this section
(§8).

**It is an Angular SPA (cds design system), and the export flow lives in the
light DOM** — no shadow-root gymnastics needed, unlike Chase. Two navigation
facts that bite:

- **Direct URL navigation mostly 404s** (`/US/ag/pageNotFound`). Reach pages
  by clicking the SPA's own nav links so the router handles it — e.g. the
  account link `a[href*="accountdetails"]` in the signed-in header. The card
  dashboard lands at `/US/ag/dashboard/credit-card?accountId=<internal guid>`.
  The guid is an internal id; the last 4 appears only in the link text. Both
  belong in `providers.local.yaml`, neither in this file.
- Give the SPA ~6–10 seconds after navigation; the shell renders before the
  content, and the Transactions tile lazy-loads.

### The Transactions tile (on the card dashboard)

Everything happens in the "Transactions" section of the card dashboard — there
is no separate download page.

- **Time Period dropdown** (`button.cds-dd2-button`): the current open cycle
  ("Since <last statement close>"), ~6 statement cycles, "Last year", "Year
  to date". **Presets are statement-shaped, not month-shaped — always use the
  custom date range instead**, so the requested window is explicit.
- **Custom range**: expand "Filter By" (`button.expand-cta`), fill the From
  and To inputs (format `MMM D, YYYY`; drive with the native value setter,
  then dispatch `input`/`change`/`blur` with `{bubbles:true}`), click the
  panel's primary **Search** button.
- **The Filter By panel collapses after every search.** Re-expand it before
  the next range. And **verify the applied range chip** (text like
  "Aug 5, 2024 - Aug 24, 2026" above the results) after every search — a
  search clicked while the panel is half-rendered does nothing and leaves the
  previous range silently active.
- **Search errors do render** (an inline `Error.` message near the fields),
  but only when the Search actually fires. Never conclude a range was
  accepted without reading the chip back.

### Export dialog

- Open it from the **download icon in the Transactions header**. Its element
  varies with tile state: sometimes `button.download-icon[aria-label=
  "download"]`, after a custom search a `div[aria-label="Download icon"]`.
  Select by aria-label, filter to the visible one, and note the same icon
  also exists for statement PDFs elsewhere on the page.
- **Clicking the icon toggles the dialog** — a second click closes it. Read
  the dialog state back instead of clicking again blindly.
- The dialog ("Exporting transactions") shows the scope it will export — the
  current view's time period ("Since …", "Date range", a statement cycle) —
  and offers **CSV (default), TXT, QFX, QBO, OFX**. Keep CSV. Click
  `button.export-button`.
- **A "Done!" confirmation modal follows every export** and blocks the page.
  Dismiss it (OK) before doing anything else — the next icon click does
  nothing while it is up.

### Retention — the hard limit

The range search enforces a window and says so explicitly when violated:
*"You can only search from <floor> to <today>. For older transactions, you
can request statements for download."*

- Observed floor: **the statement-close date ~24 months back** — a
  statement-cycle boundary, not Chase's rolling daily floor. A From at
  exactly the floor is accepted.
- Months past the floor are **statement-PDF-only** (see §4.1); the CSV export
  cannot reach them.
- Record the exact observed floor and its observation date in
  `providers.local.yaml` notes on every run — it moves with the statement
  calendar.

### Row cap

None found. A single export of the **entire ~24-month searchable window came
back complete** (hundreds of rows, oldest row exactly at the requested From).
No cap warning appears anywhere on the form. Until a cap is ever observed,
one export per planner window is fine even for wide windows — but keep the
capture-time row-count report honest so a future silent cap would surface as
a suspiciously round number.

### The CSV

Header: `Status,Date,Description,Debit,Credit,Member Name`.

- **No transaction id** — the Chase-style occurrence counter applies. Export
  order was verified stable live (two overlapping downloads, byte-identical
  overlap region).
- **Debit and Credit are separate columns** (charge in Debit, payment/refund
  in Credit), unlike Chase's signed single amount — and **Citi prints Credit
  values NEGATIVE** (verified across every live credit row, 2026-08-24). The
  parser projects `amount = abs(credit) - abs(debit)` so a payment lands as
  money IN whichever sign Citi prints. A naive `credit - debit` flipped every
  payment to money-out once already.
- **Payment credits carry several descriptions** depending on channel:
  `ONLINE PAYMENT, THANK YOU` for manual payments, `AUTOPAY ...AUTO-PMT` for
  autopay. Anything pairing payoff legs must match on amount/direction, not
  one description.
- **One Date column only.** All observed rows had Status `Cleared`; whether
  pending activity ever exports (and with what Status value) is unverified.
- `Member Name` distinguishes cardholders/authorized users.
- The filename is the **scope label, not the account or window**:
  `Since Aug 06, 2026.CSV`, `Date range.CSV`, and collisions get
  `Date range (1).CSV`. Detect a completed download ONLY by a
  marker-timestamp watch on the download folder; the name proves nothing and
  repeats across accounts and runs.

### Empty windows export nothing — silently

A range with no activity shows *"no transactions for this time period."* The
download icon stays visible but **clicking it does nothing**: no dialog, no
file, no error. This is a normal outcome — record it with
`capture.py record-empty` (§4) so the planner stops re-asking.

## 4. File each download verbatim

For every download, immediately (`--provider` comes before the subcommand):

```sh
uv run python transaction_downloader/capture.py --provider citi file \
    --account <last4> --start <YYYY-MM-DD> --end <YYYY-MM-DD> <downloaded file>
```

Always pass `--account`: Citi download filenames are scope labels ("Date
range.CSV") and carry no last 4 to infer from.

`--start`/`--end` must be the window you actually requested from Citi, not the
dates you see inside the file. That distinction is the whole coverage model: a
month fetched and found empty is covered, a month never fetched is not.

If a window produces no file because there was no activity, record it
explicitly so the planner stops re-asking — an empty window is a normal
outcome, not a failure:

```sh
uv run python transaction_downloader/capture.py --provider citi record-empty \
    --account <last4> --start <YYYY-MM-DD> --end <YYYY-MM-DD>
```

Check the user's `notes` in `providers.local.yaml` for their browser download
location before hunting for files. Identical bytes filed twice are a no-op.
Nothing is ever overwritten.

## 4.1 Months Citi will not serve

Months older than the retention floor (§3) cannot come from the CSV export at
all — the range search refuses them and points at **statement downloads**
instead. `plan.py` reports them as `BEYOND CITI RETENTION`. Two recovery
paths, both feeding `capture.py --provider citi import-legacy` (which infers
the request window from the dates inside and stamps it
`window_source: inferred`, so coverage resting on a guess stays visible):

- CSV exports downloaded before the window lapsed, if any exist in archives.
- Statement PDFs from the portal's Statements & Documents area — but that is
  parser work of a different order (PDF, not CSV) and is out of scope until
  someone decides those months are worth it.

## 5. Land into raw_documents

```sh
uv run python src/ingest_raw.py --provider citi <archive_dir> <raw_dir> <data_dir>
```

Captures classify as `csv_export` with `period_hint` set to the first of the
requested window's month. sha256 dedup makes re-runs free. Report ingested vs
deduped counts.

## 6. Normalize

```sh
uv run python src/parse_raw.py --provider citi
```

`src/providers/citi.py` handles the layout and upserts into `transactions`.
Report parsed / errored / no_parser counts and rows upserted.

Facts the parser encodes, repeated here because they are the ones that bite:

- **Citi CSVs have no transaction id**, so rows carry an `occurrence` counter
  in export order within each (account, post_date, description, amount)
  group, and it is part of the natural key. **Export order was verified
  stable** (2026-08-24): two independent downloads of overlapping windows,
  minutes apart, returned the 748-row overlap byte-identical and identically
  ordered.
- **Sign convention:** Debit and Credit are separate unsigned columns;
  `amount = credit − debit`, so negative is money out, matching Chase and the
  `transactions` table contract. `txn_type` records which column the money
  came from (DEBIT/CREDIT).
- **One date column** feeds both `txn_date` and `post_date` — Citi exports
  only one, and inventing a second would be fabrication.
- **Status column:** every observed row is `Cleared`; whether pending rows
  ever export is unverified. Restatement handling mirrors Chase's anyway —
  each capture is authoritative for its requested window, and rows that
  vanish from a newer capture of the same window are pruned (`stale
  transactions removed` in the parse summary). If Citi never restates, the
  prune is a no-op; if it does, routine overlap runs self-correct.
- **No row cap** has been observed, so unlike Chase there is no truncation
  guard — there is no known cap to guard against. Watch for suspiciously
  round row counts anyway.
- **Nothing from the source row is discarded.** Every field goes verbatim
  into `transactions.extra`, keyed by its original header text, including
  `Status` and `Member Name` and any column Citi adds later. The typed
  columns are a convenience projection; `extra` is the record.

The account comes from the capture filename, not from `account_number` — one
Citi login can cover several cards, so `citi` is listed in
`parse_raw.DERIVES_OWN_ACCOUNT_ID`.

On failure: fix parser code, bump `PARSER_VERSION`, reprocess. Never hand-edit
parsed output. Upserts are keyed naturally, so mass-reprocessing is idempotent.

## 7. Report and verify

Report: windows planned vs downloaded, per-account row counts, anything Citi
refused to export, captures filed vs deduped, ingest ingested/deduped counts,
transactions upserted, and any popup or flow change you had to work around.

- [ ] every planned window has a capture, a `record-empty` marker, or an
      explicit reason it has neither
- [ ] the planner re-run shows the fetched months covered
- [ ] no capture came back at the export row cap (silent truncation)
- [ ] re-run `ingest_raw.py` → 100% dedup, zero new rows
- [ ] `parse_raw.py` reports zero errored and zero no_parser
- [ ] re-run `parse_raw.py --status all` → same row count in `transactions`
      (reprocessing must be idempotent)
- [ ] a spot-checked month's transaction count and net total match what the
      Citi UI shows for that account and period
- [ ] sign spot-check: payments/credits land POSITIVE in `transactions`
      (money in), charges negative — Citi's negative-Credit quirk flipped
      every payment once, and a card with zero positive rows is the tell
- [ ] months reported as inferred are ones you actually imported from an
      archive

## 8. Keeping this command current

Citi will change this flow. When it does, the fix belongs here, not in a
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
