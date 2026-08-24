---
description: Chase transaction acquisition — plan the windows still needed, download each via the user's real Chrome (Claude in Chrome or AppleScript), file verbatim, land into raw_documents
argument-hint: [optional: "full" for deferred backfill months, or "YYYY-MM" for a single month]
---

# /transactions-chase — Chase transaction run

Pull Chase bank and credit-card transaction exports and land them in Cash Flow
Commander's raw store. **Raw-first rule:** every CSV is captured VERBATIM before
any parsing. `raw_documents` is the source of truth; anything derived is a
rebuildable projection.

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

`transaction_downloader/land.sh` locates the repo from its own path, so it can
be invoked from anywhere; the `uv run python ...` calls cannot.

- Read `docs/LANDING.md` for the landing architecture and ingest conventions.
- Read `transaction_downloader/README.md` for how coverage is tracked here and
  why it differs from `src/coverage.py`.
- Load the `chase` entry from `providers.local.yaml` (repo root, gitignored):
  `external_ids.accounts` (last-4 → label), `raw_dir`, `archive_dir`,
  `backfill_start`, `notes`.
- **STOP if the entry is absent.** Do not guess paths or account numbers — copy
  the `chase` block from `template_providers.yaml` into `providers.local.yaml`,
  have the user fill it in, then re-run.

## 0.1 Plan — decide what to download

```sh
uv run python transaction_downloader/plan.py
```

Every `FETCH` line is one download to perform. Use those windows; do not invent
your own and do not re-pull everything.

The planner always asks for the current month plus everything back to
`newest transaction − 5 days`, because Chase posts pending activity late and
revises it after the fact. Overlap is free — sha256 dedup at ingest — so a
window that overlaps held data is correct and a window that misses days is not.

Argument handling for `$ARGUMENTS`:

- empty → run the plan as-is
- `full` → add `--full`, which includes months the planner deferred (a long
  backfill is chunked newest-first across runs)
- `YYYY-MM` → ignore the plan and fetch just that month, for a targeted repair

Two planner behaviors that look wrong and are not:

- **The current month is listed as FETCH even immediately after a run.** It is
  the always-refetch rule, not a gap. Right after a successful run, "N windows
  to download" where every line says `[current month]` means you are done.
- **Set `backfill_start` explicitly when the goal is "all of it".** Without it
  the planner anchors history to the oldest existing capture — reasonable
  after a completed first run, but a single stray capture (e.g. one filed
  during a live verification) silently suppresses the whole backfill for that
  account. That happened once (2026-08-22, Sapphire): one July capture made a
  24-month backfill plan as a single 2-month refresh.

## 1. Browser session — the user's REAL Chrome, one way or another

- Portal: `https://secure.chase.com`
- **It must be the user's real Chrome profile.** The password manager lives
  there; embedded/collaborative browser panels (t3 preview, fresh automation
  profiles) don't autofill, which forces manual credential entry — the user
  will reject that. Two pathways that work:
  - **Claude in Chrome**, when its tools are available in the session.
  - **AppleScript on macOS**, when they are not (proven 2026-08-22 for a full
    31-window backfill): open the page with
    `osascript -e 'tell app "Google Chrome" to open location ...'`, then drive
    it with `execute <tab> javascript "..."`. Requires the one-time toggle
    **View → Developer → Allow JavaScript from Apple Events** in Chrome (have
    the user click it) plus macOS Automation consent on first use. Find the tab
    by URL match on every call — never by index, tabs move.
- The user signs in themselves — **never type, store, or echo credentials**,
  and never read them out of the password manager. Hand control over for MFA
  and wait. If Chase offers "remember this device," accept it.
- **Keep exactly ONE signed-in Chase tab.** Two dashboard tabs sharing the
  session has gotten the whole session logged out mid-run (one tab idles into
  timeout and kills both). Close extras before starting.
- **Chrome silently blocks every download after the first** until the user
  clicks Allow on the "secure.chase.com wants to download multiple files"
  prompt (near the address bar). The tell: Chase shows its confirmation page
  but no file lands. The permission then persists for the profile.
- A transient **HTTP ERROR 401** page has appeared mid-run without the session
  actually dying. Re-navigate to the download URL before concluding you are
  logged out; only a redirect to the sign-in page means re-login.

The download form is reachable directly, no dashboard navigation needed:

```
https://secure.chase.com/web/auth/dashboard#/dashboard/accountDetails/downloadAccountTransactions/index
```

Give it ~6 seconds; the SPA renders the shell before the form.

## 2. Popups, interstitials, and other things in the way

Chase inserts these unpredictably, which is why this command drives a real
browser with judgment instead of a fixed selector script. Read the page before
acting on each step.

**Dismiss, never accept.** Close, "Not now", "Remind me later", "No thanks".
Never enroll in anything, never change an account setting, never accept an
offer, never dismiss a genuine security prompt — if a modal looks like it is
about security rather than marketing, stop and ask the user.

Observed at least once:

- **Debit Card Coverage interstitial** (seen 2026-08-22, immediately after
  login) — an *overdraft settings* confirmation screen with a "Done" button.
  This one is account settings, not marketing. **Do not click through it.**
  Navigate straight to the download URL instead; that commits nothing.
- marketing interstitials — pre-qualified offers, card upsells, Zelle promos
- "go paperless" / statement-preference nags
- survey and feedback overlays
- session-timeout and "are you still there" modals
- cookie and privacy banners
- **stale restored state on the deep-link**: navigating straight to the
  download URL has landed on a leftover confirmation page or even a leftover
  error page ("You can't download your report because the number of
  transactions is more than we're able to show") from a previous session.
  Nothing is wrong — click "Download other activity" to get a fresh form.

A dashboard banner reading **"N of M accounts is hidden"** has been observed
while every account was in fact visible. Treat it as noise; do not go into
Profile & settings to "fix" it.

If a modal has no visible dismissal, re-navigate to the download URL rather
than clicking through it.

## 3. The download form — as last observed 2026-08-22

Verified live against the real portal on that date. Chase still moves this page,
so if what you see disagrees, believe the page and then update this section (§8).

**It is not a normal HTML form.** There are no `<select>` elements — Chase uses
`mds-select` web components. `document.querySelectorAll('select')` returns
nothing, and Playwright-style `select_option` cannot drive it. Element ids:

| Control | id | Notes |
| --- | --- | --- |
| Account | `#account-selector` | options in an `options` **attribute** (JSON) |
| File type | `#downloadFileTypeOption` | `CSV` `QFX` `QIF` `QBO`; **CSV is already the default** |
| Activity | `#downloadActivityOptionId` | options are child `<mds-select-option>` elements, populated only *after* an account is chosen |
| Download | `#download` | |
| Cancel | `#Canceldownload` | resets the form and closes the dialog |

Reading the account list without clicking anything:

```js
JSON.parse(document.querySelector('#account-selector').getAttribute('options'))
// -> [{value: "<internal numeric id>", name: "Checking (...NNNN)", index: 0}, ...]
```

Note the option **value is an internal numeric account id, not the last 4**. The
last 4 is only in the display `name`. Map internal id → last 4 via the `name`,
and keep both in `providers.local.yaml` — neither belongs in this file.

Reading the activity options for the currently selected account:

```js
[...document.querySelector('#downloadActivityOptionId')
    .querySelectorAll('mds-select-option')]
  .map(o => ({value: o.getAttribute('value'), label: o.getAttribute('label')}))
```

### Driving the controls — what actually registers (verified 2026-08-22)

Setting `.value` or dispatching events on the `mds-select` element itself is
**ignored**. What works is what a user does — open the dropdown, click the
option:

- **Account selector**: its options render inside its shadow root. Click the
  shadow button (`sel.shadowRoot.querySelector('button')`), wait ~1s, then
  click the shadow option:
  `sel.shadowRoot.querySelector('mds-select-option[value="<internal id>"]').click()`.
- **Activity selector**: same open-the-shadow-button step, but its options are
  **light-DOM children** — click
  `document.querySelector('#downloadActivityOptionId mds-select-option[value=DATE_RANGE]')`.
  The two components differ; check both places before concluding an option is
  missing.
- **Date fields**: nested two shadow roots deep —
  `mds-datepicker#accountActivityFromDate` → `mds-text-input` → `input`
  (same for `...ToDate`). Reach the inner input, focus it, set the value with
  the native setter
  (`Object.getOwnPropertyDescriptor(HTMLInputElement.prototype,'value').set`),
  then dispatch `input` and `change` with `{bubbles:true, composed:true}` and
  blur/`focusout`. Verify by reading the component's `value` attribute and its
  `error-messages` attribute (a JSON array; `[]` means valid).
- After every step, **verify state by reading attributes back** rather than
  assuming the click landed.

### The two account kinds behave differently

**Bank accounts (checking, savings)** — two activity options only:

- `last24monthsOption`, labelled **"All transactions"**. The label is a
  **misnomer**: it is 24 months, not all. Never rely on it.
- `DATE_RANGE`, labelled "Choose a date range".

**Credit cards** — 29 options: `YEAR_TO_DATE`, `PREVIOUS_YEAR`,
`SINCE_LAST_STATEMENT`, 24 × `STMT_CYCLE_n` (one per statement, newest first),
`ALL` (labelled "All transactions" — on a card this one really does mean all),
and `DATE_RANGE`.

So the *same label* means different things on the two kinds. Always select
`DATE_RANGE` and pass the planner's explicit window. Presets are stateful or
ambiguous and silently skip data the planner knows is missing.

### Date fields

Selecting `DATE_RANGE` reveals **From** and **To** in `mm/dd/yyyy`. They live in
shadow DOM — `document.querySelectorAll('input')` does not reach them. Drive
them by clicking the field and typing. Blur afterwards (click neutral space) to
trigger validation before pressing Download.

### Retention — the hard limit

**24 months, on every account kind, measured to the day.** Verified live
2026-08-22 on both a checking account and a credit card:

- A From or To outside the window is rejected: *"Please tell us a date range
  between 24 months ago and today."*
- With today = 2026-08-22, a From of `2024-08-22` was **refused** and
  `2024-08-23` **accepted**. The floor is a rolling daily one, not a month
  boundary — a month-aligned start would ask for days Chase will not serve.
- Widening the range does not help. A 4-years-ago-to-2-years-ago window was
  rejected on **both** fields: the rule applies to each endpoint independently,
  not to the span between them.
- **The form does not validate until both date fields are populated.** A single
  out-of-range date with the other field empty looks accepted. Do not read that
  as permission.

`plan.py` encodes this in `RETENTION_MONTHS` / `retention_floor()`, clips the
oldest window's start to the floor, and reports older months as
`BEYOND CHASE RETENTION` rather than as work to retry. Those months can only
come from archived exports — see §4.1.

### The 1,000-transaction cap

The form warns: *"If you have more than 1,000 transactions to display, you'll
need to create more than one report. Use the date range to narrow your results."*

This is the other reason windows are bounded. A month almost never exceeds
1,000 rows, while `ALL` on a busy card easily can — and the truncation is
**silent**.

`capture.py` refuses to file any export that comes back at the cap, before it
reaches `raw_documents`. When that happens, re-download that window in smaller
pieces (`--window-months 2`); there is nothing to clean up, because a refused
download is never filed and never moved.

Sizing guidance from real data: a checking account ran ~48 rows/month, a
Sapphire card ~133/month. So 6-month windows suit bank accounts and 4-month
windows suit cards. When in doubt go narrower — a refusal costs one re-download,
while a silent truncation costs a month of data you think you have.

### Layout shifts

The 1,000-transaction warning appears on some accounts and not others, which
moves every control below it down by ~28px. **Re-screenshot after changing the
account** rather than reusing coordinates from the previous account.

### Order of operations

1. Select the account. Wait ~2s — the activity options repopulate.
2. Confirm File type is `CSV` (it usually already is).
3. Select `DATE_RANGE` / "Choose a date range".
4. Type From and To from the planner window. Blur.
5. Click `#download`. Chase then shows a **confirmation page**
   (`.../confirmDownloadAccountActivity;params=<TYPE>,<SUBTYPE>,<internal id>`)
   with "Download other activity" and "Go back to accounts". Click "Download
   other activity" to queue the next window without re-navigating.
6. Repeat per account, per window. **One download per planner window** — never
   merge two windows into one wider download, because the requested window is
   what marks a month covered.

### Where the file lands, and what it is called

Not necessarily `~/Downloads` — this is browser-profile specific and has been
observed pointing at a cloud-synced Documents folder. The path belongs in the
`notes` field of `providers.local.yaml`; read it from there rather than
guessing, and if a download seems to have vanished, check there before retrying.

The filename differs by product: **card exports embed the requested window**
(`ChaseNNNN_Activity20260701_20260822_20260822.CSV`) while **bank exports do
not** (`ChaseNNNN_Activity_20260822.CSV`). Detect a completed download by
watching the folder for a file newer than a marker timestamp taken just before
clicking Download — never by predicting the name, and never by grabbing the
newest file without the timestamp check (a leftover from an earlier session
looks exactly like your download).

### Empty windows produce NO file

A window with no activity does not download an empty CSV — Chase shows
*"We couldn't find any activity that matched the date range you chose"* on the
confirmation page and serves nothing. This is a normal outcome (dormant
savings, pre-opening months), not a failure. Record it immediately so the
planner knows the month was fetched:

```sh
uv run python transaction_downloader/capture.py record-empty \
    --account <last4> --start <YYYY-MM-DD> --end <YYYY-MM-DD>
```

The marker flows through ingest as an `empty_window` document, so the
database — the durable coverage source — carries the evidence. Skipping this
step means the planner re-requests the same empty window on every run forever.

### Which date Chase filters on

Card ranges filter on **posting** date, so a card export's earliest
*transaction* date can precede the requested window — a purchase on the 29th
posting on the 1st appears in the following month's window. This is expected and
is not double-counting: `post_date` is part of the natural key, so the same
transaction lands once.

## 4. File each download verbatim

For every download, immediately:

```sh
uv run python transaction_downloader/capture.py file \
    --account <last4> --start <YYYY-MM-DD> --end <YYYY-MM-DD> <downloaded file>
```

`--start`/`--end` must be the window you actually requested from Chase, not the
dates you see inside the file. That distinction is the whole coverage model: a
month fetched and found empty is covered, a month never fetched is not.

`capture.py` refuses anything that is not a recognizable Chase export, copies
the bytes unchanged into `raw_dir` under a canonical name, and records the
window. Identical bytes filed twice are a no-op. Nothing is ever overwritten.

Check the user's `notes` in `providers.local.yaml` for their browser download
location before hunting for files.

## 4.1 Months Chase will not serve

`plan.py` reports months past the retention floor as `BEYOND CHASE RETENTION`.
Do not try to fetch these — the form rejects them. They can only come from
exports downloaded before the window lapsed:

```sh
uv run python transaction_downloader/capture.py import-legacy <old files>
```

Legacy files carry no record of the window that was requested, so one is
**inferred** from the transaction dates inside and stamped `window_source:
inferred`. `plan.py` reports those months separately — coverage that rests on a
guess should be visible as such. `--exact` is available when you would rather
claim only what the file literally proves.

Legacy captures take their newest transaction date as their capture date, which
is what orders restatement authority between overlapping captures — so an
imported archive can never override rows from a live download that re-fetched
the same window later.

## 5. Land into raw_documents

```sh
uv run python src/ingest_raw.py --provider chase <archive_dir> <raw_dir> <data_dir>
```

Captures classify as `csv_export` with `period_hint` set to the first of the
requested window's month. sha256 dedup makes re-runs free. Report ingested vs
deduped counts.

## 6. Normalize

```sh
uv run python src/parse_raw.py --provider chase
```

`src/providers/chase.py` handles all three layouts and upserts into
`transactions`. Report parsed / errored / no_parser counts and rows upserted.

Facts the parser encodes, repeated here because they are the ones that bite:

- **Chase CSVs have no transaction id**, and genuinely identical same-day rows
  occur — two identical coffees at one shop. Rows therefore carry an
  `occurrence` counter, assigned in export order within each
  (account, post_date, description, amount) group, and it is part of the natural
  key. Without it the second coffee upserts onto the first and disappears.
- **Export order is stable**, which is what makes that counter safe across
  re-downloads and mass reprocessing.
- **A bank export has only a posting date.** `txn_date` falls back to it rather
  than inventing a transaction date.
- **A file at the 1,000-row cap is refused at capture time**, so it never
  reaches the parser in normal use. The parser keeps the same guard as defence
  in depth for anything reaching it another way.
- Amounts are signed as Chase signs them: negative is money out, on both bank
  and card exports.
- **Chase restates posted amounts, and tips are the everyday case** — a
  restaurant charge posts pre-tip, then the amount changes when the tip
  settles. A changed amount is a new natural key, so upsert alone would leave
  the pre-tip row behind as a phantom duplicate. Each capture is therefore
  authoritative for the dates of its window that no newer capture covers: rows
  that vanished from the newest export of a window are pruned, reported as
  `stale transactions removed` in the parse summary. The planner's overlap rule
  re-downloads the restatement-prone tail, so tips correct themselves on
  routine runs. Genuinely identical same-day rows (two memberships on one
  account) are never collapsed by this — both appear in the export with
  distinct `occurrence` values, and both survive.
- **Nothing from the source row is discarded.** Every field is stored verbatim
  in the `transactions.extra` JSON column, keyed by its original header text and
  holding the raw unparsed string — including columns already projected into
  typed columns, values in positions past the last header (bank exports emit 8
  fields against 7 headers), and any column Chase adds in future. The typed
  columns are a convenience projection; `extra` is the record. This happens in
  parser code, deterministically — no run decides what is worth keeping.

The account comes from the capture filename, not from `account_number` —
one Chase login covers several accounts, so `chase` is listed in
`parse_raw.DERIVES_OWN_ACCOUNT_ID`.

On failure: fix parser code, bump `PARSER_VERSION`, reprocess. Never hand-edit
parsed output. Upserts are keyed naturally, so mass-reprocessing is idempotent.

## 7. Report and verify

Report: windows planned vs downloaded, per-account row counts, anything Chase
refused to export, captures filed vs deduped, ingest ingested/deduped counts,
transactions upserted, and any popup or flow change you had to work around.

- [ ] every planned window has a capture, a `record-empty` marker, or an
      explicit reason it has neither
- [ ] `transaction_downloader/plan.py` re-run shows the fetched months covered
      (only `[current month]` lines remaining means fully covered)
- [ ] no capture came back with exactly 1,000 rows (silent truncation)
- [ ] re-run `ingest_raw.py` → 100% dedup, zero new rows
- [ ] `parse_raw.py` reports zero errored and zero no_parser
- [ ] re-run `parse_raw.py --status all` → same row count in `transactions`
      (reprocessing must be idempotent)
- [ ] a spot-checked month's transaction count and net total match what the
      Chase UI shows for that account and period
- [ ] sign spot-check: card payments land POSITIVE in `transactions` and bank
      debits negative — an account with zero rows in one direction means the
      parser's sign projection is wrong (Citi shipped that way once)
- [ ] months reported as inferred are ones you actually imported from an archive

## 8. Keeping this command current

Chase will change this flow. When it does, the fix belongs here, not in a
one-off workaround you forget by next run. Before finishing, if the portal did
not match §3:

1. Update §3 to what you actually saw, and change the "as last observed" date.
2. Add any new popup to §2.
3. Put user-specific quirks (download location, which accounts export how far
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
