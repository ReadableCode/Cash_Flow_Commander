---
description: Smart Meter Texas interval acquisition. Capture the meter's 15-minute consumption and generation verbatim, land into raw_documents
argument-hint: [optional: "full" to re-pull everything the site still holds]
---

# /bills-smt: Smart Meter Texas interval run

Acquire the meter's 15-minute `consumption` and `generation` series from Smart
Meter Texas and land it in Cash Flow Commander's raw store. **Raw-first rule:**
every export is captured VERBATIM before any parsing. Raw (`raw_documents`) is
the source of truth; tables are disposable projections that can be rebuilt.

## Why this provider exists

This is a **data source, not a biller**: Smart Meter Texas sends no invoice. It
holds the meter's interval data whoever sells the power, so the series has its
own provider and its own account id (the meter identifier). It is a property of
the meter, not of the retail provider, and a change of retail provider must not
move it. The electricity commands (`/bills-rhythm`, `/bills-gexa`) land bills
only and send you here for interval data.

## 0. Orient

**This command is deployed globally, so the session may not start in the repo.**
Every path below is relative to the Cash Flow Commander clone, conventionally
`~/GitHub/Cash_Flow_Commander`. Change into it first; if it lives elsewhere on
this host, find it and use that.

- Read `docs/LANDING.md` for the landing architecture and ingest conventions.
- Load the `smt` entry from `providers.local.yaml` (repo root, gitignored):
  `service_type`, `account_number`, `external_ids`, `raw_dir`, `download_dir`,
  `notes`.
- **STOP if the entry is absent.** Do not guess paths or IDs. Tell the user to
  run `/bills-add-company` first, then re-run this command.
- `account_number` holds the **ESI ID**. `parse_raw.py` uses it as
  `usage_intervals.account_id`, and the parser never reads the identifier
  printed in the export.

This command runs the full pipeline (**coverage → acquire → ingest → parse →
dashboards**) and is safe to re-run at any time.

## 0.1 Coverage: decide what to fetch

```sh
uv run python src/coverage.py --provider smt
```

Reports two series, `consumption` and `generation`, both at `15min`. Use the
reported `FETCH:` windows. Overlap is free (sha256 dedup at ingest, natural-key
upsert at parse), so err wide.

**This source is time-critical.** Smart Meter Texas keeps about 24 months of
15-minute data. A gap that ages out of that window is permanent. When coverage
reports a recent gap, fill it this run.

One residual `FETCH:` window is normal at the end of a healthy run and is not a
gap: the site publishes one to two days in arrears. It closes on the next run.

Fetch whenever coverage reports a gap, and always when an electricity command
needs its billed-kWh reconciliation, which needs 15-minute data covering the
new bill's whole service period.

## 1. Portal and auth

As last observed 2026-09-16.

- Login URL: `https://www.smartmetertexas.com/`
- Auth type: user id and password.
- Open the portal in the user's PERSONAL Chrome profile, never with
  `open location`: `open -na "Google Chrome" --args --profile-directory=<personal dir> <url>`
  (directory name from the `notes` of providers.local.yaml). Drive it over
  AppleScript as described in `transactions-chase.md` section 1.
- **First enumerate the tabs on this portal's domain and keep exactly one**,
  closing extras by URL match in reverse index order and verifying by count.
- Chrome autofills the credentials. **Ask the user before clicking Login.**
  Never type, store, or echo credentials.
- The dashboard shows `ESIID` and `Meter Number`. Confirm the ESIID matches
  `account_number` in providers.local.yaml before exporting.
- `www.smartmetertexas.com` needs its own Chrome automatic-downloads allowance
  if a run exports more than once. Without it Chrome drops every scripted
  download after the first, silently.

## 2. The export

| Report | doc_type | Filename pattern |
| --- | --- | --- |
| Energy Data 15 Min Interval | `smt_export` | `smt_IntervalData_{YYYY-MM-DD}_{YYYY-MM-DD}.csv` |

- Set **Report Type** = `Energy Data 15 Min Interval`, then **Start date** and
  **End date** (`MM/DD/YYYY`). Both date fields default to the latest day
  published, so only the start date needs setting when the end date is the
  latest published. The site runs **one to two days in arrears** (two on
  2026-09-11, one on 2026-09-16: the dashboard's "Latest End of Day Read" was
  09/15 and the export carried 09/15 in full; two again on 2026-09-30), so do
  not ask for today.
- Clicking or setting either date field opens a calendar overlay that **covers
  the "Export My Report" button**. Press Escape or click neutral page space
  first, or the click lands on the calendar and silently does nothing.
- Driven by script (2026-09-11), the controls are `select#reporttype_input`
  (value `INTERVAL`), `input#startdatefield` and `input#enddatefield`. They have
  ids but no `name` attribute, so a `[name=...]` selector finds nothing.
  Assigning `.value` and dispatching `input`/`change`, then Escape, was enough
  for the export to honour the dates; the file arithmetic below confirmed it.
  Confirmed again 2026-09-16: dispatching `keydown`/`keyup` `Escape` on
  `document` plus a `document.body.click()` clears the overlay. Before
  clicking, `document.elementFromPoint` at the button's centre tells you
  whether something still covers it. The button sits below the fold in a
  small window (2026-09-30, 960 x 929 viewport), where `elementFromPoint`
  returns `null` and says nothing either way: `scrollIntoView({block:'center'})`
  on the button first, then test. It has no id; find it by its text among
  `button` elements (class `btn meter-search-button`), and confirm exactly one
  matches before clicking.
- **Export My Report** downloads immediately. There is no queue, no email, and
  no "Report Request Status" round trip. The file is always named
  `IntervalData.csv`; a second click yields `IntervalData (1).csv`, so it is
  easy to fire two identical exports without noticing.
- `download_dir` is shared with every other provider command and runs may
  overlap, so claim the export with `src/downloads.py`, never by listing the
  folder:

  ```sh
  uv run python src/downloads.py mark      # just before clicking Export My Report; prints a marker
  uv run python src/downloads.py claim --provider smt --since <marker>
  ```

  `claim` waits (30 s by default, `--timeout SECONDS` to change it) for an
  `IntervalData.csv` or `IntervalData (N).csv` that is newer than the marker,
  then prints its path. That path is `<file>` below. An `IntervalData.csv`
  left by an earlier session is older than the marker and is not claimed.
  Exit 1 means nothing landed: the calendar overlay swallowed the click, or
  Chrome blocked the download (section 1). Exit 2 means two landed, the double
  export above; both paths are printed.

Verify the export before filing. A silent clamp is the failure to watch for:

    head -2 <file>; tail -1 <file>; wc -l < <file>

Columns are `ESIID,USAGE_DATE,REVISION_DATE,USAGE_START_TIME,USAGE_END_TIME,USAGE_KWH,
ESTIMATED_ACTUAL,CONSUMPTION_SURPLUSGENERATION`. Both series are interleaved in
one file, so a complete export has **days x 96 x 2 + 1** lines. The
2026-07-01..2026-09-02 pull was 12,289 lines = 64 x 96 x 2 + 1, with no clamp;
2026-08-26..2026-09-09 was 2,881 = 15 x 96 x 2 + 1. Check the arithmetic rather
than trusting the range you typed.

## 3. Filing

- The browser drops the export in `download_dir`. Move it out as soon as
  `claim` (section 2) prints its path.
- Rename to `smt_IntervalData_{YYYY-MM-DD}_{YYYY-MM-DD}.csv`, using the actual
  first and last usage date in the file, and move it into `raw_dir`. Do both
  in one `mv`: `src/downloads.py` knows the export by the portal's name, so a
  renamed copy left in `download_dir` is not listed by `leftovers`.
- **Never overwrite** an existing file. Put a duplicate export in
  `<raw_dir>/_to_delete/` and tell the user what is there.
- `raw_dir` is this provider's own folder. Do not file the export in an
  electricity provider's folder: that provider's ingest would claim it, and
  ingest reports bytes held under another provider as a `provider_conflict`.

## 4. Land it (ingest CLI)

```sh
uv run python src/ingest_raw.py --provider smt <raw_dir>
```

Pass the directory explicitly. Report the ingested and deduped counts. A
non-zero `provider_conflict` count means the export is already held under
another provider; the command exits 1 and names the holder. Correct the held
row with `src/relabel_raw.py` (dry run first), then re-run.

## 5. Normalize

```sh
uv run python src/parse_raw.py --provider smt
```

Report parsed / errored / no_parser counts and usage rows upserted. The parser
is `src/providers/smt.py`. On a parse failure, fix the parser, bump
`PARSER_VERSION`, and reprocess. Never hand-edit parsed output.

## 6. Data-quality check and dashboards

```sh
uv run python src/checks.py
```

Run it without `--provider`: this source has no bills of its own, and the
checks that read interval data (`metered_days_without_bill`,
`recent_bills_partly_metered`, `bills_buyback_mismatch`) compare it with every
provider's bills.

The series feeds `deploy/grafana/energy.json` (uid `cfc-energy`) and
`deploy/grafana/solar_net_metering.json` (uid `cfc-solar-net-metering`), where
it pairs with Enphase `production`. Verify both after a run:

```sh
uv run python deploy/grafana_sync.py verify cfc-energy
uv run python deploy/grafana_sync.py verify cfc-solar-net-metering
```

## 7. Report and verify

Summarize the run: coverage window requested vs actually filled, line count
against the arithmetic in section 2, ingested/deduped counts, usage rows
upserted, anything filed to `_to_delete/`, anomalies.

- [ ] the export's line count is days x 96 x 2 + 1 for the range in its name
- [ ] `coverage.py --provider smt` re-run shows the fetched window covered and
      no new thin days
- [ ] re-run ingest → 100% dedup, zero new rows, zero `provider_conflict`
- [ ] `parse_raw.py` reports zero errored and zero no_parser
- [ ] `uv run python src/downloads.py leftovers --provider smt` exits 0
      (prints nothing)

## Keeping this command current

This site will change its pages or its quirks. When it does, the fix belongs in
this file, not in a one-off workaround you forget by the next run. Before
finishing, if reality did not match what is written above:

1. Update the section that was wrong, and date it.
2. Add any new popup, interstitial, or blocking modal to section 1.
3. Add any newly-confirmed permanent gap to the coverage notes, so future runs
   stop chasing it.
4. Put the download location in `download_dir` and other user-specific quirks
   in the `notes` field of `providers.local.yaml`, never in this file.
5. Tell the user what you changed. **Do not commit**: they review and commit.

---

## Pre-commit hygiene checklist (this repo is PUBLIC)

- [ ] No account numbers, ESI IDs or meter numbers.
- [ ] No paths containing a username.
- [ ] No email addresses, credentials, tokens, or session cookies.
- [ ] Personal values appear only symbolically, referenced from
      `providers.local.yaml`.
