---
description: Enphase Enlighten solar production acquisition — capture gross generation intervals verbatim, land into raw_documents
argument-hint: [optional: "full" to re-pull entire lifetime]
---

# Enphase Enlighten Solar Production Acquisition

Acquire Enphase Enlighten **solar production** data and land it in Cash Flow Commander's raw
store. **Raw-first rule:** every artifact gets captured VERBATIM before any parsing — API
response bodies as .json, exports as-is, email bodies as .txt. Raw (`raw_documents`) is the
source of truth; tables and CSVs are disposable projections that can be rebuilt.

## Why this provider exists

This is a **data source, not a biller** — Enphase sends no invoice. It exists to close a blind
spot in the electricity bill: under net metering with no battery, the utility meter records only
the *net* flow at the meter. Solar consumed on site never crosses the meter, so it is invisible
on both sides of the ledger — neither billed as consumption nor credited as export. The bill
therefore understates gross generation and true household consumption by the same amount.

Enlighten measures **gross production at the inverters**, which is the missing term. This system
is **production-monitoring only** (no consumption CTs), so household load is not measured
directly and must be derived from both providers:

```
self_consumption = gross_production − exported_kWh
true_consumption = imported_kWh + self_consumption
```

- `gross_production` — this provider, `production` channel.
- `exported_kWh` — the electricity provider's `generation` metric (solar buyback).
- `imported_kWh` — the electricity provider's `consumption` metric (what you were billed for).

See `/bills-rhythm`; Smart Meter Texas exports cover the same ground when the retailer's window
has lapsed. Both sides must be aggregated in the **same local timezone** before differencing, or
the result drifts by an interval at each day boundary.

## 0. Setup

**This command is deployed globally, so the session may not start in the repo.**
Every path below — `src/...`, `deploy/...`, `docs/...` — is relative to the Cash
Flow Commander clone. Change into it first, conventionally
`~/GitHub/Cash_Flow_Commander`. If it lives elsewhere on this host, find it and
use that; do not run these commands from an unrelated directory, and do not
hardcode a machine-specific path in this file.

- Read `docs/LANDING.md` for the landing architecture and ingest conventions.
- Load the `enphase_enlighten` entry from `providers.local.yaml` in the repo root. You need its
  `external_ids.system_id`, `archive_dir`, `raw_dir`, and `data_dir`.
- If `providers.local.yaml` is missing or has no `enphase_enlighten` entry, **STOP** and direct
  the user to run `/bills-add-company`. The file is gitignored — personal values never go in this
  repo.
- `account_number` holds the **system id** — Enphase has no account number, and `parse_raw.py`
  uses `account_number` as `usage_intervals.account_id`. This series is deliberately kept on its
  own account id rather than folded in with the electricity retailer's: production is a property
  of the roof, not of whoever is currently selling you power, and switching retailers must not
  break it.

This command runs the full pipeline — **coverage → acquire → ingest → parse → dashboards** —
and is safe to re-run at any time.

## 0.1 Coverage — decide what to fetch

```sh
uv run python src/coverage.py --provider enphase_enlighten
```

Use the reported `FETCH:` window for the `start_date`/`end_date` below. Overlap is free
(sha256 dedup at ingest, natural-key upsert at parse), so err wide.

Known **persistent gaps** — days the system reported nothing, confirmed absent at the source,
not capture failures. Expect these to be listed every run and do not chase them:

- 2022-11-11 (no intervals)
- 2025-07-27 (no intervals) and 2025-07-28 (day missing from the payload entirely)

`--full` retries them; there is no reason to expect a different answer.

**The current day is not a gap.** `end_date = today` always returns a final `stats` entry for
today, and its `production` array is empty until the panels start producing (and partial all
day after that). The parser skips it, so a run made before sunrise simply lands nothing for
today — confirmed 2026-09-04, run at ~06:00 local with `production: []` for that date. Do not
add today to the persistent-gap list; the next run picks it up.

Because this provider has no retention limit and serves the whole lifetime in one call, `full`
in `$ARGUMENTS` is always safe — it costs one request.

## 1. Portal session

- Portal: `https://enlighten.enphaseenergy.com`. The system id appears in every post-login URL
  (`/web/{system_id}/...`) and is echoed as `system_id` in every JSON payload.
- Open the portal in the user's Chrome (Claude-in-Chrome). The browser autofills credentials —
  **ask the user before clicking Log In.**
- A privacy/cookie consent modal appears on first load. Choose **Reject All** — never Accept.
  Decline app-install banners and battery/upgrade marketing. Do not change system settings.
- Auth is a browser session cookie, so all calls run from the logged-in page context via
  `javascript_tool` with a plain same-origin `fetch('/pv/systems/...')`.

**Two browser-tool gotchas, both learned the hard way:**

1. **Never read response headers.** Any `r.headers.get(...)` in the injected script trips the
   extension's cookie/query-string guard and the whole call returns `[BLOCKED]` with no data.
   Read `await r.text()` only. `fetch` defaults to same-origin credentials — you do not need,
   and should not pass, an explicit credentials option.
2. **Space out downloads.** Chrome throttles rapid successive programmatic downloads; put a
   ~1.2 s delay between them or later files silently never land.

## 2. API artifact catalog

Parameterized on `system_id` from providers.local.yaml. **Save every response body VERBATIM** as
.json via `Blob` + `a.download` (never return large data inline — JS tool results truncate).

| Purpose | Endpoint | doc_type | Filename pattern |
| --- | --- | --- | --- |
| **Interval production (primary)** | `GET /pv/systems/{system_id}/daily_energy?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD` | `api_usage_json` | `enphase_enlighten_api_usage_daily_energy_{start}_{end}.json` |
| Daily rollups + system start | `GET /pv/systems/{system_id}/lifetime_energy` | `api_usage_json` | `enphase_enlighten_api_usage_lifetime_energy_{YYYY-MM-DD}.json` |
| System / logger metadata | `GET /pv/systems/{system_id}/today` | `other` | `enphase_enlighten_api_system_today_{YYYY-MM-DD}.json` |

Also observed, not worth capturing: `/app-api/{system_id}/get_latest_power` (instantaneous
power), `/systems/{system_id}/weather.json?date=YYYY-MM-DD`, `/app-api/graph_view_preference`
(UI state).

**`daily_energy` is the whole story.** Despite the name it returns *sub-daily* data:

- `end_date` is honored and spans are unbounded in practice — the **entire system lifetime**
  returns in one call (~1.1 MB for ~4.5 years). `start_date` may be any date, not just the 1st.
- `stats[]` — one entry per day. Each carries `start_time` (epoch seconds at **site-local
  midnight**, DST-aware), `interval_length` (900 = 15 minutes), `production` (array of **Wh**
  per interval), and `totals.production` (Wh for the day).
- A day before the system reported has `production: []` — an empty array, not zeros. Skip those
  rather than treating them as a zero-production day.

**Filename discipline matters.** `src/ingest_raw.py::classify()` routes a `.json` whose name
contains `usage` to `api_usage_json`; anything else falls through to `other` (which still
ingests, but warns). Keep the literal substring `usage` in production captures, or pass
`--doc-type`. A dedicated solar doc_type would require extending `VALID_DOC_TYPES` and
`classify()` with tests — out of scope for a capture run.

**Scope.** `full` in `$ARGUMENTS` (or any first run) → pull the entire lifetime in one call:
`start_date` = the `start_date` from `lifetime_energy`, `end_date` = today. Otherwise pull from
the start of the previous month through today — cheap, and it picks up any gateway backfill that
restated recent intervals. Over-capturing is free; the raw store dedups by sha256.

## 3. Channel reality check (do this every run)

`stats[].` carries the full Enphase channel schema — `consumption`, `import`, `export`,
`grid_home`, `solar_home`, `solar_grid`, `battery_*`, `generator_*` — but on a
production-only site **every one of them is an empty array**. Only `production` is populated.

Do not mistake the schema for data. If any of those arrays ever comes back non-empty, consumption
CTs or a battery have been added: say so in the run report, because the derivation in
"Why this provider exists" becomes unnecessary — Enlighten would then measure consumption
directly, and the electricity-provider cross-reference becomes a check rather than an input.

## 4. Filing

- Move all `enphase_enlighten_*` captures into `raw_dir`.
- **Never overwrite an existing file** — put collisions in `archive_dir/_to_delete/` and tell the
  user what's there so they can review and trash it.
- There are no bill PDFs for this provider; `archive_dir` holds only system documents
  (commissioning paperwork, warranty) if the user files any there.
- Check `notes` in providers.local.yaml for the browser download-location quirk before hunting
  for downloaded files.

## 5. Optional Gmail supplements

- Enphase sends periodic system summary / fault-alert mail. Search Gmail for
  `from:enphaseenergy.com`, then narrow by subject.
- Save each matched message's **full body verbatim** into `raw_dir` as
  `enphase_enlighten_email_{YYYY-MM-DD}_{message_id}.txt`.
- Optional by design — this command must succeed with no email access at all.

## 6. Land into raw_documents

```
uv run python src/ingest_raw.py --provider enphase_enlighten <archive_dir> <raw_dir> <data_dir>
```

- Add `--dry-run` first to confirm captures classify as `api_usage_json`, not `other`.
- Note that `raw_dir` sits inside `archive_dir`, so the walk visits each file twice; the second
  visit dedups. Expect ingested and deduped counts to be equal on a first run — that is correct,
  not a bug.
- Re-runs are safe: sha256 dedup makes ingestion idempotent.

## 7. Normalize

```sh
uv run python src/parse_raw.py --provider enphase_enlighten
```

`src/providers/enphase_enlighten.py` handles both captures, routed by filename:
`daily_energy` → 15-minute interval rows, `lifetime_energy` → `day` rollup rows. The
`api_system_today` capture has no parser by design (system metadata, not measurements) and
will report as `no_parser` every run — that is expected, not a failure. The count is
**one per `api_system_today` capture held**, so it grows by one each run (2 as of
2026-09-04). Check that the no_parser documents are all `api_system_today`; any other
filename in that bucket is a real missing parser.

Facts the parser encodes, repeated here because they are the ones that bite:

- **Never assume 96 intervals per day.** Real counts are **92, 96, and 100** — DST transition
  days are 23 and 25 hours. Timestamps derive from `start_time + index × interval_length`.
- **Units:** Enphase reports **Wh**, `usage_intervals` stores **kWh**.
- **Metric is `production`**, deliberately not `generation`. The retailer's `generation` is
  metered *export* — a different physical quantity. Reusing the name would make the
  reconciliation compare a series against itself.
- Empty-array days are **skipped, not zeroed**. Absence of a reading is not a measurement of
  zero, and writing zeros would make an outage look like a day with no sun.
- If a non-production channel ever arrives populated, the parse **raises** — that means
  consumption CTs or a battery were added and the parser must be extended deliberately.

On failure: fix parser code, bump `PARSER_VERSION`, reprocess. Never hand-edit parsed output.
Derived self-consumption is a projection — compute it in the dashboard/report layer, never
write it back into the raw store.

## 7.05 Data-quality check — the silent failures

```sh
uv run python src/checks.py --provider enphase_enlighten
```

`parse_raw` reports what *failed*. This reports what succeeded and is still
unusable — the failures that render as plausible numbers instead of errors.

The one that motivated it: every value panel on `cfc-solar-net-metering` inner-
joins `bills` to `bill_line_items` and then filters `energy_rate_cents IS NOT
NULL`. A bill whose line items did not parse, or parsed without an energy-rate
line, silently drops out of the series. The chart does not gap and does not
error — it draws a shorter line and a smaller total, which reads as "solar was
worth less" rather than "data is missing".

Non-zero exit means at least one billing period will be missing from the value
panels. Fix it by reprocessing, never by editing rows:

```sh
uv run python src/parse_raw.py --provider enphase_enlighten --status all
```

Do not proceed to the dashboard step while this is failing — you would be
verifying a dashboard that renders successfully and understates.

## 7.1 Dashboards

`deploy/grafana/solar_net_metering.json` (uid `cfc-solar-net-metering`) is the committed
dashboard for this data. Its headline — and the reason this provider exists — is **what the
panels were worth per billing period**: avoided cost (self-consumed kWh priced at that bill's
marginal retail rate) plus buyback credit (the actual credit line on the bill). Below that sit
the billed/hidden/exported energy bars, hidden-share stat, daily flows, and a reconciliation
table.

The valuation depends on **bill line items**, not just usage — `bills` and `bill_line_items`
supply the energy rate, per-kWh delivery, taxes, and credit. So a run of `/bills-rhythm` that
fails to parse bills will quietly flatten the value panels even when solar data is perfect.

```sh
uv run python deploy/grafana_sync.py verify cfc-solar-net-metering
```

Run that after every acquisition — it confirms the new data actually reaches the panels. If a
run adds a series the dashboard does not show, add a panel, then:

```sh
uv run python deploy/grafana_sync.py export cfc-solar-net-metering deploy/grafana/solar_net_metering.json
uv run python deploy/grafana_sync.py import deploy/grafana/solar_net_metering.json
uv run python deploy/grafana_sync.py verify cfc-solar-net-metering
```

Never hand-roll the import: a dashboard whose `${DS_CASH_FLOW_COMMANDER}` placeholder goes
unresolved imports "successfully" and then shows **"No data" on every panel**. Never test a
panel by substituting a working datasource uid into the query either — that hides exactly this
failure. `verify` uses each panel's own reference for that reason.

## 8. Report + verification

Report date range covered, days captured, raw docs ingested/deduped, empty-production days, and
the reconciliation against the electricity provider. Verify:

- [ ] every API call this run has a matching verbatim .json in `raw_dir`
- [ ] `raw_documents` grew by exactly the new-artifact count; re-run ingest → 100% dedup (0 new)
- [ ] captures classified as `api_usage_json`, not `other`
- [ ] `parse_raw.py` reports zero errored (`no_parser` == the number of
      `api_system_today` captures held is expected; it grows by one per run)
- [ ] `coverage.py` re-run shows the fetched window now covered, no new thin days, and no new
      persistent gaps beyond the three known ones
- [ ] interval counts per day are only ever 92 / 96 / 100, and the off-96 days land on DST
      transition dates
- [ ] a spot-checked day's `totals.production` matches what Enlighten's own UI shows for that
      day. **The UI has no date-addressable URL** — `/web/{system_id}/{YYYY-MM-DD}/graph/hours`
      silently redirects to `/today` (confirmed 2026-09-04). Use the **Energy** nav item
      (`/web/{system_id}/history/graph/hours`), which lands on the DAY view and prints the
      last two days plus the year-ago day as rounded kWh totals — compare against those.
- [ ] consumption/import/export channels still empty (see section 3)
- [ ] the two captures agree: sum the `15min` rows per local day and compare against the `day`
      rollup rows. They come from independent endpoints, so agreement is a real check — expect
      differences under ~0.01 kWh/day (Enphase rounds each endpoint separately). **Group the
      `day` rows by their UTC date, not a local-time conversion**: they are stamped at UTC
      midnight as a *label* for the local calendar date, so converting them to
      `America/Chicago` shifts every one back a day and fakes a one-day offset against the
      interval series.
- [ ] for any billing period where electricity-provider intervals also exist:
      `gross_production − exported_kWh ≥ 0`, and derived self-consumption is neither negative
      nor larger than gross production
- [ ] the reconciliation table in `deploy/grafana/solar_net_metering.json` shows
      `Billed kWh ≈ Metered import` for closed periods — a divergence means a coverage gap,
      not a billing error

## Retention and cadence

**No retention limit was found.** `daily_energy` serves full 15-minute resolution back to system
start, so unlike the electricity provider — whose hourly interval window is ~12 months and where
a missed month becomes a permanent gap — **this provider is not time-critical**. A missed run is
always recoverable.

Cadence: monthly, after the electricity bill posts, so both sides of the reconciliation cover the
same closed billing period. Run it late rather than not at all.

## Keeping this command current

This provider will change its portal, its payload, or its quirks. When it does,
the fix belongs in this file, not in a one-off workaround you forget by the next
run. Before finishing, if reality did not match what is written above:

1. Update the section that was wrong, and date it.
2. Add any new popup, interstitial, or blocking modal to the portal-session
   section.
3. Add any newly-confirmed permanent gap to the coverage notes, so future runs
   stop chasing it.
4. Put user-specific quirks (download locations, account oddities) in the
   `notes` field of `providers.local.yaml` — never in this file.
5. Tell the user what you changed. **Do not commit** — they review and commit.

---

## Pre-commit hygiene checklist (this repo is PUBLIC)

- [x] No account numbers.
- [x] No system id, premise/meter/ESI identifiers, or other provider-issued IDs.
- [x] No paths containing a username.
- [x] No email addresses.
- [x] No contract history (rates, terms, renewal dates).
- [x] No credentials, tokens, or session cookies.
- [x] Personal values appear only symbolically, referenced from `providers.local.yaml`.
