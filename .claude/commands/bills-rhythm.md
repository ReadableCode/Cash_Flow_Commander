---
description: Rhythm Energy billing acquisition — capture raw artifacts verbatim, land into raw_documents
argument-hint: [optional: "full" to re-pull everything]
---

# Rhythm Energy Billing Acquisition

Acquire Rhythm Energy (electricity) billing artifacts and land them in Cash Flow Commander's
raw store. **Raw-first rule:** every artifact gets captured VERBATIM before any parsing — API
response bodies as .json, bill PDFs as-is, email bodies as .txt. Raw (`raw_documents`) is the
source of truth; tables and CSVs are disposable projections that can be rebuilt.

## 0. Setup

**This command is deployed globally, so the session may not start in the repo.**
Every path below — `src/...`, `deploy/...`, `docs/...` — is relative to the Cash
Flow Commander clone. Change into it first, conventionally
`~/GitHub/Cash_Flow_Commander`. If it lives elsewhere on this host, find it and
use that; do not run these commands from an unrelated directory, and do not
hardcode a machine-specific path in this file.

- Read `docs/LANDING.md` for the landing architecture and ingest conventions.
- Load the `rhythm` entry from `providers.local.yaml` in the repo root. You need its
  `account_number`, `external_ids.premise_id`, `archive_dir`, `raw_dir`, `data_dir`, and
  `download_dir`.
- If `providers.local.yaml` is missing or has no `rhythm` entry, **STOP** and direct the user
  to run `/bills-add-company` (or copy `template_providers.yaml` to `providers.local.yaml` and
  fill it in). The file is gitignored — personal values never go in this repo. Required keys:
  `service_type`, `account_number`, `external_ids` (`premise_id`, `esi_id`), `archive_dir`,
  `raw_dir`, `data_dir`, `download_dir`, and any `notes` (download quirks, TDU, contract
  status, cadence).

This command runs the full pipeline — **coverage → acquire → ingest → parse → dashboards** —
and is safe to re-run at any time.

## 0.1 Coverage — decide what to fetch

```sh
uv run python src/coverage.py --provider rhythm
```

Reports two series: `consumption` and `generation` at `hour`, from the Rhythm portal. The
meter's `15min` series belongs to Smart Meter Texas and has its own command, `/bills-smt`
(`coverage.py --provider smt`). Use the reported `FETCH:` windows; overlap is free (sha256
dedup at ingest, natural-key upsert at parse), so err wide.

**Unlike the solar provider, this one is time-critical** — the portal keeps only ~12 months of
hourly interval data and Smart Meter Texas ~24 months of 15-minute data. A gap that ages out of
both windows is permanent. When coverage reports a recent gap, fill it this run.

Read the persistent-gap list carefully here, because for this provider the two kinds are mixed:

- Old `hour` gaps (pre-2025 stretches outside the portal's retention) are **permanently
  unfillable from the portal** — do not burn a run chasing them. Smart Meter Texas can still
  cover the trailing ~24 months at finer granularity; beyond that the data is gone.
- Any gap inside the current retention windows **is** fillable and should be treated as a
  missed run, not a fact of life.

Confirmed-permanent `hour` gaps (verified 2026-09-04 — outside both the portal's ~12-month
and Smart Meter Texas's ~24-month windows; do not chase them, and do not run `--full` hoping
to fill them):

    2022-03-14..2023-02-25   2023-03-13..2024-02-26
    2024-03-11..2025-02-25   2025-03-10..2025-07-28

One residual `FETCH:` window is also normal at the end of a healthy run, not a gap: the `hour`
series only extends to the last *billed* day, because it comes from the per-invoice usage
endpoint. It closes on the next run.

The `hour` window only closes while Rhythm keeps billing (added 2026-09-11). After a switch to
another retail provider the last invoice is a short-cycle closing bill: on 2026-09-11 it was a
7-day service period, and the portal home showed a "Re-enroll with Rhythm" banner. Once that
invoice's usage is captured, the `hour` `FETCH:` window stays open for good; stop chasing it.
The meter's `15min` series does not end, because Smart Meter Texas keeps the meter's data
whoever the retail provider is; `/bills-smt` lands it.

## 1. Portal session

- Portal: `https://app.gotrhythm.com` · API: `https://api.gotrhythm.com`.
- Open the portal in the user's Chrome (Claude-in-Chrome). If it sits on the loading
  animation, go to `/sign-in`. The browser autofills credentials — **ask the user before
  clicking Log In**. On 2026-09-16 the form came up with the email filled and the password
  empty (Chrome fills the password only on a user gesture), so a scripted click on Log In
  cannot sign in anyway: leave the tab on the form and ask the user to log in. The session
  may already be live: on 2026-09-11 `/sign-in` redirected
  through `api.gotrhythm.com/api/portal/authn/login-via-token` straight to `/home` with no
  click. That redirect URL carries a one-time token; never echo it.
- **Without Claude in Chrome, drive the real Chrome over AppleScript** as described in
  `transactions-chase.md` section 1 (proven here 2026-09-11). Quirks met on this portal and SMT:
  - **Never `open location`, and never a work profile.** `open location` opens in the
    frontmost window, which can belong to a different Chrome profile with no saved logins, no
    download allowances and Apple Events JavaScript off. The tell is `Executing JavaScript
    through AppleScript is turned off` although the setting is on in the right profile. Open
    the portal in the user's PERSONAL profile explicitly —
    `open -na "Google Chrome" --args --profile-directory=<personal dir> <url>` — with the
    directory name taken from the user's local notes, never written here. If a tab has
    landed in the wrong profile, close it and reopen this way.
  - `execute javascript` does not await promises. Start the async work, record progress on a
    `window.` property, and poll it with `delay`. Pass the script as an argument
    (`osascript - "$JS"` with `on run argv`) instead of escaping quotes, and avoid `st` as an
    AppleScript variable name (reserved).
  - The script runs in an isolated world, so a native setter borrowed from
    `HTMLInputElement.prototype` throws `Illegal invocation`. Assign `el.value` and dispatch
    `input`/`change` events instead.
  - Auto mode's classifier may refuse the step that points the logged-in tab at the API origin.
    Stop and ask the user to allow `osascript`; do not route around it.
- Decline any "switch to paperless" or marketing modals. As of 2026-09-04 the one that
  fires on every login is a dialog headed **ACTION REQUIRED — "We are moving from eBill to
  fully Paperless"**; dismiss it with the **"No, I prefer to stay on eBill."** button below
  the purple "Switch to fully Paperless" call-to-action, not by closing the dialog.
- Auth is httpOnly cookies, so the API cannot be reached from outside the browser: all API
  calls must run via the browser `javascript_tool` from a logged-in page context.
- **Fetch from the `api.gotrhythm.com` origin, not `app.gotrhythm.com`** (updated 2026-09-04).
  Once logged in, navigate the tab to any `api.gotrhythm.com` URL — e.g. the invoice-history
  endpoint — and issue every `fetch` from there. Requests are then same-origin, so cookies
  flow with no `credentials` option at all.
  The old approach (`fetch(..., {credentials: 'include'})` from the `app.` page) is
  cross-subdomain, and `javascript_tool` now refuses it outright as a cookie-exfiltration
  pattern, returning `[BLOCKED: Cookie/query string data]`. Do not try to phrase around that
  block — switch origin instead.
- `javascript_tool` also returns `[BLOCKED: ...]` when the *result* of your JS is a blob of
  API JSON (long hex ids read as tokens). Return only a small summary — status, byte count,
  filename — and let the download carry the payload.
- **Chrome's automatic-downloads permission is per-origin, and the origin that matters is
  whichever one you download from.** Because captures now run from `api.gotrhythm.com`, that
  origin needs the allowance; an entry for `app.gotrhythm.com` does not cover it. Symptom: the
  first download of the run lands and every later one vanishes with **no prompt and no error** —
  `fetch` still reports 200, the file simply never appears. Chrome allows one automatic
  download per origin, then silently blocks the rest.
  Confirm the setting without touching the browser by reading, in
  `~/Library/Application Support/Google/Chrome/<Profile>/Preferences`,
  `profile.content_settings.exceptions.automatic_downloads` (setting `1` = allow). If
  `https://api.gotrhythm.com` is absent, ask the user to add it at
  `chrome://settings/content/automaticDownloads` → "Allowed to automatically download
  multiple files". Never click Chrome's own settings or dialogs yourself.

## 2. API artifact catalog

Endpoints are parameterized on your `premise_id` from providers.local.yaml. **Save every
response body VERBATIM** as a .json file via `Blob` + `a.download` (never return large data
inline — JS tool results truncate):

- Invoice list: `GET /api/portal/premises/{premise_id}/invoice-history` (paginated — follow
  `.next`) → `rhythm_api_invoice-history_p{N}_{YYYYMMDD}.json` per page.
- Per invoice id:
  - Plan snapshot: `GET /api/portal/invoices/{id}/orders` → `rhythm_api_orders_{invoice_number}.json`
  - Hourly interval data: `GET /api/portal/invoices/{id}/bill-explanation/usage` → `rhythm_api_usage_{invoice_number}.json`
- Scope: whatever step 0.1 reported, plus any new invoices. Default to the trailing ~13 months
  to catch restatements. If "$ARGUMENTS" says `full`, re-pull everything. Over-capturing is
  free — the raw store dedups by sha256, and restatements are preserved as distinct documents.
- ⚠️ The portal retains only ~12 months of hourly interval data. Run this monthly after the
  bill posts (~29th–31st) or gaps in the hourly series become permanent.

## 3. Bill PDFs

- `GET https://api.gotrhythm.com/api/premises/{premise_id}/invoice/{id}/` returns the PDF.
- Trigger as downloads named `rhythm_bill_{invoice_number}_{invoice_date}.pdf`.
- The files land in `download_dir` from providers.local.yaml. Look there before hunting for
  them; `notes` holds any other download quirks.

## 3.4 Smart Meter Texas — the 15-minute series

The portal only ever gives `hour` data, and only for *billed* periods. The meter's `15min`
consumption/generation series comes from Smart Meter Texas, which has its own provider (`smt`)
and its own command: run `/bills-smt`. Run it for the billed-kWh reconciliation in step 8,
which needs 15-minute data covering the new bill's whole service period.

## 3.5 Verify attribution after ingest, not just the counts

Downloads land in `download_dir`. Every provider command shares that folder,
including the `/transactions-*` ones. When this was observed it was the **root** of
the synced documents tree, and that shared root is also on `CFC_RAW_INGEST_DIRS`.

Observed 2026-09-04: a concurrent `/transactions-elan` run called
`ingest_raw.py --provider elan` with **no explicit path**, so it fell back to the
shared `CFC_RAW_INGEST_DIRS` and stamped `provider='elan'` onto every file sitting
there — including five of this command's captures. `src/ingest_raw.py` now refuses
`--provider` without an explicit `DIR_OR_FILE`, so that particular path is closed.

The recovery was silent too. `raw_store.ingest_bytes` dedups on `content_sha256`
alone, so re-running this command's own correct `ingest_raw.py --provider rhythm <dirs>`
over those files inserted nothing and reported `ingested 0, deduped 11`: a success
line that hid five documents filed under the wrong provider and invisible to every
rhythm query and to `coverage.py`. Ingest now reports bytes held under another
provider as `provider_conflict`, names the holder for each file, and exits 1.

Therefore:

- Always pass explicit directories to `ingest_raw.py` (step 6 already does); never
  rely on the env fallback.
- Move captures out of `download_dir` into `raw_dir`/`archive_dir` as soon as
  they land, rather than batching a whole run's downloads there.
- **A non-zero `provider_conflict` count is a failed landing.** Each listed document
  is held under the provider named beside it and is invisible to every rhythm query.
- Repair is `src/relabel_raw.py --from <wrong> --to rhythm --doc-type <type>` (a dry
  run; add `--yes` to write). It moves the held row and resets it to pending, so
  parse afterwards. The same bytes never belong to two providers, so there is
  nothing to re-ingest.

## 4. Filing

- Move new bill PDFs into your `archive_dir` as `Rythm YYYY-MM.pdf` (the provider's own
  filename spelling; YYYY-MM = invoice date month).
- Move all `rhythm_api_*.json` captures into your `raw_dir`.
- **Never overwrite an existing month's file** — put collisions in `_to_delete/` and tell the
  user what's there so they can review and trash it.

## 5. Optional Gmail supplements

Search Gmail for:
- `from:gotrhythm.com subject:"Weekly Usage Report"`
- `from:gotrhythm.com "received your payment"`

Save each matched message's **full body verbatim** into `raw_dir` as
`rhythm_email_weekly_{YYYY-MM-DD}_{message_id}.txt` /
`rhythm_email_payment_{YYYY-MM-DD}_{message_id}.txt`.

⚠️ **No parser is registered for the email doc types** (`weekly_email`, `payment_email` are in
the ingest vocabulary but absent from `_REGISTRY`). Anything captured here therefore lands as
`no_parser` and stays there. Nothing downstream reads it today — the bills and interval series
are fully covered by the API, PDF and SMT artifacts — so treat this step as genuinely optional
and skip it unless the user asks, rather than adding permanently unparseable documents.

## 6. Land into raw_documents

```
uv run python src/ingest_raw.py --provider rhythm <archive_dir> <raw_dir> <data_dir>
```

**Never omit the directory arguments.** With `--provider` set and no paths,
`ingest_raw.py` falls back to `$CFC_RAW_INGEST_DIRS` and stamps that provider
onto every file it finds there — including other providers' documents, whose
`provider` column is then simply wrong. It happened on 2026-09-04: an
argument-less `--provider elan` filed five Rhythm documents under `elan`, and
they had to be reassigned by hand afterwards. Always pass the directory
explicitly, even when you think the default is set to something harmless.

Classification (bill_pdf, api_*_json, *_email, csv_export) is the CLI's job. Report ingested
vs deduped counts per doc_type. Re-runs are safe — sha256 dedup makes ingestion idempotent.

**Expect a large `skipped` count, and check what is in it.** `archive_dir` and `raw_dir` live
in OneDrive with Files On-Demand, so most older files are dehydrated and ingest reports each as
`cloud placeholder (no local blocks)` — 225 of them on 2026-09-04. That is harmless for
documents already in `raw_documents`, but a **freshly captured file that OneDrive dehydrates
before ingest runs would be skipped just as quietly**. The skip list is long enough to hide
one, so grep it for this run's filenames rather than reading it:

```sh
uv run python src/ingest_raw.py --provider rhythm <archive_dir> <raw_dir> <data_dir> \
  2>&1 | grep -E "INV<new>|<today>|Totals"
```

`raw_dir` is nested inside `archive_dir`, so passing both walks every file twice — one visit
ingests and the second reports `deduped`. Per-doc_type counts are therefore roughly doubled;
judge success by `provider_conflict 0` and the checks in section 8, not by these numbers.

## 7. Normalize

```sh
uv run python src/parse_raw.py --provider rhythm
```

`src/providers/rhythm.py` handles api_usage_json, api_orders_json (plan terms, into `plans`),
hourly_usage.csv, api_invoice_json, bill_pdf, and payments.csv. Report parsed / errored /
no_parser counts and rows upserted per sink.

On failure: **fix parser code, bump `PARSER_VERSION`, and reprocess** — never hand-edit parsed
output. Upserts are keyed naturally, so mass-reprocessing is idempotent.

`no_parser > 0` means a document type has no parser registered in
`src/providers/__init__.py::_REGISTRY`. Write one with tests; do not hand-load.

## 7.05 Data-quality check — the silent failures

```sh
uv run python src/checks.py --provider rhythm
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
uv run python src/parse_raw.py --provider rhythm --status all
```

Do not proceed to the dashboard step while this is failing — you would be
verifying a dashboard that renders successfully and understates.

## 7.1 Dashboards

This provider feeds two committed dashboards: `deploy/grafana/energy.json` (uid `cfc-energy`)
for usage/cost/rate, and `deploy/grafana/solar_net_metering.json` (uid `cfc-solar-net-metering`),
where this provider's `consumption`/`generation` series pair with Enphase `production`. A change
to either side moves that reconciliation, so verify both after a run:

```sh
uv run python deploy/grafana_sync.py verify cfc-energy
uv run python deploy/grafana_sync.py verify cfc-solar-net-metering
```

To change a dashboard, edit it in the UI then round-trip it through the script — never copy JSON
out of the panel editor, and never hand-roll the import:

```sh
uv run python deploy/grafana_sync.py export cfc-energy deploy/grafana/energy.json
uv run python deploy/grafana_sync.py import deploy/grafana/energy.json
uv run python deploy/grafana_sync.py verify cfc-energy
```

Dashboards are committed repo artifacts, never Grafana-only state — Grafana's database is not
backed up here and a hand-built panel is unreviewable. An import whose
`${DS_CASH_FLOW_COMMANDER}` placeholder goes unresolved reports success and then renders
**"No data" on every panel**; `verify` is what catches it.

## 7.2 PDF layout reference

Extraction notes for new PDF layouts, kept for when the parser needs extending. There are
**two PDF layout generations**: labels-above (pre-2025-ish) and labels-below (after). Key line
patterns:

- Agreement: `Contract Term/Contract Valid`, plan name, contract rate ¢/kWh
- Energy: `Rhythm Energy Charge  N kWh x R ¢/kWh  $C` (sum multiple lines),
  `Solar Buyback Credit - Applied Towards Energy`
- Non-energy: `Rhythm Base Charge`, `Oncor - Delivery charge per kWh`, `... per month`,
  `City Sales Tax`, `PUC Assessment`, `Misc Gross Receipts Tax Reimbursement`
- Totals: `Total Energy Charges`, `Total Non-Energy Charges`, `Total Current Charges`,
  `Total Amount Due`, `Forward Balance`
- Solar summary: `Solar Buyback Credit  N kWh x R ¢/kWh`, `Credits Earned/Applied/Balance`
- Meter: `CURRENT/PREVIOUS METER READ`

If a parse fails on a new layout, do NOT hand-massage the data — fix the parser code, bump
`PARSER_VERSION`, and reprocess. That is the whole point of raw-first.

## 8. Report + verification

Report the coverage window requested vs actually filled, new months added, raw docs
ingested/deduped, rows upserted, parse failures, reconciliation mismatches, dashboards touched,
and unpaid bills. Verify:

- [ ] every API call made this run has a matching verbatim .json in raw_dir
- [ ] `raw_documents` grew by exactly the new-artifact count; re-run ingest → 100% dedup (0 new)
- [ ] `parse_raw.py` reports zero errored and zero no_parser
- [ ] `coverage.py` re-run shows the fetched window now covered, and no new thin days
- [ ] PDF totals reconcile with API `amount` to the cent
- [ ] billed kWh on the latest bill matches the summed 15-minute `consumption` over the same
      service period (the reconciliation table in `solar_net_metering.json` shows this directly).
      Expect agreement within a fraction of a kWh, not to the unit: 2026-09-11 summed 443.646
      kWh of SMT consumption against 443.313 billed over 2026-08-28..2026-09-03.
- [ ] new PDFs named `Rythm YYYY-MM.pdf` and filed in archive_dir
- [ ] `download_dir` holds no file whose name starts with `rhythm`

Note: for interval data older than the portal's ~12-month window, Smart Meter Texas has ~24
months of 15-minute meter data for any Texas ESI ID. See `/bills-smt`.

One `FETCH:` window normally remains open at the end of a healthy run and is **not** a failure:
the `hour` series stops at the last *billed* day because it comes from the per-invoice usage
endpoint. It closes on the next run. Say so explicitly in the report, or the next run
re-chases it.

---

## Keeping this command current

This provider will change its portal, its payload, or its quirks. When it does,
the fix belongs in this file, not in a one-off workaround you forget by the next
run. Before finishing, if reality did not match what is written above:

1. Update the section that was wrong, and date it.
2. Add any new popup, interstitial, or blocking modal to the portal-session
   section.
3. Add any newly-confirmed permanent gap to the coverage notes, so future runs
   stop chasing it.
4. Put the download location in `download_dir` and other user-specific quirks
   (account oddities) in the `notes` field of `providers.local.yaml`, never in
   this file.
5. Tell the user what you changed. **Do not commit** — they review and commit.
