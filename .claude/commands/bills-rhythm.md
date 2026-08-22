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
  `account_number`, `external_ids.premise_id`, `archive_dir`, `raw_dir`, and `data_dir`.
- If `providers.local.yaml` is missing or has no `rhythm` entry, **STOP** and direct the user
  to run `/bills-add-company` (or copy `template_providers.yaml` to `providers.local.yaml` and
  fill it in). The file is gitignored — personal values never go in this repo. Required keys:
  `service_type`, `account_number`, `external_ids` (`premise_id`, `esi_id`), `archive_dir`,
  `raw_dir`, `data_dir`, and any `notes` (download quirks, TDU, contract status, cadence).

This command runs the full pipeline — **coverage → acquire → ingest → parse → dashboards** —
and is safe to re-run at any time.

## 0.1 Coverage — decide what to fetch

```sh
uv run python src/coverage.py --provider rhythm
```

Reports four series: `consumption` and `generation` at both `15min` (Smart Meter Texas) and
`hour` (Rhythm portal). Use the reported `FETCH:` windows; overlap is free (sha256 dedup at
ingest, natural-key upsert at parse), so err wide.

**Unlike the solar provider, this one is time-critical** — the portal keeps only ~12 months of
hourly interval data and Smart Meter Texas ~24 months of 15-minute data. A gap that ages out of
both windows is permanent. When coverage reports a recent gap, fill it this run.

Read the persistent-gap list carefully here, because for this provider the two kinds are mixed:

- Old `hour` gaps (pre-2025 stretches outside the portal's retention) are **permanently
  unfillable from the portal** — do not burn a run chasing them. Smart Meter Texas can still
  cover the trailing ~24 months at finer granularity; beyond that the data is gone.
- Any gap inside the current retention windows **is** fillable and should be treated as a
  missed run, not a fact of life.

## 1. Portal session

- Portal: `https://app.gotrhythm.com` · API: `https://api.gotrhythm.com`.
- Open the portal in the user's Chrome (Claude-in-Chrome). If it sits on the loading
  animation, go to `/sign-in`. The browser autofills credentials — **ask the user before
  clicking Log In**.
- Decline any "switch to paperless" or marketing modals.
- Auth is httpOnly cookies, so the API cannot be reached from outside the browser: all API
  calls must run via the browser `javascript_tool` using `fetch(..., {credentials: 'include'})`
  from the logged-in page context.

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
- Check `notes` in providers.local.yaml for the user's browser download-location quirks
  before hunting for the files.

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

## 6. Land into raw_documents

```
uv run python src/ingest_raw.py --provider rhythm <archive_dir> <raw_dir> <data_dir>
```

Classification (bill_pdf, api_*_json, *_email, csv_export) is the CLI's job. Report ingested
vs deduped counts per doc_type. Re-runs are safe — sha256 dedup makes ingestion idempotent.

## 7. Normalize

```sh
uv run python src/parse_raw.py --provider rhythm
```

`src/providers/rhythm.py` handles api_usage_json, hourly_usage.csv, api_invoice_json, bill_pdf,
and payments.csv; `src/providers/smt.py` handles smt_export. Report parsed / errored /
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
      service period (the reconciliation table in `solar_net_metering.json` shows this directly)
- [ ] new PDFs named `Rythm YYYY-MM.pdf` and filed in archive_dir

Note: for interval data older than the portal's ~12-month window, Smart Meter Texas
(smartmetertexas.com) has ~24 months of 15-minute meter data for any Texas ESI ID — exports
land as the `smt_export` doc_type via the same ingest CLI.

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
4. Put user-specific quirks (download locations, account oddities) in the
   `notes` field of `providers.local.yaml` — never in this file.
5. Tell the user what you changed. **Do not commit** — they review and commit.
