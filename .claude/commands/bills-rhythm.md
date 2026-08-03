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

- Read `docs/LANDING.md` for the landing architecture and ingest conventions.
- Load the `rhythm` entry from `providers.local.yaml` in the repo root. You need its
  `account_number`, `external_ids.premise_id`, `archive_dir`, `raw_dir`, and `data_dir`.
- If `providers.local.yaml` is missing or has no `rhythm` entry, **STOP** and direct the user
  to run `/bills-add-company` (or copy `template_providers.yaml` to `providers.local.yaml` and
  fill it in). The file is gitignored — personal values never go in this repo. Required keys:
  `service_type`, `account_number`, `external_ids` (`premise_id`, `esi_id`), `archive_dir`,
  `raw_dir`, `data_dir`, and any `notes` (download quirks, TDU, contract status, cadence).

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
- Scope: new invoices, plus re-pull the trailing ~13 months to catch restatements. If
  "$ARGUMENTS" says `full`, re-pull everything. Over-capturing is free — the raw store dedups
  by sha256, and restatements are preserved as distinct documents.
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

## 7. Normalize (transitional until repo parsers land)

Until this repo's parsers exist, extract from new PDFs with `pdftotext -layout` + regex.
There are **two PDF layout generations**: labels-above (pre-2025-ish) and labels-below
(after). Key line patterns:

- Agreement: `Contract Term/Contract Valid`, plan name, contract rate ¢/kWh
- Energy: `Rhythm Energy Charge  N kWh x R ¢/kWh  $C` (sum multiple lines),
  `Solar Buyback Credit - Applied Towards Energy`
- Non-energy: `Rhythm Base Charge`, `Oncor - Delivery charge per kWh`, `... per month`,
  `City Sales Tax`, `PUC Assessment`, `Misc Gross Receipts Tax Reimbursement`
- Totals: `Total Energy Charges`, `Total Non-Energy Charges`, `Total Current Charges`,
  `Total Amount Due`, `Forward Balance`
- Solar summary: `Solar Buyback Credit  N kWh x R ¢/kWh`, `Credits Earned/Applied/Balance`
- Meter: `CURRENT/PREVIOUS METER READ`

**Once repo parsers exist:** run them instead. If a parse fails on a new layout, do NOT
hand-massage the data — fix the parser code, bump `parser_version`, and reprocess. That is
the whole point of raw-first.

## 8. Report + verification

Report new months added, hourly coverage range, raw docs ingested/deduped, parse failures,
reconciliation mismatches, and unpaid bills. Verify:

- [ ] every API call made this run has a matching verbatim .json in raw_dir
- [ ] `raw_documents` grew by exactly the new-artifact count; re-run ingest → 100% dedup (0 new)
- [ ] PDF totals reconcile with API `amount` to the cent
- [ ] hourly series has no gap against last run's max datetime
- [ ] new PDFs named `Rythm YYYY-MM.pdf` and filed in archive_dir

Note: for interval data older than the portal's ~12-month window, Smart Meter Texas
(smartmetertexas.com) has ~24 months of 15-minute meter data for any Texas ESI ID — exports
land as the `smt_export` doc_type via the same ingest CLI.
