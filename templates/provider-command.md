---
description: "{{One-line summary: pull, file, and ingest {{Provider Name}} bills and usage data}}"
---

# /bills-{{slug}} — {{Provider Name}} billing run

Template for a new provider command. Copy to `.claude/commands/bills-{{slug}}.md`,
replace every `{{placeholder}}`, delete guidance that does not apply, and run the
pre-commit hygiene checklist at the bottom before committing. Personal values
(account numbers, IDs, local paths) never go in this file — they live in
`providers.local.yaml` and are referenced symbolically.

## 0. Orient

- Read `docs/LANDING.md` for repo conventions and the current state of the pipeline.
- Load the `{{slug}}` entry from `providers.local.yaml` (repo root, gitignored):
  `service_type`, `account_number`, `external_ids`, `archive_dir`, `raw_dir`,
  `data_dir`, `notes`.
- **STOP if the entry is absent.** Do not guess paths or IDs — tell the user to
  run `/bills-add-company` first, then re-run this command.

## 1. Portal and auth

- Login URL: `{{https://portal.example.com/login}}`
- Auth type: `{{password | password+MFA | magic link | SSO}}`
- Autofill etiquette: let the browser/password manager fill credentials, but
  **ask the user before clicking Log In** (MFA prompts, lockout risk).
- Decline marketing modals, cookie upsells, "go paperless" nags, and app
  install banners — dismiss, never accept, and do not change account settings.
- Quirks: `{{session timeout, popup blockers, per-provider oddities — also see
  the notes field in providers.local.yaml}}`

## 2. API artifact catalog

Capture responses verbatim into `raw_dir` (no reformatting, no pretty-printing).
One row per endpoint you pull:

| Endpoint | doc_type | Filename pattern |
| --- | --- | --- |
| `{{/api/v1/invoices}}` | `{{api_invoice_json}}` | `{{slug}}_invoices_{{YYYY-MM-DD}}.json` |
| `{{/api/v1/usage?granularity=...}}` | `{{api_usage_json}}` | `{{slug}}_usage_{{YYYY-MM-DD}}.json` |

- `doc_type` must be one of the values accepted by `src/ingest_raw.py`
  (`bill_pdf`, `api_invoice_json`, `api_usage_json`, `api_orders_json`,
  `weekly_email`, `payment_email`, `csv_export`, `smt_export`, `other`).
- Pagination: `{{how the API pages — cursor/offset/page param — and how you know
  you are done}}`.
- Re-pull window: `{{always re-pull the trailing N months}}` — providers restate
  recent bills; sha256 dedup at ingest makes overlapping pulls safe.

## 3. Document downloads

- Where in the portal: `{{Billing → Documents → ...}}`
- What to grab: `{{bill PDFs, contract/EFL PDFs, CSV exports}}`
- Naming: `{{slug}}_bill_{{YYYY-MM-DD}}.pdf` (statement date, not download
  date). Keep one deterministic pattern per doc_type. Note: the filename
  classifier in `src/ingest_raw.py` currently recognizes only the existing
  providers' patterns — when onboarding a new provider, either pass
  `--doc-type` per path group at ingest time or extend `classify()` (and its
  tests) to know your patterns.
- Download to a staging location first — never straight into `archive_dir`.

## 4. Filing conventions

- File finished documents into `archive_dir` from `providers.local.yaml`.
- **Never overwrite** an existing file in `archive_dir`.
- On a filename collision, the existing file stays put: move the **new** copy
  into `{{archive_dir}}/_to_delete/` and note it in the run report for the user
  to adjudicate. (Content-identical copies are harmless either way — the raw
  store dedups by sha256.)

## 5. Email artifacts (optional)

- Gmail search queries: `{{from:(billing@example.com) subject:(your bill)}}`
- Save each relevant message as plain text (`.txt`) into `raw_dir`, including
  the message id in the filename: `{{slug}}_email_{{YYYY-MM-DD}}_{{message_id}}.txt`.
- Typical doc_types: `weekly_email`, `payment_email`.
- Skip this section entirely if the provider sends nothing worth keeping.

## 6. Land it (ingest CLI)

```sh
uv run python src/ingest_raw.py --provider {{slug}} <archive_dir> <raw_dir> <data_dir>
```

- Substitute the real paths from `providers.local.yaml`; omit `<data_dir>` if
  unset. Add `--dry-run` first if unsure how files will classify.
- Report the counts per doc_type: how many ingested, how many deduped as
  already-known.
- Re-runs are safe: content is deduplicated by sha256, so overlapping pulls and
  repeated invocations do not create duplicates.

## 7. Normalize

<!-- Transitional: keep inline parse notes here until repo parsers exist for
     {{slug}}. Once parsers land, replace this whole section with:
     "Run the parsers. On failure, fix parser code, bump parser_version, and
     reprocess — never hand-edit data." -->

- `{{Inline notes: which fields to extract from each doc_type, where they live
  in the PDF/JSON, units, sign conventions, known layout changes by date.}}`
- Derived data is always reproducible from raw documents — never hand-edit
  parsed output; fix the extraction and re-run instead.

## 8. Report and verify

Summarize the run: date range covered, documents pulled per doc_type,
ingested/deduped counts, anything filed to `_to_delete/`, anomalies.

Provider-specific verification checklist:

- [ ] `{{Latest bill present and its total matches the portal}}`
- [ ] `{{No gap months in the statement sequence}}`
- [ ] `{{Usage totals roughly consistent with the bill period}}`
- [ ] `{{Add checks specific to this provider}}`

---

## Pre-commit hygiene checklist (this repo is PUBLIC)

Before committing the filled-in command file, verify it contains:

- [ ] No account numbers.
- [ ] No premise/meter/ESI-style identifiers, or any provider-issued IDs.
- [ ] No paths containing a username (no `/Users/<name>/...`, `/home/<name>/...`).
- [ ] No email addresses.
- [ ] No contract history (rates, terms, renewal dates).
- [ ] No credentials, tokens, or session cookies.
- [ ] Personal values appear only symbolically — referenced from
      `providers.local.yaml`, never inlined.
