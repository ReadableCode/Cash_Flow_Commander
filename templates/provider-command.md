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

This command runs the full pipeline — **coverage → acquire → ingest → parse →
dashboards** — and is safe to re-run at any time. Stopping at `raw_documents`
leaves the data invisible to Grafana; stopping before the dashboard re-export
leaves new series unseen.

## 0.1 Coverage — decide what to fetch

```sh
uv run python src/coverage.py --provider {{slug}}
```

Use the reported `FETCH:` window. Do not invent one, do not re-pull everything
by default, and do not assume "since last run" — that misses interior gaps a
provider backfilled late.

- Overlap is free (sha256 dedup at ingest, natural-key upsert at parse), so
  **err wide**. A window that overlaps held data is correct; one that misses a
  day may become a permanent gap.
- Gaps reported as *persistent* are older than the lookback bound and are not
  fetched — often they are permanent facts about the source (provider outage,
  data that never existed). Retry them deliberately with `--full`, and only
  when there is reason to think they are fillable.
- On a first run the series is empty and the answer is "fetch full history".

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

```sh
uv run python src/parse_raw.py --provider {{slug}}
```

Report parsed / errored / no_parser counts and rows upserted per sink.

- `no_parser > 0` means a document type has no parser registered in
  `src/providers/__init__.py::_REGISTRY` — write one (with tests), do not
  hand-load the data.
- On a parse failure, **fix parser code, bump `PARSER_VERSION`, and reprocess**
  — never hand-edit parsed output. Upserts are keyed naturally, so
  mass-reprocessing is idempotent.

<!-- Transitional: while no parser exists for {{slug}}, keep inline extraction
     notes here — which fields live where, units, sign conventions, layout
     changes by date — and delete them once the parser lands. -->

## 8. Dashboards

New or changed series must reach Grafana in the same run, or the work is
invisible. Dashboards are committed repo artifacts, never Grafana-only state.

- Dashboards live in `deploy/grafana/<name>.json`.
- If this run added a metric, granularity, or account that no committed
  dashboard shows, add or update a panel.
- Move dashboards only with `deploy/grafana_sync.py` — never by hand, never by
  copying JSON out of the UI panel editor:

  ```sh
  uv run python deploy/grafana_sync.py export <uid> deploy/grafana/<name>.json
  uv run python deploy/grafana_sync.py import deploy/grafana/<name>.json
  uv run python deploy/grafana_sync.py verify <uid>
  ```

- Commit the exported file alongside the code change.
- **Always finish with `verify`.** It runs every panel through Grafana using
  each panel's own datasource reference. A dashboard can import "successfully"
  and still render "No data" on every panel if its `${DS_CASH_FLOW_COMMANDER}`
  placeholder went unresolved — `verify` is what catches that.
- Schema-qualify every table (`cash_flow_commander.<table>`) and use a literal
  timezone in date bucketing, not a dashboard variable.

## 8.5 Data-quality check — the silent failures

```sh
uv run python src/checks.py --provider {{slug}}
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
uv run python src/parse_raw.py --provider {{slug}} --status all
```

Do not proceed to the dashboard step while this is failing — you would be
verifying a dashboard that renders successfully and understates.

## 9. Report and verify

Summarize the run: coverage window requested vs actually filled, documents
pulled per doc_type, ingested/deduped counts, rows upserted, anything filed to
`_to_delete/`, dashboards touched, anomalies.

Standard checks (every provider):

- [ ] `coverage.py` re-run shows the intended days now covered, no new thin days
- [ ] re-run ingest → 100% dedup, zero new rows
- [ ] `parse_raw.py` reports zero errored and zero no_parser
- [ ] any new series appears on a committed dashboard

Provider-specific verification checklist:

- [ ] `{{Latest bill present and its total matches the portal}}`
- [ ] `{{No gap months in the statement sequence}}`
- [ ] `{{Usage totals roughly consistent with the bill period}}`
- [ ] `{{Add checks specific to this provider}}`

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

Before committing the filled-in command file, verify it contains:

- [ ] No account numbers.
- [ ] No premise/meter/ESI-style identifiers, or any provider-issued IDs.
- [ ] No paths containing a username (no `/Users/<name>/...`, `/home/<name>/...`).
- [ ] No email addresses.
- [ ] No contract history (rates, terms, renewal dates).
- [ ] No credentials, tokens, or session cookies.
- [ ] Personal values appear only symbolically — referenced from
      `providers.local.yaml`, never inlined.
