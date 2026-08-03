# Landing Contract

The shared contract every per-provider bill-extraction command in `.claude/commands/bills-<slug>.md` follows. Read this before writing or running any provider command.

## 1. Raw-first rule

Capture every artifact VERBATIM before any parsing:

- API response bodies → save as `.json`, byte-for-byte as returned.
- Bill PDFs → save as-is, exactly as downloaded.
- Email bodies → save as `.txt`.

Raw is the source of truth — the `raw_documents` table. Every structured table and CSV is a rebuildable projection of it. If parsing is wrong, fix the parser and reproject; never patch the projection.

## 2. Division of labor

- **Agents acquire.** Browser sessions, portal logins, navigation, judgment calls about what to fetch.
- **Scripts land and normalize.** Deterministic, stamped with `parser_version`, safe to mass-reprocess.
- **Agents run the scripts, review the reports, and on failure fix parser CODE.** Never hand-massage data — a hand-edited row is unreproducible and will be silently clobbered on the next reprocess.

## 3. Config loading

Step zero of every provider command: read your entry from `providers.local.yaml` (repo root, gitignored). If your entry is missing, STOP and direct the user to `/bills-add-company` (or to copy `template_providers.yaml`). Personal values — account numbers, `premise_id`, credentials, directories — come only from there; they never appear in commands, code, or docs.

## 4. Artifact naming

Pattern: `{provider}_{doc_type}_{qualifier}.{ext}`

Examples:

- `rhythm_api_usage_INV123.json`
- `rhythm_email_weekly_2026-08-01_<msgid>.txt`

`doc_type` vocabulary comes from the ingest layer: `bill_pdf`, `api_invoice_json`, `api_usage_json`, `api_orders_json`, `weekly_email`, `payment_email`, `csv_export`, `smt_export`, `other`.

## 5. Staging

Everything lands in the provider's `raw_dir` before ingestion. For browser-fetched data, use Blob + `a.download` downloads for anything large — JS tool results truncate, and a truncated capture is not verbatim.

## 6. Landing

From the repo root:

```
uv run python src/ingest_raw.py --provider <slug> <archive_dir> <raw_dir> <data_dir>
```

sha256 dedup makes re-runs free — when in doubt, over-capture and re-ingest. The CLI can also infer the provider from first-level folder names when `--provider` is omitted, but provider commands always pass it explicitly.

## 7. Email capture is optional

Email capture depends on the user's connected tools. It is a supplementary artifact source, never required — a provider command must succeed without it.

## 8. Verification checklist

After every run:

- [ ] Every fetched artifact has a verbatim file on disk.
- [ ] `raw_documents` growth == new-artifact count.
- [ ] Re-run ingest → 100% dedup, zero new rows.
- [ ] Provider-specific reconciliation when both sources exist (e.g. PDF total vs API amount).
