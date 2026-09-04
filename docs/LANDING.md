# Landing Contract

The shared contract every per-provider bill-extraction command in `.claude/commands/bills-<slug>.md` follows. Read this before writing or running any provider command.

## 0. The pipeline, end to end

A provider run is not done when files are on disk. It is done when the data is visible in Grafana:

```
coverage  →  acquire  →  ingest_raw  →  parse_raw  →  Grafana
(what's     (agent,      (raw_        (typed rows,   (dashboards
 missing)    only the     documents)   usage_         committed in
             gap)                      intervals)     deploy/grafana/)
```

Every provider command implements all five stages. A command that stops at `raw_documents` has done half a job: nothing downstream can see the data, because Grafana reads `usage_intervals` and `bills`, never the raw store.

## 0.1 Re-runnable by construction

Every command must be safe and cheap to run repeatedly, forever. Three properties make that true, and none of them may be weakened:

- **Overlap is free.** Raw documents dedup by sha256 at ingest; `usage_intervals` upserts on `(account_id, ts, granularity, metric)` and `bills` on its own natural key. Re-fetching a period already held costs bandwidth and nothing else.
- **Therefore always err wide.** Never try to fetch precisely. A window that overlaps what you hold is correct; a window that misses a day is a permanent gap if the provider's retention lapses first.
- **Never fetch blindly.** "Everything, every time" wastes a retention-limited window and, on some providers, is impossible. "Since last run" silently loses interior gaps a provider backfilled late. Ask the database instead — see below.

## 0.2 Step one is always coverage

```
uv run python src/coverage.py --provider <slug>
```

Reports, per account/metric/granularity: covered range, interior gaps, thin (partially-captured) days, and the recommended fetch window. Use that window; do not invent one.

It distinguishes two kinds of hole, and the distinction matters:

- **Recent gaps** drive the fetch window — probably a missed run, probably fillable.
- **Persistent gaps** are older than `--max-lookback-days` (default 60) and are reported but not fetched. Some gaps are permanent facts about the source: a day the solar gateway was offline has no data and never will. Without this bound, one dead day in 2022 drags the window back to 2022 on every run forever, turning an incremental workflow into a full re-pull that can never succeed.

Pass `--full` to deliberately retry every known gap.

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
uv run python src/ingest_raw.py --provider <slug> <dirs that hold captures>
```

sha256 dedup makes re-runs free — when in doubt, over-capture and re-ingest. The CLI can also infer the provider from first-level folder names when `--provider` is omitted, but provider commands always pass it explicitly.

## 7. Email capture is optional

Email capture depends on the user's connected tools. It is a supplementary artifact source, never required — a provider command must succeed without it.

## 8. Normalize

```
uv run python src/parse_raw.py --provider <slug>
```

Routes each raw document to its parser via `src/providers/__init__.py::_REGISTRY` and upserts the typed rows. A document with no registered parser stays pending and is reported — that is the signal to write a parser, never to hand-load data.

On a parse failure: fix the parser code, bump its `PARSER_VERSION`, and re-run. Never hand-edit parsed output. Upserts are keyed naturally, so mass-reprocessing is idempotent.

## 9. Dashboards

Dashboards are **repo artifacts, not Grafana-only state**. Grafana's database is not backed up by this repo and a hand-built panel is unreviewable and unreproducible.

- Every dashboard lives in `deploy/grafana/<name>.json`, committed.
- Move them with `deploy/grafana_sync.py`, never by hand and never by copying JSON out of the UI panel editor:

  ```sh
  uv run python deploy/grafana_sync.py export <uid> deploy/grafana/<name>.json
  uv run python deploy/grafana_sync.py import deploy/grafana/<name>.json
  uv run python deploy/grafana_sync.py verify <uid>
  ```

- The committed file is instance-independent: its datasource is `${DS_CASH_FLOW_COMMANDER}`, declared in `__inputs`. **That block must survive into the import payload** — Grafana resolves the placeholder only when it is present, and stripping it yields a dashboard whose every panel says "No data".
- Schema-qualify every table (`cash_flow_commander.usage_intervals`) — the tables are not in `public`.
- Bucket by local time with a **literal** timezone (`timezone('America/Chicago', ts)`). Dashboard-variable timezones are not interpolated on the `/api/ds/query` path.
- Verify with `grafana_sync.py verify`, which runs each panel through Grafana using the panel's **own** datasource reference. Never substitute a known-good uid when testing — that hides an unresolved placeholder and makes a broken dashboard look healthy.

**When a provider run adds or changes a series, re-export the affected dashboards and commit them in the same change.** A new metric that no dashboard shows is invisible work.

## 10. Verification checklist

After every run:

- [ ] Every fetched artifact has a verbatim file on disk.
- [ ] `raw_documents` growth == new-artifact count.
- [ ] Re-run ingest → 100% dedup, zero new rows.
- [ ] `parse_raw.py` reports zero errored and zero no_parser documents.
- [ ] `coverage.py` re-run shows the intended days now covered, and no new thin days.
- [ ] Provider-specific reconciliation when both sources exist (e.g. PDF total vs API amount).
- [ ] Dashboards still render, and any new series is on one of them.
