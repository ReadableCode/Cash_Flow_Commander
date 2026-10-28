---
description: "Voya Financial retirement account acquisition — capture statements, the balance and account activity verbatim, land into raw_documents"
---

# /bills-voya — Voya Financial retirement account run

Voya administers employer 401(k) plans. The deliverable is not a bill: it is
the account's balance over time plus the activity that moved it
(contributions, earnings, fees). Statements are quarterly.

Personal values (plan numbers, plan names, local paths) never go in this
file. They live in the `voya` entry of `providers.local.yaml`, whose
`display_name` and `notes` name the plan.

## 0. Orient

- Read `docs/LANDING.md` for repo conventions and the current state of the pipeline.
- Load the `voya` entry from `providers.local.yaml` (repo root, gitignored):
  `service_type`, `display_name`, `account_number` (the plan number),
  `raw_dir`, `download_dir`, `notes`. `archive_dir` is blank on purpose.
- **STOP if the entry is absent.** Do not guess paths or IDs — tell the user to
  run `/bills-add-company` first, then re-run this command.

This command runs the full pipeline — **decide → acquire → ingest → parse →
check → dashboards** — and is safe to re-run at any time.

Sinks: `account_balances` (one row per account and date: a statement's closing
balance with its period's flows, or the portal's current balance alone) and
`account_activity` (one row per activity line). Both are keyed naturally, so
re-parsing is idempotent.

## 0.1 Decide what to fetch

`coverage.py --provider voya` reports `No matching series in usage_intervals`,
and that is correct: this provider has no interval data. What to fetch is
decided by what is held:

```sh
uv run python src/checks.py --provider voya
```

`statements_unchained` names any quarter missing between two held statements.
Then, on every run:

- **The balance snapshot**, always (one small request, section 2).
- **The activity CSV**, always, for the full history (section 2). It is
  32 KB for five years.
- **Statements only for periods not already held** in `raw_dir`
  (`voya_statement_<period end>.pdf`). Do NOT re-download held statements: the
  portal renders each PDF fresh, so the same statement fetched twice has
  different bytes (same size, new sha256, seen 2026-10-21) and would land as
  a second raw document. A new statement appears a few weeks after each quarter
  ends.
- If "$ARGUMENTS" says `full`, re-fetch every statement anyway, into a scratch
  folder, and land only those whose period is missing.

## 1. Portal and auth

As last observed 2026-10-21.

The browser session is the user's real Chrome, personal profile, per the repo
`CLAUDE.md`: enumerate the tabs on `voya.com` first and close all but one, by
URL match in reverse index order, and verify by count. Open with
`open -na "Google Chrome" --args --profile-directory=<personal dir> <url>` and
drive with `execute <tab> javascript` (AppleScript, as in
`transactions-chase.md` section 1), finding the tab by URL match every call.

- Login URL: `https://my.voya.com/`, which redirects to
  `login.voya.com/voyassoui/index.html#/login-pweb`.
- The user signs in themselves. Never type, store or echo credentials; hand
  over for MFA and wait.
- Signed in, the landing page is `my.voya.com/myvoyageui/#/workplace-dashboard`.
- A cookie banner ("I agree") sits at the bottom of every page. It does not
  block anything; leave it.
- **"Go paperless" modal** (`V-MODAL#edelivery`, a web component with a shadow
  root and no close button): shown on every page while the account was not
  paperless. Its only "Cancel" belongs to the email-edit field and does not
  close it. Never click "Save & Continue" yourself; it changes an account
  setting. Script calls work underneath it. The user turned paperless on
  2026-10-21, so it should not come back; if it does, say so and carry on.
- Decline every other offer (advice, managed accounts, rollover pitches,
  beneficiary nags). Never change an account, investment, contribution or
  beneficiary setting. A modal that looks like security or account settings:
  stop and ask the user.
- **Chrome blocks every download after the first** until the user clicks Allow
  on "my.voya.com wants to download multiple files". The tell: `claim` times
  out while the fetch itself returns 200. Allowed for the personal profile on
  2026-10-21; the permission persists.
- `execute javascript` does not await promises. Start the async work, record
  the outcome on a `window.` property, and poll it after a delay.

## 2. API artifact catalog

As last observed 2026-10-21. All requests are made from the signed-in tab with
`fetch(..., {credentials: "include"})`; the session cookie is the auth. Save
each response verbatim with `Blob` + `a.download` under the names below, which
all start with `voya_`, which is what `src/downloads.py claim` matches on.

| Request | doc_type | Filename |
| --- | --- | --- |
| `GET /myvoyage/ws/ers/dashboard/accounts?s=<session id>` | `api_balance_json` | `voya_api_balance_<YYYY-MM-DD>.json` |
| `POST /eportal/ExcelTransactionDownload.action` | `csv_export` | `voya_activity_<YYYYMMDD from>_<YYYYMMDD to>_captured<YYYYMMDD>.csv` |
| `GET /pptcrsp/pullStatements?planId=<plan>&idx=<n>&cl=INGWIN&domain=null` | `statement_pdf` | `voya_statement_<period end YYYY-MM-DD>.pdf` |

Balance snapshot:

- The session id is the 32-hex `s=` value the dashboard's own requests carry;
  read it from `performance.getEntriesByType("resource")` on the dashboard.
  It is uppercase hex (seen 2026-10-28): match `[0-9A-Fa-f]{32}`, or take it
  straight from the `dashboard/accounts?s=` entry.
- One JSON object. `categorizedAccounts[].accounts[]` carries `planId`,
  `accountBalance` (string) and `accountBalanceAsOf` (`MM/DD/YYYY`, the prior
  business day). The parser lands one balance per plan.
- `/myvoyage/ws/ers/service/portfolioBalanceHistory` answered `{}` to a GET;
  there is no balance history to capture. The history comes from statements.

Activity CSV:

- Reached from the dashboard's "Download Transactions to Excel" link, which
  opens `/eportal/preauth.do?...page=TXDOWNLOAD`. The page holds a plain form;
  post to it from that page with `application/x-www-form-urlencoded` fields
  `type=date_range`, `bnMonth`, `bnDay`, `bnYear`, `edMonth`, `edDay`,
  `edYear` (two-digit month and day, four-digit year), `days=30`,
  `dwd_type=excel`, `alt_acct=false`.
- Always request `01/01/2000` through today. The start date is accepted as is
  and the file starts at the first activity (2021-12-22). No cap: two halves of
  the range held 134 + 118 rows and the full range 252.
- The response is `application/csv;charset=ISO-8859-1`, named
  `TransactionHistory.csv` by the server. Save it from `r.blob()`, not
  `r.text()`, so the bytes stay verbatim.
- Layout: a blank line, a `<plan sponsor name>  <plan number>` line, the
  header `Activity Date,Activity,Fund,Money Source,# of Units,Unit Price,Amount`,
  a blank line, then rows with two extra unnamed code columns. Negative numbers
  are in parentheses. Rows are not in date order.
- An empty window is not an empty file: the body is the sponsor line and
  "You have no transaction history." The parser lands no rows for it.
- A failed export is a 200 `text/html` page (about 11.7 KB, starting
  `<!-- force_pref_structure.jsp`) whose text is "System Unavailable. Sorry
  for the Interruption." Check the content type before saving; never save
  that page. On 2026-10-28 every request got it for 15 minutes, the full
  range, a six-month window and the form's own "Prior 30 days" alike, and
  re-navigating from the dashboard did not clear it, while the balance and
  statement endpoints answered normally. Skip the CSV that run and report it;
  the next run's full-history request covers the gap.
- The page's own `doSubmit()` (`/static/eportal/js/txn_download.js`,
  `validateData`) allows only windows of six months or less that start after
  a date two years back. That is client-side only; the direct POST above
  ignored it on 2026-10-21.
- The dashboard's "View My Statements" and "Download Transactions to Excel"
  links have no `href`; clicking one navigates the same tab (no new tab).

Statements:

- Reached from the dashboard's "View My Statements" link, which opens
  `/pwebui/pptcrsp/statements?...`. The page lists every quarter, newest first,
  each row with its period (`MM/DD/YYYY - MM/DD/YYYY`) and a `Statements` link
  whose `href` carries `idx`. Read the period-to-`idx` map from the table rows
  on every run: `idx` counts from the newest, so it shifts each quarter.
- Each link returns `application/pdf` directly (300 KB to 4.3 MB; the larger
  ones carry an annual disclosure). Privacy notice inserts beside some rows are
  public documents on `voyaretirementplans.com`; not captured.
- Retention: every quarter since the account opened (2021-10-01) was listed on
  2026-10-21.

## 3. Document downloads

The browser drops downloads in `download_dir`. Every provider command shares
that folder and runs may overlap, so claim each download, never by predicting
the path or listing the folder:

```sh
uv run python src/downloads.py mark      # just before triggering the download; prints a marker
uv run python src/downloads.py claim --provider voya --since <marker>
```

`claim` waits (30 s by default, `--timeout SECONDS` to change it) and prints
the landed path. Move each capture into `raw_dir` as soon as it is claimed,
with `mv -n`. A late download from an earlier, blocked attempt lands with the
plain name, and the retry then lands as `name (1).pdf`: keep one, and move the
other to the Trash, not into `raw_dir`.

## 4. Filing conventions

- Captures live in `raw_dir` and stay there after ingest; that folder is the
  archive of Voya originals.
- **Never overwrite** a file in `raw_dir`. On a collision the existing file
  stays and the new copy goes to `<raw_dir>/_to_delete/` for the user.
- `archive_dir` is blank: `raw_dir` is the archive. The folder around it holds
  hand-filed letters of more than one plan (notices beside `raw/`, other
  plans in their own folders). Never ingest it; only `raw_dir` is ingested.

## 5. Email artifacts (optional)

Voya's mail is notices and marketing. Nothing to capture.

## 6. Land it (ingest CLI)

```sh
uv run python src/ingest_raw.py --provider voya --dry-run <raw_dir>
uv run python src/ingest_raw.py --provider voya <raw_dir>
```

Each capture classifies by name: `statement_pdf`, `csv_export`,
`api_balance_json`. Report ingested and deduped counts per doc_type.

## 7. Normalize

```sh
uv run python src/parse_raw.py --provider voya
```

Report parsed / errored / no_parser counts and rows upserted per sink.

- A statement whose summary does not add up (beginning + contributions +
  withdrawals + earnings != ending) errors instead of landing. Fix the parser
  (`src/providers/voya.py`), bump `PARSER_VERSION`, and reprocess with
  `--status all`; never edit rows.
- The summary's "Employer Contributions" line is absent when there were none;
  that parses as zero. "Withdrawals" has so far held only the asset-based fee.

## 8. Dashboards

`deploy/grafana/retirement.json` (uid `cfc-retirement`) reads
`account_balances` for every `retirement` account, not just Voya's: the total
across accounts, balance by account, quarterly change by source, to-date
totals and the statement table. A new retirement provider appears on it with
no dashboard change.

```sh
uv run python deploy/grafana_sync.py verify cfc-retirement
```

If a run changed the dashboard, re-export it with
`deploy/grafana_sync.py export cfc-retirement deploy/grafana/retirement.json`.

## 9. Report and verify

Summarize the run: statements fetched (periods), the snapshot date and
balance, activity rows, ingested/deduped counts, rows upserted, anything filed
to `_to_delete/`, anomalies.

- [ ] re-run ingest → 100% dedup, zero new rows
- [ ] `parse_raw.py` reports zero errored and zero no_parser
- [ ] `uv run python src/checks.py --provider voya` exits 0
- [ ] `uv run python src/downloads.py leftovers --provider voya` exits 0
      (prints nothing)
- [ ] the newest statement's ending balance matches the portal's statement
      page, and the snapshot's balance matches the dashboard
- [ ] summed statement contributions equal the activity CSV's summed
      `Contribution` rows over the same quarters
- [ ] `grafana_sync.py verify cfc-retirement` passes

## Keeping this command current

This provider will change its portal, its payload, or its quirks. When it does,
the fix belongs in this file. Before finishing, if reality did not match what
is written above:

1. Update the section that was wrong, and date it.
2. Add any new popup, interstitial, or blocking modal to section 1.
3. Put the download location in `download_dir` and other user-specific quirks
   in the `notes` field of `providers.local.yaml`, never in this file.
4. Tell the user what you changed. **Do not commit** — they review and commit.
