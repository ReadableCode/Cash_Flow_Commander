---
description: Gexa Energy billing acquisition — capture the invoice list and bill PDFs verbatim, land into raw_documents
argument-hint: [optional: "full" to re-pull every invoice the portal lists]
---

# /bills-gexa — Gexa Energy billing run

Acquire Gexa Energy (electricity) billing artifacts and land them in Cash Flow
Commander's raw store. **Raw-first rule:** every artifact is captured VERBATIM
before any parsing. Raw (`raw_documents`) is the source of truth; tables are
disposable projections that can be rebuilt.

## 0. Orient

**This command is deployed globally, so the session may not start in the repo.**
Every path below is relative to the Cash Flow Commander clone, conventionally
`~/GitHub/Cash_Flow_Commander`. Change into it first; if it lives elsewhere on
this host, find it and use that.

- Read `docs/LANDING.md` for the landing architecture and ingest conventions.
- Load the `gexa` entry from `providers.local.yaml` (repo root, gitignored):
  `service_type`, `account_number`, `external_ids`, `archive_dir`, `raw_dir`,
  `data_dir`, `download_dir`, `tdu_fixed_monthly_charges`, `notes`.
- **STOP if the entry is absent.** Do not guess paths or IDs — tell the user to
  run `/bills-add-company` first, then re-run this command.

This command runs the full pipeline — **coverage → acquire → ingest → parse →
dashboards** — and is safe to re-run at any time.

## 0.1 Coverage — decide what to fetch

```sh
uv run python src/coverage.py --provider gexa
```

This reports `No matching series in usage_intervals`, and that is correct, not
a gap: this provider contributes bills only. Its portal usage data is
deliberately not landed (section 2.1), and interval data for the meter comes
from Smart Meter Texas (section 3.4).

What to fetch is therefore decided by invoices, not by intervals:

- Capture the invoice list on every run. It is one small request.
- Download the PDF of every listed invoice that has no `bills` row with line
  items yet, plus the trailing three invoices to catch restatements. Overlap is
  free (sha256 dedup at ingest, natural-key upsert at parse), so err wide.
- If "$ARGUMENTS" says `full`, download the PDF of every invoice listed.

## 1. Portal and auth

As last observed 2026-09-28.

- Login URL: `https://myaccount.gexaenergy.com/`
- Auth type: email, then password, as two steps of one form (enter the email,
  CONTINUE, then the password field appears). The form also offers a one-time
  code by text or email.
- Open the portal in the user's PERSONAL Chrome profile, never with
  `open location`: `open -na "Google Chrome" --args --profile-directory=<personal dir> <url>`
  (directory name from the `notes` of providers.local.yaml). Drive it over
  AppleScript as described in `transactions-chase.md` section 1.
- **First enumerate the tabs on this portal's domain and keep exactly one**,
  closing extras by URL match in reverse index order and verifying by count.
- The user signs in themselves — never type, store, or echo credentials. On
  2026-09-28 the email field came up empty, so there was nothing for a script to
  submit anyway: leave the tab on the form and ask the user to sign in.
- A new account has no login until it is registered. Registration is three
  steps (account number; date of birth or the last four of the tax id; a
  one-time code plus a new password). The identity and password steps belong
  to the user.
- Decline marketing modals and offers. Never change an account setting.
- **Never enroll in, change, or drive Auto Pay.** The payment-accounts page
  renders inside a cross-origin frame on a separate payment origin, so a script
  in the portal tab cannot read it at all, including which card or bank account
  is on file. The readable signal is the bill card on the dashboard, which
  says `You have Auto Payments enabled` and gives the scheduled date once
  enrolled. Do not read the Next Steps list for this: its `Set up Auto Pay`
  item stays listed after enrollment, and only the completion percentage moves.

### 1.1 Calling the portal's endpoints

The portal is server-rendered pages plus JSON endpoints on the same origin.
Every JSON call needs the session cookie and two request headers:

    Access_Token: <token>
    Is_Ajax_Request: true

- The token lives in `localStorage` under `SelectedAccount_Gexa`, shaped
  `<customer number>-<account id>*<token>`. **The stored value is a
  JSON-encoded string, quotes included — `JSON.parse` it before splitting.**
  Splitting the raw value leaves a trailing quote on the token, and the portal
  then answers `204` or `401` with no hint that the token was malformed.
- `Accounts_Gexa` in `localStorage` holds the account record, including the
  site identifier the usage endpoints take.
- Read both inside the page and use them there. **Never return or echo the
  token**; return only a small summary (status, byte count, sha256).
- **`204` with an empty body means an empty result, not a failure.** The
  payments list answered `204` while the account had no payments. Do not save
  a zero-byte file for it and do not retry it as an error.
- `execute javascript` does not await promises. Start the async work, record
  the outcome on a `window.` property, and poll it after a `delay`.
- The portal loads a third-party bot-protection script. Calls made the way the
  page makes them, from the signed-in tab, were served normally.

## 2. API artifact catalog

As last observed 2026-09-28. Capture responses verbatim into `raw_dir` via
`Blob` + `a.download` (no reformatting, no pretty-printing). Every name below
starts with `gexa_`, which is what `src/downloads.py claim` matches on; section
3 has the claim procedure.

| Endpoint | doc_type | Filename pattern |
| --- | --- | --- |
| `GET /Payments/Invoices?noofMonths={N}` | `api_invoice_json` | `gexa_api_invoice-history_{YYYYMMDD}.json` |
| `GET /PlansServices/CurrentPlanInfo?CustomerAccountId={id}&IsMigratedCustomer=false&IsOttoCustomer=false` | `api_orders_json` | `gexa_api_current-plan_{YYYYMMDD}.json` |

- The response is a bare JSON list, newest first, with `MM/DD/YYYY` dates and
  the amount as a JSON number. It carries the invoice number, its base-36 form
  (needed for the PDF), invoice date, due date, amount, and paid state. It has
  no service period and no kWh; the bill PDF supplies those.
- Pagination: none. `noofMonths` widens the window instead; the page asks for
  `6` and its Show More button asks for `60`. Always ask for `60`.
- Re-pull window: the whole list, every run.

The plan capture, as first observed 2026-09-30:

- It is what the Plans & Moving → My Service Plans page loads. `{id}` is the
  account id, the second part of `<customer number>-<account id>` in
  `SelectedAccount_Gexa` (section 1.1).
- The response is one object. `CurrentPlan` carries the plan name, the contract
  start and end (`M/D/YYYY`), the monthly fee in dollars, the energy charge and
  the advertised averages in cents per kWh as JSON numbers, and `EFLLink`. It
  states no buyback rate; the parser records a plan that pays none, and refuses
  a plan flagged `IsSolarBundled` rather than guess.
- Capture it on every run. **A repeat capture never dedups.** `CurrentPlan.Id`
  is a fresh identifier on every response (verified 2026-09-30: three requests
  minutes apart gave three values and were identical in every other field), so
  the sha256 differs each time and each run lands one more raw document. The
  `plans` upsert on account and contract start is what keeps the term to one
  row, and a renewal lands as a new term by itself.
- To tell whether the plan itself changed, compare the parsed JSON with the
  newest held capture ignoring `CurrentPlan.Id`, not the sha256.
- A second capture on the same day has the same filename and different bytes,
  so it is a collision under section 4 and goes to `_to_delete/`.
- `RenewedPlan` was null. Its shape once a renewal is signed is unknown; the
  parser reads `CurrentPlan` only.

<!-- TODO: fill after the account has history — retention is unprobed. On
     2026-09-28 the account held one invoice, so whether noofMonths=60 is
     honoured, clamped, or capped at some row count could not be tested.
     Probe it live once a year of invoices exists, and check whether asking
     for more than the portal keeps fails loudly or truncates silently. -->

<!-- TODO: payments. `GET /Payments/Payments?noofMonths={N}` answered 204
     (no payments yet) on 2026-09-28, so its payload shape is unknown. No
     parser is registered for it; capture it only once one is written, or it
     lands as a permanent no_parser document. -->

### 2.1 Usage endpoints — documented, deliberately not captured

| Endpoint | Returns |
| --- | --- |
| `GET /Home/GetAccountUsageSummary?CustomerAccountId={id}` | one row per billing period |
| same, plus `&StartDate=&EndDate=&ResolutionCode=P&SiteIdentifier=&Source=` | one row per day of that period |
| same, plus `&StartDate={day}&ResolutionCode=D` | 24 hourly rows for that day |

**Do not land these.** Verified 2026-09-28 against Smart Meter Texas for one
whole day: the portal's kWh for each hour is consumption **plus** surplus
generation added together, and the day's total matched the sum of the two
meter channels to the thousandth. On a site with solar it therefore overstates
consumption by exactly what was exported, and the daily rows for a billing
period sum to more than the kWh that period was billed for. The page itself
labels the figures an estimate. No usage parser is registered for this
provider, and `tests/test_gexa.py` asserts it stays that way.

Two more traps, should these endpoints ever be needed:

- Hourly rows are labelled **hour-ending**. The first row of a day is `01:00`
  and the last is `00:00` carrying the same date, meaning the hour that ends at
  the following midnight.
- **A day with no data returns 24 rows of zeros, not an empty response** — both
  for days before service started and for days not yet published. A zero day is
  indistinguishable from real data unless every hour is checked.

### 2.2 The delivery utility's fixed charge — check it every run

The bill prints delivery as one lump sum, and the parser finds the per-kWh
part by subtracting the delivery utility's fixed charge, which it reads from
`tdu_fixed_monthly_charges` in providers.local.yaml (effective date → dollars).

- The provider publishes the current fixed and per-kWh charge for each
  delivery utility at `https://mygexa2.gexaenergy.com/tdu-charges`, a public
  page with its own "updated" date.
- Compare the fixed charge for the user's delivery utility with the newest
  entry in the config. If it changed, tell the user and add a new dated entry;
  never edit an old one, because older bills were billed at the older charge.
  Then reparse (section 7) so bills after the change are split correctly.
- A bill with no entry in effect fails to parse, by design.

## 3. Document downloads

As last observed 2026-09-28.

- Where in the portal: Payment Center → Invoice & Payment History → INVOICES.
- What to grab: the bill PDF of each invoice chosen in section 0.1.
- Endpoint: `GET /Payments/DownloadInvoice?accessToken={token}&invoiceNumber={base36}`,
  where `{base36}` is the invoice's `Invoice_Number_Base36` from the list. For
  an invoice whose `IsMigrated` is true the page sends the plain invoice number
  and adds `&isMig=true` (not exercised; no migrated invoice existed).
- **The token rides in the query string.** The page opens this URL in a new
  tab; do not. `fetch` it inside the signed-in tab, check that the body starts
  with `%PDF-`, and hand it to `Blob` + `a.download`. Never print the URL.
- Naming: `gexa_bill_{invoice_number}_{YYYY-MM-DD}.pdf`, the date being the
  invoice date. `src/ingest_raw.py` classifies this pattern as `bill_pdf`.
- Compute the sha256 in the page before triggering the download and compare it
  with `shasum -a 256` on the landed file.
- `download_dir` in providers.local.yaml is where this browser puts downloads.
  Every provider command shares it and runs may overlap, so claim each landed
  file with `src/downloads.py`, never by predicting the path or listing the
  folder:

  ```sh
  uv run python src/downloads.py mark      # just before the download; prints a marker
  uv run python src/downloads.py claim --provider gexa --since <marker>
  ```

  `claim` waits (30 s by default, `--timeout SECONDS` to change it) for a file
  that is newer than the marker and named `gexa_...`, then prints its path.
  When one script saves several files (the invoice list, the plan capture and
  the PDFs), add `--expect <N>`. Exit 1 means fewer landed than expected, which
  is what the automatic-downloads block below looks like. Exit 2 means more
  did.
- **Chrome's automatic-downloads permission is per origin.** Without an
  allowance for the portal origin the first download of a run lands and every
  later one vanishes with no prompt and no error — `fetch` still reports 200.
  Seen 2026-09-28: the PDF landed, the invoice list after it did not, and the
  same request landed once the origin was allowed. Confirm
  the setting by reading
  `profile.content_settings.exceptions.automatic_downloads` in the profile's
  `Preferences` file; if the origin is absent, ask the user to add it under
  `chrome://settings/content/automaticDownloads`. Never click Chrome's own
  settings or dialogs yourself.
- Move each capture out of `download_dir` as soon as it is claimed.

The Electricity Facts Label, as first observed 2026-09-30:

- `EFLLink` in the plan capture (the same link as PLAN DETAILS → Electricity
  Facts Label on My Service Plans) is a public viewer URL that answers with a
  two-page PDF. It needs no session, so fetch it with `curl` straight to its
  place; it does not pass through the browser's download folder.
- Fetch it once per plan, when the plan capture differs from the newest held
  one in anything other than `CurrentPlan.Id` (section 2), and file it as
  `<archive_dir>/_contract/gexa_efl_{ProductCode}_{YYYY-MM-DD}.pdf`, the date
  being the label's own date. It is the contract document behind the plan
  capture. It is kept, not ingested: ingest skips folders whose name starts
  with `_`, and the plan capture is what the parser reads.
- The label's line on purchasing excess generation is boilerplate about the
  provider's eligible plans. It states no rate for this plan.

## 3.4 Interval data — Smart Meter Texas

The meter's 15-minute consumption and generation come from Smart Meter Texas,
which holds them for the meter whoever the retail provider is. That series has
its own provider (`smt`) and its own command: run `/bills-smt`. The billed-kWh
reconciliation in section 9 needs 15-minute data covering the new bill's whole
service period.

## 4. Filing conventions

- Move bill PDFs into `archive_dir`, keeping the capture name.
- Move `gexa_api_*.json` captures into `raw_dir`.
- Move the Electricity Facts Label into `<archive_dir>/_contract/`.
- **Never overwrite** an existing file. On a filename collision the existing
  file stays put: move the new copy into `<archive_dir>/_to_delete/` and note
  it in the run report for the user to adjudicate.

## 5. Email artifacts (optional)

<!-- TODO: fill once the provider sends a bill or payment email. As of
     2026-09-28 it had sent only a welcome message, a welcome-letter PDF and
     verification codes; the first bill posted to the portal with no email.
     No parser is registered for email doc types, so skip this section until
     one is, rather than adding permanently unparseable documents. -->

## 6. Land it (ingest CLI)

```sh
uv run python src/ingest_raw.py --provider gexa --source portal_api <archive_dir>
```

**Never omit the directory argument.** `raw_dir` and `data_dir` sit inside
`archive_dir`, so that one path walks all three; passing them separately visits
every file twice and doubles the counts.

- Add `--dry-run` first to see how files classify.
- Report ingested vs deduped counts per doc_type.
- **A non-zero `provider_conflict` count is a failed landing.** The command
  exits 1 and names the provider each listed document is held under. Correct
  the held row with `src/relabel_raw.py` (dry run first), then parse.
- The archive lives in OneDrive with Files On-Demand. A freshly captured file
  that is dehydrated before ingest runs is skipped as a cloud placeholder, so
  grep the output for this run's filenames rather than reading the skip list.

## 7. Normalize

```sh
uv run python src/parse_raw.py --provider gexa
```

`src/providers/gexa.py` handles `api_invoice_json`, `bill_pdf` and
`api_orders_json`. The plan capture lands one row in `plans` for the contract
term, which is what lets `checks.py` judge this provider's bills for a buyback
line that should not be there. The invoice list creates each `bills` row
(invoice number, invoice date, due date, amount due); the PDF then patches
that row by invoice date (service period, billed kWh, energy rate, balances,
total current charges) and writes its line items.
`parse_raw` orders the two within a run, so **a PDF whose invoice is missing
from every captured list fails as unresolved** — capture the list first.

Two things the parser does that the bill does not print:

- **It splits the delivery lump sum** into `delivery_variable` and
  `delivery_fixed` line items, both marked `(derived)` in their description.
  The fixed part is the charge from section 2.2, in full for a period of 27
  days or more and prorated over a 30-day month for a shorter first or last
  period; the per-kWh part is the remainder. The two still sum to the printed
  line. The dashboards price a kWh that was never bought at the energy rate
  plus the per-kWh part, so a bill without a `delivery_variable` line would be
  priced as if delivery were free; `checks.py` reports that.
- **It stores `service_end` as the day before the printed end date.** The bill
  prints two meter read dates, and the closing read opens the next period.
  Verified 2026-09-28 on the first bill: 15-minute consumption over the period
  ending the day before the closing read summed to within half a kWh of the
  billed kWh, and including the read date added a whole day's usage.

A delivery line worded any other way than `TDU Delivery Charges` lands in
`other`, not in a delivery category. That is deliberate: it fails the check
instead of being guessed at. Extend the parser, with a test.

Report parsed / errored / no_parser counts and rows upserted per sink.

On failure: **fix parser code, bump `BILL_PARSER_VERSION`, and reprocess** —
never hand-edit parsed output.

### 7.1 PDF layout reference

As last observed 2026-09-28, a two-page bill:

- Page 1, account summary: `Invoice date: Mon DD YYYY, Invoice No: N`,
  `Opening Balance`, `Balance Forward`, the two block totals repeated,
  `Total Current Charges`, `Total Amount Due`, the late-penalty lines, and
  `Due Date MM/DD/YYYY`.
- Page 2, meter table: meter number, read dates, read type, previous and
  current reads, then `Total Usage N`.
- Page 2, two charge blocks, each opened by
  `<Electricity|TDU> Charges and Taxes Billing Period: MM/DD/YYYY - MM/DD/YYYY`
  and closed by its `Total ... Charges and Taxes` line. The electricity block
  maps to section `energy`, the TDU block to `non_energy`.
- The energy line reads `*Energy Charge <kWh> <rate> $<amount>` with the rate
  in **dollars** per kWh; the parser stores cents. A leading `*` marks lines
  counted in the bill's average price.
- TDU delivery is one lump-sum line with no kWh and no rate. On the first
  bill the per-kWh part the parser derived from it came to within a thousandth
  of a cent of the per-kWh charge the provider publishes.
- The plan name is not printed anywhere on the bill. It comes from the plan
  capture (section 2).

The parser fails the document unless the line items sum to `Total Current
Charges` to the cent.

## 8. Dashboards

This provider feeds `bills` and `bill_line_items`, which `deploy/grafana/energy.json`
(uid `cfc-energy`) and `deploy/grafana/solar_net_metering.json`
(uid `cfc-solar-net-metering`) read. Verify both after a run:

```sh
uv run python deploy/grafana_sync.py verify cfc-energy
uv run python deploy/grafana_sync.py verify cfc-solar-net-metering
```

As last observed 2026-09-28, after the first bill landed:

- The three solar value panels read `delivery_variable` and `solar_buyback`
  line items, never a description. A bill from this provider has no
  `solar_buyback` line, so its buyback is zero and the panels are worth exactly
  the energy that did not have to be bought: self-consumed kWh at the energy
  rate plus per-kWh delivery, grossed up for tax.
- **A billing period appears on those panels only when production, import and
  export intervals all cover it.** Run `/bills-enphase_enlighten` and the
  Smart Meter Texas export (section 3.4) in the same sitting, or the period is
  missing or understated; `checks.py` reports which days are short.
- `Monthly cost by category (stacked)` on `cfc-energy` groups by category, so
  delivery shows as two series.

To change a dashboard, edit it in the UI then round-trip it through
`deploy/grafana_sync.py` (`export`, `import`, `verify`) — never copy JSON out
of the panel editor. Dashboards are committed repo artifacts.

## 8.5 Data-quality check — the silent failures

```sh
uv run python src/checks.py --provider gexa
```

`parse_raw` reports what *failed*. This reports what succeeded and is still
unusable: a bill with no line items, no energy rate, or no positive kWh drops
out of the value panels without a gap or an error; a bill with no per-kWh
delivery, a metered day no bill covers, or a recent billing period its
interval data does not fully cover is understated by them.

`--provider gexa` limits the bill checks to this provider's bills. Interval
data is read across every account either way, because the meter's series and
the production series are each filed under an account of their own.

Non-zero exit means at least one billing period will be missing from the value
panels. Fix it by reprocessing, never by editing rows:

```sh
uv run python src/parse_raw.py --provider gexa --status all
```

Do not proceed to the dashboard step while this is failing.

## 9. Report and verify

Summarize the run: invoices listed vs held, documents pulled per doc_type,
ingested/deduped counts, rows upserted, anything filed to `_to_delete/`,
dashboards touched, unpaid bills, anomalies.

Standard checks (every provider):

- [ ] re-run ingest → 100% dedup, zero new rows
- [ ] `parse_raw.py` reports zero errored and zero no_parser
- [ ] `checks.py` exits zero
- [ ] `uv run python src/downloads.py leftovers --provider gexa` exits 0
      (prints nothing)

Provider-specific verification checklist:

- [ ] every invoice in the captured list has a `bills` row with line items
- [ ] `plans` holds a term for this account that covers the latest bill's
      service period
- [ ] no gap in the invoice sequence: each service period starts where the
      previous one ended
- [ ] each PDF's `Total Amount Due` equals the list's amount for that invoice
- [ ] each landed file's sha256 equals the one computed in the page
- [ ] billed kWh on the latest bill reconciles with the summed 15-minute
      `consumption` over the same service period

Expect agreement within a kWh, not to the unit: the bill prints whole kWh from
two register reads. Observed 2026-09-28 on the first bill, 1333.328 kWh summed
against 1333 billed.

## Keeping this command current

This provider will change its portal, its payload, or its quirks. When it does,
the fix belongs in this file, not in a one-off workaround you forget by the next
run. Before finishing, if reality did not match what is written above:

1. Update the section that was wrong, and date it.
2. Add any new popup, interstitial, or blocking modal to the portal-session
   section.
3. Replace a TODO block with what was observed once it has been observed.
4. Put the download location in `download_dir` and other user-specific quirks
   (account oddities) in the `notes` field of `providers.local.yaml`, never in
   this file.
5. Tell the user what you changed. **Do not commit** — they review and commit.
