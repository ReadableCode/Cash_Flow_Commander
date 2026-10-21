---
description: "KeyBank Solar Lending loan acquisition — capture the monthly statement PDFs from the mailbox verbatim, land into raw_documents"
---

# /bills-keybank_solar_loan — KeyBank solar loan statement run

KeyBank Solar Lending services the loan that financed the solar panels. The
deliverable is the principal owed on each monthly statement date and how each
payment split between principal and interest.

Personal values (the loan id, paths) never go in this file. They live in the
`keybank_solar_loan` entry of `providers.local.yaml`.

## 0. Orient

- Read `docs/LANDING.md` for repo conventions.
- Load the `keybank_solar_loan` entry from `providers.local.yaml`:
  `account_number` (the loan id), `raw_dir`, `notes`.
- **STOP if the entry is absent** and send the user to `/bills-add-company`.

Sinks: `account_balances` (account_kind `loan`, one row per statement date:
the principal owed, opening at the previous statement's close) and
`loan_transactions` (one row per payment, with its principal and interest
share).

## 0.1 Decide what to fetch

`coverage.py` reports `No matching series in usage_intervals`; that is
correct, this provider has no interval data. Run

```sh
uv run python src/checks.py --provider keybank_solar_loan
```

`statements_unchained` names any month missing between two held statements.
Fetch every statement mail whose statement is not yet in `raw_dir`
(`keybank_solar_loan_statement_<statement date>.pdf`). A new statement is
dated the 9th and mailed on the 10th.

## 1. Source: the mailbox, no portal

As last observed 2026-10-21.

Every statement arrives as the attachment `AFCLoanStatement.pdf` on the mail
"Account Alert: Your KeyBank Solar Lending Monthly Statement" from
`servicing@solar.key.com`. Every statement since the first (2022-02-09) is in
the mailbox, so no portal session is needed; the mail's portal link is not
used.

- Search with the Gmail connector: `from:servicing@solar.key.com has:attachment`.
- Save each attachment with the connector's attachment download into a scratch
  folder outside `raw_dir` and outside any repo, so the bytes are written as
  sent. Read the `Statement Date:` from the PDF and move it into `raw_dir` as
  `keybank_solar_loan_statement_<YYYY-MM-DD>.pdf` with `mv -n`.
- No browser download is involved, so `src/downloads.py` does not apply. The
  `keybank_solar_loan_.+` pattern exists so the shared download folder still
  recognises a file of this provider's if one lands there.
- Re-fetching a statement is harmless: the attachment bytes are fixed, so
  sha256 dedup at ingest collapses it.

## 2. Statement layout

As last observed 2026-10-21. pypdf extracts page 1 one value per line.

- Header fields, each followed by its value on the next line: `Statement
  Date:`, `Account Number:` (the loan id), `Outstanding Principal:`.
- The transaction table runs from `<date> Beginning Balance <amount>` (dated
  the previous statement date) to `<statement date> Ending Balance <amount>`.
  Each transaction is a date (from 2026-10 also an effective date), the signed
  amount, a description, the signed split values and the principal balance
  after it.
- Seen descriptions: `Payment` (principal and interest), `P+I Interest
  Payment` (2022-02 and 2022-03: interest only, principal unchanged). The
  first statement (2022-02-09) has no transactions.
- The split values are not labelled one by one, so the parser takes the
  principal share from the balance change and the rest as interest, and
  errors when the split values do not sum to the amount or the balances
  disagree.

## 3. Filing conventions

- `raw_dir` is the archive of the statements; files stay there after ingest.
- **Never overwrite** a file in `raw_dir`. On a collision the existing file
  stays and the new copy goes to `<raw_dir>/_to_delete/` for the user.
- `archive_dir` holds the original loan disclosure and other solar papers.
  Never ingest it.

## 4. Land it

```sh
uv run python src/ingest_raw.py --provider keybank_solar_loan --dry-run <raw_dir>
uv run python src/ingest_raw.py --provider keybank_solar_loan <raw_dir>
uv run python src/parse_raw.py --provider keybank_solar_loan
```

Each file classifies as `statement_pdf`. On a parse error fix
`src/providers/keybank_solar_loan.py`, bump `PARSER_VERSION`, and reprocess
with `--status all`; never edit rows.

## 5. Dashboards

`deploy/grafana/loans.json` (uid `cfc-loans`) reads every `loan` account:
principal owed (all loans and per loan), each payment's principal and interest
split, and to-date totals. A new loan provider appears there with no dashboard
change.

```sh
uv run python deploy/grafana_sync.py verify cfc-loans
```

## 6. Report and verify

Summarize: statements fetched (dates), the newest principal owed, payments
landed, ingested/deduped counts, anomalies.

- [ ] re-run ingest → 100% dedup, zero new rows
- [ ] `parse_raw.py` reports zero errored and zero no_parser
- [ ] `uv run python src/checks.py --provider keybank_solar_loan` exits 0
- [ ] the newest statement's balance matches the mail's statement
- [ ] the loan's payments match the KeyBank drafts in the checking account's
      `transactions` over the same dates (count and sum)
- [ ] `grafana_sync.py verify cfc-loans` passes

## Keeping this command current

When the statement, the mail or the source changes, fix this file and date
the section. User-specific facts go in the `notes` of `providers.local.yaml`.
Tell the user what changed. **Do not commit** — they review and commit.
