# bills: KeyBank solar loan statements are not captured

    found:  2026-09-30
    status: open
    verify: grep -c "^keybank_solar_loan:" providers.local.yaml

The verify line prints `1` once the provider is configured. It printed `0` on
2026-09-30.

The solar loan is paid monthly by draft. Its payment reaches `transactions`
with no statement behind it, so principal, interest and the remaining balance
are held nowhere. The solar value panels compare production against the
electricity bill and never against what the system costs.

## Evidence

Configured providers on 2026-09-30, from `providers.local.yaml`:

    rhythm, gexa, smt, enphase_enlighten, chase, citi, elan, our_cash

Mail from KeyBank Solar Lending in the personal mailbox over the year to 2026-09-29, read
from Mail.app: 15 messages, 10 monthly statement notices and 4
payment posted notices.

How the document arrives: in the portal. The mail is a notice. Read from the subject
line "Your KeyBank Solar Lending Monthly Statement"; no message was opened.

## fix

    /bills-add-company

with the slug `keybank_solar_loan` and service type `loan`. It scaffolds
`.claude/commands/bills-keybank_solar_loan.md` from `templates/provider-command.md`, stubs
the `keybank_solar_loan` entry in `providers.local.yaml`, and adds `keybank_solar_loan_.+` to
`DOWNLOAD_NAME_PATTERNS` in `src/downloads.py` with a sample in
`tests/test_downloads.py`.

A loan statement carries principal paid, interest paid, escrow if any and the
remaining balance. `bills` has no column for any of them, and `usage_intervals`
is for metered quantities. The sink is the decision below; the parser follows
it.

Archive the captures under `${ONEDRIVE_DOCS}/FinancialLegal/Homes and Real Estate`, which is where paper
copies are filed already, and add the new command to the overlay manifest that
deploys the other `bills-*` commands.

## blast radius

Additive once the sink exists. Both loan entries (`bills-keybank_solar_loan.md`
and `bills-toyota_financial.md`) share the schema decision, so settle it once
and build both parsers on it.

## not doing yet

The sink is undecided: a `loan_statements` table keyed on
`(account_id, statement_date)` with principal, interest, balance and payment
columns, or a `bills` row plus `bill_line_items` for principal and interest.
The balance argues for its own table, since a line item cannot carry a running
total. Decide before scaffolding either loan.
