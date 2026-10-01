# bills: HOA dues receipts are not captured

    found:  2026-09-30
    status: open
    verify: grep -c "^hoa:" providers.local.yaml

The verify line prints `1` once the provider is configured. It printed `0` on
2026-09-30.

The HOA dues are drafted monthly. The draft reaches `transactions` as a bank
row, and the receipt the portal mails is the only record of what period it
covered. Nothing reconciles the draft against the dues schedule.

## Evidence

Configured providers on 2026-09-30, from `providers.local.yaml`:

    rhythm, gexa, smt, enphase_enlighten, chase, citi, elan, our_cash

Mail from the homeowners association's payment portal in the personal mailbox over the year to 2026-09-29, read
from Mail.app: 15 messages, a recurring payment reminder before each
draft and a payment receipt after it.

How the document arrives: in the mail body. The reminder names the draft date;
whether the receipt names the amount and the period is unconfirmed, as no
message was opened.

## fix

    /bills-add-company

with the slug `hoa` and service type `hoa`. It scaffolds
`.claude/commands/bills-hoa.md` from `templates/provider-command.md`, stubs
the `hoa` entry in `providers.local.yaml`, and adds `hoa_.+` to
`DOWNLOAD_NAME_PATTERNS` in `src/downloads.py` with a sample in
`tests/test_downloads.py`.

Each receipt becomes a `payments` row, and the dues become an expected series
matched to the bank draft so a missed or changed draft shows on the board.
The association's notices and board minutes are documents, not money, and
stay out of this repo.

Archive the captures under `${ONEDRIVE_DOCS}/FinancialLegal/Homes and Real Estate/<the property's folder>`, which is where paper
copies are filed already, and add the new command to the overlay manifest that
deploys the other `bills-*` commands.

## blast radius

Additive: one command, one parser, one config entry. If an expected series
for the dues already exists, only the receipt parser is new; check
`expected_series` first.

## not doing yet

Whether the receipt carries enough to be worth parsing (amount, period) is
unconfirmed. Open two receipts before scaffolding. If the payment portal has
no document beyond the receipt mail, this is a mail source and waits on the
same transport decision as `bills-city_utility.md`.
