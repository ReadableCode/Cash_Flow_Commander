# bills: the city's water statements are not captured

    found:  2026-09-30
    status: open
    verify: grep -c "^city_utility:" providers.local.yaml

The verify line prints `1` once the provider is configured. It printed `0` on
2026-09-30.

The city bills water, sewer and waste on one monthly statement. Its charge
reaches `transactions` as a bank draft with no statement, no service period
and no gallons behind it. Water is the one metered utility this repo does
not hold, beside electricity, solar and the smart meter.

## Evidence

Configured providers on 2026-09-30, from `providers.local.yaml`:

    rhythm, gexa, smt, enphase_enlighten, chase, citi, elan, our_cash

Mail from the city's utility office in the personal mailbox over the year to 2026-09-29, read
from Mail.app: 24 messages, 9 of them the monthly statement, the rest
library newsletters from the same sender.

How the document arrives: as a PDF attached to the statement mail. Confirmed from
the message text: "Your statement is attached to this email". Mail.app had not
stored the attachment of the 2026-09-29 statement, so the file was not read.
Whether the city's portal offers the same PDF or a usage export was not
looked at.

## fix

    /bills-add-company

with the slug `city_utility` and service type `water`. The city is not named
anywhere in this repo: the slug is generic, and the city's name, portal host
and folder live only in `providers.local.yaml` (`display_name`). It scaffolds
`.claude/commands/bills-city_utility.md` from `templates/provider-command.md`, stubs
the `city_utility` entry in `providers.local.yaml`, and adds `city_utility_.+` to
`DOWNLOAD_NAME_PATTERNS` in `src/downloads.py` with a sample in
`tests/test_downloads.py`.

The parser reads one `bill_pdf` into a `bills` row (statement date, due date,
amount, service period) and the month's water volume into `usage_intervals`
at monthly granularity under a `water` metric, the way the gas entry does for
gas.

Archive the captures in the city's folder under `${ONEDRIVE_DOCS}/FinancialLegal/Utilities/`, named in
`providers.local.yaml`, which is where paper copies are filed already, and add the new command to the overlay manifest that
deploys the other `bills-*` commands.

## blast radius

Additive: one command, one parser, one registry row, one config entry, one
new usage metric. Nothing existing changes.

## not doing yet

The statement is already in the mailbox, so the cheapest acquisition is to
read the attachment rather than open a portal. This repo has no mail
transport: `docs/LANDING.md` section 7 treats mail as optional and no parser
is registered for any mail document type. Whether an attachment handed over
by Mail.app is an accepted transport, or the portal must be discovered, is
the decision to make first.
