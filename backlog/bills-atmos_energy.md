# bills: Atmos Energy gas bills are not captured

    found:  2026-09-30
    status: open
    verify: grep -c "^atmos_energy:" providers.local.yaml

The verify line prints `1` once the provider is configured. It printed `0` on
2026-09-30.

Gas is billed monthly and paid by draft, so its charge reaches `transactions`
with no bill, no service period and no usage behind it. With electricity,
solar and the smart meter already held, gas is the missing half of the home
energy picture.

## Evidence

Configured providers on 2026-09-30, from `providers.local.yaml`:

    rhythm, gexa, smt, enphase_enlighten, chase, citi, elan, our_cash

Mail from Atmos Energy in the personal mailbox over the year to 2026-09-29, read
from Mail.app: 23 messages, 10 "bill is available" notices and 11 payment
receipts.

How the document arrives: in the portal. The mail is a notice. Read from the subject
line "Your Atmos Energy Bill is Available online"; no message was opened.

## fix

    /bills-add-company

with the slug `atmos_energy` and service type `gas`. It scaffolds
`.claude/commands/bills-atmos_energy.md` from `templates/provider-command.md`, stubs
the `atmos_energy` entry in `providers.local.yaml`, and adds `atmos_energy_.+` to
`DOWNLOAD_NAME_PATTERNS` in `src/downloads.py` with a sample in
`tests/test_downloads.py`.

The parser reads one `bill_pdf` into a `bills` row and the billed volume into
`usage_intervals` at monthly granularity under a `gas` metric, in the unit
the bill prints. Delivery and gas cost split into `bill_line_items` the way
the electricity parsers split delivery from energy.

Archive the captures under `${ONEDRIVE_DOCS}/FinancialLegal/Utilities/Atmos`, which is where paper
copies are filed already, and add the new command to the overlay manifest that
deploys the other `bills-*` commands.

## blast radius

Additive: one command, one parser, one registry row, one config entry, one
new usage metric. A gas series shares nothing with the electricity series, so
no dashboard panel changes until one is added for it.

## not doing yet

Needs a discovery session against the portal: whether it serves a bill list
endpoint or PDFs only, how far back it keeps them, and what the PDF prints
for volume and rate.
