# bills: garage door service orders and invoices are not captured

    found:  2026-09-30
    status: open
    verify: grep -c "^garage_door_service:" providers.local.yaml

The verify line prints `1` once the provider is configured. It printed `0` on
2026-09-30.

Garage door service is occasional and each visit produces a service order and
an invoice. Neither is held anywhere; the charge reaches `transactions` as a
card row.

## Evidence

Configured providers on 2026-09-30, from `providers.local.yaml`:

    rhythm, gexa, smt, enphase_enlighten, chase, citi, elan, our_cash

Mail from the garage door company in the personal mailbox over the year to 2026-09-29, read
from Mail.app: 5 messages: service orders and invoices "available for
viewing".

How the document arrives: behind a link in the mail. The document is not in the
message; the link opens it on the company's site.

## fix

    /bills-add-company

with the slug `garage_door_service` and service type `home_service`. The
company is local, so it is not named in this repo; its name and site live only
in `providers.local.yaml` (`display_name`). It scaffolds
`.claude/commands/bills-garage_door_service.md` from `templates/provider-command.md`, stubs
the `garage_door_service` entry in `providers.local.yaml`, and adds `garage_door_service_.+` to
`DOWNLOAD_NAME_PATTERNS` in `src/downloads.py` with a sample in
`tests/test_downloads.py`.

Each invoice becomes a `bills` row with no usage and its lines in
`bill_line_items`. Each service order is kept as a raw document only.

Archive the captures under `${ONEDRIVE_DOCS}/FinancialLegal/Homes and Real Estate`, which is where paper
copies are filed already, and add the new command to the overlay manifest that
deploys the other `bills-*` commands.

## blast radius

Additive: one command, one parser, one config entry. Volume is a few
documents a year.

## not doing yet

The link in the mail has not been followed, so whether it needs a sign-in,
serves a PDF or renders a page is unknown. One session to open a past invoice
settles that and decides whether `/bills-add-company` is the right shape or
the invoice is simply saved as a PDF by hand.
