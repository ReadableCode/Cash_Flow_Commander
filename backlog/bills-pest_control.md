# bills: pest control service records and renewals are not captured

    found:  2026-09-30
    status: open
    verify: grep -c "^pest_control:" providers.local.yaml

The verify line prints `1` once the provider is configured. It printed `0` on
2026-09-30.

Pest control is a service contract with an annual renewal and a visit every
few weeks. What was done on each visit and what the contract costs are held
nowhere; the charge reaches `transactions` as a card row.

## Evidence

Configured providers on 2026-09-30, from `providers.local.yaml`:

    rhythm, gexa, smt, enphase_enlighten, chase, citi, elan, our_cash

Mail from the pest control company in the personal mailbox over the year to 2026-09-29, read
from Mail.app: 11 messages, 10 completed service notices and 1 annual
renewal.

How the document arrives: in the mail body, judging by the subject lines
("Completed Service", "New ANNUAL RENEWAL"); no message was opened, so whether
the notice carries the invoice amount is unconfirmed.

## fix

    /bills-add-company

with the slug `pest_control` and service type `home_service`. The company is
regional, so it is not named in this repo; its name lives only in
`providers.local.yaml` (`display_name`). It scaffolds
`.claude/commands/bills-pest_control.md` from `templates/provider-command.md`, stubs
the `pest_control` entry in `providers.local.yaml`, and adds `pest_control_.+` to
`DOWNLOAD_NAME_PATTERNS` in `src/downloads.py` with a sample in
`tests/test_downloads.py`.

Each completed service notice becomes a `bills` row with no usage, the
renewal becomes the row that carries the contract amount, and the contract
itself is an expected series so a missed renewal shows on the board.

Archive the captures under the company's folder under `${ONEDRIVE_DOCS}/FinancialLegal/`, which is where paper
copies are filed already, and add the new command to the overlay manifest that
deploys the other `bills-*` commands.

## blast radius

Additive: one command, one parser, one config entry. A service notice with no
amount produces a `bills` row with a null amount, which `checks.py` may flag;
that is decided when the first notice is read.

## not doing yet

Open three notices first. If the notice carries no amount, the acquisition
is the company's portal, if it has one, and this needs a discovery session;
if it does, this is a mail source and waits on the same transport decision as
`bills-city_utility.md`.
