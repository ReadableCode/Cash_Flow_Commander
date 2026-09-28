# usage: the smart-meter series is filed under a retail provider, and that provider has been replaced

    found:  2026-09-28
    status: open
    verify: grep -n '"smt_export"' src/providers/__init__.py

Smart Meter Texas holds 15-minute data for the meter, whoever sells the power.
Here it is routed and keyed as if it belonged to the retail provider: the only
`smt_export` parser is registered under `rhythm`, the capture is named
`rhythm_smt_IntervalData_...`, and the rows take their `account_id` from the
`rhythm` entry in `providers.local.yaml`. Retail service moved from Rhythm to
Gexa on 2026-09-04, so the meter's series now keeps growing under the account
of a provider that no longer serves the address.

## Evidence

`verify:` returned one line on 2026-09-28:

    85:    ("rhythm", "smt_export", _any_name, smt.parse_interval_csv, smt.PARSER_VERSION),

`src/providers/smt.py` ignores the meter identifier in the file and keys on the
configured account:

    the series is keyed on the configured account id from ctx, never on values
    parsed from the document.

`usage_intervals` on 2026-09-28, grouped by account, granularity and metric:

    <rhythm account>   15min  consumption  2024-08-04 .. 2026-09-16   74200 rows
    <rhythm account>   15min  generation   2024-08-04 .. 2026-09-16   74200 rows
    <rhythm account>   hour   consumption  2022-02-26 .. 2026-09-04   10987 rows
    <rhythm account>   hour   generation   2022-02-26 .. 2026-09-04   10987 rows

The 15-minute rows from 2026-09-04 to 2026-09-16 were metered while Gexa was
the provider and sit under the Rhythm account.

`uv run python src/coverage.py --provider gexa` prints
`No matching series in usage_intervals`, so the new provider's command cannot
ask coverage what interval data is missing; that question can only be asked
with `--provider rhythm`. `.claude/commands/bills-gexa.md` section 3.4 works
around it by pointing at the Rhythm command's Smart Meter Texas procedure.

The portal of the new provider is not an alternative source: its usage figures
add consumption and exported generation together (2026-09-10: portal 73.718 kWh
for the day, smart meter consumption 65.016 + generation 8.702).

Nothing is wrong on a dashboard today. `deploy/grafana/energy.json` and
`deploy/grafana/solar_net_metering.json` do not filter usage by `account_id`,
so the series renders continuously across the switch.

## fix

Give the meter its own provider entry, the way solar production already has
one (`enphase_enlighten`: "production is a property of the roof, not of whoever
is currently selling you power").

1. Add a `smt` entry to `template_providers.yaml` and `providers.local.yaml`,
   with the meter identifier as `account_number` and its own `raw_dir`.
2. In `src/providers/__init__.py` register
   `("smt", "smt_export", _any_name, smt.parse_interval_csv, smt.PARSER_VERSION)`
   and remove the `rhythm` one.
3. Move section 3.4 of `.claude/commands/bills-rhythm.md` into its own command,
   name captures `smt_IntervalData_{start}_{end}.csv`, and point both
   electricity commands at it.
4. Re-key what is held. The existing `smt_export` documents are filed with
   `provider='rhythm'`, and re-ingesting them under another provider inserts
   nothing (see `raw-dedup-ignores-provider.md`), so that entry's fix comes
   first. Then reparse under the new provider and remove the old 15-minute
   rows, which is a filtered delete on `usage_intervals` and needs explicit
   confirmation.
5. Tests: registry routing for `smt`, and a coverage test that the meter's
   series is found under its own provider.

## blast radius

Every 15-minute row changes `account_id` (148,400 rows on 2026-09-28). The
reconciliation table on `cfc-solar-net-metering` pairs 15-minute consumption
with bills; check whether it joins on `account_id` before re-keying, because
bills stay under each retail provider's own account. The `hour` series came
from Rhythm's invoices and correctly stays with Rhythm.

## not doing yet

It depends on `raw-dedup-ignores-provider.md`, and step 4 deletes rows from a
shared database. The series is intact and rendering, and nothing is lost by
waiting: Smart Meter Texas keeps about 24 months. It was found while onboarding
Gexa, which needed only the bill path.
