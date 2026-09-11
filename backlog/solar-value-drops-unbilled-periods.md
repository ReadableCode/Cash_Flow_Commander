# checks: solar value panels silently drop metered days no bill covers

    found:  2026-09-11
    status: open
    verify: uv run python -c "from sqlalchemy import text; from src.db import get_engine; print(get_engine().connect().execute(text(\"SELECT count(*), string_agg(d::text, ' ' ORDER BY d) FROM (SELECT DISTINCT timezone('America/Chicago', ts)::date AS d FROM cash_flow_commander.usage_intervals WHERE granularity = '15min' AND metric IN ('consumption', 'generation')) x WHERE NOT EXISTS (SELECT 1 FROM cash_flow_commander.bills b WHERE d BETWEEN b.service_start AND b.service_end)\")).one())"

The solar value panels on `cfc-solar-net-metering` only value days that fall
inside some bill's service period (`usage` joins `usage_intervals` to each bill
with `BETWEEN m.service_start AND m.service_end`). Rhythm's last service period
ended 2026-09-03. Smart Meter Texas keeps recording the meter under the new
retailer, but no bills from that provider can land until it is onboarded. Every
day in between is dropped from "Total solar value" and the per-period bars, and
the charts read as solar value stopping rather than bills missing.

`src/checks.py` exists to catch exactly this kind of quiet understatement, but
all three of its checks (`bills_without_line_items`,
`bills_without_energy_rate`, `bills_without_usable_kwh`) select from `bills`. A
bill that does not exist produces no finding.

## Evidence

`verify:` on 2026-09-11:

    (6, '2026-09-04 2026-09-05 2026-09-06 2026-09-07 2026-09-08 2026-09-09')

That grows by a day each time SMT publishes, until the new provider's bills are
parsed. Without the `metric IN ('consumption', 'generation')` filter the query
also returns 2021-12-23..2021-12-28, which hold only `production` rows and are
not valuable anyway (the panels need SMT import and export).

The same day, `uv run python src/checks.py --provider rhythm` printed
`No unvaluable billing periods. Dashboard value panels cover every bill.` and
exited 0.

Some trailing uncovered days are normal even with a live retailer: SMT runs
about two days in arrears while bills only cover through the last billed day, so
up to a full billing cycle of metered days is always unbilled.

## fix

Add `metered_days_without_bill` to `CHECKS` in `src/checks.py`, built on the
`verify:` query:

- Match bills across **all** accounts. SMT rows stay under the Rhythm account,
  while the new provider's bills will carry its own account id, and the panels
  do not filter by account either.
- Report interior uncovered days always.
- Report trailing uncovered days (after the latest `service_end`) only when the
  oldest is further back than the longest normal billing lag. Measure the cycle
  first:

      uv run python -c "from sqlalchemy import text; from src.db import get_engine; print(get_engine().connect().execute(text('SELECT max(service_end - service_start) FROM cash_flow_commander.bills')).scalar())"

  and set the threshold a margin above it (for example 45 days).

Test it alongside the existing checks tests.

## blast radius

No data changes. It adds an exit-1 finding, so `/bills-rhythm` and any other run
of `checks.py` fails once the trailing gap passes the threshold (around mid
October 2026 if the new provider is not onboarded by then). That alarm is the
point. It does not affect the dashboards.

## not doing yet

Deferred on 2026-09-11 to the onboarding of the new provider, where it will be
picked up. The trailing threshold depends on that provider's billing cycle, which
is unknown until its login exists. Pairs with
`solar-buyback-inferred-from-generic-credit.md`.
