"""Report data holes that dashboards render as plausible-looking numbers.

Some failures are loud: a parser raises, ingest reports an error, `verify` says
a panel is broken. This tool is for the quiet ones — where a panel renders
successfully and is simply *wrong*, because the SQL behind it dropped rows it
could not join.

The case that motivated it: every value panel on `cfc-solar-net-metering`
starts with

    FROM bills b JOIN bill_line_items li USING (invoice_number)
    ...
    WHERE energy_rate_cents IS NOT NULL

Both are inner filters. A bill whose line items failed to parse, or which parsed
without an energy-rate line, silently vanishes from the series. The chart does
not gap, it does not error, it just draws a shorter line and a smaller total —
so a `/bills-rhythm` run that failed to parse bills flattens the solar value
panels even when the solar data is perfect. Nothing in the existing pipeline
reports that, because from `parse_raw`'s point of view nothing failed.

Usage:

    uv run python src/checks.py --provider rhythm
    uv run python src/checks.py --json

Three more checks cover the ways the same panels understate without dropping a
bill. A bill with no per-kWh delivery line is priced as if delivery were free.
A metered day no bill covers is valued at nothing, and a bill that does not
exist cannot show up in a check that reads bills. A billed period whose
interval data stops partway is valued on the days that happen to be held.

Exit code is 1 when any check finds something, so a provider command cannot
finish green while its dashboards are quietly understating.
"""

# %%
# Imports #

import argparse
import datetime
import json
import os
import sys
from typing import Any

from zoneinfo import ZoneInfo

import yaml
from sqlalchemy import and_, func, select
from sqlalchemy.engine import Engine

_SRC_DIR = os.path.dirname(os.path.abspath(__file__))
if _SRC_DIR not in sys.path:
    sys.path.insert(0, _SRC_DIR)

import bootstrap  # noqa: E402
import db  # noqa: E402
import user_paths  # noqa: E402

# %%
# Constants #

_REPO_ROOT = os.path.dirname(_SRC_DIR)
PROVIDERS_YAML_PATH = os.path.join(_REPO_ROOT, "providers.local.yaml")

# Line-item category that carries the marginal energy rate the valuation needs.
ENERGY_CATEGORY = "energy"

# Line-item category that carries the per-kWh part of delivery, the other half
# of what a kWh that was never bought would have cost.
VARIABLE_DELIVERY_CATEGORY = "delivery_variable"

# Days are site-local: a bill's service period is a range of local dates.
SITE_TZ = ZoneInfo("America/Chicago")
INTERVAL_GRANULARITY = "15min"

# The smart meter's two channels, and the three series the valuation joins.
METERED_METRICS = ("consumption", "generation")
VALUED_METRICS = ("production", "consumption", "generation")

# Metered days after the last billed day are normal: the meter publishes within
# two days and a bill arrives a cycle later. Past this many days it is a bill
# that never landed. The longest cycle held was 35 days.
TRAILING_UNBILLED_DAYS = 45

# Only a recent bill's interval data can still be fetched; an older hole is a
# fact about the source (the gateway was offline, the meter data starts later).
RECENT_BILL_DAYS = 60

NO_BILL = "(no bill)"


# %%
# Config #


def _load_providers_config(path: str) -> dict[str, Any]:
    """Load providers.local.yaml; {} when missing or unreadable."""
    user_paths.check_config_readable(path)
    if not os.path.isfile(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as handle:
            loaded = yaml.safe_load(handle)
    except (OSError, yaml.YAMLError):
        return {}
    user_paths.check_not_desymlinked(path, loaded)
    return loaded if isinstance(loaded, dict) else {}


def account_id_for_provider(provider: str, config: dict[str, Any]) -> str | None:
    """Resolve a provider slug to its account id, or None when unset."""
    entry = config.get(provider)
    if not isinstance(entry, dict):
        return None
    account = entry.get("account_number")
    return str(account) if account is not None and str(account).strip() else None


# %%
# Checks #


def bills_without_line_items(engine: Engine, account_id: str | None) -> list[dict[str, Any]]:
    """Bills that have no line items at all — dropped by the valuation's inner join."""
    line_item_count = (
        select(func.count())
        .select_from(db.bill_line_items)
        .where(db.bill_line_items.c.invoice_number == db.bills.c.invoice_number)
        .scalar_subquery()
    )
    stmt = select(
        db.bills.c.account_id,
        db.bills.c.invoice_number,
        db.bills.c.service_start,
        db.bills.c.service_end,
    ).where(line_item_count == 0)
    if account_id is not None:
        stmt = stmt.where(db.bills.c.account_id == account_id)

    with engine.connect() as conn:
        rows = conn.execute(stmt.order_by(db.bills.c.service_start)).mappings().all()
    return [{**dict(row), "problem": "no line items"} for row in rows]


def bills_without_energy_rate(engine: Engine, account_id: str | None) -> list[dict[str, Any]]:
    """Bills whose line items carry no energy rate — dropped by `energy_rate_cents IS NOT NULL`."""
    energy_rates = (
        select(func.count())
        .select_from(db.bill_line_items)
        .where(
            and_(
                db.bill_line_items.c.invoice_number == db.bills.c.invoice_number,
                db.bill_line_items.c.category == ENERGY_CATEGORY,
                db.bill_line_items.c.rate_cents_kwh.isnot(None),
            )
        )
        .scalar_subquery()
    )
    any_line_items = (
        select(func.count())
        .select_from(db.bill_line_items)
        .where(db.bill_line_items.c.invoice_number == db.bills.c.invoice_number)
        .scalar_subquery()
    )
    stmt = select(
        db.bills.c.account_id,
        db.bills.c.invoice_number,
        db.bills.c.service_start,
        db.bills.c.service_end,
    ).where(and_(any_line_items > 0, energy_rates == 0))
    if account_id is not None:
        stmt = stmt.where(db.bills.c.account_id == account_id)

    with engine.connect() as conn:
        rows = conn.execute(stmt.order_by(db.bills.c.service_start)).mappings().all()
    return [{**dict(row), "problem": "line items but no energy rate"} for row in rows]


def bills_without_usable_kwh(engine: Engine, account_id: str | None) -> list[dict[str, Any]]:
    """Bills with no positive total_kwh — dropped by `b.total_kwh > 0`, and the
    marginal-rate arithmetic divides by it besides."""
    stmt = select(
        db.bills.c.account_id,
        db.bills.c.invoice_number,
        db.bills.c.service_start,
        db.bills.c.service_end,
    ).where((db.bills.c.total_kwh.is_(None)) | (db.bills.c.total_kwh <= 0))
    if account_id is not None:
        stmt = stmt.where(db.bills.c.account_id == account_id)

    with engine.connect() as conn:
        rows = conn.execute(stmt.order_by(db.bills.c.service_start)).mappings().all()
    return [{**dict(row), "problem": "no positive total_kwh"} for row in rows]


def bills_without_variable_delivery(engine: Engine, account_id: str | None) -> list[dict[str, Any]]:
    """Bills whose line items carry no per-kWh delivery — valued as if delivery were free."""
    variable_lines = (
        select(func.count())
        .select_from(db.bill_line_items)
        .where(
            and_(
                db.bill_line_items.c.invoice_number == db.bills.c.invoice_number,
                db.bill_line_items.c.category == VARIABLE_DELIVERY_CATEGORY,
            )
        )
        .scalar_subquery()
    )
    any_line_items = (
        select(func.count())
        .select_from(db.bill_line_items)
        .where(db.bill_line_items.c.invoice_number == db.bills.c.invoice_number)
        .scalar_subquery()
    )
    stmt = select(
        db.bills.c.account_id,
        db.bills.c.invoice_number,
        db.bills.c.service_start,
        db.bills.c.service_end,
    ).where(and_(any_line_items > 0, variable_lines == 0))
    if account_id is not None:
        stmt = stmt.where(db.bills.c.account_id == account_id)

    with engine.connect() as conn:
        rows = conn.execute(stmt.order_by(db.bills.c.service_start)).mappings().all()
    return [{**dict(row), "problem": "line items but no per-kWh delivery"} for row in rows]


def _local_day(ts: datetime.datetime) -> datetime.date:
    """The site-local date of an interval start; a naive value is UTC."""
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=datetime.timezone.utc)
    return ts.astimezone(SITE_TZ).date()


def _metered_days(
    engine: Engine, metrics: tuple[str, ...], first_day: datetime.date | None = None
) -> dict[str, set[datetime.date]]:
    """Return {metric: local days holding at least one 15-minute reading}.

    Days are bucketed here rather than in SQL so the same code runs on both
    backends. first_day bounds the read; it is padded by a day because a local
    day starts before the same UTC date does.
    """
    stmt = select(db.usage_intervals.c.metric, db.usage_intervals.c.ts).where(
        db.usage_intervals.c.granularity == INTERVAL_GRANULARITY,
        db.usage_intervals.c.metric.in_(metrics),
    )
    if first_day is not None:
        padded = datetime.datetime.combine(
            first_day - datetime.timedelta(days=1), datetime.time(), tzinfo=datetime.timezone.utc
        )
        stmt = stmt.where(db.usage_intervals.c.ts >= padded)

    days: dict[str, set[datetime.date]] = {metric: set() for metric in metrics}
    with engine.connect() as conn:
        for metric, ts in conn.execute(stmt):
            days[metric].add(_local_day(ts))
    return days


def _day_ranges(days: list[datetime.date]) -> list[tuple[datetime.date, datetime.date]]:
    """Collapse a sorted date list into [(start, end), ...] inclusive runs."""
    ranges: list[tuple[datetime.date, datetime.date]] = []
    for day in days:
        if ranges and day == ranges[-1][1] + datetime.timedelta(days=1):
            ranges[-1] = (ranges[-1][0], day)
        else:
            ranges.append((day, day))
    return ranges


def metered_days_without_bill(
    engine: Engine, account_id: str | None, today: datetime.date
) -> list[dict[str, Any]]:
    """Days the smart meter recorded that no bill's service period covers.

    Bills are matched across every account, whatever account_id says: the
    meter's series stays under one account while each retail provider bills
    under its own, and the panels do not filter by account either. A gap
    between two bills is always reported. Days after the last billed day are
    reported only once the oldest is more than TRAILING_UNBILLED_DAYS back.
    """
    metered = _metered_days(engine, METERED_METRICS)
    days = sorted(set().union(*metered.values()))
    stmt = select(db.bills.c.service_start, db.bills.c.service_end).where(
        db.bills.c.service_start.isnot(None), db.bills.c.service_end.isnot(None)
    )
    with engine.connect() as conn:
        periods = [(row[0], row[1]) for row in conn.execute(stmt)]
    if not days or not periods:
        return []

    last_billed = max(end for _, end in periods)
    uncovered = [day for day in days if not any(start <= day <= end for start, end in periods)]
    findings: list[dict[str, Any]] = []
    for start, end in _day_ranges(uncovered):
        if start > last_billed and (today - start).days <= TRAILING_UNBILLED_DAYS:
            continue
        findings.append(
            {
                "account_id": None,
                "invoice_number": NO_BILL,
                "service_start": start,
                "service_end": end,
                "problem": "metered days no bill covers",
            }
        )
    return findings


def recent_bills_partly_metered(
    engine: Engine, account_id: str | None, today: datetime.date
) -> list[dict[str, Any]]:
    """Recent bills whose service period is not fully covered by interval data.

    The valuation sums whatever 15-minute rows fall inside the period, so a
    series that stops partway is valued on the days held and reads as a poor
    month. Only bills that ended within RECENT_BILL_DAYS are checked.
    """
    horizon = today - datetime.timedelta(days=RECENT_BILL_DAYS)
    stmt = select(
        db.bills.c.account_id,
        db.bills.c.invoice_number,
        db.bills.c.service_start,
        db.bills.c.service_end,
    ).where(
        db.bills.c.service_start.isnot(None),
        db.bills.c.service_end >= horizon,
    )
    if account_id is not None:
        stmt = stmt.where(db.bills.c.account_id == account_id)
    with engine.connect() as conn:
        bills = [dict(row) for row in conn.execute(stmt.order_by(db.bills.c.service_start)).mappings()]
    if not bills:
        return []

    metered = _metered_days(engine, VALUED_METRICS, min(bill["service_start"] for bill in bills))
    findings: list[dict[str, Any]] = []
    for bill in bills:
        period_days = (bill["service_end"] - bill["service_start"]).days + 1
        for metric in VALUED_METRICS:
            held = sum(1 for day in metered[metric] if bill["service_start"] <= day <= bill["service_end"])
            if held < period_days:
                findings.append({**bill, "problem": f"{metric}: {held} of {period_days} days metered"})
    return findings


CHECKS = (
    ("bills_without_line_items", bills_without_line_items),
    ("bills_without_energy_rate", bills_without_energy_rate),
    ("bills_without_usable_kwh", bills_without_usable_kwh),
    ("bills_without_variable_delivery", bills_without_variable_delivery),
)

# Checks that compare against the calendar take the day to measure from.
DATED_CHECKS = (
    ("metered_days_without_bill", metered_days_without_bill),
    ("recent_bills_partly_metered", recent_bills_partly_metered),
)


def run_checks(
    engine: Engine, account_id: str | None = None, today: datetime.date | None = None
) -> dict[str, list[dict[str, Any]]]:
    """Run every check; returns {check name: findings}."""
    if today is None:
        today = datetime.date.today()
    results = {name: check(engine, account_id) for name, check in CHECKS}
    results.update({name: check(engine, account_id, today) for name, check in DATED_CHECKS})
    return results


# %%
# Reporting #


def _fmt(value: Any) -> str:
    """Render a date or None for the text report."""
    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.isoformat()[:10]
    return "—" if value is None else str(value)


def _print_report(results: dict[str, list[dict[str, Any]]]) -> None:
    """Print a human-readable report of everything found."""
    total = sum(len(findings) for findings in results.values())
    if not total:
        print("No unvaluable billing periods. Dashboard value panels cover every bill.")
        return

    print(f"{total} period(s) will be MISSING from dashboard value panels, or understated by them.\n")
    print("These do not error anywhere. The panels simply draw a shorter line and a")
    print("smaller total, which reads as 'solar was worth less' rather than 'data is missing'.\n")
    for name, findings in results.items():
        if not findings:
            continue
        print(f"  {name}  ({len(findings)})")
        for row in findings:
            print(
                f"    {row['invoice_number']:<16} "
                f"{_fmt(row['service_start'])} .. {_fmt(row['service_end'])}   {row['problem']}"
            )
    print("\nFix by reprocessing the affected bills, not by editing rows:")
    print("  uv run python src/parse_raw.py --provider <slug> --status all")
    print("A day or a period that is not metered or not billed needs its capture run instead.")


# %%
# CLI #


def build_arg_parser() -> argparse.ArgumentParser:
    """Build the checks CLI argument parser."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--provider", default=None, help="provider slug; resolves account via providers.local.yaml")
    parser.add_argument("--account", default=None, help="account id, bypassing providers.local.yaml")
    parser.add_argument("--json", action="store_true", help="emit JSON instead of a text report")
    return parser


def main(argv: list[str] | None = None) -> int:
    """Entry point; returns 1 when any check finds something, else 0."""
    args = build_arg_parser().parse_args(argv)

    account_id = args.account
    if account_id is None and args.provider:
        account_id = account_id_for_provider(args.provider, _load_providers_config(PROVIDERS_YAML_PATH))

    engine = db.get_engine()
    bootstrap.ensure_schema(engine)
    results = run_checks(engine, account_id)

    if args.json:
        print(json.dumps(results, indent=2, default=str))
    else:
        _print_report(results)

    return 1 if any(results.values()) else 0


if __name__ == "__main__":
    raise SystemExit(main())


# %%
