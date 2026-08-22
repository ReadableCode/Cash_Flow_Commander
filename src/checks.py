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

import yaml
from sqlalchemy import and_, func, select
from sqlalchemy.engine import Engine

_SRC_DIR = os.path.dirname(os.path.abspath(__file__))
if _SRC_DIR not in sys.path:
    sys.path.insert(0, _SRC_DIR)

import db  # noqa: E402

# %%
# Constants #

_REPO_ROOT = os.path.dirname(_SRC_DIR)
PROVIDERS_YAML_PATH = os.path.join(_REPO_ROOT, "providers.local.yaml")

# Line-item category that carries the marginal energy rate the valuation needs.
ENERGY_CATEGORY = "energy"


# %%
# Config #


def _load_providers_config(path: str) -> dict[str, Any]:
    """Load providers.local.yaml; {} when missing or unreadable."""
    if not os.path.isfile(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as handle:
            loaded = yaml.safe_load(handle)
    except (OSError, yaml.YAMLError):
        return {}
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


CHECKS = (
    ("bills_without_line_items", bills_without_line_items),
    ("bills_without_energy_rate", bills_without_energy_rate),
    ("bills_without_usable_kwh", bills_without_usable_kwh),
)


def run_checks(engine: Engine, account_id: str | None = None) -> dict[str, list[dict[str, Any]]]:
    """Run every check; returns {check name: findings}."""
    return {name: check(engine, account_id) for name, check in CHECKS}


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

    print(f"{total} billing period(s) will be MISSING from dashboard value panels.\n")
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
    results = run_checks(engine, account_id)

    if args.json:
        print(json.dumps(results, indent=2, default=str))
    else:
        _print_report(results)

    return 1 if any(results.values()) else 0


if __name__ == "__main__":
    raise SystemExit(main())


# %%
