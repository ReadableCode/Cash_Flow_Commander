# %%
# Imports #

import datetime as dt
import importlib
import os
import sys
from typing import Any

import pytest

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if os.path.join(_ROOT, "src") not in sys.path:
    sys.path.insert(0, os.path.join(_ROOT, "src"))

import checks  # noqa: E402
import db  # noqa: E402

# %%
# Fixtures #

ACCOUNT = "ACCT-CHECKS"
PARSER = "test-1"


def _reload_modules() -> None:
    """Reload db and the modules holding references to its Table objects."""
    importlib.reload(db)
    importlib.reload(checks)


def _restore_env(name: str, value: str | None) -> None:
    """Put an environment variable back the way it was."""
    if value is None:
        os.environ.pop(name, None)
    else:
        os.environ[name] = value


@pytest.fixture()
def engine(tmp_path: Any) -> Any:
    """Engine on a throwaway SQLite file, with CFC_DB_SCHEMA forced off.

    Set to "" rather than popped so db's load_dotenv(override=False) cannot
    re-populate it from a developer's real .env on reload.
    """
    saved_url = os.environ.get("CFC_DATABASE_URL")
    saved_schema = os.environ.get("CFC_DB_SCHEMA")
    os.environ["CFC_DATABASE_URL"] = f"sqlite:///{tmp_path / 'checks_test.db'}"
    os.environ["CFC_DB_SCHEMA"] = ""
    try:
        _reload_modules()
        eng = db.get_engine()
        db.create_tables(eng)
        yield eng
    finally:
        _restore_env("CFC_DATABASE_URL", saved_url)
        _restore_env("CFC_DB_SCHEMA", saved_schema)
        _reload_modules()


def _add_bill(
    eng: Any,
    invoice: str,
    *,
    total_kwh: float | None = 900.0,
    account: str = ACCOUNT,
    start: dt.date = dt.date(2026, 6, 1),
    end: dt.date = dt.date(2026, 6, 30),
) -> None:
    """Insert one bill."""
    with eng.begin() as conn:
        conn.execute(
            db.bills.insert(),
            [{
                "account_id": account,
                "invoice_number": invoice,
                "service_start": start,
                "service_end": end,
                "total_kwh": total_kwh,
                "total_current_charges": 145.00,
                "parser_version": PARSER,
            }],
        )


def _add_line_item(eng: Any, invoice: str, *, category: str, rate: float | None, line_no: int = 1) -> None:
    """Insert one bill line item."""
    with eng.begin() as conn:
        conn.execute(
            db.bill_line_items.insert(),
            [{
                "account_id": ACCOUNT,
                "invoice_number": invoice,
                "line_no": line_no,
                "section": "energy",
                "category": category,
                "description": "Energy Charge",
                "rate_cents_kwh": rate,
                "amount": 100.00,
                "parser_version": PARSER,
            }],
        )


def _add_healthy_bill(eng: Any, invoice: str, **bill: Any) -> None:
    """Insert a bill the valuation can price: an energy rate and per-kWh delivery."""
    _add_bill(eng, invoice, **bill)
    _add_line_item(eng, invoice, category="energy", rate=9.4)
    _add_line_item(eng, invoice, category="delivery_variable", rate=None, line_no=2)


def _add_metered_days(eng: Any, metric: str, first: dt.date, last: dt.date, account: str = "ACCT-METER") -> None:
    """Insert one 15-minute reading at local noon on every day from first to last."""
    rows = []
    day = first
    while day <= last:
        noon = dt.datetime.combine(day, dt.time(12), tzinfo=checks.SITE_TZ)
        rows.append({
            "account_id": account,
            "ts": noon.astimezone(dt.timezone.utc),
            "granularity": "15min",
            "metric": metric,
            "value": 1.0,
            "parser_version": PARSER,
        })
        day += dt.timedelta(days=1)
    with eng.begin() as conn:
        conn.execute(db.usage_intervals.insert(), rows)


def _problems(eng: Any) -> set[str]:
    """Every problem string the checks report."""
    return {row["problem"] for findings in checks.run_checks(eng, ACCOUNT).values() for row in findings}


# %%
# The failure the checks exist to catch #


def test_a_fully_parsed_bill_is_clean(engine: Any) -> None:
    """The healthy case: line items present, with an energy rate and per-kWh delivery."""
    _add_healthy_bill(engine, "INV-OK")

    assert _problems(engine) == set()


def test_a_bill_with_no_line_items_is_reported(engine: Any) -> None:
    """This is the one that silently shortens the solar value series."""
    _add_bill(engine, "INV-NOLINES")

    findings = checks.run_checks(engine, ACCOUNT)["bills_without_line_items"]

    assert [row["invoice_number"] for row in findings] == ["INV-NOLINES"]


def test_line_items_without_an_energy_rate_are_reported(engine: Any) -> None:
    """Line items can parse while the rate line does not — the panel drops it either way."""
    _add_bill(engine, "INV-NORATE")
    _add_line_item(engine, "INV-NORATE", category="delivery_variable", rate=None)

    findings = checks.run_checks(engine, ACCOUNT)["bills_without_energy_rate"]

    assert [row["invoice_number"] for row in findings] == ["INV-NORATE"]


def test_a_bill_without_positive_kwh_is_reported(engine: Any) -> None:
    """total_kwh is both a filter and a divisor in the marginal-rate arithmetic."""
    _add_bill(engine, "INV-NOKWH", total_kwh=None)
    _add_line_item(engine, "INV-NOKWH", category="energy", rate=9.4)

    findings = checks.run_checks(engine, ACCOUNT)["bills_without_usable_kwh"]

    assert [row["invoice_number"] for row in findings] == ["INV-NOKWH"]


def test_the_two_line_item_checks_do_not_double_report(engine: Any) -> None:
    """A bill with no line items is not also 'has line items but no rate'."""
    _add_bill(engine, "INV-NOLINES")

    results = checks.run_checks(engine, ACCOUNT)

    assert len(results["bills_without_line_items"]) == 1
    assert results["bills_without_energy_rate"] == []


def test_exit_code_is_nonzero_when_something_is_found(engine: Any, capsys: Any) -> None:
    """A provider command must not be able to finish green while panels understate."""
    _add_bill(engine, "INV-NOLINES")

    assert checks.main(["--account", ACCOUNT]) == 1
    assert "MISSING from dashboard value panels" in capsys.readouterr().out


def test_exit_code_is_zero_when_clean(engine: Any) -> None:
    """And must not cry wolf when everything parsed."""
    _add_healthy_bill(engine, "INV-OK")

    assert checks.main(["--account", ACCOUNT]) == 0


# %%
# Delivery priced at zero #


def test_a_bill_with_no_per_kwh_delivery_is_reported(engine: Any) -> None:
    """A lump sum or a reworded line the parser did not split: delivery reads as free."""
    _add_bill(engine, "INV-LUMP")
    _add_line_item(engine, "INV-LUMP", category="energy", rate=9.4)
    _add_line_item(engine, "INV-LUMP", category="other", rate=None, line_no=2)

    findings = checks.run_checks(engine, ACCOUNT)["bills_without_variable_delivery"]

    assert [row["invoice_number"] for row in findings] == ["INV-LUMP"]


# %%
# Metered days no bill covers #

TODAY = dt.date(2026, 9, 28)


def _unbilled(eng: Any, today: dt.date = TODAY) -> list[tuple[dt.date, dt.date]]:
    """The (first, last) day of every uncovered range reported."""
    findings = checks.run_checks(eng, ACCOUNT, today=today)["metered_days_without_bill"]
    return [(row["service_start"], row["service_end"]) for row in findings]


def test_days_after_the_last_bill_are_normal_for_a_while(engine: Any) -> None:
    """The meter publishes in two days and a bill arrives a cycle later."""
    _add_healthy_bill(engine, "INV-AUG", start=dt.date(2026, 8, 1), end=dt.date(2026, 8, 31))
    _add_metered_days(engine, "consumption", dt.date(2026, 8, 1), dt.date(2026, 9, 26))

    assert _unbilled(engine) == []


def test_days_long_after_the_last_bill_are_reported(engine: Any) -> None:
    """A retail provider whose bills never landed: the series reads as solar stopping."""
    _add_healthy_bill(engine, "INV-JUL", start=dt.date(2026, 7, 1), end=dt.date(2026, 7, 31))
    _add_metered_days(engine, "consumption", dt.date(2026, 7, 1), dt.date(2026, 9, 26))

    assert _unbilled(engine) == [(dt.date(2026, 8, 1), dt.date(2026, 9, 26))]


def test_a_gap_between_two_bills_is_always_reported(engine: Any) -> None:
    _add_healthy_bill(engine, "INV-A", start=dt.date(2026, 8, 1), end=dt.date(2026, 8, 27))
    _add_healthy_bill(engine, "INV-B", start=dt.date(2026, 9, 1), end=dt.date(2026, 9, 20))
    _add_metered_days(engine, "generation", dt.date(2026, 8, 1), dt.date(2026, 9, 20))

    assert _unbilled(engine) == [(dt.date(2026, 8, 28), dt.date(2026, 8, 31))]


def test_a_bill_under_another_account_covers_the_meter(engine: Any) -> None:
    """The meter stays under one account; each retail provider bills under its own."""
    _add_healthy_bill(engine, "INV-OLD", start=dt.date(2026, 7, 1), end=dt.date(2026, 7, 31))
    _add_healthy_bill(
        engine, "INV-NEW", account="ACCT-NEW-RETAILER", start=dt.date(2026, 8, 1), end=dt.date(2026, 8, 31)
    )
    _add_metered_days(engine, "consumption", dt.date(2026, 7, 1), dt.date(2026, 8, 31))

    assert _unbilled(engine, today=dt.date(2026, 12, 1)) == []


def test_production_alone_is_not_a_metered_day(engine: Any) -> None:
    """Days holding only solar production cannot be valued, billed or not."""
    _add_healthy_bill(engine, "INV-AUG", start=dt.date(2026, 8, 1), end=dt.date(2026, 8, 31))
    _add_metered_days(engine, "production", dt.date(2026, 6, 1), dt.date(2026, 8, 31))

    assert _unbilled(engine, today=dt.date(2026, 12, 1)) == []


# %%
# A billed period the interval data stops partway through #


def _partly_metered(eng: Any, today: dt.date = TODAY) -> set[str]:
    findings = checks.run_checks(eng, ACCOUNT, today=today)["recent_bills_partly_metered"]
    return {row["problem"] for row in findings}


def test_a_fully_metered_recent_bill_is_clean(engine: Any) -> None:
    _add_healthy_bill(engine, "INV-SEP", start=dt.date(2026, 9, 4), end=dt.date(2026, 9, 24))
    for metric in checks.VALUED_METRICS:
        _add_metered_days(engine, metric, dt.date(2026, 9, 1), dt.date(2026, 9, 26))

    assert _partly_metered(engine) == set()


def test_a_series_that_stops_partway_is_reported(engine: Any) -> None:
    """The valuation would sum the days held and read as a poor month."""
    _add_healthy_bill(engine, "INV-SEP", start=dt.date(2026, 9, 4), end=dt.date(2026, 9, 24))
    _add_metered_days(engine, "consumption", dt.date(2026, 9, 1), dt.date(2026, 9, 15))
    _add_metered_days(engine, "generation", dt.date(2026, 9, 1), dt.date(2026, 9, 26))

    assert _partly_metered(engine) == {
        "production: 0 of 21 days metered",
        "consumption: 12 of 21 days metered",
    }


def test_an_old_bill_is_not_chased(engine: Any) -> None:
    """A hole older than the horizon is a fact about the source, not a missed run."""
    _add_healthy_bill(engine, "INV-JUN")

    assert _partly_metered(engine) == set()


# %%
