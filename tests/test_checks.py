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


def _add_line_item(
    eng: Any, invoice: str, *, category: str, rate: float | None, line_no: int = 1, account: str = ACCOUNT
) -> None:
    """Insert one bill line item."""
    with eng.begin() as conn:
        conn.execute(
            db.bill_line_items.insert(),
            [{
                "account_id": account,
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
    account = bill.get("account", ACCOUNT)
    _add_line_item(eng, invoice, category="energy", rate=9.4, account=account)
    _add_line_item(eng, invoice, category="delivery_variable", rate=None, line_no=2, account=account)


def _add_metered_days(
    eng: Any, metric: str, first: dt.date, last: dt.date, account: str = "ACCT-METER", value: float = 1.0
) -> None:
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
            "value": value,
            "parser_version": PARSER,
        })
        day += dt.timedelta(days=1)
    with eng.begin() as conn:
        conn.execute(db.usage_intervals.insert(), rows)


def _add_plan(
    eng: Any,
    start: dt.date,
    end: dt.date | None,
    *,
    buyback: float | None,
    account: str = ACCOUNT,
) -> None:
    """Insert one plan term; buyback None is a plan that pays none."""
    with eng.begin() as conn:
        conn.execute(
            db.plans.insert(),
            [{
                "account_id": account,
                "start_date": start,
                "end_date": end,
                "plan_name": "Synthetic Plan",
                "buyback_rate_cents_kwh": buyback,
                "parser_version": PARSER,
            }],
        )


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
# A buyback line that disagrees with the plan #

JUNE = (dt.date(2026, 6, 1), dt.date(2026, 6, 30))
NO_PLAN_BUYBACK = "buyback line on a plan that pays no buyback"
MISSING_BUYBACK = "plan pays buyback and the meter shows export, but the bill has no buyback line"


def _add_buyback_line(eng: Any, invoice: str, account: str = ACCOUNT) -> None:
    _add_line_item(eng, invoice, category="solar_buyback", rate=5.0, line_no=3, account=account)


def _buyback(eng: Any) -> list[tuple[str, str]]:
    """The (invoice, problem) of every buyback mismatch reported."""
    findings = checks.run_checks(eng, ACCOUNT)["bills_buyback_mismatch"]
    return [(row["invoice_number"], row["problem"]) for row in findings]


def test_a_buyback_line_on_a_plan_that_pays_buyback_is_clean(engine: Any) -> None:
    _add_healthy_bill(engine, "INV-JUN")
    _add_buyback_line(engine, "INV-JUN")
    _add_plan(engine, dt.date(2026, 6, 1), dt.date(2026, 7, 1), buyback=5.0)
    _add_metered_days(engine, "generation", *JUNE)

    assert _buyback(engine) == []
    assert _problems(engine) == set()


def test_a_buyback_line_on_a_plan_that_pays_none_is_reported(engine: Any) -> None:
    """A line the parser filed under buyback that the plan cannot have paid."""
    _add_healthy_bill(engine, "INV-JUN")
    _add_buyback_line(engine, "INV-JUN")
    _add_plan(engine, dt.date(2026, 6, 1), dt.date(2026, 7, 1), buyback=None)

    findings = checks.run_checks(engine, ACCOUNT)["bills_buyback_mismatch"]

    assert findings == [{
        "account_id": ACCOUNT,
        "invoice_number": "INV-JUN",
        "service_start": JUNE[0],
        "service_end": JUNE[1],
        "problem": NO_PLAN_BUYBACK,
    }]


def test_a_missing_buyback_line_is_reported_when_the_meter_shows_export(engine: Any) -> None:
    """The credit that failed to parse: the panels would read it as a buyback of zero."""
    _add_healthy_bill(engine, "INV-JUN")
    _add_plan(engine, dt.date(2026, 6, 1), dt.date(2026, 7, 1), buyback=5.0)
    _add_metered_days(engine, "generation", *JUNE, account=ACCOUNT)

    assert _buyback(engine) == [("INV-JUN", MISSING_BUYBACK)]


def test_a_missing_buyback_line_is_fine_when_nothing_was_exported(engine: Any) -> None:
    """No export, no credit: consumption, zero readings and other months do not count."""
    _add_healthy_bill(engine, "INV-JUN")
    _add_plan(engine, dt.date(2026, 6, 1), dt.date(2026, 7, 1), buyback=5.0)
    _add_metered_days(engine, "consumption", *JUNE)
    _add_metered_days(engine, "generation", *JUNE, value=0.0)
    _add_metered_days(engine, "generation", dt.date(2026, 5, 1), dt.date(2026, 5, 31), account="ACCT-MAY")
    _add_metered_days(engine, "generation", dt.date(2026, 7, 1), dt.date(2026, 7, 31), account="ACCT-JUL")

    assert _buyback(engine) == []


def test_export_under_another_account_still_counts(engine: Any) -> None:
    """The meter's series is not filed under the retail provider's account."""
    _add_healthy_bill(engine, "INV-JUN")
    _add_plan(engine, dt.date(2026, 6, 1), dt.date(2026, 7, 1), buyback=5.0)
    _add_metered_days(engine, "generation", dt.date(2026, 6, 15), dt.date(2026, 6, 15), account="ACCT-THE-METER")

    assert _buyback(engine) == [("INV-JUN", MISSING_BUYBACK)]


def _add_hourly_export(eng: Any, day: dt.date, hour: int) -> None:
    """Insert one hourly generation reading starting at a site-local hour."""
    local = dt.datetime.combine(day, dt.time(hour), tzinfo=checks.SITE_TZ)
    with eng.begin() as conn:
        conn.execute(
            db.usage_intervals.insert(),
            [{
                "account_id": "ACCT-METER",
                "ts": local.astimezone(dt.timezone.utc),
                "granularity": "hour",
                "metric": "generation",
                "value": 0.5,
                "parser_version": PARSER,
            }],
        )


def test_export_at_any_granularity_counts(engine: Any) -> None:
    _add_hourly_export(engine, dt.date(2026, 6, 15), 12)
    _add_healthy_bill(engine, "INV-JUN")
    _add_plan(engine, dt.date(2026, 6, 1), dt.date(2026, 7, 1), buyback=5.0)

    assert _buyback(engine) == [("INV-JUN", MISSING_BUYBACK)]


def test_export_is_placed_on_its_site_local_day(engine: Any) -> None:
    """Late evening is already the next UTC date; the service period is local dates."""
    _add_healthy_bill(engine, "INV-JUN")
    _add_plan(engine, dt.date(2026, 6, 1), dt.date(2026, 7, 1), buyback=5.0)

    _add_hourly_export(engine, dt.date(2026, 5, 31), 23)
    _add_hourly_export(engine, dt.date(2026, 7, 1), 0)
    assert _buyback(engine) == []

    _add_hourly_export(engine, dt.date(2026, 6, 30), 23)
    assert _buyback(engine) == [("INV-JUN", MISSING_BUYBACK)]


def test_a_bill_with_no_recorded_plan_term_is_not_judged(engine: Any) -> None:
    """The rule is scoped to bills whose plan is recorded, in either direction."""
    _add_healthy_bill(engine, "INV-LINE")
    _add_buyback_line(engine, "INV-LINE")
    _add_healthy_bill(engine, "INV-NOLINE", start=dt.date(2026, 7, 1), end=dt.date(2026, 7, 31))
    _add_metered_days(engine, "generation", dt.date(2026, 6, 1), dt.date(2026, 7, 31))
    # A term that ended the day the first bill started, one that starts after
    # the last bill ended, and another account's term over both.
    _add_plan(engine, dt.date(2026, 5, 1), dt.date(2026, 6, 1), buyback=None)
    _add_plan(engine, dt.date(2026, 8, 1), None, buyback=5.0)
    _add_plan(engine, dt.date(2026, 1, 1), None, buyback=None, account="ACCT-OTHER")

    assert _buyback(engine) == []


def test_an_open_ended_term_covers_every_later_bill(engine: Any) -> None:
    _add_healthy_bill(engine, "INV-JUN")
    _add_buyback_line(engine, "INV-JUN")
    _add_plan(engine, dt.date(2026, 1, 1), None, buyback=None)

    assert _buyback(engine) == [("INV-JUN", NO_PLAN_BUYBACK)]


def test_a_straddling_bill_is_clean_when_one_of_its_terms_pays_buyback(engine: Any) -> None:
    """A renewal mid-period: the buyback line belongs to the term that pays it."""
    _add_healthy_bill(engine, "INV-JUN")
    _add_buyback_line(engine, "INV-JUN")
    _add_plan(engine, dt.date(2026, 5, 15), dt.date(2026, 6, 15), buyback=5.0)
    _add_plan(engine, dt.date(2026, 6, 15), dt.date(2026, 7, 15), buyback=None)
    _add_metered_days(engine, "generation", *JUNE)

    assert _buyback(engine) == []


def test_a_buyback_mismatch_fails_the_run(engine: Any, capsys: Any) -> None:
    _add_healthy_bill(engine, "INV-JUN")
    _add_buyback_line(engine, "INV-JUN")
    _add_plan(engine, dt.date(2026, 6, 1), dt.date(2026, 7, 1), buyback=None)

    assert checks.main(["--account", ACCOUNT]) == 1
    assert NO_PLAN_BUYBACK in capsys.readouterr().out


# %%
# Two accounts that share an invoice number #

OTHER_ACCOUNT = "ACCT-CHECKS-OTHER"


def test_line_items_on_another_accounts_bill_do_not_count(engine: Any) -> None:
    """An invoice number is unique within one provider; a bill is (account, invoice)."""
    _add_healthy_bill(engine, "INV-SHARED")
    _add_bill(engine, "INV-SHARED", account=OTHER_ACCOUNT)

    findings = checks.run_checks(engine)["bills_without_line_items"]

    assert [(row["account_id"], row["invoice_number"]) for row in findings] == [(OTHER_ACCOUNT, "INV-SHARED")]


def test_a_buyback_line_on_another_accounts_bill_does_not_count(engine: Any) -> None:
    """The other bill's credit must not hide this bill's missing one."""
    for account in (ACCOUNT, OTHER_ACCOUNT):
        _add_healthy_bill(engine, "INV-SHARED", account=account)
        _add_plan(engine, dt.date(2026, 6, 1), dt.date(2026, 7, 1), buyback=5.0, account=account)
    _add_buyback_line(engine, "INV-SHARED")
    _add_metered_days(engine, "generation", *JUNE)

    findings = checks.run_checks(engine)["bills_buyback_mismatch"]

    assert [(row["account_id"], row["problem"]) for row in findings] == [(OTHER_ACCOUNT, MISSING_BUYBACK)]


# %%
