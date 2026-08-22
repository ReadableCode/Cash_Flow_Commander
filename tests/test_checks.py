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


def _add_bill(eng: Any, invoice: str, *, total_kwh: float | None = 900.0) -> None:
    """Insert one bill."""
    with eng.begin() as conn:
        conn.execute(
            db.bills.insert(),
            [{
                "account_id": ACCOUNT,
                "invoice_number": invoice,
                "service_start": dt.date(2026, 6, 1),
                "service_end": dt.date(2026, 6, 30),
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


def _problems(eng: Any) -> set[str]:
    """Every problem string the checks report."""
    return {row["problem"] for findings in checks.run_checks(eng, ACCOUNT).values() for row in findings}


# %%
# The failure the checks exist to catch #


def test_a_fully_parsed_bill_is_clean(engine: Any) -> None:
    """The healthy case: line items present, with an energy rate."""
    _add_bill(engine, "INV-OK")
    _add_line_item(engine, "INV-OK", category="energy", rate=9.4)

    assert _problems(engine) == set()


def test_a_bill_with_no_line_items_is_reported(engine: Any) -> None:
    """This is the one that silently shortens the solar value series."""
    _add_bill(engine, "INV-NOLINES")

    findings = checks.run_checks(engine, ACCOUNT)["bills_without_line_items"]

    assert [row["invoice_number"] for row in findings] == ["INV-NOLINES"]


def test_line_items_without_an_energy_rate_are_reported(engine: Any) -> None:
    """Line items can parse while the rate line does not — the panel drops it either way."""
    _add_bill(engine, "INV-NORATE")
    _add_line_item(engine, "INV-NORATE", category="delivery", rate=None)

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
    _add_bill(engine, "INV-OK")
    _add_line_item(engine, "INV-OK", category="energy", rate=9.4)

    assert checks.main(["--account", ACCOUNT]) == 0


# %%
