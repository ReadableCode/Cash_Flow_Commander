# %%
# Imports #

import datetime as dt
import importlib
import os
import sys
from typing import Any

import pandas as pd
import pytest
from sqlalchemy import insert

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if os.path.join(_ROOT, "src") not in sys.path:
    sys.path.insert(0, os.path.join(_ROOT, "src"))

import db  # noqa: E402
import expected_import_sheet  # noqa: E402
import expected_store  # noqa: E402

# %%
# Fixtures #

CONFIG = {
    "account_aliases": {"Chase Bank": "1234", "Chase Sapphire": "5678"},
    "transfer_accounts": {"Chase Sapphire Card": "5678"},
}


def _restore_env(name: str, value: str | None) -> None:
    if value is None:
        os.environ.pop(name, None)
    else:
        os.environ[name] = value


@pytest.fixture()
def engine(tmp_path: Any) -> Any:
    saved_url = os.environ.get("CFC_DATABASE_URL")
    saved_schema = os.environ.get("CFC_DB_SCHEMA")
    os.environ["CFC_DATABASE_URL"] = f"sqlite:///{tmp_path / 'import_test.db'}"
    os.environ["CFC_DB_SCHEMA"] = ""
    try:
        importlib.reload(db)
        eng = db.get_engine()
        db.create_tables(eng)
        yield eng
    finally:
        _restore_env("CFC_DATABASE_URL", saved_url)
        _restore_env("CFC_DB_SCHEMA", saved_schema)
        importlib.reload(db)


def add_transaction(engine, account_id, post_date, description, amount, occurrence=0):
    with engine.begin() as conn:
        conn.execute(
            insert(db.transactions).values(
                account_id=account_id,
                account_kind="bank",
                txn_date=post_date,
                post_date=post_date,
                description=description,
                amount=amount,
                occurrence=occurrence,
                parser_version="test-1",
            )
        )


def income_expense_row(**overrides):
    row = {
        "Account_Name": "Mortgage",
        "Category": "Debt",
        "Sub_Category": "House",
        "Type": "monthly",
        "When": "6",
        "Amount": -2294.00,
        "Auto_Pay_Account": "Chase Bank",
        "AfterDays": 1,
        "Maturity Date": "12/12/2100",
    }
    row.update(overrides)
    return row


def report_row(**overrides):
    row = {
        "Date": dt.date(2026, 1, 6),
        "Category": "Debt",
        "Type": "monthly",
        "Account_Name": "Mortgage",
        "Auto_Pay_Account": "Chase Bank",
        "Amount": -2294.00,
        "Amount_Paid": 0.0,
        "Date_Paid": "",
    }
    row.update(overrides)
    return row


# %%
# Series building #


def test_schedules_parse_from_sheet_columns():
    monthly = expected_import_sheet._parse_schedule("monthly", pd.Series({"When": "6"}))
    assert monthly == {"schedule_type": "monthly", "day_of_month": 6}

    yearly = expected_import_sheet._parse_schedule(
        "yearly", pd.Series({"When": "13-Jul"})
    )
    assert yearly == {"schedule_type": "yearly", "month_of_year": 7, "day_of_month": 13}

    biweekly = expected_import_sheet._parse_schedule(
        "biweekly", pd.Series({"When": "5/28/2021"})
    )
    assert biweekly == {
        "schedule_type": "biweekly",
        "anchor_date": dt.date(2021, 5, 28),
    }


def test_oncely_rows_sharing_a_name_become_one_series():
    df_income_expense = pd.DataFrame(
        [
            income_expense_row(
                Account_Name="Alta Rodent Payment",
                Type="oncely",
                When="2/9/2024",
                Amount=-554.0,
            ),
            income_expense_row(
                Account_Name="Alta Rodent Payment",
                Type="oncely",
                When="3/9/2024",
                Amount=-554.0,
            ),
        ]
    )
    series_rows, once_rows = expected_import_sheet.build_series_rows(
        df_income_expense, pd.DataFrame(columns=["Account_Name", "Date"]), CONFIG
    )
    assert len(series_rows) == 1
    assert series_rows[0]["schedule_type"] == "once"
    assert [row["due_date"] for row in once_rows] == [
        dt.date(2024, 2, 9),
        dt.date(2024, 3, 9),
    ]


def test_report_only_names_become_closed_series():
    df_income_expense = pd.DataFrame([income_expense_row()])
    df_report = pd.DataFrame(
        [
            report_row(
                Account_Name="Home Team Pest Control",
                Category="Expense",
                Date=dt.date(2024, 3, 13),
            ),
            report_row(
                Account_Name="Home Team Pest Control",
                Category="Expense",
                Date=dt.date(2024, 6, 13),
            ),
        ]
    )
    closed = expected_import_sheet.build_closed_series_rows(
        df_income_expense, df_report, CONFIG
    )
    assert len(closed) == 1
    assert closed[0]["name"] == "Home Team Pest Control"
    assert closed[0]["active_from"] == dt.date(2024, 3, 13)
    assert closed[0]["active_until"] == dt.date(2024, 6, 13)


def test_maturity_dates():
    assert expected_import_sheet._parse_maturity("12/12/2100") is None
    assert expected_import_sheet._parse_maturity("2/15/2026") == dt.date(2026, 2, 15)


# %%
# Match recovery #


def setup_mortgage(engine):
    series_id = expected_store.add_series(
        engine,
        {
            "name": "Mortgage",
            "category": "Debt",
            "schedule_type": "monthly",
            "day_of_month": 6,
            "amount": -2294.00,
            "auto_pay_account_id": "1234",
            "active_from": dt.date(2024, 1, 1),
        },
    )
    expected_store.add_occurrence(
        engine, series_id, dt.date(2026, 1, 6), -2294.00, source="import_sheet"
    )
    return {"Mortgage": series_id}


def test_unique_hit_becomes_a_match(engine):
    series_ids = setup_mortgage(engine)
    add_transaction(engine, "1234", dt.date(2026, 1, 7), "MORTGAGE CO", -2294.13)
    df_report = pd.DataFrame([report_row(Amount_Paid=-2294.13, Date_Paid="2026-01-07")])
    df_outcomes = expected_import_sheet.recover_matches(
        engine, df_report, series_ids, CONFIG
    )
    assert list(df_outcomes["result"]) == ["matched"]
    row = expected_store.get_occurrence_status_df(
        engine, dt.date(2026, 1, 6), dt.date(2026, 1, 6)
    ).iloc[0]
    assert row["status"] == "paid"


def test_ambiguous_amount_is_left_unmatched(engine):
    series_ids = setup_mortgage(engine)
    # Two identical amounts in the window, in an account the sheet did not
    # name — nothing safe to pick.
    add_transaction(engine, "5678", dt.date(2026, 1, 7), "SOMETHING", -2294.13)
    add_transaction(engine, "5678", dt.date(2026, 1, 8), "SOMETHING ELSE", -2294.13)
    df_report = pd.DataFrame(
        [report_row(Auto_Pay_Account="", Amount_Paid=-2294.13, Date_Paid="2026-01-07")]
    )
    df_outcomes = expected_import_sheet.recover_matches(
        engine, df_report, series_ids, CONFIG
    )
    assert list(df_outcomes["result"]) == ["no_unique_hit"]


def test_preferred_account_wins_over_an_ambiguous_field(engine):
    series_ids = setup_mortgage(engine)
    add_transaction(engine, "1234", dt.date(2026, 1, 7), "MORTGAGE CO", -2294.13)
    add_transaction(engine, "5678", dt.date(2026, 1, 7), "COINCIDENCE", -2294.13)
    df_report = pd.DataFrame([report_row(Amount_Paid=-2294.13, Date_Paid="2026-01-07")])
    df_outcomes = expected_import_sheet.recover_matches(
        engine, df_report, series_ids, CONFIG
    )
    assert list(df_outcomes["result"]) == ["matched"]
    df_matches = expected_store.get_active_matches_df(engine)
    assert list(df_matches["txn_account_id"]) == ["1234"]


def test_pre_coverage_payment_is_skipped_with_the_sheet_facts(engine):
    series_ids = setup_mortgage(engine)
    # Coverage starts 2026 — this 2023 payment predates every held transaction.
    add_transaction(engine, "1234", dt.date(2026, 1, 7), "MORTGAGE CO", -2294.13)
    series_id = series_ids["Mortgage"]
    expected_store.add_occurrence(
        engine, series_id, dt.date(2023, 7, 6), -2294.00, source="import_sheet"
    )
    df_report = pd.DataFrame(
        [
            report_row(
                Date=dt.date(2023, 7, 6), Amount_Paid=-2294.00, Date_Paid="2023-07-07"
            )
        ]
    )
    df_outcomes = expected_import_sheet.recover_matches(
        engine, df_report, series_ids, CONFIG
    )
    assert list(df_outcomes["result"]) == ["skipped_pre_coverage"]
    row = expected_store.get_occurrence_status_df(
        engine, dt.date(2023, 7, 6), dt.date(2023, 7, 6)
    ).iloc[0]
    assert row["status"] == "skipped"
    assert "2023-07-07" in row["skip_note"]


def test_card_payoff_matches_both_legs(engine):
    series_id = expected_store.add_series(
        engine,
        {
            "name": "Chase Sapphire Card",
            "category": "Credit Card",
            "schedule_type": "monthly",
            "day_of_month": 16,
            "amount": -681.00,
            "auto_pay_account_id": "1234",
            "is_transfer": True,
            "transfer_account_id": "5678",
            "active_from": dt.date(2024, 1, 1),
        },
    )
    expected_store.add_occurrence(
        engine, series_id, dt.date(2026, 1, 16), -681.00, source="import_sheet"
    )
    add_transaction(
        engine, "1234", dt.date(2026, 1, 16), "Payment to Chase card", -800.00
    )
    add_transaction(engine, "5678", dt.date(2026, 1, 17), "Payment Thank You", 800.00)
    df_report = pd.DataFrame(
        [
            report_row(
                Account_Name="Chase Sapphire Card",
                Category="Credit Card",
                Date=dt.date(2026, 1, 16),
                Amount=-681.00,
                Amount_Paid=-800.00,
                Date_Paid="2026-01-16",
            )
        ]
    )
    df_outcomes = expected_import_sheet.recover_matches(
        engine, df_report, {"Chase Sapphire Card": series_id}, CONFIG
    )
    assert list(df_outcomes["result"]) == ["matched_both_legs"]
    row = expected_store.get_occurrence_status_df(
        engine, dt.date(2026, 1, 16), dt.date(2026, 1, 16)
    ).iloc[0]
    assert row["status"] == "paid"
    assert row["net_amount"] == pytest.approx(-800.00)  # card leg excluded
