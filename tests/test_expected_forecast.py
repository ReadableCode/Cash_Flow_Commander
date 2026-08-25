# %%
# Imports #

import datetime as dt
import importlib
import os
import sys
from typing import Any

import pytest
from sqlalchemy import insert

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if os.path.join(_ROOT, "src") not in sys.path:
    sys.path.insert(0, os.path.join(_ROOT, "src"))

import db  # noqa: E402
import expected_forecast  # noqa: E402
import expected_store  # noqa: E402

# %%
# Fixtures #

CONFIG = {"anchor_account_id": "1234", "account_labels": {"1234": "Checking"}}


def _restore_env(name: str, value: str | None) -> None:
    if value is None:
        os.environ.pop(name, None)
    else:
        os.environ[name] = value


@pytest.fixture()
def engine(tmp_path: Any) -> Any:
    saved_url = os.environ.get("CFC_DATABASE_URL")
    saved_schema = os.environ.get("CFC_DB_SCHEMA")
    os.environ["CFC_DATABASE_URL"] = f"sqlite:///{tmp_path / 'forecast_test.db'}"
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


def add_bank_transaction(engine, post_date, description, amount, balance):
    with engine.begin() as conn:
        conn.execute(
            insert(db.transactions).values(
                account_id="1234",
                account_kind="bank",
                txn_date=post_date,
                post_date=post_date,
                description=description,
                amount=amount,
                balance=balance,
                occurrence=0,
                parser_version="test-1",
            )
        )


# %%
# Tests #


def test_anchor_is_the_newest_balance(engine):
    add_bank_transaction(engine, dt.date(2026, 1, 5), "OLD", -10.00, 900.00)
    add_bank_transaction(engine, dt.date(2026, 1, 8), "NEW", -10.00, 890.00)
    anchor_date, anchor_balance = expected_forecast.get_anchor(engine, "1234")
    assert anchor_date == dt.date(2026, 1, 8)
    assert anchor_balance == 890.00


def test_unpaid_rows_chain_the_balance_and_paid_rows_do_not(engine):
    add_bank_transaction(engine, dt.date(2026, 1, 8), "ANCHOR", -10.00, 1000.00)

    series_id = expected_store.add_series(
        engine,
        {
            "name": "Rent",
            "category": "Expense",
            "schedule_type": "monthly",
            "day_of_month": 10,
            "amount": -300.00,
            "auto_pay_account_id": "1234",
            "active_from": dt.date(2026, 1, 1),
        },
    )
    expected_store.generate_occurrences(
        engine, start=dt.date(2026, 1, 1), horizon_days=70
    )

    # January's rent is paid; its transaction is already inside the anchor.
    add_bank_transaction(engine, dt.date(2026, 1, 10), "RENT LLC", -301.50, 700.00)
    df = expected_store.get_occurrence_status_df(
        engine, dt.date(2026, 1, 10), dt.date(2026, 1, 10)
    )
    expected_store.add_match(
        engine,
        int(df.iloc[0]["occurrence_id"]),
        {
            "account_id": "1234",
            "post_date": dt.date(2026, 1, 10),
            "description": "RENT LLC",
            "amount": -301.50,
            "occurrence": 0,
        },
        source="manual",
    )
    # March's rent is skipped; it must not appear or count.
    df = expected_store.get_occurrence_status_df(
        engine, dt.date(2026, 3, 10), dt.date(2026, 3, 10)
    )
    expected_store.skip_occurrence(
        engine, int(df.iloc[0]["occurrence_id"]), "moved out"
    )

    df_cast = expected_forecast.build_future_cast(engine, config=CONFIG)

    # The rent payment is now the newest balance-bearing transaction, so the
    # anchor is 700 — the paid January row must NOT subtract again.
    january = df_cast[df_cast["Date"] == "2026-01-10"].iloc[0]
    assert january["Amount_Paid"] == -301.50
    assert january["Date_Paid"] == "2026-01-10"
    assert january["Running_Balance"] == 700.00

    february = df_cast[df_cast["Date"] == "2026-02-10"].iloc[0]
    assert february["Amount_Paid"] == ""
    assert february["Running_Balance"] == 400.00  # 700 + (-300)

    # Skipped rows appear IN ORDER (the TUI shows the whole story) but move
    # nothing; the sheet publisher strips them via _status.
    march = df_cast[df_cast["Date"] == "2026-03-10"].iloc[0]
    assert march["_status"] == "skipped"
    assert march["Running_Balance"] == february["Running_Balance"]
    assert series_id is not None


def test_forecast_days_outflows_hit_before_inflows(engine):
    add_bank_transaction(engine, dt.date(2026, 1, 8), "ANCHOR", -10.00, 100.00)

    # Same day: rent out -300, paycheck in +500. The day ENDS at 300, but the
    # worst moment is -200 — that must be flagged, not hidden by the paycheck.
    rent_id = expected_store.add_series(
        engine,
        {
            "name": "Rent",
            "category": "Expense",
            "schedule_type": "once",
            "amount": -300.00,
            "auto_pay_account_id": "1234",
            "active_from": dt.date(2026, 1, 1),
        },
    )
    pay_id = expected_store.add_series(
        engine,
        {
            "name": "Paycheck",
            "category": "Income",
            "schedule_type": "once",
            "amount": 500.00,
            "auto_pay_account_id": "1234",
            "active_from": dt.date(2026, 1, 1),
        },
    )
    expected_store.add_occurrence(engine, rent_id, dt.date(2026, 1, 15), -300.00)
    expected_store.add_occurrence(engine, pay_id, dt.date(2026, 1, 15), 500.00)
    # An unpaid bill already past due still counts — it lands on the first day.
    expected_store.add_occurrence(engine, rent_id, dt.date(2026, 1, 2), -40.00)

    count = expected_forecast.rebuild_forecast_days(
        engine, horizon_days=30, config=CONFIG
    )
    assert count > 0

    import pandas as pd
    from sqlalchemy import select

    df_days = pd.read_sql(select(db.forecast_days), engine)
    df_days["day"] = pd.to_datetime(df_days["day"]).dt.date

    first = df_days[df_days["day"] == dt.date(2026, 1, 9)].iloc[0]
    assert float(first["outflows"]) == -40.00  # the late bill lands tomorrow
    assert float(first["end_balance"]) == 60.00

    pay_day = df_days[df_days["day"] == dt.date(2026, 1, 15)].iloc[0]
    assert float(pay_day["start_balance"]) == 60.00
    assert float(pay_day["trough_balance"]) == -240.00  # 60 - 300, before pay
    assert float(pay_day["end_balance"]) == 260.00


def test_anchor_uses_the_days_final_balance_regardless_of_row_order(engine):
    # Payday: the paycheck row and an autopay land on the same day, and the
    # autopay is chronologically last even though it was inserted first.
    add_bank_transaction(engine, dt.date(2026, 1, 7), "PRIOR DAY", -5.00, 1000.00)
    add_bank_transaction(engine, dt.date(2026, 1, 8), "AUTOPAY", -20.80, 3090.62)
    add_bank_transaction(engine, dt.date(2026, 1, 8), "PAYROLL", 2111.42, 3111.42)
    anchor_date, anchor_balance = expected_forecast.get_anchor(engine, "1234")
    assert anchor_date == dt.date(2026, 1, 8)
    assert anchor_balance == 3090.62  # after BOTH rows, not the paycheck's
