# %%
# Imports #

import datetime as dt
import importlib
import os
import sys
from typing import Any

import pytest
from sqlalchemy import delete, insert

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if os.path.join(_ROOT, "src") not in sys.path:
    sys.path.insert(0, os.path.join(_ROOT, "src"))

import db  # noqa: E402
import expected_store  # noqa: E402

# %%
# Fixtures #


def _restore_env(name: str, value: str | None) -> None:
    if value is None:
        os.environ.pop(name, None)
    else:
        os.environ[name] = value


@pytest.fixture()
def engine(tmp_path: Any) -> Any:
    """Engine on a throwaway SQLite file, with CFC_DB_SCHEMA forced off."""
    saved_url = os.environ.get("CFC_DATABASE_URL")
    saved_schema = os.environ.get("CFC_DB_SCHEMA")
    os.environ["CFC_DATABASE_URL"] = f"sqlite:///{tmp_path / 'expected_test.db'}"
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


# %%
# Helpers #


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


def txn(account_id, post_date, description, amount, occurrence=0):
    """A transaction natural key as add_match expects it."""
    return {
        "account_id": account_id,
        "post_date": post_date,
        "description": description,
        "amount": amount,
        "occurrence": occurrence,
    }


def mortgage_series(**overrides):
    series = {
        "name": "Mortgage",
        "category": "Debt",
        "schedule_type": "monthly",
        "day_of_month": 6,
        "amount": -2294.00,
        "auto_pay_account_id": "1234",
        "active_from": dt.date(2026, 1, 1),
    }
    series.update(overrides)
    return series


def status_of(engine, series_name, due_date):
    df = expected_store.get_occurrence_status_df(engine, due_date, due_date)
    row = df[df["series_name"] == series_name].iloc[0]
    return row


# %%
# Series and occurrences #


def test_generate_occurrences_is_idempotent(engine):
    expected_store.add_series(engine, mortgage_series())
    expected_store.generate_occurrences(
        engine, start=dt.date(2026, 1, 1), horizon_days=90
    )
    df_first = expected_store.get_occurrence_status_df(
        engine, dt.date(2026, 1, 1), dt.date(2026, 12, 31)
    )

    expected_store.generate_occurrences(
        engine, start=dt.date(2026, 1, 1), horizon_days=90
    )
    df_second = expected_store.get_occurrence_status_df(
        engine, dt.date(2026, 1, 1), dt.date(2026, 12, 31)
    )

    assert len(df_first) == 3  # Jan 6, Feb 6, Mar 6 — the 90-day horizon ends Apr 1
    assert len(df_second) == len(df_first)


def test_generate_stops_at_active_until(engine):
    expected_store.add_series(
        engine, mortgage_series(active_until=dt.date(2026, 2, 28))
    )
    expected_store.generate_occurrences(
        engine, start=dt.date(2026, 1, 1), horizon_days=365
    )
    df = expected_store.get_occurrence_status_df(
        engine, dt.date(2026, 1, 1), dt.date(2026, 12, 31)
    )
    assert list(df["due_date"].astype(str)) == ["2026-01-06", "2026-02-06"]


def test_generate_does_not_overwrite_edited_amounts(engine):
    series_id = expected_store.add_series(engine, mortgage_series())
    expected_store.add_occurrence(
        engine, series_id, dt.date(2026, 1, 6), -9999.00, source="manual"
    )
    expected_store.generate_occurrences(
        engine, start=dt.date(2026, 1, 1), horizon_days=40
    )
    row = status_of(engine, "Mortgage", dt.date(2026, 1, 6))
    assert float(row["amount"]) == -9999.00


# %%
# Derived status #


def test_unpaid_then_paid(engine):
    expected_store.add_series(engine, mortgage_series())
    expected_store.generate_occurrences(
        engine, start=dt.date(2026, 1, 1), horizon_days=40
    )
    row = status_of(engine, "Mortgage", dt.date(2026, 1, 6))
    assert row["status"] == "unpaid"

    add_transaction(
        engine, "1234", dt.date(2026, 1, 7), "MORTGAGE CO PAYMENT", -2294.13
    )
    expected_store.add_match(
        engine,
        int(row["occurrence_id"]),
        txn("1234", dt.date(2026, 1, 7), "MORTGAGE CO PAYMENT", -2294.13),
        source="manual",
    )
    row = status_of(engine, "Mortgage", dt.date(2026, 1, 6))
    assert row["status"] == "paid"
    assert row["net_amount"] == pytest.approx(-2294.13)


def test_removed_transaction_flips_paid_back_to_broken(engine):
    expected_store.add_series(engine, mortgage_series())
    expected_store.generate_occurrences(
        engine, start=dt.date(2026, 1, 1), horizon_days=40
    )
    row = status_of(engine, "Mortgage", dt.date(2026, 1, 6))
    add_transaction(
        engine, "1234", dt.date(2026, 1, 7), "MORTGAGE CO PAYMENT", -2294.13
    )
    expected_store.add_match(
        engine,
        int(row["occurrence_id"]),
        txn("1234", dt.date(2026, 1, 7), "MORTGAGE CO PAYMENT", -2294.13),
        source="manual",
    )

    # A re-download restates the transaction: the old row is gone.
    with engine.begin() as conn:
        conn.execute(delete(db.transactions))
    row = status_of(engine, "Mortgage", dt.date(2026, 1, 6))
    assert row["status"] == "broken"


def test_payment_plus_reversal_shows_net_zero(engine):
    expected_store.add_series(engine, mortgage_series())
    expected_store.generate_occurrences(
        engine, start=dt.date(2026, 1, 1), horizon_days=40
    )
    row = status_of(engine, "Mortgage", dt.date(2026, 1, 6))
    add_transaction(
        engine, "1234", dt.date(2026, 1, 7), "MORTGAGE CO PAYMENT", -2294.13
    )
    add_transaction(
        engine, "1234", dt.date(2026, 1, 9), "MORTGAGE CO REVERSAL", 2294.13
    )
    occurrence_id = int(row["occurrence_id"])
    expected_store.add_match(
        engine,
        occurrence_id,
        txn("1234", dt.date(2026, 1, 7), "MORTGAGE CO PAYMENT", -2294.13),
        source="manual",
    )
    expected_store.add_match(
        engine,
        occurrence_id,
        txn("1234", dt.date(2026, 1, 9), "MORTGAGE CO REVERSAL", 2294.13),
        source="manual",
    )
    row = status_of(engine, "Mortgage", dt.date(2026, 1, 6))
    assert row["status"] == "net_zero"


def test_transfer_leg_is_excluded_from_the_net(engine):
    expected_store.add_series(
        engine,
        mortgage_series(
            name="Sapphire Payoff",
            category="Credit Card",
            amount=-681.00,
            is_transfer=True,
            transfer_account_id="5678",
        ),
    )
    expected_store.generate_occurrences(
        engine, start=dt.date(2026, 1, 1), horizon_days=40
    )
    row = status_of(engine, "Sapphire Payoff", dt.date(2026, 1, 6))
    add_transaction(
        engine, "1234", dt.date(2026, 1, 6), "Payment to Chase card", -700.00
    )
    add_transaction(engine, "5678", dt.date(2026, 1, 7), "Payment Thank You", 700.00)
    occurrence_id = int(row["occurrence_id"])
    expected_store.add_match(
        engine,
        occurrence_id,
        txn("1234", dt.date(2026, 1, 6), "Payment to Chase card", -700.00),
        source="manual",
    )
    expected_store.add_match(
        engine,
        occurrence_id,
        txn("5678", dt.date(2026, 1, 7), "Payment Thank You", 700.00),
        source="manual",
    )
    row = status_of(engine, "Sapphire Payoff", dt.date(2026, 1, 6))
    # Both legs matched, but only the cash leg counts — not net-zero.
    assert row["status"] == "paid"
    assert row["net_amount"] == pytest.approx(-700.00)


def test_skip_and_unskip(engine):
    expected_store.add_series(engine, mortgage_series())
    expected_store.generate_occurrences(
        engine, start=dt.date(2026, 1, 1), horizon_days=40
    )
    row = status_of(engine, "Mortgage", dt.date(2026, 1, 6))
    expected_store.skip_occurrence(
        engine, int(row["occurrence_id"]), "covered by double payment"
    )
    row = status_of(engine, "Mortgage", dt.date(2026, 1, 6))
    assert row["status"] == "skipped"
    expected_store.unskip_occurrence(engine, int(row["occurrence_id"]))
    row = status_of(engine, "Mortgage", dt.date(2026, 1, 6))
    assert row["status"] == "unpaid"


# %%
# Matches #


def test_one_transaction_cannot_be_matched_twice(engine):
    expected_store.add_series(engine, mortgage_series())
    expected_store.add_series(
        engine, mortgage_series(name="Friend Mortgage", amount=-2294.00)
    )
    expected_store.generate_occurrences(
        engine, start=dt.date(2026, 1, 1), horizon_days=40
    )
    df = expected_store.get_occurrence_status_df(
        engine, dt.date(2026, 1, 6), dt.date(2026, 1, 6)
    )
    add_transaction(
        engine, "1234", dt.date(2026, 1, 7), "MORTGAGE CO PAYMENT", -2294.13
    )
    payment = txn("1234", dt.date(2026, 1, 7), "MORTGAGE CO PAYMENT", -2294.13)

    first_id = int(df.iloc[0]["occurrence_id"])
    second_id = int(df.iloc[1]["occurrence_id"])
    expected_store.add_match(engine, first_id, payment, source="manual")
    with pytest.raises(ValueError):
        expected_store.add_match(engine, second_id, payment, source="manual")

    # Voiding frees the transaction for the right occurrence.
    df_matches = expected_store.get_active_matches_df(engine)
    expected_store.void_match(
        engine, int(df_matches.iloc[0]["id"]), note="wrong mortgage"
    )
    expected_store.add_match(engine, second_id, payment, source="manual")


def test_unmatched_transactions_hides_matched_ones(engine):
    expected_store.add_series(engine, mortgage_series())
    expected_store.generate_occurrences(
        engine, start=dt.date(2026, 1, 1), horizon_days=40
    )
    add_transaction(
        engine, "1234", dt.date(2026, 1, 7), "MORTGAGE CO PAYMENT", -2294.13
    )
    add_transaction(engine, "1234", dt.date(2026, 1, 8), "COFFEE", -4.50)

    df = expected_store.get_unmatched_transactions_df(
        engine, dt.date(2026, 1, 1), dt.date(2026, 1, 31)
    )
    assert len(df) == 2

    row = status_of(engine, "Mortgage", dt.date(2026, 1, 6))
    expected_store.add_match(
        engine,
        int(row["occurrence_id"]),
        txn("1234", dt.date(2026, 1, 7), "MORTGAGE CO PAYMENT", -2294.13),
        source="manual",
    )
    df = expected_store.get_unmatched_transactions_df(
        engine, dt.date(2026, 1, 1), dt.date(2026, 1, 31)
    )
    assert list(df["description"]) == ["COFFEE"]


# %%
# Split matches (one bill pays several expected items) #


def test_one_bill_can_pay_mortgage_and_pmi_via_splits(engine):
    expected_store.add_series(engine, mortgage_series(amount=-2062.17))
    expected_store.add_series(
        engine, mortgage_series(name="Mortgage Insurance", amount=-200.00)
    )
    expected_store.generate_occurrences(
        engine, start=dt.date(2026, 1, 1), horizon_days=40
    )
    df = expected_store.get_occurrence_status_df(
        engine, dt.date(2026, 1, 6), dt.date(2026, 1, 6)
    )
    mortgage_id = int(df[df["series_name"] == "Mortgage"].iloc[0]["occurrence_id"])
    pmi_id = int(df[df["series_name"] == "Mortgage Insurance"].iloc[0]["occurrence_id"])

    add_transaction(engine, "1234", dt.date(2026, 1, 7), "BIG MORTGAGE CO", -2262.17)
    payment = txn("1234", dt.date(2026, 1, 7), "BIG MORTGAGE CO", -2262.17)

    expected_store.add_match(
        engine, mortgage_id, payment, source="manual", matched_amount=-2062.17
    )
    expected_store.add_match(
        engine, pmi_id, payment, source="manual", matched_amount=-200.00
    )

    mortgage_row = status_of(engine, "Mortgage", dt.date(2026, 1, 6))
    pmi_row = status_of(engine, "Mortgage Insurance", dt.date(2026, 1, 6))
    assert mortgage_row["status"] == "paid"
    assert mortgage_row["net_amount"] == pytest.approx(-2062.17)
    assert pmi_row["status"] == "paid"
    assert pmi_row["net_amount"] == pytest.approx(-200.00)


def test_sharing_a_fully_claimed_transaction_requires_voiding_first(engine):
    expected_store.add_series(engine, mortgage_series())
    expected_store.add_series(
        engine, mortgage_series(name="Mortgage Insurance", amount=-200.00)
    )
    expected_store.generate_occurrences(
        engine, start=dt.date(2026, 1, 1), horizon_days=40
    )
    df = expected_store.get_occurrence_status_df(
        engine, dt.date(2026, 1, 6), dt.date(2026, 1, 6)
    )
    mortgage_id = int(df[df["series_name"] == "Mortgage"].iloc[0]["occurrence_id"])
    pmi_id = int(df[df["series_name"] == "Mortgage Insurance"].iloc[0]["occurrence_id"])

    add_transaction(engine, "1234", dt.date(2026, 1, 7), "BIG MORTGAGE CO", -2262.17)
    payment = txn("1234", dt.date(2026, 1, 7), "BIG MORTGAGE CO", -2262.17)

    # Whole-transaction claim first, then a share elsewhere: refused, so the
    # 200 can never be counted twice.
    expected_store.add_match(engine, mortgage_id, payment, source="manual")
    with pytest.raises(ValueError):
        expected_store.add_match(
            engine, pmi_id, payment, source="manual", matched_amount=-200.00
        )
    # And a second whole-transaction claim is refused outright.
    with pytest.raises(ValueError):
        expected_store.add_match(engine, pmi_id, payment, source="manual")


# %%
# Closing a series cleans up its future #


def test_close_series_removes_untouched_future_occurrences(engine):
    series_id = expected_store.add_series(engine, mortgage_series())
    expected_store.generate_occurrences(
        engine, start=dt.date(2026, 1, 1), horizon_days=150
    )
    df = expected_store.get_occurrence_status_df(
        engine, dt.date(2026, 1, 1), dt.date(2026, 12, 31)
    )
    assert len(df) == 5  # Jan..May

    # Pair January, skip February, then close the series at end of February.
    jan_row = status_of(engine, "Mortgage", dt.date(2026, 1, 6))
    add_transaction(
        engine, "1234", dt.date(2026, 1, 7), "MORTGAGE CO PAYMENT", -2294.13
    )
    expected_store.add_match(
        engine,
        int(jan_row["occurrence_id"]),
        txn("1234", dt.date(2026, 1, 7), "MORTGAGE CO PAYMENT", -2294.13),
        source="manual",
    )
    feb_row = status_of(engine, "Mortgage", dt.date(2026, 2, 6))
    expected_store.skip_occurrence(engine, int(feb_row["occurrence_id"]), "note")

    removed = expected_store.close_series(engine, series_id, dt.date(2026, 2, 28))
    assert removed == 3  # Mar, Apr, May — untouched and generated

    df = expected_store.get_occurrence_status_df(
        engine, dt.date(2026, 1, 1), dt.date(2026, 12, 31)
    )
    # The paid January and the skipped February survive.
    assert list(df["due_date"].astype(str)) == ["2026-01-06", "2026-02-06"]
