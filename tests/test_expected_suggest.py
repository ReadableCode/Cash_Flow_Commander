# %%
# Imports #

import datetime as dt

import pandas as pd

import expected_suggest

# %%
# Helpers #


def occurrence(**overrides):
    row = {
        "occurrence_id": 1,
        "series_name": "Chase Freedom Card",
        "due_date": dt.date(2026, 3, 19),
        "amount": -23.00,  # the placeholder minimum payment, never what is paid
        "amount_tolerance": None,
        "auto_pay_account_id": "8718",
        "is_transfer": True,
        "transfer_account_id": "4261",
        "match_pattern": None,
    }
    row.update(overrides)
    return pd.Series(row)


def transactions(rows):
    return pd.DataFrame(
        rows, columns=["account_id", "post_date", "description", "amount", "occurrence"]
    )


def scores(df_suggested):
    return {str(r["description"]): r["score"] for _, r in df_suggested.iterrows()}


# %%
# Transfers #


def test_payoff_ignores_the_placeholder_amount_and_names_the_card():
    df = transactions(
        [
            (
                "8718",
                dt.date(2026, 3, 21),
                "Payment to Chase card ending in 4261 03/21",
                -1512.40,
                0,
            ),
            (
                "8718",
                dt.date(2026, 3, 20),
                "Payment to Chase card ending in 3590 03/20",
                -681.00,
                0,
            ),
            ("8718", dt.date(2026, 3, 19), "HEB GROCERY", -23.00, 0),
            ("4261", dt.date(2026, 3, 22), "Payment Thank You - Web", 1512.40, 0),
        ]
    )
    result = scores(expected_suggest.suggest_for_occurrence(occurrence(), df))
    # the bank leg naming this card, with its counterpart nearby, wins outright
    assert max(result, key=result.get) == "Payment to Chase card ending in 4261 03/21"
    assert (
        result["Payment to Chase card ending in 4261 03/21"]
        > result["Payment to Chase card ending in 3590 03/20"] + 3
    )
    # an exact-amount match on the placeholder is worth nothing for a payoff
    assert (
        result["Payment to Chase card ending in 4261 03/21"] > result["HEB GROCERY"] + 5
    )
    # the card-side leg scores too (account + direction + counterpart), just below the bank leg
    assert result["Payment Thank You - Web"] > result["HEB GROCERY"]
    assert (
        result["Payment to Chase card ending in 4261 03/21"]
        > result["Payment Thank You - Web"]
    )


def test_payoff_direction_matters_and_other_accounts_score_nothing():
    df = transactions(
        [
            (
                "4261",
                dt.date(2026, 3, 20),
                "AMAZON MKTPL",
                -88.10,
                0,
            ),  # a purchase on the card
            (
                "8718",
                dt.date(2026, 3, 20),
                "ACME CORP PAYROLL",
                3100.00,
                0,
            ),  # money into the bank
            (
                "9639",
                dt.date(2026, 3, 20),
                "ONLINE PAYMENT, THANK YOU",
                900.00,
                0,
            ),  # another card
        ]
    )
    result = scores(expected_suggest.suggest_for_occurrence(occurrence(), df))
    assert result["AMAZON MKTPL"] < 1.5
    assert result["ACME CORP PAYROLL"] < 1.5
    assert result["ONLINE PAYMENT, THANK YOU"] < 1.0


def test_payoff_window_is_wider_than_a_bills():
    df = transactions(
        [
            (
                "8718",
                dt.date(2026, 4, 5),
                "Payment to Chase card ending in 4261",
                -400.00,
                0,
            )
        ]
    )
    assert (
        len(expected_suggest.suggest_for_occurrence(occurrence(), df)) == 1
    )  # 17 days off
    assert (
        len(
            expected_suggest.suggest_for_occurrence(
                occurrence(is_transfer=False, transfer_account_id=None), df
            )
        )
        == 0
    )


# %%
# Bills #


def test_a_plain_bill_still_scores_on_amount():
    df = transactions(
        [
            ("8718", dt.date(2026, 3, 20), "RHYTHM ENERGY", -180.00, 0),
            ("8718", dt.date(2026, 3, 20), "RHYTHM ENERGY", -900.00, 0),
        ]
    )
    bill = occurrence(
        series_name="Electric",
        amount=-180.00,
        is_transfer=False,
        transfer_account_id=None,
        match_pattern="RHYTHM",
    )
    df_suggested = expected_suggest.suggest_for_occurrence(bill, df)
    assert float(df_suggested.iloc[0]["amount"]) == -180.00
    assert df_suggested.iloc[0]["score"] > df_suggested.iloc[1]["score"]


# %%
