"""Rank which unmatched transactions probably belong to an occurrence.

Suggestions only — nothing here writes to the database, ever. The pairing
screen shows the ranked list and a person confirms or ignores it. That is
what keeps the friend's-mortgage problem impossible: a transaction that
LOOKS like your mortgage never becomes your mortgage without you saying so.
"""

# %%
# Imports #

import pandas as pd

# %%
# Constants #

# How many days either side of the due date a payment can reasonably land.
DATE_WINDOW_DAYS = 10

# Score weights. The exact numbers only set the ranking; nothing acts on them.
SCORE_EXACT_AMOUNT = 4.0
SCORE_CLOSE_AMOUNT = 3.0  # within the series' tolerance
SCORE_PATTERN_HIT = 3.0
SCORE_ACCOUNT_HIT = 2.0
SCORE_DATE_MAX = 1.0  # scales down as the date drifts from the due date

DEFAULT_AMOUNT_TOLERANCE = 5.00


# %%
# Functions #


def suggest_for_occurrence(
    occurrence_row, df_unmatched_transactions: pd.DataFrame, top_n: int = 8
) -> pd.DataFrame:
    """Score every unmatched transaction near the due date; best first.

    occurrence_row comes from expected_store.get_occurrence_status_df, so it
    carries the series' amount, accounts, and match_pattern alongside the
    occurrence's due date.

    Returns the candidate transactions with a `score` column, highest first,
    at most top_n rows. An empty result just means nothing is nearby.
    """
    due_date = pd.to_datetime(occurrence_row["due_date"])
    expected_amount = float(occurrence_row["amount"])

    tolerance = occurrence_row.get("amount_tolerance")
    if pd.isna(tolerance):
        tolerance = DEFAULT_AMOUNT_TOLERANCE
    tolerance = float(tolerance)

    df_candidates = df_unmatched_transactions.copy()
    df_candidates["days_off"] = (
        pd.to_datetime(df_candidates["post_date"]) - due_date
    ).dt.days.abs()
    df_candidates = df_candidates[df_candidates["days_off"] <= DATE_WINDOW_DAYS]
    if len(df_candidates) == 0:
        return df_candidates

    scores = []
    for _, txn_row in df_candidates.iterrows():
        scores.append(_score_one(occurrence_row, txn_row, expected_amount, tolerance))
    df_candidates["score"] = scores

    df_candidates = df_candidates.sort_values(
        by=["score", "days_off"], ascending=[False, True]
    )
    return df_candidates.head(top_n).reset_index(drop=True)


def _score_one(
    occurrence_row, txn_row, expected_amount: float, tolerance: float
) -> float:
    score = 0.0
    txn_amount = float(txn_row["amount"])

    # Amount. For a transfer, the counterparty leg (+X on the card) is just as
    # much a hit as the cash leg (-X from the bank).
    amount_targets = [expected_amount]
    if occurrence_row["is_transfer"]:
        amount_targets.append(-expected_amount)
    for target in amount_targets:
        if txn_amount == target and target != 0:
            score = score + SCORE_EXACT_AMOUNT
            break
        if abs(txn_amount - target) <= tolerance and target != 0:
            score = score + SCORE_CLOSE_AMOUNT
            break

    # Account: the series knows where this money usually moves.
    expected_accounts = []
    if pd.notna(occurrence_row["auto_pay_account_id"]):
        expected_accounts.append(str(occurrence_row["auto_pay_account_id"]))
    if pd.notna(occurrence_row["transfer_account_id"]):
        expected_accounts.append(str(occurrence_row["transfer_account_id"]))
    if str(txn_row["account_id"]) in expected_accounts:
        score = score + SCORE_ACCOUNT_HIT

    # Description: match_pattern if set, otherwise the series name itself.
    pattern = occurrence_row.get("match_pattern")
    if pd.isna(pattern):
        pattern = occurrence_row["series_name"]
    if str(pattern).lower() in str(txn_row["description"]).lower():
        score = score + SCORE_PATTERN_HIT

    # Date: closer to the due date is better.
    score = score + SCORE_DATE_MAX * (1 - txn_row["days_off"] / (DATE_WINDOW_DAYS + 1))

    return round(score, 3)


# %%
