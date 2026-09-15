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
# Card payoffs are paid when they are paid, not on the statement day.
DATE_WINDOW_DAYS_TRANSFER = 20
# How far apart the two legs of one payoff can post (the card credits a day
# or three after the bank debits).
LEG_PAIR_DAYS = 5

# Score weights. The exact numbers only set the ranking; nothing acts on them.
SCORE_EXACT_AMOUNT = 4.0
SCORE_CLOSE_AMOUNT = 3.0  # within the series' tolerance
SCORE_PATTERN_HIT = 3.0
SCORE_ACCOUNT_HIT = 2.0
SCORE_DATE_MAX = 1.0  # scales down as the date drifts from the due date

# Transfer (card payoff) signals. The expected amount on a payoff series is a
# placeholder (the minimum payment, never what is actually paid), so it is
# not scored at all; these stand in for it.
SCORE_CARD_ID_IN_DESCRIPTION = 4.0  # "Payment to Chase card ending in 4261"
SCORE_LEG_PAIR = 3.0  # the other leg (equal, opposite, other account) is nearby
SCORE_DIRECTION = 1.0  # money leaves the bank / lands on the card
SCORE_BANK_LEG = 0.5  # the bank leg is the one that counts as paid, so list it first

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
    is_transfer = bool(occurrence_row["is_transfer"])
    window_days = DATE_WINDOW_DAYS_TRANSFER if is_transfer else DATE_WINDOW_DAYS

    tolerance = occurrence_row.get("amount_tolerance")
    if pd.isna(tolerance):
        tolerance = DEFAULT_AMOUNT_TOLERANCE
    tolerance = float(tolerance)

    df_candidates = df_unmatched_transactions.copy()
    df_candidates["days_off"] = (
        pd.to_datetime(df_candidates["post_date"]) - due_date
    ).dt.days.abs()
    df_candidates = df_candidates[df_candidates["days_off"] <= window_days]
    if len(df_candidates) == 0:
        return df_candidates

    scores = []
    for _, txn_row in df_candidates.iterrows():
        if is_transfer:
            scores.append(
                _score_transfer(
                    occurrence_row, txn_row, df_unmatched_transactions, window_days
                )
            )
        else:
            scores.append(
                _score_one(occurrence_row, txn_row, expected_amount, tolerance)
            )
    df_candidates["score"] = scores

    df_candidates = df_candidates.sort_values(
        by=["score", "days_off"], ascending=[False, True]
    )
    return df_candidates.head(top_n).reset_index(drop=True)


def _score_transfer(
    occurrence_row, txn_row, df_unmatched_transactions: pd.DataFrame, window_days: int
) -> float:
    """Score a candidate for a card payoff, ignoring the placeholder amount.

    What identifies a payoff instead: it moves between the series' two
    accounts in the right direction (out of the bank, onto the card), the
    bank-side memo usually names the card ("...card ending in 4261"), the
    series' own match_pattern, and the other leg — same amount, opposite
    sign, other account — posting within a few days. A transaction that is
    neither on the bank account nor on the card scores nothing.
    """
    bank_account = occurrence_row.get("auto_pay_account_id")
    card_account = occurrence_row.get("transfer_account_id")
    bank_account = None if pd.isna(bank_account) else str(bank_account)
    card_account = None if pd.isna(card_account) else str(card_account)
    txn_account = str(txn_row["account_id"])
    txn_amount = float(txn_row["amount"])
    description = str(txn_row["description"]).lower()

    on_bank = bank_account is not None and txn_account == bank_account
    on_card = card_account is not None and txn_account == card_account
    if not on_bank and not on_card:
        return 0.0

    score = SCORE_ACCOUNT_HIT + (SCORE_BANK_LEG if on_bank else 0.0)
    if (on_bank and txn_amount < 0) or (on_card and txn_amount > 0):
        score = score + SCORE_DIRECTION
    else:
        return round(score * 0.25, 3)  # a card purchase or a bank credit: not a payoff

    if card_account is not None and card_account in description:
        score = score + SCORE_CARD_ID_IN_DESCRIPTION

    pattern = occurrence_row.get("match_pattern")
    if pd.notna(pattern) and str(pattern).lower() in description:
        score = score + SCORE_PATTERN_HIT

    other_account = card_account if on_bank else bank_account
    if other_account is not None and _has_counterpart(
        df_unmatched_transactions, other_account, -txn_amount, txn_row["post_date"]
    ):
        score = score + SCORE_LEG_PAIR

    score = score + SCORE_DATE_MAX * (1 - txn_row["days_off"] / (window_days + 1))
    return round(score, 3)


def _has_counterpart(
    df_transactions: pd.DataFrame, account_id: str, amount: float, post_date
) -> bool:
    """Is there an equal, opposite leg on the other account within LEG_PAIR_DAYS?"""
    if len(df_transactions) == 0:
        return False
    on_account = df_transactions["account_id"].astype(str) == str(account_id)
    same_amount = (df_transactions["amount"].astype(float) - amount).abs() < 0.005
    days_apart = (
        pd.to_datetime(df_transactions["post_date"]) - pd.to_datetime(post_date)
    ).dt.days.abs()
    return bool((on_account & same_amount & (days_apart <= LEG_PAIR_DAYS)).any())


def _score_one(
    occurrence_row, txn_row, expected_amount: float, tolerance: float
) -> float:
    score = 0.0
    txn_amount = float(txn_row["amount"])

    amount_targets = [expected_amount]
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
