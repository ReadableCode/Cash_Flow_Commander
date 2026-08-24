"""Find recurring transactions that no expected series accounts for.

Walks every held transaction, groups them by a normalized merchant name, and
reports groups that recur on a steady cadence but are not matched to any
series — candidate bills that never made it into the plan. Read-only: it
prints; a person decides what becomes a series.

    uv run python src/expected_discover.py
    uv run python src/expected_discover.py --min-amount 10 --min-occurrences 4
"""

# %%
# Imports #

import argparse
import os
import re
import sys

import pandas as pd
from sqlalchemy import select

_SRC_DIR = os.path.dirname(os.path.abspath(__file__))
if _SRC_DIR not in sys.path:
    sys.path.insert(0, _SRC_DIR)

import db  # noqa: E402
import expected_store  # noqa: E402

# %%
# Constants #

DEFAULT_MIN_OCCURRENCES = 3
DEFAULT_MIN_AMOUNT = 5.00

# A group is 'steady' when the spread of its gaps stays within this fraction
# of the typical gap — Netflix lands within a day or two of every 30.
CADENCE_TOLERANCE = 0.25

# Named cadences for the report; anything else prints as 'every ~N days'.
KNOWN_CADENCES = [
    (7, "weekly"),
    (14, "biweekly"),
    (30, "monthly"),
    (61, "every 2 months"),
    (91, "quarterly"),
    (152, "every 5 months"),
    (182, "every 6 months"),
    (365, "yearly"),
]


# %%
# Functions #


def merchant_key(description: str) -> str:
    """Collapse a raw bank description to a stable merchant name.

    Store numbers, confirmation ids, and dates vary charge to charge —
    'FIBERFIRST DFW LLC 833-3427444 NC 08/22' and
    'FIBERFIRST FIBERFIRST.CO TX 07/20' should land in one group. Digits go,
    then the first three words are kept.
    """
    text = re.sub(r"[\d/#*\-]+", " ", str(description).upper())
    words = text.split()
    return " ".join(words[:3])


def cadence_name(median_gap_days: float) -> str | None:
    """The human name for a gap, or None when it is not a steady cadence."""
    for days, name in KNOWN_CADENCES:
        if abs(median_gap_days - days) <= days * CADENCE_TOLERANCE:
            return name
    return None


def find_recurring_unbudgeted(
    engine,
    min_occurrences: int = DEFAULT_MIN_OCCURRENCES,
    min_amount: float = DEFAULT_MIN_AMOUNT,
) -> pd.DataFrame:
    """Recurring transaction groups with no series, best candidates first."""
    df_transactions = pd.read_sql(
        select(
            db.transactions.c.account_id,
            db.transactions.c.post_date,
            db.transactions.c.description,
            db.transactions.c.amount,
        ),
        engine,
    )
    df_transactions["amount"] = df_transactions["amount"].astype(float)
    df_transactions["merchant"] = df_transactions["description"].map(merchant_key)

    # A merchant is 'budgeted' when any of its charges is already matched to a
    # series — even if most are not (the pairing backlog is not a new bill).
    df_matches = expected_store.get_active_matches_df(engine)
    budgeted_merchants = set(df_matches["txn_description"].map(merchant_key))

    candidates = []
    for merchant, df_group in df_transactions.groupby("merchant"):
        if merchant == "" or merchant in budgeted_merchants:
            continue
        if len(df_group) < min_occurrences:
            continue

        typical_amount = df_group["amount"].median()
        if abs(typical_amount) < min_amount:
            continue
        # Bills are one-directional; mixed signs mean refunds/noise, not a bill.
        if (df_group["amount"] > 0).any() and (df_group["amount"] < 0).any():
            continue
        # Steady amount: most charges near the typical one.
        near_typical = df_group["amount"].sub(typical_amount).abs() <= max(
            5.0, abs(typical_amount) * 0.25
        )
        if near_typical.mean() < 0.7:
            continue

        dates = sorted(pd.to_datetime(df_group["post_date"]).dt.date)
        gaps = [(later - earlier).days for earlier, later in zip(dates, dates[1:])]
        gaps = [gap for gap in gaps if gap > 0]  # same-day repeats are one event
        if len(gaps) < min_occurrences - 1:
            continue
        median_gap = float(pd.Series(gaps).median())
        cadence = cadence_name(median_gap)
        if cadence is None:
            continue
        steady = pd.Series(gaps).sub(median_gap).abs() <= max(
            3.0, median_gap * CADENCE_TOLERANCE
        )
        if steady.mean() < 0.7:
            continue

        candidates.append(
            {
                "merchant": merchant,
                "cadence": cadence,
                "typical_amount": round(typical_amount, 2),
                "times_seen": len(df_group),
                "first_seen": dates[0],
                "last_seen": dates[-1],
                "account_id": df_group["account_id"].mode().iloc[0],
                "example": df_group["description"].iloc[-1][:60],
                "monthly_cost": round(abs(typical_amount) * 30.4 / median_gap, 2),
            }
        )

    df_candidates = pd.DataFrame(candidates)
    if len(df_candidates) > 0:
        df_candidates = df_candidates.sort_values("monthly_cost", ascending=False)
    return df_candidates.reset_index(drop=True)


# %%
# Run #


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--min-occurrences", type=int, default=DEFAULT_MIN_OCCURRENCES)
    parser.add_argument("--min-amount", type=float, default=DEFAULT_MIN_AMOUNT)
    args = parser.parse_args()

    engine = db.get_engine()
    df_candidates = find_recurring_unbudgeted(
        engine, min_occurrences=args.min_occurrences, min_amount=args.min_amount
    )
    if len(df_candidates) == 0:
        print("expected_discover: no unbudgeted recurring transactions found")
        return 0
    print(df_candidates.to_string(index=False))
    print(f"\nexpected_discover: {len(df_candidates)} candidate(s)")
    return 0


if __name__ == "__main__":
    sys.exit(main())


# %%
