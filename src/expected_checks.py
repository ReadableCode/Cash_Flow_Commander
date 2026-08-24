"""Report expected-transaction problems that no screen makes loud on its own.

The pairing screen shows one month at a time, so a mortgage that quietly
un-paid itself three months back (Chase restated the transaction, a reversal
landed, a re-download pruned a row) would sit invisible forever. This walks
the recent window and reports:

- broken:    a match whose transaction no longer exists — re-pair it.
- net_zero:  matches that cancel out (payment + reversal) — effectively unpaid.
- past_due:  unpaid occurrences older than the grace period.
- one_leg:   a transfer marked paid whose counterparty leg was never matched
             (money left the bank, nothing landed on the card).
- drift:     paid, but the actual differs from expected by more than the
             series tolerance — the early-warning for a price increase.

Usage:

    uv run python src/expected_checks.py
    uv run python src/expected_checks.py --days-back 180

Exit code is 1 when anything is found.
"""

# %%
# Imports #

import argparse
import datetime
import os
import sys

import pandas as pd

_SRC_DIR = os.path.dirname(os.path.abspath(__file__))
if _SRC_DIR not in sys.path:
    sys.path.insert(0, _SRC_DIR)

import db  # noqa: E402
import expected_store  # noqa: E402

# %%
# Constants #

DEFAULT_DAYS_BACK = 90

# Days past due before an unpaid occurrence is worth flagging. Payments
# routinely land a few days after their due date.
GRACE_DAYS = 7


# %%
# Checks #


def find_problems(engine, days_back: int = DEFAULT_DAYS_BACK) -> pd.DataFrame:
    """One row per problem: problem, series_name, due_date, detail."""
    today = datetime.date.today()
    start = today - datetime.timedelta(days=days_back)
    df_status = expected_store.get_occurrence_status_df(engine, start, today)
    df_matches = expected_store.get_active_matches_df(engine)

    problems = []
    for _, row in df_status.iterrows():
        expected_amount = float(row["amount"])
        due_date = pd.to_datetime(row["due_date"]).date()

        if row["status"] == "broken":
            problems.append(
                _problem("broken", row, "a matched transaction no longer exists")
            )

        if row["status"] == "net_zero":
            problems.append(
                _problem(
                    "net_zero",
                    row,
                    f"matches net to {row['net_amount']:.2f} — reversed?",
                )
            )

        if row["status"] == "unpaid" and expected_amount != 0:
            if (today - due_date).days > GRACE_DAYS:
                problems.append(
                    _problem("past_due", row, f"expected {expected_amount:.2f}")
                )

        if row["status"] == "paid" and pd.notna(row["transfer_account_id"]):
            occurrence_matches = df_matches[
                df_matches["occurrence_id"] == row["occurrence_id"]
            ]
            legs_on_transfer_account = occurrence_matches[
                occurrence_matches["txn_account_id"].astype(str)
                == str(row["transfer_account_id"])
            ]
            if len(legs_on_transfer_account) == 0:
                problems.append(
                    _problem(
                        "one_leg",
                        row,
                        f"cash left but nothing matched on account {row['transfer_account_id']}",
                    )
                )

        if row["status"] == "paid" and expected_amount != 0:
            tolerance = row["amount_tolerance"]
            if pd.isna(tolerance):
                tolerance = expected_store.DEFAULT_AMOUNT_TOLERANCE
            if abs(row["amount_diff"]) > float(tolerance):
                problems.append(
                    _problem(
                        "drift",
                        row,
                        f"expected {expected_amount:.2f}, actually {row['net_amount']:.2f}",
                    )
                )

    return pd.DataFrame(
        problems, columns=["problem", "series_name", "due_date", "detail"]
    )


def _problem(problem: str, row, detail: str) -> dict:
    return {
        "problem": problem,
        "series_name": row["series_name"],
        "due_date": row["due_date"],
        "detail": detail,
    }


# %%
# Run #


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--days-back", type=int, default=DEFAULT_DAYS_BACK)
    args = parser.parse_args()

    engine = db.get_engine()
    df_problems = find_problems(engine, days_back=args.days_back)

    if len(df_problems) == 0:
        print("expected_checks: no problems found")
        return 0

    print(df_problems.to_string(index=False))
    print(f"\nexpected_checks: {len(df_problems)} problem(s) found")
    return 1


if __name__ == "__main__":
    sys.exit(main())


# %%
