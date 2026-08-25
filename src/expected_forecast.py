"""Future cash forecast built from expected occurrences, not the sheet.

This replaces the manual half of the old Google Sheets flow. The sheet's
Transactions_Report becomes a RENDERED report: this module computes it from
the database and writes it out, and nobody types Amount_Paid into it again.

The model is one sentence:

    future balance = anchor balance + every unpaid, unskipped occurrence
                     from the anchor date forward.

One deliberate exception: card-payoff occurrences are excluded — this is a
PLANNED-NEEDS forecast, and card spending beyond the budgeted bills (which
already deduct on their own due dates) is optional. See _drop_card_payoffs.

Paid occurrences drop out on their own — their real transactions are already
inside the anchor balance. The anchor comes straight from the bank: Chase
bank exports carry a running balance on every transaction, so the newest
balance-bearing transaction on the anchor account IS the current balance.

    uv run python src/expected_forecast.py             # print a preview
    uv run python src/expected_forecast.py --publish   # write the sheet reports
"""

# %%
# Imports #

import argparse
import datetime
import os
import sys

import pandas as pd
import yaml
from sqlalchemy import delete, func, insert, select

_SRC_DIR = os.path.dirname(os.path.abspath(__file__))
if _SRC_DIR not in sys.path:
    sys.path.insert(0, _SRC_DIR)

import db  # noqa: E402
import expected_store  # noqa: E402

# %%
# Constants #

_REPO_ROOT = os.path.dirname(_SRC_DIR)
PROVIDERS_YAML_PATH = os.path.join(_REPO_ROOT, "providers.local.yaml")

HORIZON_DAYS = 365 * 2

# Recently-paid rows are shown (with their actuals) for context, and unpaid
# rows older than this still count against the balance — an unpaid bill does
# not stop being owed just because it is late.
DAYS_BACK_SHOWN = 60

# The sheet's old Type vocabulary, kept so downstream tabs and charts that
# filter on 'oncely' keep working.
SHEET_TYPE_NAMES = {"once": "oncely", "every_x_days": "everyXDays"}


# %%
# Config #


def load_forecast_config() -> dict:
    """Anchor account and account labels from providers.local.yaml.

    Labels merge from every provider section that enumerates accounts under
    external_ids.accounts (chase and citi both do).
    """
    with open(PROVIDERS_YAML_PATH, "r", encoding="utf-8") as handle:
        config = yaml.safe_load(handle) or {}
    labels = {}
    for provider_entry in config.values():
        if not isinstance(provider_entry, dict):
            continue
        accounts = provider_entry.get("external_ids", {}).get("accounts", {})
        for account_id, details in accounts.items():
            labels[str(account_id)] = details.get("label", str(account_id))
    return {
        "anchor_account_id": str(
            config.get("our_cash", {}).get("forecast_anchor_account", "")
        ),
        "account_labels": labels,
        "emergency_fund": float(config.get("our_cash", {}).get("emergency_fund", 0)),
    }


# %%
# Anchor #


def get_anchor(engine, account_id: str):
    """(date, balance) after the LAST transaction of the newest balanced day.

    Several transactions can land on one day and row ids don't promise their
    order, so the running-balance chain itself decides which came last: each
    row's balance equals the previous row's balance plus its own amount, and
    the final row is the one no other row's balance points back to. Without
    this, a payday anchor could read the pre-autopay balance and overstate
    the whole forecast.
    """
    day_stmt = select(func.max(db.transactions.c.post_date)).where(
        db.transactions.c.account_id == account_id,
        db.transactions.c.balance.is_not(None),
    )
    with engine.connect() as conn:
        anchor_date = conn.execute(day_stmt).scalar()
    if anchor_date is None:
        raise ValueError(f"No balance-bearing transactions for account {account_id!r}")

    rows_stmt = (
        select(db.transactions.c.amount, db.transactions.c.balance)
        .where(
            db.transactions.c.account_id == account_id,
            db.transactions.c.post_date == anchor_date,
            db.transactions.c.balance.is_not(None),
        )
        .order_by(db.transactions.c.id)
    )
    with engine.connect() as conn:
        day_rows = [
            (float(r.amount), float(r.balance)) for r in conn.execute(rows_stmt)
        ]

    # A row is NOT last if some other row continues the chain from it.
    predecessor_balances = {round(balance - amount, 2) for amount, balance in day_rows}
    last_candidates = [
        balance
        for amount, balance in day_rows
        if round(balance, 2) not in predecessor_balances
    ]
    if len(last_candidates) == 1:
        return anchor_date, last_candidates[0]
    # Chain ambiguous (identical amounts, restated rows): any of the day's
    # balances is close; take the one from the highest row id as before.
    return anchor_date, day_rows[-1][1]


def get_paid_dates(engine) -> dict:
    """occurrence_id -> latest matched transaction date (for the Date_Paid column)."""
    df_matches = expected_store.get_active_matches_df(engine)
    paid_dates = {}
    for _, match_row in df_matches.iterrows():
        occurrence_id = int(match_row["occurrence_id"])
        match_date = pd.to_datetime(match_row["txn_post_date"]).date()
        if occurrence_id not in paid_dates or match_date > paid_dates[occurrence_id]:
            paid_dates[occurrence_id] = match_date
    return paid_dates


# %%
# Card payoff exclusion #


def _drop_card_payoffs(engine, df_status: pd.DataFrame) -> pd.DataFrame:
    """Remove card-payoff occurrences: this is a PLANNED-NEEDS forecast.

    Jason's call (2026-08-24, reversing an estimate-based approach tried the
    same day): the checking forecast shows planned expenses only. Bills
    billed to a card already reduce checking on their own due dates, and the
    card spend beyond them is optional — the whole point of the forecast is
    what the plan REQUIRES, not what current habits would spend. Card
    payoffs (transfer series whose target account is credit-kind) therefore
    contribute nothing here; they stay visible on the budget dashboard's
    Transfers panel instead. Bank-target transfers (savings moves) are
    planned needs and keep deducting.

    Excluding rather than zeroing also prevents a real double count: a
    payoff that already left checking before the anchor date is inside the
    anchor balance, and its still-unpaired occurrence would deduct it again.
    """
    with engine.connect() as conn:
        card_accounts = {
            row[0]
            for row in conn.execute(
                select(db.transactions.c.account_id)
                .where(db.transactions.c.account_kind == "credit")
                .distinct()
            )
        }
    is_card_payoff = df_status["is_transfer"].fillna(False).astype(bool) & df_status[
        "transfer_account_id"
    ].isin(card_accounts)
    return df_status[~is_card_payoff]


# %%
# Forecast #


def build_future_cast(
    engine,
    horizon_days: int = HORIZON_DAYS,
    config: dict | None = None,
    days_back_shown: int | None = DAYS_BACK_SHOWN,
) -> pd.DataFrame:
    """The Transactions_Report dataframe, same columns the sheet always had.

    Recently-paid rows show their actual net and paid date and leave the
    running balance alone. Unpaid rows (including late ones, and 'broken' /
    'net_zero' ones — effectively unpaid) chain the balance forward from the
    anchor. Skipped rows appear IN ORDER but move nothing — they are part of
    the story of the money even though they are not owed. The sheet publisher
    strips them (the sheet never showed skipped rows); the TUI shows all of
    it.
    """
    if config is None:
        config = load_forecast_config()
    anchor_date, anchor_balance = get_anchor(engine, config["anchor_account_id"])
    account_labels = config["account_labels"]

    # Unpaid rows reach back over ALL of history — an unpaid bill does not
    # stop counting against the balance however old it is, exactly like the
    # sheet this replaced. Resolved rows (paid, skipped) are context only:
    # the TUI shows the recent window (days_back_shown) plus everything
    # ahead, while the sheet publish passes None to keep the full paid
    # history the sheet always carried.
    start = datetime.date(2000, 1, 1)
    end = datetime.date.today() + datetime.timedelta(days=horizon_days)
    df_status = expected_store.get_occurrence_status_df(engine, start, end)
    df_status = _drop_card_payoffs(engine, df_status)
    if days_back_shown is not None:
        shown_from = anchor_date - datetime.timedelta(days=days_back_shown)
        is_recent = pd.to_datetime(df_status["due_date"]).dt.date >= shown_from
        is_resolved = df_status["status"].isin(["paid", "skipped"])
        df_status = df_status[~is_resolved | is_recent]
    paid_dates = get_paid_dates(engine)

    df_status = df_status.sort_values(["due_date", "amount"]).reset_index(drop=True)

    rows = []
    running_balance = anchor_balance
    for _, occurrence in df_status.iterrows():
        is_paid = occurrence["status"] == "paid"
        amount = float(occurrence["amount"])
        if not is_paid and occurrence["status"] != "skipped":
            running_balance = round(running_balance + amount, 2)
        account_id = occurrence["auto_pay_account_id"]
        paid_date = paid_dates.get(int(occurrence["occurrence_id"]))
        rows.append(
            {
                "Date": str(occurrence["due_date"]),
                "Category": occurrence["category"],
                "Type": SHEET_TYPE_NAMES.get(
                    occurrence["schedule_type"], occurrence["schedule_type"]
                ),
                "Account_Name": occurrence["series_name"],
                "Auto_Pay_Account": (
                    account_labels.get(str(account_id), "")
                    if pd.notna(account_id)
                    else ""
                ),
                "Amount": amount,
                "Amount_Paid": (
                    round(float(occurrence["net_amount"]), 2) if is_paid else ""
                ),
                "Date_Paid": str(paid_date) if is_paid and paid_date else "",
                "Running_Balance": running_balance,
                # Internal columns (leading underscore): consumed by the TUI's
                # Forecast screen so rows can be paired/skipped in place;
                # stripped before anything is published to the sheet.
                "_occurrence_id": int(occurrence["occurrence_id"]),
                "_status": occurrence["status"],
            }
        )
    return pd.DataFrame(rows)


# %%
# Daily projection for Grafana #


def rebuild_forecast_days(
    engine, horizon_days: int = HORIZON_DAYS, config: dict | None = None
) -> int:
    """Rebuild the forecast_days table: one row per day from the anchor out.

    Within a day the ORDER money moves is unknowable ahead of time, so we
    assume the worst: every outflow lands before any inflow. trough_balance
    is that worst moment; end_balance is where the day actually closes.
    A trough below zero is the day a bill could bounce even though the
    day ends positive.

    Unpaid occurrences already past due land on tomorrow — the money is
    still owed and could leave at any moment.
    """
    if config is None:
        config = load_forecast_config()
    anchor_date, anchor_balance = get_anchor(engine, config["anchor_account_id"])

    first_day = anchor_date + datetime.timedelta(days=1)
    last_day = datetime.date.today() + datetime.timedelta(days=horizon_days)
    # ALL of history: an unpaid occurrence never ages out of being owed.
    df_status = expected_store.get_occurrence_status_df(
        engine, datetime.date(2000, 1, 1), last_day
    )
    df_status = df_status[~df_status["status"].isin(["skipped", "paid"])]
    df_status = _drop_card_payoffs(engine, df_status)

    outflows_by_day: dict = {}
    inflows_by_day: dict = {}
    for _, occurrence in df_status.iterrows():
        amount = float(occurrence["amount"])
        day = pd.to_datetime(occurrence["due_date"]).date()
        if day < first_day:
            day = first_day
        if amount < 0:
            outflows_by_day[day] = outflows_by_day.get(day, 0.0) + amount
        else:
            inflows_by_day[day] = inflows_by_day.get(day, 0.0) + amount

    generated_at = datetime.datetime.now()
    emergency_fund = config.get("emergency_fund", 0.0)
    rows = []
    balance = anchor_balance
    day = first_day
    while day <= last_day:
        outflows = round(outflows_by_day.get(day, 0.0), 2)
        inflows = round(inflows_by_day.get(day, 0.0), 2)
        trough = round(balance + outflows, 2)
        end = round(trough + inflows, 2)
        rows.append(
            {
                "day": day,
                "start_balance": round(balance, 2),
                "outflows": outflows,
                "inflows": inflows,
                "trough_balance": trough,
                "end_balance": end,
                "emergency_fund": emergency_fund,
                "generated_at": generated_at,
            }
        )
        balance = end
        day = day + datetime.timedelta(days=1)

    with engine.begin() as conn:
        conn.execute(delete(db.forecast_days))
        conn.execute(insert(db.forecast_days), rows)
    return len(rows)


# %%
# Run #


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--publish", action="store_true", help="write the Google Sheet reports"
    )
    args = parser.parse_args()

    engine = db.get_engine()
    config = load_forecast_config()
    anchor_date, anchor_balance = get_anchor(engine, config["anchor_account_id"])
    print(
        f"anchor: {anchor_balance:.2f} on {anchor_date} (account {config['anchor_account_id']})"
    )

    day_count = rebuild_forecast_days(engine)
    print(f"forecast_days rebuilt: {day_count} days (Grafana reads this)")

    # The sheet keeps its full paid history (days_back_shown=None), like it
    # always had before the DB took over.
    df_future_cast = build_future_cast(engine, days_back_shown=None)
    print(
        f"future cast: {len(df_future_cast)} rows, {df_future_cast['Date'].min()} → {df_future_cast['Date'].max()}"
    )
    unpaid = df_future_cast[df_future_cast["Amount_Paid"] == ""]
    lowest = (
        unpaid.loc[pd.to_numeric(unpaid["Running_Balance"]).idxmin()]
        if len(unpaid)
        else None
    )
    if lowest is not None:
        print(
            f"lowest projected balance: {lowest['Running_Balance']:.2f} on {lowest['Date']}"
        )

    # The TUI shows skipped rows in order; the sheet never did. Internal
    # (underscore) columns exist for the TUI; the sheet gets the exact rows
    # and columns it always had.
    df_future_cast = df_future_cast[df_future_cast["_status"] != "skipped"]
    df_future_cast = df_future_cast[
        [name for name in df_future_cast.columns if not name.startswith("_")]
    ]

    if not args.publish:
        print("\n(preview only — pass --publish to write the sheet)")
        print(df_future_cast.head(30).to_string(index=False))
        return 0

    # The sheet-writing helpers (and the summary/daily-balance pages that
    # hang off the future cast) already exist in the legacy module; reuse them.
    from cash_flow_commander import OurCashData, SheetsStorage

    sheets_storage = SheetsStorage()
    our_cash_data = OurCashData(sheets_storage)
    sheets_storage.write_transaction_report(df_future_cast)
    df_daily = our_cash_data.generate_daily_balance_report(df_future_cast)
    sheets_storage.write_daily_balance_report(df_daily)
    sheets_storage.write_sheets_summary_page(
        our_cash_data.generate_future_cast_alert_dates_df(df_future_cast),
        our_cash_data.isolate_label_dates(df_future_cast),
    )
    our_cash_data.write_account_balances_report(
        our_cash_data.generate_account_balances_report()
    )
    print(
        "published: Transactions_Report, Daily_Balance_Report, Summary, "
        "Account_Balances_Report"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())


# %%
