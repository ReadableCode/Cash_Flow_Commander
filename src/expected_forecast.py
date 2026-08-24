"""Future cash forecast built from expected occurrences, not the sheet.

This replaces the manual half of the old Google Sheets flow. The sheet's
Transactions_Report becomes a RENDERED report: this module computes it from
the database and writes it out, and nobody types Amount_Paid into it again.

The model is one sentence:

    future balance = anchor balance + every unpaid, unskipped occurrence
                     from the anchor date forward.

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
from sqlalchemy import delete, insert, select

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
    """Anchor account and account labels from providers.local.yaml."""
    with open(PROVIDERS_YAML_PATH, "r", encoding="utf-8") as handle:
        config = yaml.safe_load(handle) or {}
    accounts = config.get("chase", {}).get("external_ids", {}).get("accounts", {})
    labels = {}
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
    """(date, balance) of the newest balance-bearing transaction on the account."""
    stmt = (
        select(db.transactions.c.post_date, db.transactions.c.balance)
        .where(
            db.transactions.c.account_id == account_id,
            db.transactions.c.balance.is_not(None),
        )
        .order_by(db.transactions.c.post_date.desc(), db.transactions.c.id.desc())
        .limit(1)
    )
    with engine.connect() as conn:
        row = conn.execute(stmt).first()
    if row is None:
        raise ValueError(f"No balance-bearing transactions for account {account_id!r}")
    return row.post_date, float(row.balance)


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
# Forecast #


def build_future_cast(
    engine, horizon_days: int = HORIZON_DAYS, config: dict | None = None
) -> pd.DataFrame:
    """The Transactions_Report dataframe, same columns the sheet always had.

    Recently-paid rows show their actual net and paid date and leave the
    running balance alone. Unpaid rows (including late ones, and 'broken' /
    'net_zero' ones — effectively unpaid) chain the balance forward from the
    anchor. Skipped occurrences do not appear at all.
    """
    if config is None:
        config = load_forecast_config()
    anchor_date, anchor_balance = get_anchor(engine, config["anchor_account_id"])
    account_labels = config["account_labels"]

    start = anchor_date - datetime.timedelta(days=DAYS_BACK_SHOWN)
    end = datetime.date.today() + datetime.timedelta(days=horizon_days)
    df_status = expected_store.get_occurrence_status_df(engine, start, end)
    df_status = df_status[df_status["status"] != "skipped"]
    paid_dates = get_paid_dates(engine)

    df_status = df_status.sort_values(["due_date", "amount"]).reset_index(drop=True)

    rows = []
    running_balance = anchor_balance
    for _, occurrence in df_status.iterrows():
        is_paid = occurrence["status"] == "paid"
        if not is_paid:
            running_balance = round(running_balance + float(occurrence["amount"]), 2)
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
                "Amount": float(occurrence["amount"]),
                "Amount_Paid": (
                    round(float(occurrence["net_amount"]), 2) if is_paid else ""
                ),
                "Date_Paid": str(paid_date) if is_paid and paid_date else "",
                "Running_Balance": running_balance,
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
    df_status = expected_store.get_occurrence_status_df(
        engine, anchor_date - datetime.timedelta(days=365), last_day
    )
    df_status = df_status[~df_status["status"].isin(["skipped", "paid"])]

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

    df_future_cast = build_future_cast(engine)
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
