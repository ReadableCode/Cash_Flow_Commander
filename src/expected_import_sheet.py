"""One-time import of the Our_Cash sheet into the expected-transaction tables.

Reads the backed-up CSVs (never the live sheet — the backup is the record of
what was imported) and builds:

- expected_series      from Income_Expense rows. Names that appear only in
                       Transactions_Report are bills that were replaced before
                       this import existed; they become CLOSED series so their
                       history has something to belong to.
- expected_occurrences past Transactions_Report rows verbatim (they ARE the
                       history), plus generated future occurrences.
- expected_matches     best-effort: each row the sheet says was paid is looked
                       up in the transactions table by exact amount near the
                       paid date. Only an unambiguous single hit becomes a
                       match — everything else is left unpaid for the pairing
                       screen, and listed in the import report.

The sheet's Auto_Pay_Account labels ('Chase Bank') map to transaction account
ids ('1234') through the our_cash section of providers.local.yaml, so no
personal account data lives in this public repo.

Usage:

    uv run python src/expected_import_sheet.py --backup-dir data/sheet_backups/2026-08-24
    uv run python src/expected_import_sheet.py --backup-dir ... --wipe   # start over

Refuses to run twice without --wipe: a re-import would duplicate series and
could clobber pairing work done since.
"""

# %%
# Imports #

import argparse
import datetime
import os
import sys

import pandas as pd
import yaml
from sqlalchemy import delete, func, select

_SRC_DIR = os.path.dirname(os.path.abspath(__file__))
if _SRC_DIR not in sys.path:
    sys.path.insert(0, _SRC_DIR)

import db  # noqa: E402
import expected_store  # noqa: E402

# %%
# Constants #

_REPO_ROOT = os.path.dirname(_SRC_DIR)
PROVIDERS_YAML_PATH = os.path.join(_REPO_ROOT, "providers.local.yaml")

# Maturity dates at or past this year mean 'no end date'.
FOREVER_YEAR = 2100

# A sheet payment is looked for within this many days of its Date_Paid.
MATCH_WINDOW_DAYS = 5


# %%
# Config #


def load_our_cash_config() -> dict:
    """The our_cash section of providers.local.yaml (aliases live there)."""
    with open(PROVIDERS_YAML_PATH, "r", encoding="utf-8") as handle:
        config = yaml.safe_load(handle)
    our_cash = config.get("our_cash", {})
    our_cash.setdefault("account_aliases", {})
    our_cash.setdefault("transfer_accounts", {})
    return our_cash


# %%
# Series from Income_Expense #


def build_series_rows(
    df_income_expense: pd.DataFrame, df_report: pd.DataFrame, config: dict
):
    """Turn Income_Expense rows into expected_series dicts.

    Returns (series_rows, once_occurrence_rows). 'oncely' sheet rows sharing a
    name (e.g. three 'Alta Rodent Payment' installments) become ONE series of
    schedule_type 'once' with one occurrence per row.
    """
    account_aliases = config["account_aliases"]
    transfer_accounts = config["transfer_accounts"]

    # Earliest report date per name = when we first started planning this bill.
    earliest_report_date = (
        df_report.groupby("Account_Name")["Date"].min().to_dict()
        if len(df_report)
        else {}
    )
    today = datetime.date.today()

    series_rows = []
    once_occurrence_rows = []
    seen_once_names = set()

    for _, sheet_row in df_income_expense.iterrows():
        name = str(sheet_row["Account_Name"]).strip()
        sheet_type = str(sheet_row["Type"]).strip()

        if sheet_type == "oncely":
            once_occurrence_rows.append(
                {
                    "series_name": name,
                    "due_date": pd.to_datetime(
                        sheet_row["When"], format="%m/%d/%Y"
                    ).date(),
                    "amount": float(sheet_row["Amount"]),
                }
            )
            if name in seen_once_names:
                continue  # one series covers all this name's dates
            seen_once_names.add(name)

        series = {
            "name": name,
            "category": str(sheet_row["Category"]).strip(),
            "sub_category": str(sheet_row["Sub_Category"]).strip(),
            "amount": float(sheet_row["Amount"]),
            "auto_pay_account_id": account_aliases.get(
                str(sheet_row["Auto_Pay_Account"]).strip()
            ),
            "is_transfer": str(sheet_row["Category"]).strip() == "Credit Card",
            "transfer_account_id": transfer_accounts.get(name),
            "active_from": earliest_report_date.get(name, today),
            "active_until": _parse_maturity(sheet_row["Maturity Date"]),
            "notes": "imported from Income_Expense",
        }
        series.update(_parse_schedule(sheet_type, sheet_row))
        series_rows.append(series)

    return series_rows, once_occurrence_rows


def _parse_schedule(sheet_type: str, sheet_row) -> dict:
    """Map the sheet's Type/When/AfterDays columns onto schedule columns."""
    if sheet_type == "monthly":
        return {"schedule_type": "monthly", "day_of_month": int(sheet_row["When"])}
    if sheet_type == "yearly":
        # '13-Jul' has no year; pin a leap year so a Feb 29 autopay parses.
        when = pd.to_datetime(str(sheet_row["When"]) + "-2024", format="%d-%b-%Y")
        return {
            "schedule_type": "yearly",
            "month_of_year": when.month,
            "day_of_month": when.day,
        }
    if sheet_type == "biweekly":
        anchor = pd.to_datetime(sheet_row["When"], format="%m/%d/%Y").date()
        return {"schedule_type": "biweekly", "anchor_date": anchor}
    if sheet_type == "everyXDays":
        anchor = pd.to_datetime(sheet_row["When"], format="%m/%d/%Y").date()
        return {
            "schedule_type": "every_x_days",
            "anchor_date": anchor,
            "interval_days": int(sheet_row["AfterDays"]),
        }
    if sheet_type == "oncely":
        return {"schedule_type": "once"}
    raise ValueError(f"Unknown sheet Type: {sheet_type!r}")


def _parse_maturity(maturity_text) -> datetime.date | None:
    maturity = pd.to_datetime(maturity_text, format="%m/%d/%Y", errors="coerce")
    if pd.isna(maturity) or maturity.year >= FOREVER_YEAR:
        return None
    return maturity.date()


def build_closed_series_rows(
    df_income_expense: pd.DataFrame, df_report: pd.DataFrame, config: dict
):
    """Series for names that exist only in Transactions_Report history.

    These are bills that were replaced or ended before this import (the old
    pest control, the paid-off card). We know their history but not their
    schedule details, so they get no schedule fields — nothing will ever be
    generated for them — and they are closed on their last report date.
    """
    account_aliases = config["account_aliases"]
    current_names = set(df_income_expense["Account_Name"].astype(str).str.strip())
    report_only = df_report[
        ~df_report["Account_Name"].astype(str).str.strip().isin(current_names)
    ]

    series_rows = []
    for name, df_name in report_only.groupby("Account_Name"):
        last_row = df_name.sort_values("Date").iloc[-1]
        series_rows.append(
            {
                "name": str(name).strip(),
                "category": str(last_row["Category"]).strip(),
                "sub_category": None,
                "schedule_type": "once",  # history only; dates were imported directly
                "amount": float(last_row["Amount"]),
                "auto_pay_account_id": account_aliases.get(
                    str(last_row["Auto_Pay_Account"]).strip()
                ),
                "is_transfer": str(last_row["Category"]).strip() == "Credit Card",
                "transfer_account_id": None,
                "active_from": df_name["Date"].min(),
                "active_until": df_name["Date"].max(),
                "notes": "imported from Transactions_Report history; already replaced or ended",
            }
        )
    return series_rows


# %%
# Match recovery #


def recover_matches(
    engine, df_report_past: pd.DataFrame, series_ids: dict, config: dict
):
    """Create matches for report rows the sheet says were paid.

    The sheet has no transaction key, so this is deliberately conservative:
    only an exact-amount hit within MATCH_WINDOW_DAYS of Date_Paid, that no
    other row has claimed, and that is the ONLY such hit, becomes a match.
    Everything else stays unpaid for the pairing screen.

    Returns a DataFrame describing what happened to every paid row.
    """
    account_aliases = config["account_aliases"]
    transfer_accounts = config["transfer_accounts"]

    df_transactions = pd.read_sql(
        select(
            db.transactions.c.account_id,
            db.transactions.c.post_date,
            db.transactions.c.description,
            db.transactions.c.amount,
            db.transactions.c.occurrence,
        ),
        engine,
    )
    df_transactions["amount"] = df_transactions["amount"].astype(float).round(2)
    df_transactions["post_date"] = pd.to_datetime(df_transactions["post_date"]).dt.date
    coverage_start = df_transactions["post_date"].min()

    df_occurrences = pd.read_sql(select(db.expected_occurrences), engine)
    df_occurrences["due_date"] = pd.to_datetime(df_occurrences["due_date"]).dt.date
    occurrence_ids = {}
    for _, occurrence_row in df_occurrences.iterrows():
        occurrence_ids[(occurrence_row["series_id"], occurrence_row["due_date"])] = int(
            occurrence_row["id"]
        )

    claimed_txn_keys = set()
    outcomes = []

    for _, report_row in df_report_past.iterrows():
        amount_paid = round(float(report_row["Amount_Paid"]), 2)
        if amount_paid == 0:
            continue  # nothing moved; the occurrence stays as-is

        name = str(report_row["Account_Name"]).strip()
        due_date = report_row["Date"]
        date_paid = pd.to_datetime(report_row["Date_Paid"], errors="coerce")

        outcome = {
            "series_name": name,
            "due_date": due_date,
            "amount_paid": amount_paid,
            "date_paid": None if pd.isna(date_paid) else date_paid.date(),
        }
        outcomes.append(outcome)

        series_id = series_ids.get(name)
        occurrence_id = occurrence_ids.get((series_id, due_date))
        if series_id is None or occurrence_id is None:
            outcome["result"] = "no_occurrence"
            continue
        if pd.isna(date_paid):
            outcome["result"] = "bad_date_paid"
            continue
        date_paid = date_paid.date()

        if date_paid < coverage_start:
            # Chase only keeps 24 months; this payment predates what we hold
            # and can never be paired. Skip the occurrence, preserving what
            # the sheet knew, so it stays out of the pairing queue.
            expected_store.skip_occurrence(
                engine,
                occurrence_id,
                f"sheet import: paid {amount_paid} on {date_paid}, predates transaction history",
            )
            outcome["result"] = "skipped_pre_coverage"
            continue

        preferred_account = account_aliases.get(
            str(report_row["Auto_Pay_Account"]).strip()
        )
        cash_txn = _find_single_hit(
            df_transactions, amount_paid, date_paid, preferred_account, claimed_txn_keys
        )
        if cash_txn is None:
            outcome["result"] = "no_unique_hit"
            continue

        _claim(engine, occurrence_id, cash_txn, claimed_txn_keys)
        outcome["result"] = "matched"

        # A card payoff has a second leg: +X landing on the card itself.
        transfer_account = transfer_accounts.get(name)
        if transfer_account is not None:
            leg_txn = _find_single_hit(
                df_transactions,
                -amount_paid,
                date_paid,
                transfer_account,
                claimed_txn_keys,
                only_preferred=True,
            )
            if leg_txn is not None:
                _claim(engine, occurrence_id, leg_txn, claimed_txn_keys)
                outcome["result"] = "matched_both_legs"

    return pd.DataFrame(outcomes)


def _find_single_hit(
    df_transactions,
    amount,
    date_paid,
    preferred_account,
    claimed_txn_keys,
    only_preferred=False,
):
    """The one unclaimed transaction of this amount near this date, or None.

    Tries the expected account first; a unique hit there wins even if other
    accounts also have one. Falls back to all accounts unless only_preferred.
    """
    window_start = date_paid - datetime.timedelta(days=MATCH_WINDOW_DAYS)
    window_end = date_paid + datetime.timedelta(days=MATCH_WINDOW_DAYS)
    df_hits = df_transactions[
        (df_transactions["amount"] == amount)
        & (df_transactions["post_date"] >= window_start)
        & (df_transactions["post_date"] <= window_end)
    ]

    if len(df_hits) == 0:
        return None

    is_unclaimed = []
    for _, txn_row in df_hits.iterrows():
        is_unclaimed.append(_key_of(txn_row) not in claimed_txn_keys)
    df_hits = df_hits[is_unclaimed]

    if preferred_account is not None:
        df_preferred = df_hits[
            df_hits["account_id"].astype(str) == str(preferred_account)
        ]
        if len(df_preferred) == 1:
            return df_preferred.iloc[0]
        if only_preferred:
            return None

    if len(df_hits) == 1:
        return df_hits.iloc[0]
    return None


def _key_of(txn_row) -> tuple:
    return (
        str(txn_row["account_id"]),
        txn_row["post_date"].isoformat(),
        str(txn_row["description"]),
        f"{txn_row['amount']:.2f}",
        int(txn_row["occurrence"]),
    )


def _claim(engine, occurrence_id, txn_row, claimed_txn_keys) -> None:
    expected_store.add_match(
        engine,
        occurrence_id,
        {
            "account_id": txn_row["account_id"],
            "post_date": txn_row["post_date"],
            "description": txn_row["description"],
            "amount": txn_row["amount"],
            "occurrence": txn_row["occurrence"],
        },
        source="import_sheet",
    )
    claimed_txn_keys.add(_key_of(txn_row))


# %%
# Run #


def wipe_expected_tables(engine) -> None:
    """Delete all expected data, children first. Only sane before adoption."""
    with engine.begin() as conn:
        conn.execute(delete(db.expected_matches))
        conn.execute(delete(db.expected_occurrences))
        conn.execute(delete(db.expected_series))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--backup-dir", required=True, help="dir holding the backed-up sheet CSVs"
    )
    parser.add_argument(
        "--wipe", action="store_true", help="delete existing expected data first"
    )
    args = parser.parse_args()

    engine = db.get_engine()
    db.create_tables(engine)

    if args.wipe:
        wipe_expected_tables(engine)
    with engine.connect() as conn:
        series_count = conn.execute(
            select(func.count()).select_from(db.expected_series)
        ).scalar()
    if series_count and series_count > 0:
        print(
            f"expected_series already holds {series_count} rows; re-run with --wipe to start over."
        )
        return 1

    config = load_our_cash_config()
    today = datetime.date.today()

    df_income_expense = pd.read_csv(os.path.join(args.backup_dir, "Income_Expense.csv"))
    df_report = pd.read_csv(os.path.join(args.backup_dir, "Transactions_Report.csv"))
    df_report["Date"] = pd.to_datetime(df_report["Date"]).dt.date
    df_report["Amount_Paid"] = df_report["Amount_Paid"].fillna(0).astype(float)
    # Future report rows are the OLD forecast; we regenerate those ourselves.
    df_report_past = df_report[df_report["Date"] < today]

    # Series.
    series_rows, once_occurrence_rows = build_series_rows(
        df_income_expense, df_report_past, config
    )
    closed_series_rows = build_closed_series_rows(
        df_income_expense, df_report_past, config
    )
    series_ids = {}
    for series in series_rows + closed_series_rows:
        series_ids[series["name"]] = expected_store.add_series(engine, series)
    print(
        f"series: {len(series_rows)} from Income_Expense, {len(closed_series_rows)} closed from history"
    )

    # Past occurrences, verbatim from the report.
    for _, report_row in df_report_past.iterrows():
        series_id = series_ids.get(str(report_row["Account_Name"]).strip())
        if series_id is None:
            continue
        expected_store.add_occurrence(
            engine,
            series_id,
            report_row["Date"],
            float(report_row["Amount"]),
            source="import_sheet",
        )
    print(f"occurrences: {len(df_report_past)} past report rows imported")

    # 'once' dates from Income_Expense (past AND future — future oncely rows
    # like next year's taxes are real commitments, not forecast noise).
    for once_row in once_occurrence_rows:
        expected_store.add_occurrence(
            engine,
            series_ids[once_row["series_name"]],
            once_row["due_date"],
            once_row["amount"],
            source="import_sheet",
        )

    generated = expected_store.generate_occurrences(engine)
    print(f"occurrences: {generated} future occurrences generated")

    # Matches.
    df_outcomes = recover_matches(engine, df_report_past, series_ids, config)
    report_path = os.path.join(args.backup_dir, "import_report.csv")
    df_outcomes.to_csv(report_path, index=False)
    print(f"\nmatch recovery ({len(df_outcomes)} paid rows) -> {report_path}")
    print(df_outcomes["result"].value_counts().to_string())
    return 0


if __name__ == "__main__":
    sys.exit(main())


# %%
