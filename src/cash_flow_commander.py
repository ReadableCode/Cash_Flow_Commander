# %%
# Running Imports #

import os
import warnings
from typing import Optional

import pandas as pd
import yaml
from dotenv import load_dotenv

from config import parent_dir
from readable_utils.display_tools import pprint_df, print_logger  # noqa F401
from readable_utils.google_tools import (
    WriteToSheets,
    clear_range_of_sheet_obj,
    get_book_sheet,
    get_book_sheet_df,
    write_df_to_range_of_sheet_obj,
)

warnings.filterwarnings("ignore")


# %%
# Environment #

dotenv_path = os.path.join(parent_dir, ".env")
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)

PROVIDERS_YAML_PATH = os.path.join(parent_dir, "providers.local.yaml")


# %%
# Class #


class SheetsStorage:
    """Google Sheets access for the Our_Cash workbook.

    The sheet is a RENDERED REPORT now — the expected-transaction tables in
    the database are the source of truth, and src/expected_forecast.py calls
    the write_* methods here to publish. The only tabs still READ are the
    manually-maintained balance entries for accounts we hold no transactions
    for (Account_Date_Balances, Account_Details).
    """

    def __init__(self):
        self._dict_sheets_dfs = {}

    def _get_sheet_data(self, key, sheet_name, force_update=False) -> pd.DataFrame:
        """Generic method to fetch and cache sheet data"""
        if key in self._dict_sheets_dfs and not force_update:
            return self._dict_sheets_dfs[key].copy()

        df = get_book_sheet_df("Our_Cash", sheet_name)
        self._dict_sheets_dfs[key] = df.copy()
        return df.copy()

    def get_account_balances(self, force_update=False):
        """Get account balances data"""
        df_account_balances = self._get_sheet_data(
            key="account_balances",
            sheet_name="Account_Date_Balances",
            force_update=force_update,
        )

        df_account_balances["Date"] = pd.to_datetime(
            df_account_balances["Date"]
        ).dt.date
        df_account_balances["Balance"] = df_account_balances["Balance"].astype(float)
        df_account_balances["Account_Name"] = df_account_balances[
            "Account_Name"
        ].astype(str)

        return df_account_balances

    def get_account_details(self, force_update=False):
        """Get account details data"""
        df_account_details = self._get_sheet_data(
            key="account_details",
            sheet_name="Account_Details",
            force_update=force_update,
        )

        df_account_details["Account_Name"] = df_account_details["Account_Name"].astype(
            str
        )
        df_account_details["Category"] = df_account_details["Category"].astype(str)
        df_account_details["Sub_Category"] = df_account_details["Sub_Category"].astype(
            str
        )

        return df_account_details

    def write_transaction_report(self, df_future_cast):
        """Write the transactions report to Google Sheets"""
        WriteToSheets(
            "Our_Cash",
            "Transactions_Report",
            df_future_cast,
        )
        print_logger("Transactions report updated successfully.")

    def write_daily_balance_report(self, df_daily_balance_report):
        """Write the daily balance report to Google Sheets"""
        WriteToSheets(
            "Our_Cash",
            "Daily_Balance_Report",
            df_daily_balance_report,
        )
        print_logger("Daily balance report updated successfully.")

    def write_sheets_summary_page(
        self, df_future_cast_alert_dates, df_future_cast_label_dates
    ):
        """Write the summary page to Google Sheets"""
        sheet_summary = get_book_sheet("Our_Cash", "Summary")

        # Clear existing data
        clear_range_of_sheet_obj(sheet_obj=sheet_summary, start="A11", end="B41")
        clear_range_of_sheet_obj(sheet_obj=sheet_summary, start="A44", end="C74")

        # Write alert dates
        write_df_to_range_of_sheet_obj(
            sheet_obj=sheet_summary,
            df=df_future_cast_alert_dates.head(30),
            start="A11",
            fit=False,
            copy_head=True,
        )

        # Write one-time transactions
        write_df_to_range_of_sheet_obj(
            sheet_obj=sheet_summary,
            df=df_future_cast_label_dates.head(30),
            start="A44",
            fit=False,
            copy_head=True,
        )

        print_logger("Summary page updated successfully.")


# %%


class OurCashData:
    """Report shaping for the sheet pages, fed by the database forecast."""

    def __init__(self, sheets_storage: Optional[SheetsStorage] = None):
        self.sheets_storage = sheets_storage or SheetsStorage()
        self.THRESHOLD_FOR_ALERT = 1000

    def generate_account_balances_report(self):
        df_pivot: pd.DataFrame = self.sheets_storage.get_account_balances()

        # Merge back with the original DataFrame to include the Sub_Category
        df_account_details: pd.DataFrame = self.sheets_storage.get_account_details()
        df_account_details = df_account_details[
            ["Account_Name", "Category", "Sub_Category"]
        ]

        df_pivot = df_pivot.pivot(
            index="Date", columns="Account_Name", values="Balance"
        )
        df_pivot = df_pivot.reset_index()

        # Forward fill missing values for each account
        df_pivot = df_pivot.ffill()
        # fillna with 0
        df_pivot = df_pivot.fillna(0)

        # for each category, add a column with sum of columns that are in that category
        for category in df_account_details["Category"].unique():
            accounts_in_category = df_account_details[
                df_account_details["Category"] == category
            ]["Account_Name"].tolist()
            # check if accounts_in_category are in df_pivot columns
            accounts_in_category = [
                account
                for account in accounts_in_category
                if account in df_pivot.columns
            ]
            if len(accounts_in_category) > 0:
                df_pivot[f"Total_{category}"] = df_pivot[accounts_in_category].sum(
                    axis=1
                )

        # Forward fill missing values for each account
        df_pivot = df_pivot.ffill()
        # fillna with 0
        df_pivot = df_pivot.fillna(0)
        ls_non_sum_cols = ["Date"]
        df_pivot["Total"] = df_pivot[
            [col for col in df_pivot.columns if col not in ls_non_sum_cols]
        ].sum(axis=1)

        df_pivot = df_pivot.sort_values(by=["Date"])

        return df_pivot

    def write_account_balances_report(self, df_pivot):
        """Write the account balances report to Google Sheets"""
        WriteToSheets(
            "Our_Cash",
            "Account_Balances_Report",
            df_pivot,
        )
        print_logger("Account balances report updated successfully.")

    def get_emergency_fund_amount(self):
        """Six months of essential expenses, hardcoded in providers.local.yaml.

        (our_cash.emergency_fund, a positive dollar amount). Covers checking
        and savings together — the forecast line only charts checking, which
        is fine until we forecast per account.
        """
        with open(PROVIDERS_YAML_PATH, "r", encoding="utf-8") as handle:
            config = yaml.safe_load(handle) or {}
        return float(config.get("our_cash", {}).get("emergency_fund", 0))

    def isolate_label_dates(self, df_future_cast):
        df_future_cast_label_dates = df_future_cast.copy()

        df_future_cast_label_dates = df_future_cast_label_dates[
            df_future_cast_label_dates["Type"] == "oncely"
        ]
        df_future_cast_label_dates = df_future_cast_label_dates.rename(
            columns={"Amount": "Label_Amount", "Account_Name": "Label_Item"}
        )
        df_future_cast_label_dates = df_future_cast_label_dates[
            ["Date", "Label_Item", "Label_Amount"]
        ]

        return df_future_cast_label_dates

    def isolate_ending_daily_balance(self, df_future_cast):
        df_future_cast_end_of_each_day = df_future_cast.copy()

        df_future_cast_end_of_each_day = df_future_cast_end_of_each_day[
            [
                "Date",
                "Running_Balance",
            ]
        ]
        df_future_cast_end_of_each_day = df_future_cast_end_of_each_day.drop_duplicates(
            subset=["Date"], keep="last"
        )

        return df_future_cast_end_of_each_day

    def generate_future_cast_alert_dates_df(self, df_future_cast):
        df_future_cast_alert_dates = self.isolate_ending_daily_balance(df_future_cast)

        df_future_cast_alert_dates = df_future_cast_alert_dates[
            df_future_cast_alert_dates["Running_Balance"] < self.THRESHOLD_FOR_ALERT
        ]

        # date is after ten days ago
        df_future_cast_alert_dates = df_future_cast_alert_dates[
            (
                pd.to_datetime("today")
                - pd.to_datetime(df_future_cast_alert_dates["Date"])
            ).dt.days
            <= 1
        ]

        df_future_cast_alert_dates = df_future_cast_alert_dates[
            ["Date", "Running_Balance"]
        ]

        return df_future_cast_alert_dates

    def generate_daily_balance_report(self, df_future_cast):
        df_future_cast_end_of_each_day = self.isolate_ending_daily_balance(
            df_future_cast
        )

        df_future_cast_label_dates = self.isolate_label_dates(df_future_cast)

        df_future_cast_end_of_each_day = pd.merge(
            df_future_cast_end_of_each_day,
            df_future_cast_label_dates,
            how="left",
            on="Date",
        )

        df_future_cast_end_of_each_day["Label_Item"] = df_future_cast_end_of_each_day[
            "Label_Item"
        ].fillna("")

        df_future_cast_end_of_each_day["Emergency_Fund_Amount"] = (
            self.get_emergency_fund_amount()
        )
        df_future_cast_end_of_each_day["Alert_Threshold"] = self.THRESHOLD_FOR_ALERT
        df_future_cast_end_of_each_day["Zero"] = 0

        df_future_cast_end_of_each_day["Date"] = pd.to_datetime(
            df_future_cast_end_of_each_day["Date"]
        )

        # date is after ten days ago
        df_future_cast_end_of_each_day = df_future_cast_end_of_each_day[
            (
                pd.to_datetime("today")
                - pd.to_datetime(df_future_cast_end_of_each_day["Date"])
            ).dt.days
            <= 10
        ]

        return df_future_cast_end_of_each_day


# %%
