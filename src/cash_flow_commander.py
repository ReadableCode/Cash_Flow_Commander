# %%
# Running Imports #

import os
import warnings
from typing import Optional

import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm

from config import data_dir, parent_dir
from utils.display_tools import pprint_df, print_logger
from utils.google_tools import (
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


# %%
# Class #


class SheetsStorage:
    """Handles all Google Sheets data access and caching"""

    def __init__(self):
        self._dict_sheets_dfs = {}
        self._sheet_id = os.getenv("OUR_CASH_SHEET_ID")
        self._sheet_link = (
            f"https://docs.google.com/spreadsheets/d/{self._sheet_id}/edit#gid=0"
        )

    def _get_sheet_data(self, key, sheet_name, force_update=False) -> pd.DataFrame:
        """Generic method to fetch and cache sheet data"""
        if key in self._dict_sheets_dfs and not force_update:
            return self._dict_sheets_dfs[key].copy()

        df = get_book_sheet_df("Our_Cash", sheet_name)
        self._dict_sheets_dfs[key] = df.copy()
        return df.copy()

    def get_income_expense_df(self, force_update=False):
        """Get income/expense data with proper data type conversion"""
        df_income_expense = self._get_sheet_data(
            key="income_expense_df",
            sheet_name="Income_Expense",
            force_update=force_update,
        )

        df_income_expense["Amount"] = df_income_expense["Amount"].astype(float)
        df_income_expense["Maturity Date"] = pd.to_datetime(
            df_income_expense["Maturity Date"]
        ).dt.date
        df_income_expense["AfterDays"] = df_income_expense["AfterDays"].astype(int)
        df_income_expense["Auto_Pay_Amount"] = df_income_expense[
            "Auto_Pay_Amount"
        ].astype(str)
        df_income_expense["AverageMonthlyCost"] = df_income_expense[
            "AverageMonthlyCost"
        ].astype(float)
        df_income_expense["Balance"] = (
            df_income_expense["Balance"].replace("", 0).astype(float)
        )
        df_income_expense["Limit"] = (
            df_income_expense["Limit"].replace("", 0).astype(float)
        )
        df_income_expense["Available Credit"] = (
            df_income_expense["Available Credit"].replace("", 0).astype(float)
        )
        df_income_expense["Interest Rate"] = (
            df_income_expense["Interest Rate"]
            .str.replace("%", "")
            .replace("", 0)
            .astype(float)
        ) / 100  # Convert percentage to decimal
        df_income_expense["Monthly Interest Incurred"] = (
            df_income_expense["Monthly Interest Incurred"].replace("", 0).astype(float)
        )
        df_income_expense["Payoff Order"] = (
            df_income_expense["Payoff Order"].replace("", 0).astype(int)
        )
        df_income_expense["Priority"] = (
            df_income_expense["Priority"].replace("", 0).astype(int)
        )
        df_income_expense["Account_Name"] = df_income_expense["Account_Name"].astype(
            str
        )
        df_income_expense["Category"] = df_income_expense["Category"].astype(str)
        df_income_expense["Sub_Category"] = df_income_expense["Sub_Category"].astype(
            str
        )
        df_income_expense["Type"] = df_income_expense["Type"].astype(str)
        df_income_expense["Auto_Pay_Account"] = df_income_expense[
            "Auto_Pay_Account"
        ].astype(str)

        return df_income_expense

    def get_oncely_transactions(self):
        df_oncely_transactions = self.get_income_expense_df()

        df_oncely_transactions = df_oncely_transactions[
            (df_oncely_transactions["Type"] == "oncely")
        ]

        # convert from format 2/29/2024
        df_oncely_transactions["Date"] = pd.to_datetime(
            df_oncely_transactions["When"], format="%m/%d/%Y"
        )
        df_oncely_transactions = df_oncely_transactions.drop(columns=["When"])

        return df_oncely_transactions

    def get_yearly_transactions(self):
        df_yearly_transactions = self.get_income_expense_df()

        df_yearly_transactions = df_yearly_transactions[
            (df_yearly_transactions["Type"] == "yearly")
        ]

        # convert from format 13-Jul
        df_yearly_transactions["Month_Number"] = pd.to_datetime(
            df_yearly_transactions["When"], format="%d-%b"
        ).dt.month
        df_yearly_transactions["Day_Of_Month"] = pd.to_datetime(
            df_yearly_transactions["When"], format="%d-%b"
        ).dt.day
        df_yearly_transactions = df_yearly_transactions.drop(columns=["When"])

        return df_yearly_transactions

    def get_monthly_transactions(self):
        df_monthly_transactions = self.get_income_expense_df()

        df_monthly_transactions = df_monthly_transactions[
            (df_monthly_transactions["Type"] == "monthly")
        ]

        # convert from format 25
        df_monthly_transactions["Day_Of_Month"] = df_monthly_transactions[
            "When"
        ].astype(int)
        df_monthly_transactions = df_monthly_transactions.drop(columns=["When"])

        return df_monthly_transactions

    def get_bi_weekly_transactions(self):
        df_bi_weekly_transactions = self.get_income_expense_df()

        df_bi_weekly_transactions = df_bi_weekly_transactions[
            (df_bi_weekly_transactions["Type"] == "biweekly")
        ]

        # convert from format 2/29/2024
        df_bi_weekly_transactions["An_Occur_Date"] = pd.to_datetime(
            df_bi_weekly_transactions["When"], format="%m/%d/%Y"
        )
        df_bi_weekly_transactions = df_bi_weekly_transactions.drop(columns=["When"])

        return df_bi_weekly_transactions

    def get_every_x_days_transactions(self):
        df_every_x_days_transactions = self.get_income_expense_df()

        df_every_x_days_transactions = df_every_x_days_transactions[
            (df_every_x_days_transactions["Type"] == "everyXDays")
        ]

        # convert from format 2/29/2024
        df_every_x_days_transactions["An_Occur_Date"] = pd.to_datetime(
            df_every_x_days_transactions["When"], format="%m/%d/%Y"
        )
        df_every_x_days_transactions = df_every_x_days_transactions.drop(
            columns=["When"]
        )

        return df_every_x_days_transactions

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
        df_account_details["Limit"] = (
            df_account_details["Limit"].replace("", 0).astype(float)
        )
        df_account_details["Interest Rate"] = (
            df_account_details["Interest Rate"]
            .str.replace("%", "")
            .replace("", 0)
            .astype(float)
        ) / 100  # Convert percentage to decimal
        df_account_details["Maturity Date"] = pd.to_datetime(
            df_account_details["Maturity Date"], errors="coerce"
        ).dt.date
        df_account_details["Link"] = df_account_details["Link"].astype(str)

        return df_account_details

    def get_transactions_report(self, force_update=False):
        """Get transactions report data"""
        df_transactions_report = self._get_sheet_data(
            key="transactions_report",
            sheet_name="Transactions_Report",
            force_update=force_update,
        )

        df_transactions_report["Date"] = pd.to_datetime(
            df_transactions_report["Date"]
        ).dt.date
        df_transactions_report["Amount"] = df_transactions_report["Amount"].astype(
            float
        )
        df_transactions_report["Amount_Paid"] = (
            df_transactions_report["Amount_Paid"].replace("", 0).astype(float)
        )
        df_transactions_report["Running_Balance"] = (
            df_transactions_report["Running_Balance"].replace("", 0).astype(float)
        )
        df_transactions_report["Account_Name"] = df_transactions_report[
            "Account_Name"
        ].astype(str)
        df_transactions_report["Category"] = df_transactions_report["Category"].astype(
            str
        )
        df_transactions_report["Type"] = df_transactions_report["Type"].astype(str)
        df_transactions_report["Auto_Pay_Account"] = df_transactions_report[
            "Auto_Pay_Account"
        ].astype(str)

        return df_transactions_report

    def update_income_expense_from_sheets(self):
        df_income_expense = self.get_income_expense_df(force_update=True)

        return df_income_expense

    def update_account_balances_from_sheets(self):
        df_account_balances = self.get_account_balances(force_update=True)

        return df_account_balances

    def update_account_details_from_sheets(self):
        df_account_details = self.get_account_details(force_update=True)

        return df_account_details

    def update_transactions_report_from_sheets(self):
        df_transactions_report = self.get_transactions_report(force_update=True)

        return df_transactions_report


# %%


class OurCashData:
    """Handles cash flow analysis and business logic"""

    def __init__(self, sheets_storage: Optional[SheetsStorage] = None):
        self.sheets_storage = sheets_storage or SheetsStorage()
        self.THRESHOLD_FOR_ALERT = 1000
        self.NUM_DAYS = 365 * 2

    def get_account_balances_with_details_filled(self):
        df_pivot: pd.DataFrame = self.sheets_storage.get_account_balances()

        df_pivot = df_pivot.pivot(
            index="Date", columns="Account_Name", values="Balance"
        )
        df_pivot = df_pivot.sort_index()

        # Forward fill missing values for each account
        df_pivot = df_pivot.ffill()
        df_pivot["Total"] = df_pivot.sum(axis=1)

        # Unpivot the DataFrame back to the original format
        df_pivot = df_pivot.reset_index().melt(
            id_vars="Date", value_name="Balance", var_name="Account_Name"
        )

        df_pivot = df_pivot.sort_values(by=["Date", "Account_Name"])

        # Merge back with the original DataFrame to include the Sub_Category
        df_account_details: pd.DataFrame = self.sheets_storage.get_account_details()
        df_account_details = df_account_details[
            ["Account_Name", "Category", "Sub_Category"]
        ]
        df_pivot = df_pivot.merge(df_account_details, on=["Account_Name"], how="left")

        df_pivot.loc[df_pivot["Account_Name"] == "Total", "Category"] = "Total"
        df_pivot.loc[df_pivot["Account_Name"] == "Total", "Sub_Category"] = "Total"

        return df_pivot

    def get_account_balances_with_details_filled_grouped(self) -> pd.DataFrame:
        df_pivot = self.get_account_balances_with_details_filled()

        df_grouped = (
            df_pivot.groupby(["Date", "Sub_Category"], as_index=False)
            .agg({"Balance": "sum"})
            .reset_index(drop=True)
        )

        return df_grouped

    def get_current_balance(self, account_name):
        df_current_balance = self.sheets_storage.get_account_balances()

        df_current_balance = df_current_balance[
            df_current_balance["Account_Name"] == account_name
        ]

        # get max of string date column Data
        max_date = df_current_balance["Date"].max()
        df_current_balance = df_current_balance[df_current_balance["Date"] == max_date]

        return df_current_balance["Balance"].iloc[0]

    def get_emergency_fund_amount(self):
        df_income_expense_emergency_fund = self.sheets_storage.get_income_expense_df()

        df_income_expense_emergency_fund = df_income_expense_emergency_fund[
            (df_income_expense_emergency_fund["Priority"] == 1)
        ]
        return df_income_expense_emergency_fund["AverageMonthlyCost"].sum() * 6

    def get_oncely_transactions_for_date(self, date):
        df_oncely_transactions = self.sheets_storage.get_oncely_transactions()

        df_oncely_transactions = df_oncely_transactions[
            (df_oncely_transactions["Maturity Date"] > date)
            & (df_oncely_transactions["Date"] == pd.to_datetime(date))
        ]

        return df_oncely_transactions

    def get_yearly_transactions_for_date(self, date):
        day_of_month = pd.to_datetime(date).day
        month_of_year = pd.to_datetime(date).month
        df_yearly_transactions = self.sheets_storage.get_yearly_transactions()

        df_yearly_transactions = df_yearly_transactions[
            (df_yearly_transactions["Maturity Date"] > date)
            & (df_yearly_transactions["Month_Number"] == month_of_year)
            & (df_yearly_transactions["Day_Of_Month"] == day_of_month)
        ]

        return df_yearly_transactions

    def get_monthly_transactions_for_date(self, date):
        day_of_month = pd.to_datetime(date).day
        df_monthly_transactions = self.sheets_storage.get_monthly_transactions()

        df_monthly_transactions = df_monthly_transactions[
            (df_monthly_transactions["Maturity Date"] > date)
            & (df_monthly_transactions["Day_Of_Month"] == day_of_month)
        ]
        return df_monthly_transactions

    def get_bi_weekly_transactions_for_date(self, date):
        df_bi_weekly_transactions = self.sheets_storage.get_bi_weekly_transactions()

        df_bi_weekly_transactions = df_bi_weekly_transactions[
            (df_bi_weekly_transactions["Maturity Date"] > date)
            & (
                (
                    pd.to_datetime(date) - df_bi_weekly_transactions["An_Occur_Date"]
                ).dt.days
                % 14
                == 0
            )
        ]

        return df_bi_weekly_transactions

    def get_every_x_days_transactions_for_date(self, date):
        df_every_x_days_transactions = (
            self.sheets_storage.get_every_x_days_transactions()
        )

        df_every_x_days_transactions = df_every_x_days_transactions[
            (df_every_x_days_transactions["Maturity Date"] > date)
            & (
                (
                    pd.to_datetime(date) - df_every_x_days_transactions["An_Occur_Date"]
                ).dt.days
                % df_every_x_days_transactions["AfterDays"]
                == 0
            )
        ]

        return df_every_x_days_transactions

    def get_all_transactions_for_date(self, date):
        df_all_transactions = pd.concat(
            [
                self.get_oncely_transactions_for_date(date),
                self.get_yearly_transactions_for_date(date),
                self.get_monthly_transactions_for_date(date),
                self.get_bi_weekly_transactions_for_date(date),
                self.get_every_x_days_transactions_for_date(date),
            ]
        )
        return df_all_transactions

    def get_expected_transactions_for_date_range(self, num_days_back, num_days_forward):
        ls_columns = [
            "Date",
            "Category",
            "Type",
            "Account_Name",
            "Auto_Pay_Account",
            "Amount",
            "Amount_Paid",
            "Date_Paid",
        ]

        df_recent_transactions = pd.DataFrame(columns=ls_columns)

        iterator = tqdm(
            range(0, num_days_back + 1 + num_days_forward),
            desc="Fetching transactions for date range",
            total=num_days_back + 1 + num_days_forward,
        )

        for i in iterator:
            # start num_days ago
            date = (
                pd.to_datetime("today") - pd.Timedelta(days=num_days_back - i)
            ).date()
            df_all_transactions_for_date = self.get_all_transactions_for_date(date)
            df_all_transactions_for_date["Date"] = date

            df_recent_transactions = pd.concat(
                [df_recent_transactions, df_all_transactions_for_date],
                ignore_index=True,
            )

            df_recent_transactions = df_recent_transactions[ls_columns]

        return df_recent_transactions

    def update_transactions(self):
        num_days_back = 5
        num_days_forward = self.NUM_DAYS

        current_balance = self.get_current_balance("Chase Checking")
        print(f"current_balance of Chase Checking: {current_balance}")

        df_existing_data_from_sheets = self.sheets_storage.get_transactions_report(
            force_update=True
        ).fillna(0)
        df_existing_data_from_sheets["Running_Balance"] = 0

        # keep only paid transactions or transactions before today
        df_existing_data_from_sheets = df_existing_data_from_sheets[
            (df_existing_data_from_sheets["Amount_Paid"] != "")  # Paid transactions
            | (
                (
                    pd.to_datetime("today")
                    - pd.to_datetime(df_existing_data_from_sheets["Date"])
                ).dt.days
                > 0
            )  # Transactions before today
        ]

        # get transactions expected in the next num_days_forward days since num_days_back
        df_updated_transactions = self.get_expected_transactions_for_date_range(
            num_days_back, num_days_forward
        )

        # add the transactions from the past, paid, and expected to a dataframe
        df_updated_transactions = pd.concat(
            [df_existing_data_from_sheets, df_updated_transactions], ignore_index=True
        )

        # drop duplicates keeping the first occurrence which would be the existing data if not future
        df_updated_transactions = df_updated_transactions.drop_duplicates(
            subset=["Date", "Account_Name"],
            keep="first",
        )

        # sort by date and amount so that expenses for a day come first in that day
        df_updated_transactions = df_updated_transactions.sort_values(
            by=["Date", "Amount"], ascending=True
        )

        # for each row after today, add the previous row's Running_Balance to the current row's amount,
        # only for days less than 10 days ago and only if the "Paid" column is ""
        previous_balance = current_balance
        for index, row in df_updated_transactions.iterrows():
            # if not paid and after 10 days ago
            if row["Date_Paid"] == "" or pd.isna(row["Date_Paid"]):
                df_updated_transactions.at[index, "Running_Balance"] = (
                    previous_balance + row["Amount"]
                )
                previous_balance = df_updated_transactions.at[index, "Running_Balance"]
            else:
                df_updated_transactions.at[index, "Running_Balance"] = previous_balance

        return df_updated_transactions

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

        emergency_fund_amount = self.get_emergency_fund_amount()
        df_future_cast_end_of_each_day["Emergency_Fund_Amount"] = (
            emergency_fund_amount * -1
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
# Run #


if __name__ == "__main__":
    sheets_storage = SheetsStorage()
    our_cash_data = OurCashData(sheets_storage)

    TEST_SHEETS_STORAGE = False
    if TEST_SHEETS_STORAGE:
        df_income_expense = sheets_storage.get_income_expense_df()
        df_account_balances = sheets_storage.get_account_balances()
        df_account_details = sheets_storage.get_account_details()
        df_transactions = sheets_storage.get_transactions_report()

        print_logger("df_income_expense (tail):")
        pprint_df(df_income_expense.tail(10))

        print_logger("df_account_balances (tail):")
        pprint_df(df_account_balances.tail(10))

        print_logger("df_account_details (tail):")
        pprint_df(df_account_details.tail(10))

        print_logger("df_transactions (tail):")
        pprint_df(df_transactions.tail(10))

    TEST_OUR_CASH = False
    if TEST_OUR_CASH:
        df_account_balances_filled = (
            our_cash_data.get_account_balances_with_details_filled()
        )

        print_logger("df_account_balances_filled (tail):")
        pprint_df(df_account_balances_filled.tail(20))

        df_account_balances_filled_grouped = (
            our_cash_data.get_account_balances_with_details_filled_grouped()
        )
        print_logger("df_account_balances_filled_grouped (tail):")
        pprint_df(df_account_balances_filled_grouped.tail(20))

    sheets_storage.update_income_expense_from_sheets()
    sheets_storage.update_account_balances_from_sheets()
    sheets_storage.update_account_details_from_sheets()
    sheets_storage.update_transactions_report_from_sheets()

    df_future_cast = our_cash_data.update_transactions()
    df_future_cast.to_csv(os.path.join(data_dir, "future_cast.csv"), index=False)
    WriteToSheets(
        "Our_Cash",
        "Transactions_Report",
        df_future_cast,
    )
    print("df_future_cast (head):")
    pprint_df(
        df_future_cast[
            (
                (
                    pd.to_datetime("today") - pd.to_datetime(df_future_cast["Date"])
                ).dt.days
                <= 1
            )
            | (df_future_cast["Amount_Paid"] == "")
        ].head(20)
    )

    df_daily_balance_report = our_cash_data.generate_daily_balance_report(
        df_future_cast
    )
    WriteToSheets(
        "Our_Cash",
        "Daily_Balance_Report",
        df_daily_balance_report,
    )
    print("df_daily_balance_report (head):")
    pprint_df(
        df_daily_balance_report[
            (
                (
                    pd.to_datetime("today")
                    - pd.to_datetime(df_daily_balance_report["Date"])
                ).dt.days
                <= 1
            )
        ]
        .fillna("")
        .head(20)
    )

    df_future_cast_label_dates = our_cash_data.isolate_label_dates(df_future_cast)
    print("df_future_cast_label_dates:")
    pprint_df(
        df_future_cast_label_dates[
            (
                (
                    pd.to_datetime("today")
                    - pd.to_datetime(df_future_cast_label_dates["Date"])
                ).dt.days
                <= 1
            )
        ]
    )

    df_future_cast_alert_dates = our_cash_data.generate_future_cast_alert_dates_df(
        df_future_cast
    )
    print("df_future_cast_alert_dates:")
    pprint_df(df_future_cast_alert_dates)

    # get summary sheet
    sheet_summary = get_book_sheet("Our_Cash", "Summary")

    # alert dates
    clear_range_of_sheet_obj(sheet_obj=sheet_summary, start="A11", end="B41")
    write_df_to_range_of_sheet_obj(
        sheet_obj=sheet_summary,
        df=df_future_cast_alert_dates.head(30),
        start="A11",
        fit=False,
        copy_head=True,
    )

    # one time transactions
    clear_range_of_sheet_obj(sheet_obj=sheet_summary, start="A44", end="C74")
    write_df_to_range_of_sheet_obj(
        sheet_obj=sheet_summary,
        df=df_future_cast_label_dates[
            (
                (
                    pd.to_datetime("today")
                    - pd.to_datetime(df_future_cast_label_dates["Date"])
                ).dt.days
                <= 10
            )
        ].head(30),
        start="A44",
        fit=False,
        copy_head=True,
    )

    print_logger("Done")


# %%
