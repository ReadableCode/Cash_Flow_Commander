# %%
# Running Imports #

import os
import warnings
from typing import Optional

import pandas as pd
from dotenv import load_dotenv
from tqdm import tqdm

from config import parent_dir
from utils.display_tools import pprint_df, print_logger  # noqa F401
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

    useable_cols = [
        # "Category",
        # "Sub_Category",
        "Type",
        "When",
        "Account_Name",
        "Amount",
        "Auto_Pay_Account",
        "Auto_Pay_Amount",
        # "AfterDays",
        # "AverageMonthlyCost",
        # "Balance",
        # "Limit",
        # "Available Credit",
        # "Interest Rate",
        # "Monthly Interest Incurred",
        # "Payoff Order",
        "Start_Date",
        "Maturity_Date",
        # "Priority",
    ]

    def __init__(self):
        self._dict_sheets_dfs = {}
        self._sheet_id = os.getenv("OUR_CASH_SHEET_ID")
        self._sheet_link = (
            f"https://docs.google.com/spreadsheets/d/{self._sheet_id}/edit#gid=0"
        )
        self.start_date_cal = "2000-01-01"
        self.end_date_cal = "2100-12-31"

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

        # rename columns to remove spaces
        df_income_expense = df_income_expense.rename(
            columns={
                "Maturity Date": "Maturity_Date",
            }
        )

        df_income_expense["Amount"] = df_income_expense["Amount"].astype(float)
        df_income_expense["Start_Date"] = pd.to_datetime(
            df_income_expense["Start_Date"]
        ).dt.date
        df_income_expense["Maturity_Date"] = pd.to_datetime(
            df_income_expense["Maturity_Date"]
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

        # only useable columns
        df_income_expense = df_income_expense[self.useable_cols]

        return df_income_expense

    def get_monthly_budgets(self, force_update=False):
        df: pd.DataFrame = self.get_income_expense_df(force_update=force_update)

        # filter to Type == monthly
        df = df[df["Type"] == "monthly"]

        # rename
        df = df.rename(columns={"When": "Day_of_Month"})

        # dayofmonth to int
        df["Day_of_Month"] = df["Day_of_Month"].astype(int)

        df = df[
            [
                "Type",
                "Account_Name",
                "Day_of_Month",
                "Start_Date",
                "Maturity_Date",
                "Amount",
            ]
        ]

        return df

    def get_yearly_budgets(self, force_update=False):
        df: pd.DataFrame = self.get_income_expense_df(force_update=force_update)

        # filter to Type == yearly
        df = df[df["Type"] == "yearly"]

        # rename
        df = df.rename(columns={"When": "Month_Day_Num"})

        # convert 31-Dec to datetime and extract month and day
        df["Month_Day_Num"] = pd.to_datetime(
            df["Month_Day_Num"], format="%d-%b"
        ).dt.dayofyear
        df["Month_of_Year"] = pd.to_datetime(df["Month_Day_Num"], format="%j").dt.month
        df["Day_of_Month"] = pd.to_datetime(df["Month_Day_Num"], format="%j").dt.day

        # monthofyear to int
        df["Month_of_Year"] = df["Month_of_Year"].astype(int)
        df["Day_of_Month"] = df["Day_of_Month"].astype(int)

        df = df[
            [
                "Type",
                "Account_Name",
                "Month_of_Year",
                "Day_of_Month",
                "Start_Date",
                "Maturity_Date",
                "Amount",
            ]
        ]

        return df

    def get_one_time_budgets(self, force_update=False):
        df: pd.DataFrame = self.get_income_expense_df(force_update=force_update)

        # filter to Type == one_time
        df = df[df["Type"] == "oncely"]

        # rename
        df = df.rename(columns={"When": "Date"})

        # convert Date to datetime
        df["Date"] = pd.to_datetime(df["Date"]).dt.date

        df = df[
            [
                "Type",
                "Account_Name",
                "Date",
                "Start_Date",
                "Maturity_Date",
                "Amount",
            ]
        ]

        return df

    def get_bi_weekly_budgets(self, force_update=False):
        df: pd.DataFrame = self.get_income_expense_df(force_update=force_update)

        # filter to Type == bi_weekly
        df = df[df["Type"] == "biweekly"]

        # rename
        df = df.rename(columns={"When": "Occur_Date"})

        # convert Occur_Date to datetime
        df["Occur_Date"] = pd.to_datetime(df["Occur_Date"]).dt.date

        df = df[
            [
                "Type",
                "Account_Name",
                "Occur_Date",
                "Start_Date",
                "Maturity_Date",
                "Amount",
            ]
        ]

        # populate date for range starting from start_date to end_date with bi-weekly frequency
        list_rows = []
        for _, row in df.iterrows():
            start_date = min(
                pd.to_datetime(row["Occur_Date"]).date(),
                pd.to_datetime(self.start_date_cal).date(),
            )
            current_date = start_date
            while current_date <= pd.to_datetime(self.end_date_cal).date():
                list_rows.append(
                    {
                        "Type": row["Type"],
                        "Account_Name": row["Account_Name"],
                        "Date": current_date,
                        "Start_Date": row["Start_Date"],
                        "Maturity_Date": row["Maturity_Date"],
                        "Amount": row["Amount"],
                    }
                )
                current_date += pd.Timedelta(weeks=2)

        df = pd.DataFrame(list_rows)

        # sort by date
        df = df.sort_values(by=["Date"])

        return df

    def get_full_calendar(self):
        # create a calendar that is mergable from 2000 to 2100
        df_calendar = pd.DataFrame(
            {
                "Date": pd.date_range(
                    start=self.start_date_cal, end=self.end_date_cal, freq="D"
                )
            }
        )

        # add columns for year, month, day, dayofweek, dayofyear, weekofyear
        df_calendar["Year"] = df_calendar["Date"].dt.year
        df_calendar["Month_of_Year"] = df_calendar["Date"].dt.month
        df_calendar["Day_of_Month"] = df_calendar["Date"].dt.day
        df_calendar["Day_of_Week"] = df_calendar["Date"].dt.dayofweek

        # convert date to date not datetime
        df_calendar["Date"] = df_calendar["Date"].dt.date

        df_calendar = df_calendar[
            ["Date", "Year", "Month_of_Year", "Day_of_Month", "Day_of_Week"]
        ]

        return df_calendar

    def get_planned_budgets(self, force_update=False):
        df_monthly = self.get_monthly_budgets(force_update=force_update)
        print_logger("Monthly Budgets Retrieved")

        df_yearly = self.get_yearly_budgets(force_update=force_update)
        print_logger("Yearly Budgets Retrieved")

        df_one_time = self.get_one_time_budgets(force_update=force_update)
        print_logger("One Time Budgets Retrieved")

        df_bi_weekly = self.get_bi_weekly_budgets(force_update=force_update)
        print_logger("Bi-Weekly Budgets Retrieved")

        df_calendar = self.get_full_calendar()
        print_logger("Full Calendar Retrieved")

        # merge monthly on dayofmonth
        df_monthly = df_calendar.merge(df_monthly, how="left", on="Day_of_Month")

        # merge yearly on monthofyear and dayofmonth
        df_yearly = df_calendar.merge(
            df_yearly, how="left", on=["Month_of_Year", "Day_of_Month"]
        )

        # merge one_time on date
        df_one_time = df_calendar.merge(df_one_time, how="left", on="Date")

        # merge bi_weekly on date
        df_bi_weekly = df_calendar.merge(df_bi_weekly, how="left", on="Date")

        df_calendar = pd.concat(
            [
                df_monthly,
                df_yearly,
                df_one_time,
                df_bi_weekly,
            ],
            ignore_index=True,
        )

        # take out rows where amount is null
        df_calendar = df_calendar[~df_calendar["Amount"].isna()]

        # take out rows where date is not between start_date and Maturity_Date
        df_calendar = df_calendar[
            (df_calendar["Date"] >= pd.to_datetime(df_calendar["Start_Date"]).dt.date)
            & (
                (df_calendar["Maturity_Date"].isna())
                | (
                    df_calendar["Date"]
                    <= pd.to_datetime(df_calendar["Maturity_Date"]).dt.date
                )
            )
        ]

        # sort by date, amount
        df_calendar = df_calendar.sort_values(
            by=["Date", "Amount"], ascending=[True, False]
        )

        return df_calendar


# %%
# Main #

if __name__ == "__main__":
    ss = SheetsStorage()
    df = ss.get_income_expense_df(force_update=True)
    pprint_df(df)


# %%
