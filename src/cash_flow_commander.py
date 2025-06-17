# %%
# Running Imports #

import datetime
import os
import warnings
from dataclasses import dataclass
from datetime import date
from typing import Optional, Union

import pandas as pd
from dotenv import load_dotenv

from config import parent_dir
from utils.display_tools import pprint_df, print_logger
from utils.google_tools import get_book_sheet_df

warnings.filterwarnings("ignore")


# %%
# Environment #


dotenv_path = os.path.join(parent_dir, ".env")
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)


# %%
# Functions #


def print_objects_list(objects_list, title="Objects", max_items=None):
    """Print a list of objects with a clean format."""
    print_logger(title, as_break=True)

    items_to_show = objects_list[:max_items] if max_items else objects_list

    for i, obj in enumerate(items_to_show, 1):
        print(f"--- Item {i} ---")
        print(obj)
        print()

    if max_items and len(objects_list) > max_items:
        print(f"... and {len(objects_list) - max_items} more items")


# %%
# Class #
@dataclass
class AccountBalance:
    account_name: str
    date: date
    balance: float

    def __str__(self) -> str:
        return (
            f"AccountBalance(account_name='{self.account_name}', "
            f"date={self.date}, balance=${self.balance:,.2f})"
        )

    def __repr__(self) -> str:
        return self.__str__()


@dataclass
class IncomeExpense:
    Category: str
    Sub_Category: str
    Type: str
    When: Union[str, int]
    Account_Name: str
    Amount: float
    Auto_Pay_Account: str
    Auto_Pay_Amount: str
    AfterDays: int
    AverageMonthlyCost: float
    Balance: float
    Limit: float
    Available_Credit: float
    Interest_Rate: float
    Monthly_Interest_Incurred: float
    Payoff_Order: int
    Maturity_Date: Optional[date]
    Priority: int

    def __str__(self) -> str:
        return (
            f"IncomeExpense(\n"
            f"  Category='{self.Category}',\n"
            f"  Sub_Category='{self.Sub_Category}',\n"
            f"  Type='{self.Type}',\n"
            f"  When='{self.When}',\n"
            f"  Account_Name='{self.Account_Name}',\n"
            f"  Amount=${self.Amount:,.2f},\n"
            f"  Auto_Pay_Account='{self.Auto_Pay_Account}',\n"
            f"  Balance=${self.Balance:,.2f},\n"
            f"  Limit=${self.Limit:,.2f},\n"
            f"  Interest_Rate={self.Interest_Rate:.2%},\n"
            f"  Priority={self.Priority}\n"
            f")"
        )

    def __repr__(self) -> str:
        return self.__str__()


@dataclass
class AccountDetail:
    Category: str
    Sub_Category: str
    Account_Name: str
    Limit: float
    Interest_Rate: float
    Maturity_Date: Optional[date]
    Link: str

    def __str__(self) -> str:
        return (
            f"AccountDetail(\n"
            f"  Category='{self.Category}',\n"
            f"  Sub_Category='{self.Sub_Category}',\n"
            f"  Account_Name='{self.Account_Name}',\n"
            f"  Limit=${self.Limit:,.2f},\n"
            f"  Interest_Rate={self.Interest_Rate:.2%},\n"
            f"  Maturity_Date={self.Maturity_Date}\n"
            f")"
        )

    def __repr__(self) -> str:
        return self.__str__()


@dataclass
class TransactionReport:
    Date: date
    Category: str
    Type: str
    Account_Name: str
    Auto_Pay_Account: str
    Amount: float
    Amount_Paid: float
    Date_Paid: Optional[date]
    Running_Balance: float

    def __str__(self) -> str:
        return (
            f"TransactionReport(\n"
            f"  Date={self.Date},\n"
            f"  Category='{self.Category}',\n"
            f"  Type='{self.Type}',\n"
            f"  Account_Name='{self.Account_Name}',\n"
            f"  Auto_Pay_Account='{self.Auto_Pay_Account}',\n"
            f"  Amount=${self.Amount:,.2f},\n"
            f"  Amount_Paid=${self.Amount_Paid:,.2f},\n"
            f"  Date_Paid={self.Date_Paid},\n"
            f"  Running_Balance=${self.Running_Balance:,.2f}\n"
            f")"
        )

    def __repr__(self) -> str:
        return self.__str__()


# %%


class DataSource:
    def __init__(self):
        self.sheet_id = os.getenv("OUR_CASH_SHEET_ID")
        self.sheet_link = (
            f"https://docs.google.com/spreadsheets/d/{self.sheet_id}/edit#gid=0"
        )
        self.dict_dfs = {}

    def get_sheet_data(self, key, sheet_name, force_update=False) -> pd.DataFrame:
        if key in self.dict_dfs and not force_update:
            return self.dict_dfs[key].copy()

        df = get_book_sheet_df("Our_Cash", sheet_name)

        self.dict_dfs[key] = df.copy()
        return df.copy()

    def get_income_expense_df(self, force_update=False):
        df_income_expense = self.get_sheet_data(
            key="income_expense_df",
            sheet_name="Income_Expense",
            force_update=force_update,
        )

        df_income_expense["Amount"] = df_income_expense["Amount"].astype(float)
        df_income_expense["Maturity Date"] = pd.to_datetime(
            df_income_expense["Maturity Date"]
        ).dt.date
        df_income_expense["When"] = pd.to_datetime(
            df_income_expense["When"], errors="coerce"
        ).dt.strftime("%d-%b")
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

        # convert to list of IncomeExpense dataclass
        ls_income_expense = [
            IncomeExpense(
                Category=row["Category"],
                Sub_Category=row["Sub_Category"],
                Type=row["Type"],
                When=row["When"],
                Account_Name=row["Account_Name"],
                Amount=row["Amount"],
                Auto_Pay_Account=row["Auto_Pay_Account"],
                Auto_Pay_Amount=row["Auto_Pay_Amount"],
                AfterDays=row["AfterDays"],
                AverageMonthlyCost=row["AverageMonthlyCost"],
                Balance=row["Balance"],
                Limit=row["Limit"],
                Available_Credit=row["Available Credit"],
                Interest_Rate=row["Interest Rate"],
                Monthly_Interest_Incurred=row["Monthly Interest Incurred"],
                Payoff_Order=row["Payoff Order"],
                Maturity_Date=(
                    row["Maturity Date"] if pd.notna(row["Maturity Date"]) else None
                ),
                Priority=row["Priority"],
            )
            for _, row in df_income_expense.iterrows()
        ]
        return ls_income_expense

    def get_account_balances(self, force_update=False):
        df_account_balances = self.get_sheet_data(
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

        # convert to list of AccountBalance dataclass
        ls_account_balances = [
            AccountBalance(
                account_name=row["Account_Name"],
                date=row["Date"],
                balance=row["Balance"],
            )
            for _, row in df_account_balances.iterrows()
        ]

        return ls_account_balances

    def get_account_details(self, force_update=False):
        df_account_details = self.get_sheet_data(
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

        # convert to list of AccountDetail dataclass
        ls_account_details = [
            AccountDetail(
                Category=row["Category"],
                Sub_Category=row["Sub_Category"],
                Account_Name=row["Account_Name"],
                Limit=row["Limit"],
                Interest_Rate=row["Interest Rate"],
                Maturity_Date=(
                    row["Maturity Date"] if pd.notna(row["Maturity Date"]) else None
                ),
                Link=row["Link"],
            )
            for _, row in df_account_details.iterrows()
        ]
        return ls_account_details

    def get_transactions_report(self, force_update=False):
        df_transactions_report = self.get_sheet_data(
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
        df_transactions_report["Date_Paid"] = pd.to_datetime(
            df_transactions_report["Date_Paid"], errors="coerce"
        ).dt.date
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

        # convert to list of TransactionReport dataclass
        ls_transactions_report = [
            TransactionReport(
                Date=row["Date"],
                Category=row["Category"],
                Type=row["Type"],
                Account_Name=row["Account_Name"],
                Auto_Pay_Account=row["Auto_Pay_Account"],
                Amount=row["Amount"],
                Amount_Paid=row["Amount_Paid"],
                Date_Paid=(row["Date_Paid"] if pd.notna(row["Date_Paid"]) else None),
                Running_Balance=row["Running_Balance"],
            )
            for _, row in df_transactions_report.iterrows()
        ]
        return ls_transactions_report

    def update_income_expense_from_sheets(self):
        ls_income_expense = self.get_income_expense_df(force_update=True)

        return ls_income_expense

    def update_account_balances_from_sheets(self):
        ls_account_balances = self.get_account_balances(force_update=True)

        return ls_account_balances

    def update_account_details_from_sheets(self):
        ls_account_details = self.get_account_details(force_update=True)

        return ls_account_details

    def update_transactions_report_from_sheets(self):
        ls_transactions_report = self.get_transactions_report(force_update=True)

        return ls_transactions_report


data_source = DataSource()


# %%


print_logger("Account balances", as_break=True)
ls_account_balances = data_source.get_account_balances()
print(type(ls_account_balances))
max_date_of_balances = max(
    [account_balance.date for account_balance in ls_account_balances]
)
print(f"Max date of account balances: {max_date_of_balances}")

print_objects_list(
    [
        account_balance
        for account_balance in ls_account_balances
        if account_balance.date == max_date_of_balances
    ],
    title="Account Balances (Latest)",
    max_items=10,
)

# %%

ls_income_expenses = data_source.get_income_expense_df()
print_objects_list(ls_income_expenses, title="Income/Expenses", max_items=5)


# %%

ls_account_details = data_source.get_account_details()
print_objects_list(ls_account_details, title="Account Details", max_items=5)


# %%
print_logger("df_transactions_report (filtered)", as_break=True)
ls_transactions_report = data_source.get_transactions_report()
filtered_transactions = [
    transaction_report
    for transaction_report in ls_transactions_report
    if transaction_report.Date is not None
    and datetime.date(2025, 6, 1)
    <= transaction_report.Date
    <= datetime.date(2025, 6, 10)
]
print_objects_list(
    filtered_transactions, title="Transactions (2025-06-01 to 2025-06-10)"
)


# %%


class OurCashData:
    def __init__(self):
        self.THRESHOLD_FOR_ALERT = 1000
        self.NUM_DAYS = 365 * 2

    def get_account_balances_with_details_filled(self):
        df_pivot: pd.DataFrame = self.get_account_balances()

        df_pivot = df_pivot.pivot(
            index="Date", columns="Account_Name", values="Balance"
        )
        df_pivot = df_pivot.sort_index()

        # Forward fill missing values for each account
        df_pivot = df_pivot.fillna(method="ffill")  # type: ignore
        df_pivot["Total"] = df_pivot.sum(axis=1)

        # Unpivot the DataFrame back to the original format
        df_pivot = df_pivot.reset_index().melt(
            id_vars="Date", value_name="Balance", var_name="Account_Name"
        )

        df_pivot = df_pivot.sort_values(by=["Date", "Account_Name"])

        # Merge back with the original DataFrame to include the Sub_Category
        df_account_details: pd.DataFrame = self.get_account_details()
        df_account_details = df_account_details[
            ["Account_Name", "Category", "Sub_Category"]
        ]
        df_pivot = df_pivot.merge(df_account_details, on=["Account_Name"], how="left")

        df_pivot.loc[df_pivot["Account_Name"] == "Total", "Category"] = "Total"
        df_pivot.loc[df_pivot["Account_Name"] == "Total", "Sub_Category"] = "Total"

        return df_pivot

    def get_account_balances_with_details_filled_grouped(self) -> pd.DataFrame:
        df_pivot = self.get_account_balances_with_details_filled()

        df_pivot = df_pivot.groupby(["Date", "Sub_Category"], as_index=False)[
            "Balance"
        ].sum()

        return df_pivot

    def get_current_balance(self, account_name):
        df_current_balance = self.get_account_balances()

        df_current_balance = df_current_balance[
            df_current_balance["Account_Name"] == account_name
        ]

        # get max of string date column Data
        max_date = df_current_balance["Date"].max()
        df_current_balance = df_current_balance[df_current_balance["Date"] == max_date]

        return df_current_balance["Balance"].iloc[0]

    def get_emergency_fund_amount(self):
        df_income_expense_emergency_fund = self.get_income_expense_df()

        df_income_expense_emergency_fund = df_income_expense_emergency_fund[
            (df_income_expense_emergency_fund["Priority"] == 1)
        ]
        return df_income_expense_emergency_fund["AverageMonthlyCost"].sum() * 6

    def get_monthly_transactions_for_date(self, date):
        day_of_month = pd.to_datetime(date).day
        df_monthly_transactions = self.get_income_expense_df()

        # make sure all the values can be converted to expected format by filtering to this type
        df_monthly_transactions = df_monthly_transactions[
            (df_monthly_transactions["Type"] == "monthly")
        ]

        df_monthly_transactions = df_monthly_transactions[
            (df_monthly_transactions["When"] == day_of_month)
            & (df_monthly_transactions["Maturity Date"] > date)
        ]
        return df_monthly_transactions

    def get_yearly_transactions_for_date(self, date):
        day_of_month = pd.to_datetime(date).day
        month_of_year = pd.to_datetime(date).month
        df_yearly_transactions = self.get_income_expense_df()

        # make sure all the values can be converted to expected format by filtering to this type
        df_yearly_transactions = df_yearly_transactions[
            (df_yearly_transactions["Type"] == "yearly")
            & (df_yearly_transactions["Maturity Date"] > date)
        ]

        df_yearly_transactions["When"] = pd.to_datetime(
            df_yearly_transactions["When"], format="%d-%b"
        )

        df_yearly_transactions = df_yearly_transactions[
            (df_yearly_transactions["Type"] == "yearly")
            & (df_yearly_transactions["When"].dt.month == month_of_year)
            & (df_yearly_transactions["When"].dt.day == day_of_month)
        ]

        return df_yearly_transactions

    def get_bi_weekly_transactions_for_date(self, date):
        df_bi_weekly_transactions = self.get_income_expense_df()

        # make sure all the values can be converted to expected format by filtering to this type
        df_bi_weekly_transactions = df_bi_weekly_transactions[
            (df_bi_weekly_transactions["Type"] == "biweekly")
        ]

        df_bi_weekly_transactions["When"] = pd.to_datetime(
            df_bi_weekly_transactions["When"]
        )

        df_bi_weekly_transactions = df_bi_weekly_transactions[
            (
                (pd.to_datetime(date) - df_bi_weekly_transactions["When"]).dt.days % 14
                == 0
            )
            & (df_bi_weekly_transactions["Maturity Date"] > date)
        ]

        return df_bi_weekly_transactions

    def get_oncely_transactions_for_date(self, date):
        df_oncely_transactions = self.get_income_expense_df()

        df_oncely_transactions = df_oncely_transactions[
            (df_oncely_transactions["Type"] == "oncely")
        ]

        df_oncely_transactions["When"] = pd.to_datetime(df_oncely_transactions["When"])

        df_oncely_transactions = df_oncely_transactions[
            df_oncely_transactions["When"] == pd.to_datetime(date)
        ]

        return df_oncely_transactions

    def get_every_x_days_transactions_for_date(self, date):
        df_every_x_days_transactions = self.get_income_expense_df()

        df_every_x_days_transactions = df_every_x_days_transactions[
            (df_every_x_days_transactions["Type"] == "everyXDays")
        ]

        df_every_x_days_transactions["When"] = pd.to_datetime(
            df_every_x_days_transactions["When"]
        )

        df_every_x_days_transactions = df_every_x_days_transactions[
            (
                (pd.to_datetime(date) - df_every_x_days_transactions["When"]).dt.days
                % df_every_x_days_transactions["AfterDays"]
                == 0
            )
            & (df_every_x_days_transactions["Maturity Date"] > date)
        ]

        return df_every_x_days_transactions

    def get_all_transactions_for_date(self, date):
        df_all_transactions = pd.concat(
            [
                self.get_monthly_transactions_for_date(date),
                self.get_yearly_transactions_for_date(date),
                self.get_bi_weekly_transactions_for_date(date),
                self.get_oncely_transactions_for_date(date),
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

        for i in range(0, num_days_back + 1 + num_days_forward):
            # start num_days ago
            date = (
                pd.to_datetime("today") - pd.Timedelta(days=num_days_back - i)
            ).strftime("%Y-%m-%d")
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

        df_existing_data_from_sheets = self.get_transactions_report(
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
            num_days_ago = (pd.to_datetime("today") - pd.to_datetime(row["Date"])).days
            # if not paid and after 10 days ago
            if (row["Date_Paid"] == "" or pd.isna(row["Date_Paid"])) and (
                num_days_ago <= 10
            ):
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


our_cash_data = OurCashData()

# %%


df_income_expense = our_cash_data.get_income_expense_df()
df_account_balances = our_cash_data.get_account_balances()
df_account_details = our_cash_data.get_account_details()
df_transactions = our_cash_data.get_transactions_report()

print_logger("df_income_expense (tail):")
pprint_df(df_income_expense.tail(10))

print_logger("df_account_balances (tail):")
pprint_df(df_account_balances.tail(10))

print_logger("df_account_details (tail):")
pprint_df(df_account_details.tail(10))

print_logger("df_transactions (tail):")
pprint_df(df_transactions.tail(10))


# %%

df_account_balances_filled = our_cash_data.get_account_balances_with_details_filled()

print_logger("df_account_balances_filled (tail):")
pprint_df(df_account_balances_filled.tail(20))

df_account_balances_filled_grouped = (
    our_cash_data.get_account_balances_with_details_filled_grouped()
)
print_logger("df_account_balances_filled_grouped (tail):")
pprint_df(df_account_balances_filled_grouped.tail(20))


# %%


df_future_cast = our_cash_data.update_transactions()


# %%


print_logger("df_future_cast near today (tail):")
pprint_df(
    df_future_cast[
        (
            abs(
                (
                    pd.to_datetime("today") - pd.to_datetime(df_future_cast["Date"])
                ).dt.days
            )
            <= 50
        )
    ].tail(100)
)

# %%

df_label_dates = our_cash_data.isolate_label_dates(df_future_cast)
print_logger("df_future_cast_label_dates near today (tail):")
pprint_df(
    df_label_dates[
        (
            abs(
                (
                    pd.to_datetime("today") - pd.to_datetime(df_label_dates["Date"])
                ).dt.days
            )
            <= 500
        )
    ].tail(100)
)

df_isolate_ending_daily_balance = our_cash_data.isolate_ending_daily_balance(
    df_future_cast
)
print_logger("df_isolate_ending_daily_balance near today (tail):")
pprint_df(
    df_isolate_ending_daily_balance[
        (
            abs(
                (
                    pd.to_datetime("today")
                    - pd.to_datetime(df_isolate_ending_daily_balance["Date"])
                ).dt.days
            )
            <= 50
        )
    ].tail(100)
)

df_alert_dates = our_cash_data.generate_future_cast_alert_dates_df(df_future_cast)
print_logger("df_future_cast_alert_dates near today (tail):")
pprint_df(
    df_alert_dates[
        (
            abs(
                (
                    pd.to_datetime("today") - pd.to_datetime(df_alert_dates["Date"])
                ).dt.days
            )
            <= 50
        )
    ].tail(100)
)

df_daily_balance_report = our_cash_data.generate_daily_balance_report(df_future_cast)
print_logger("df_daily_balance_report near today (tail):")
pprint_df(
    df_daily_balance_report[
        (
            abs(
                (
                    pd.to_datetime("today")
                    - pd.to_datetime(df_daily_balance_report["Date"])
                ).dt.days
            )
            <= 50
        )
    ].tail(100)
)


# %%
# Run #


if __name__ == "__main__":
    our_cash_data = OurCashData()

    our_cash_data.update_income_expense_from_sheets()
    our_cash_data.update_account_balances_from_sheets()
    our_cash_data.update_account_details_from_sheets()
    our_cash_data.update_transactions_report_from_sheets()

    df_future_cast = our_cash_data.update_transactions()
    # df_future_cast.to_csv(os.path.join(data_dir, "future_cast.csv"), index=False)
    # WriteToSheets(
    #     "Our_Cash",
    #     "Transactions_Report",
    #     df_future_cast,
    # )
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
    # WriteToSheets(
    #     "Our_Cash",
    #     "Daily_Balance_Report",
    #     df_daily_balance_report,
    # )
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
    # sheet_summary = get_book_sheet("Our_Cash", "Summary")

    # alert dates
    # clear_range_of_sheet_obj(sheet_obj=sheet_summary, start="A11", end="B41")
    # write_df_to_range_of_sheet_obj(
    #     sheet_obj=sheet_summary,
    #     df=df_future_cast_alert_dates.head(30),
    #     start="A11",
    #     fit=False,
    #     copy_head=True,
    # )

    # one time transactions
    # clear_range_of_sheet_obj(sheet_obj=sheet_summary, start="A44", end="C74")
    # write_df_to_range_of_sheet_obj(
    #     sheet_obj=sheet_summary,
    #     df=df_future_cast_label_dates[
    #         (
    #             (
    #                 pd.to_datetime("today")
    #                 - pd.to_datetime(df_future_cast_label_dates["Date"])
    #             ).dt.days
    #             <= 10
    #         )
    #     ].head(30),
    #     start="A44",
    #     fit=False,
    #     copy_head=True,
    # )

    print_logger("Done")


# %%
