# %%
# Running Imports #

import os
import warnings

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd

from config import data_dir
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
# Variables #

dict_dataframes: dict[str, pd.DataFrame] = {}

THRESHOLD_FOR_ALERT = 1000
NUM_DAYS = 365 * 2


# %%
# Get From Sheets #


def get_income_expense_df(force_update=False):
    key = "income_expense_df"
    if key in dict_dataframes.keys() and not force_update:
        return dict_dataframes[key].copy()

    df_income_expense = get_book_sheet_df(
        "Our_Cash",
        "Income_Expense",
    )

    df_income_expense["Amount"] = df_income_expense["Amount"].astype(float)
    df_income_expense["Maturity Date"] = pd.to_datetime(
        df_income_expense["Maturity Date"]
    )

    dict_dataframes[key] = df_income_expense.copy()

    return df_income_expense


df_income_expense = get_income_expense_df()
pprint_df(df_income_expense.head(10))
print(df_income_expense.info())

# %%


def get_account_balances(force_update=False):
    key = "account_balances"
    if key in dict_dataframes.keys() and not force_update:
        return dict_dataframes[key].copy()

    df_account_balances = get_book_sheet_df(
        "Our_Cash",
        "Account_Date_Balances",
    )

    dict_dataframes[key] = df_account_balances.copy()

    return df_account_balances


def get_account_details(force_update=False):
    key = "account_details"
    if key in dict_dataframes.keys() and not force_update:
        return dict_dataframes[key].copy()

    df_account_details = get_book_sheet_df(
        "Our_Cash",
        "Account_Details",
    )

    dict_dataframes[key] = df_account_details.copy()

    return df_account_details


def get_transactions_report(force_update=False):
    key = "transactions_report"
    if key in dict_dataframes.keys() and not force_update:
        return dict_dataframes[key].copy()

    df_transactions_report = get_book_sheet_df(
        "Our_Cash",
        "Transactions_Report",
    )

    dict_dataframes[key] = df_transactions_report.copy()

    return df_transactions_report


# %%
# Update From Sheets #


def update_income_expense_from_sheets():
    df_income_expense = get_income_expense_df(force_update=True)

    return df_income_expense


def update_account_balances_from_sheets():
    df_account_balances = get_account_balances(force_update=True)

    return df_account_balances


def update_account_details_from_sheets():
    df_account_details = get_account_details(force_update=True)

    return df_account_details


def update_transactions_report_from_sheets():
    df_transactions_report = get_transactions_report(force_update=True)

    return df_transactions_report


# %%
# External Variables #


def get_account_balances_with_details_filled():
    df_account_balances = get_account_balances()

    # Pivot the DataFrame
    df_pivot = df_account_balances.copy()
    df_pivot = df_pivot.pivot(index="Date", columns="Account_Name", values="Balance")
    df_pivot = df_pivot.sort_index()

    # Forward fill missing values for each account
    df_filled = df_pivot.copy()
    df_filled = df_filled.fillna(method="ffill")
    df_filled["Total"] = df_filled.sum(axis=1)

    # Unpivot the DataFrame back to the original format
    df_unpivot = df_filled.copy()
    df_unpivot = df_unpivot.reset_index().melt(
        id_vars="Date", value_name="Balance", var_name="Account_Name"
    )

    df_unpivot = df_unpivot.sort_values(by=["Date", "Account_Name"])

    # Merge back with the original DataFrame to include the Sub_Category
    df_unpivot_merged_back = df_unpivot.copy()
    df_account_details = get_account_details()
    df_unpivot_merged_back = df_unpivot_merged_back.merge(
        df_account_details, on=["Account_Name"], how="left"
    )

    df_unpivot_merged_back.loc[
        df_unpivot_merged_back["Account_Name"] == "Total", "Sub_Category"
    ] = "Total"

    df_unpivot_merged_back = df_unpivot_merged_back.groupby(
        ["Date", "Sub_Category"], as_index=False
    )["Balance"].sum()

    return df_unpivot_merged_back


def get_current_balance(account_name):
    df_current_balance = get_account_balances()

    df_current_balance = df_current_balance[
        df_current_balance["Account_Name"] == account_name
    ]

    # get max of string date column Data
    max_date = df_current_balance["Date"].max()
    df_current_balance = df_current_balance[df_current_balance["Date"] == max_date]

    return df_current_balance["Balance"].iloc[0]


def get_current_available_balances():
    df_curr_available_balances = get_income_expense_df()

    # filter where Available Credit is not ""
    df_curr_available_balances = df_curr_available_balances[
        df_curr_available_balances["Available Credit"] != ""
    ]

    # calculate monthly interest
    df_curr_available_balances["Interest Rate (Numeric)"] = (
        df_curr_available_balances["Interest Rate"].str.replace("%", "").astype(float)
    )
    df_curr_available_balances["Interest Rate (Numeric)"] = (
        df_curr_available_balances["Interest Rate (Numeric)"] / 100
    )
    df_curr_available_balances["Monthly Interest"] = (
        df_curr_available_balances["Interest Rate (Numeric)"]
        * df_curr_available_balances["Balance"]
        / 12
    )

    df_curr_available_balances = df_curr_available_balances[
        [
            "Account_Name",
            "Balance",
            "Limit",
            "Available Credit",
            "Interest Rate",
            "Monthly Interest",
        ]
    ]

    return df_curr_available_balances


def get_emergency_fund_amount():
    df_income_expense_emergency_fund = get_income_expense_df()

    df_income_expense_emergency_fund = df_income_expense_emergency_fund[
        (df_income_expense_emergency_fund["Priority"] == 1)
    ]
    return df_income_expense_emergency_fund["AverageMonthlyCost"].sum() * 6


# %%
# Get Expected Transactions #


def get_monthly_transactions_for_date(date):
    day_of_month = pd.to_datetime(date).day
    df_monthly_transactions = get_income_expense_df()

    df_monthly_transactions = df_monthly_transactions[
        (df_monthly_transactions["Type"] == "monthly")
        & (df_monthly_transactions["When"] == day_of_month)
        & (df_monthly_transactions["Maturity Date"] > date)
    ]
    return df_monthly_transactions


def get_yearly_transactions_for_date(date):
    day_of_month = pd.to_datetime(date).day
    month_of_year = pd.to_datetime(date).month
    df_yearly_transactions = get_income_expense_df()

    df_yearly_transactions = df_yearly_transactions[
        (df_yearly_transactions["Type"] == "yearly")
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


def get_bi_weekly_transactions_for_date(date):
    df_bi_weekly_transactions = get_income_expense_df()

    df_bi_weekly_transactions = df_bi_weekly_transactions[
        (df_bi_weekly_transactions["Type"] == "biweekly")
    ]

    df_bi_weekly_transactions["When"] = pd.to_datetime(
        df_bi_weekly_transactions["When"]
    )

    df_bi_weekly_transactions = df_bi_weekly_transactions[
        (pd.to_datetime(date) - df_bi_weekly_transactions["When"]).dt.days % 14 == 0
    ]

    return df_bi_weekly_transactions


def get_oncely_transactions_for_date(date):
    df_oncely_transactions = get_income_expense_df()

    df_oncely_transactions = df_oncely_transactions[
        (df_oncely_transactions["Type"] == "oncely")
    ]

    df_oncely_transactions["When"] = pd.to_datetime(df_oncely_transactions["When"])

    df_oncely_transactions = df_oncely_transactions[
        df_oncely_transactions["When"] == pd.to_datetime(date)
    ]

    return df_oncely_transactions


def get_every_x_days_transactions_for_date(date):
    df_every_x_days_transactions = get_income_expense_df()

    df_every_x_days_transactions = df_every_x_days_transactions[
        (df_every_x_days_transactions["Type"] == "everyXDays")
    ]

    df_every_x_days_transactions["When"] = pd.to_datetime(
        df_every_x_days_transactions["When"]
    )

    df_every_x_days_transactions = df_every_x_days_transactions[
        (pd.to_datetime(date) - df_every_x_days_transactions["When"]).dt.days
        % df_every_x_days_transactions["AfterDays"]
        == 0
    ]

    return df_every_x_days_transactions


def get_all_transactions_for_date(date):
    df_all_transactions = pd.concat(
        [
            get_monthly_transactions_for_date(date),
            get_yearly_transactions_for_date(date),
            get_bi_weekly_transactions_for_date(date),
            get_oncely_transactions_for_date(date),
            get_every_x_days_transactions_for_date(date),
        ]
    )
    return df_all_transactions


# %%
# Future Cast #


def get_expected_transactions_for_date_range(num_days_back, num_days_forward):
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
        df_all_transactions_for_date = get_all_transactions_for_date(date)
        df_all_transactions_for_date["Date"] = date

        df_recent_transactions = pd.concat(
            [df_recent_transactions, df_all_transactions_for_date], ignore_index=True
        )

        df_recent_transactions = df_recent_transactions[ls_columns]

    return df_recent_transactions


def update_transactions():
    num_days_back = 5
    num_days_forward = NUM_DAYS

    # get current balance from chase: https://www.chase.com/
    current_balance = get_current_balance("Chase Checking")
    print(f"current_balance of Chase Checking: {current_balance}")

    df_existing_data_from_sheets = get_transactions_report().fillna(0)
    df_existing_data_from_sheets["Running_Balance"] = 0

    df_updated_transactions = get_expected_transactions_for_date_range(
        num_days_back, num_days_forward
    )

    df_updated_transactions = pd.concat(
        [df_existing_data_from_sheets, df_updated_transactions], ignore_index=True
    )

    df_updated_transactions = df_updated_transactions.drop_duplicates(
        subset=["Date", "Account_Name"],
        keep="first",
    )

    # sort by date and amount
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

    df_updated_transactions.to_csv(
        os.path.join(data_dir, "future_cast.csv"), index=False
    )

    WriteToSheets(
        "Our_Cash",
        "Transactions_Report",
        df_updated_transactions,
    )

    return df_updated_transactions


# %%
# Chart #


def get_label_dates_df(df_future_cast):
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


def get_daily_balances_df(df_future_cast):
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


def get_future_cast_alert_dates_df(df_future_cast):
    df_future_cast_alert_dates = get_daily_balances_df(df_future_cast)

    df_future_cast_alert_dates = df_future_cast_alert_dates[
        df_future_cast_alert_dates["Running_Balance"] < THRESHOLD_FOR_ALERT
    ]

    # date is after ten days ago
    df_future_cast_alert_dates = df_future_cast_alert_dates[
        (
            pd.to_datetime("today") - pd.to_datetime(df_future_cast_alert_dates["Date"])
        ).dt.days
        <= 1
    ]

    df_future_cast_alert_dates = df_future_cast_alert_dates[["Date", "Running_Balance"]]

    return df_future_cast_alert_dates


def get_daily_balance_report(df_future_cast):
    df_future_cast_end_of_each_day = get_daily_balances_df(df_future_cast)

    df_future_cast_label_dates = get_label_dates_df(df_future_cast)

    df_future_cast_end_of_each_day = pd.merge(
        df_future_cast_end_of_each_day,
        df_future_cast_label_dates,
        how="left",
        on="Date",
    )

    df_future_cast_end_of_each_day["Label_Item"] = df_future_cast_end_of_each_day[
        "Label_Item"
    ].fillna("")

    emergency_fund_amount = get_emergency_fund_amount()
    df_future_cast_end_of_each_day["Emergency_Fund_Amount"] = emergency_fund_amount * -1
    df_future_cast_end_of_each_day["Alert_Threshold"] = THRESHOLD_FOR_ALERT
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

    WriteToSheets(
        "Our_Cash",
        "Daily_Balance_Report",
        df_future_cast_end_of_each_day,
    )

    return df_future_cast_end_of_each_day


def chart_future_cast(df_future_cast):
    df_future_cast_end_of_each_day = get_daily_balance_report(df_future_cast)

    # Set figure size
    fig, ax = plt.subplots(figsize=(12, 6))

    # Plot the line chart for columns except Label_Amount
    columns_to_plot = [
        "Running_Balance",
        "Emergency_Fund_Amount",
        "Alert_Threshold",
        "Zero",
    ]
    df_future_cast_end_of_each_day.plot(
        x="Date",
        y=columns_to_plot,
        kind="line",
        ax=ax,
    )

    # Plot dots for Label_Amount where there are values
    label_amount_data = df_future_cast_end_of_each_day["Label_Amount"]
    label_amount_mask = ~label_amount_data.isna()
    ax.plot(
        df_future_cast_end_of_each_day.loc[label_amount_mask, "Date"],
        df_future_cast_end_of_each_day.loc[label_amount_mask, "Label_Amount"],
        marker="o",
        linestyle="",
        label="One Time Transactions",
    )

    # Add in-line labels for Label_Amount with corresponding Label_Item
    for idx, row in df_future_cast_end_of_each_day.loc[label_amount_mask].iterrows():
        ax.annotate(
            f"{row['Label_Item']}: {row['Label_Amount']}",
            (row["Date"], row["Label_Amount"]),
            textcoords="offset points",
            xytext=(10, 0),  # Adjust x-coordinate to move labels to the right
            ha="left",  # Align labels to the left of the dots
            va="center",  # Center the labels vertically
        )

    # Set labels for the axes
    ax.set_xlabel("Date")
    ax.set_ylabel("Running_Balance")
    ax.set_title("Running_Balance_Over_Time")

    # Customize x-axis tick labels
    ax.xaxis.set_major_locator(mdates.MonthLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    ax.tick_params(axis="x", rotation=90)
    ax.set_ylim([-10000, 60000])

    # Show only the first day of each month
    ax.xaxis.set_minor_locator(mdates.MonthLocator(bymonthday=1))

    # Show the chart with legend
    ax.legend()
    plt.show()

    print("One Time Transactions:")
    pprint_df(df_future_cast_end_of_each_day.loc[label_amount_mask])


# %%
# Run #


if __name__ == "__main__":
    update_income_expense_from_sheets()
    update_account_balances_from_sheets()
    update_account_details_from_sheets()
    update_transactions_report_from_sheets()

    df_future_cast = update_transactions()
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

    df_daily_balance_report = get_daily_balance_report(df_future_cast)
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

    df_future_cast_label_dates = get_label_dates_df(df_future_cast)
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

    df_future_cast_alert_dates = get_future_cast_alert_dates_df(df_future_cast)
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

    chart_future_cast(df_future_cast)

    print_logger("Done")


# %%
