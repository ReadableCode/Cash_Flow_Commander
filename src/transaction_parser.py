# %%
## Running Imports ##

import pandas as pd
import glob
import os
import functools

import warnings

warnings.filterwarnings("ignore")

from config import (
    file_dir,
    parent_dir,
    grandparent_dir,
    great_grandparent_dir,
    data_dir,
    log_dir,
)

from utils.google_tools import (
    WriteToSheets,
    get_book_sheet_df,
    get_book_sheet_from_id_name,
    get_book_from_id,
    get_book,
    get_book_sheet,
)

from utils.display_tools import print_logger, pprint_df, pprint_ls


# %% [markdown]
## Get Transactions and Balances from Sites ##


## Chase

# - [Chase](https://secure02ea.chase.com/web/auth/dashboard#/dashboard/overviewAccounts/overview/multiProduct)

#   - Click each account and then download arrow top right corner, download max range of transactions including overlap of what is currently downloaded


# %%
## Variables ##

dict_col_renames = {
    "Posting Date": "Post Date",
    "Date": "Transaction Date",
    "Details": "Transactions",
    "Name": "Description",
}
ls_cols_initialize_if_missing = [
    "Income/Expense",
    "Income/Expense_Category",
    "Client_Name",
    "Exclude_From_Income_Expenses",
    "Post Month",
    "Category",
    "Check or Slip #",
    "Transaction Date",
    "Memo",
    "Transactions",
]
transactions_col_order = [
    "Income/Expense",
    "Income/Expense_Category",
    "Client_Name",
    "Exclude_From_Income_Expenses",
    "Post Month",
    "Post Date",
    "Description",
    "Amount",
    "Type",
    "Check or Slip #",
    "Account Name",
    "Category",
    "Transaction Date",
    "Memo",
    "Transactions",
]

accountant_columns = [
    "Income/Expense",
    "Income/Expense_Category",
    "Client_Name",
    "Post Month",
    "Post Date",
    "Description",
    "Amount",
]


# %%
## Category Functions ##


def get_transaction_category_details(
    description, income_expense, income_expense_category, client_name, exclude
):
    description = description.strip().lower()
    description = " ".join(description.split())
    if exclude == "TRUE":
        return "", "", ""

    if "zelle" in description and "from" in description:
        income_expense = "Income"
        income_expense_category = "Zelle"
        client_name = " ".join(
            description.split("from")[-1].strip().split()[0:-1]
        ).title()
    elif "venmo" in description and "cashout" in description:
        income_expense = "Income"
        income_expense_category = "Venmo"
    elif "remote online deposit" in description:
        income_expense = "Income"
        income_expense_category = "Check"

    return income_expense, income_expense_category, client_name


# %%
## Output Generation ##


def generate_accountant_export(df_transactions):
    # if any of "Income/Expense", "Income/Expense_Category", "Client_Name" are not empty and exclude is not TRUE then include
    df_transactions = df_transactions[
        (
            (df_transactions["Income/Expense"] != "")
            | (df_transactions["Income/Expense_Category"] != "")
            | (df_transactions["Client_Name"] != "")
        )
        & (df_transactions["Exclude_From_Income_Expenses"] != "TRUE")
    ]

    pprint_df(df_transactions.head(20))

    df_transactions = df_transactions[accountant_columns]

    return df_transactions


# %%
## Transaction Functions ##


def get_transactions_from_file(file_path):
    df_transactions = pd.read_csv(
        file_path,
        low_memory=False,
    )
    df_transactions.rename(columns=dict_col_renames, inplace=True)
    if "Balance" in df_transactions.columns.tolist():
        df_transactions["Account Name"] = "Chase Debit"
    else:
        df_transactions["Account Name"] = "Chase Rewards"
    for col in ls_cols_initialize_if_missing:
        if col not in df_transactions.columns.tolist():
            df_transactions[col] = ""
    df_transactions = df_transactions[transactions_col_order]

    return df_transactions


def get_transactions_from_incoming_files():
    df_new_transactions = pd.DataFrame()

    folder_path = os.path.join(data_dir, "laura_transactions")
    done_folder_path = os.path.join(folder_path, "done")
    ls_files = glob.glob(os.path.join(folder_path, "*.csv"))
    for file_path in ls_files:
        df_this_file = get_transactions_from_file(file_path)
        df_new_transactions = pd.concat([df_new_transactions, df_this_file])
        # move to done folder
        file_name = os.path.basename(file_path)
        os.rename(file_path, os.path.join(done_folder_path, file_name))

    return df_new_transactions


def update_and_get_transactions():
    df_current_transactions = get_book_sheet_df("Profit and Loss", "Transactions")
    df_new_transactions = get_transactions_from_incoming_files()
    df_transactions = pd.concat([df_current_transactions, df_new_transactions])

    # datetime date column
    df_transactions["Post Date"] = pd.to_datetime(df_transactions["Post Date"])
    df_transactions["Post Month"] = df_transactions["Post Date"].dt.strftime("%Y-%m")

    df_transactions.drop_duplicates(
        subset=[
            "Post Date",
            "Description",
            "Amount",
        ],
        inplace=True,
        keep="first",
    )

    # apply get_transaction_category_details
    df_transactions[
        ["Income/Expense", "Income/Expense_Category", "Client_Name"]
    ] = df_transactions.apply(
        lambda row: pd.Series(
            get_transaction_category_details(
                row["Description"],
                row["Income/Expense"],
                row["Income/Expense_Category"],
                row["Client_Name"],
                row["Exclude_From_Income_Expenses"],
            )
        ),
        axis=1,
    )

    df_transactions.sort_values(by=["Post Date"], ascending=False, inplace=True)

    WriteToSheets("Profit and Loss", "Transactions", df_transactions)

    df_accountant = generate_accountant_export(df_transactions.copy())
    WriteToSheets("Profit and Loss", "Accountant_Export", df_accountant)

    df_grouped = df_accountant.copy()
    df_grouped["Amount"] = df_grouped["Amount"].astype(float)
    df_grouped = (
        df_grouped.groupby(
            [
                "Post Month",
                "Income/Expense",
                "Income/Expense_Category",
                "Client_Name",
            ]
        )
        .agg({"Amount": "sum"})
        .reset_index()
    )
    WriteToSheets("Profit and Loss", "Summary", df_grouped)

    df_pivot = df_grouped.copy()
    # group to drop client
    df_pivot = (
        df_pivot.groupby(
            [
                "Post Month",
                "Income/Expense",
                "Income/Expense_Category",
            ]
        )
        .agg({"Amount": "sum"})
        .reset_index()
    )
    df_pivot["Amount"] = df_pivot["Amount"].astype(float)
    df_pivot = df_pivot.pivot(
        index=["Income/Expense", "Income/Expense_Category"],
        columns="Post Month",
        values="Amount",
    ).reset_index()
    df_pivot.fillna(0, inplace=True)
    WriteToSheets("Profit and Loss", "Pivot", df_pivot)

    return df_transactions


# %%
## Main ##

if __name__ == "__main__":
    print_logger("Running laura_transaction_parser.py")

    df_transactions = update_and_get_transactions()

    print_logger("Done running laura_transaction_parser.py")


# %%
