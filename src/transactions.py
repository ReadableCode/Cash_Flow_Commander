# %%
# Running Imports #

import pandas as pd

from utils.display_tools import pprint_df, print_logger  # noqa F401
from utils.google_drive_tools import (
    download_and_get_drive_file_path,
    get_file_list_from_folder_id_file_path,
)
from utils.google_tools import WriteToSheets

# %%
# Vars #

laura_folder_id = ""
this_year_sheet_id = ""
year = ""

dict_dfs = {}

# %%
# Get Transactions #


def get_transactions_files():
    ls_transaction_files = get_file_list_from_folder_id_file_path(
        laura_folder_id, [year]
    )
    print(f"Files in Laura's folder: {ls_transaction_files}")
    return ls_transaction_files


def read_chase_csv(file_path):
    # index_col=False prevents pandas from using the first data column as the
    # index when trailing commas create more fields than headers.
    df = pd.read_csv(file_path, index_col=False, low_memory=False)
    # Drop any unnamed columns (extra columns from trailing commas)
    df = df.loc[:, ~df.columns.str.contains("^Unnamed")]
    return df


def get_all_transactions():
    key = "all_transactions"
    if key in dict_dfs:
        print(f"Using cached DataFrame for key: {key}")
        return dict_dfs[key].copy()

    ls_transaction_files = get_transactions_files()
    ls_dfs = []

    for file_obj in ls_transaction_files:
        # download the file
        file_path = download_and_get_drive_file_path(
            root_folder_id=laura_folder_id,
            ls_file_path=[year, file_obj["name"]],
            force_download=False,
            dest_root_dir_override=None,
        )

        print(f"Reading file: {file_path}")
        df = read_chase_csv(file_path)
        # add column for file path
        df["file_name"] = file_obj["name"]
        # if balance in columns then column says checking, else credit card
        if "Balance" in df.columns:
            df["account_type"] = "checking"
        else:
            df["account_type"] = "credit_card"

        # rename multiple types to the same schema
        df.rename(
            columns={
                "Transaction Date": "transaction_date",
                "Post Date": "post_date",
                "Description": "description",
                "Category": "category",
                "Type": "type",
                "Amount": "amount",
                "Memo": "memo",
                "Details": "details",
                "Posting Date": "post_date",
                "Balance": "balance",
                "Check or Slip #": "check_or_slip_number",
            },
            inplace=True,
        )

        ls_dfs.append(df)

    df_all_transactions = pd.concat(ls_dfs, ignore_index=True)

    df_all_transactions = df_all_transactions[
        [
            "account_type",
            "file_name",
            # "transaction_date",
            "post_date",
            "description",
            # "category",
            "type",
            "amount",
            # "memo",
            # "details",
            # "balance",
            # "check_or_slip_number",
        ]
    ]

    print(f"All transactions DataFrame shape: {df_all_transactions.shape}")

    dict_dfs[key] = df_all_transactions.copy()

    return df_all_transactions


def get_formatted_transactions():
    df = get_all_transactions()

    # filter out checking account where negative
    df = df[~((df["account_type"] == "checking") & (df["amount"] < 0))]

    # filter out credit card payments
    df = df[~((df["account_type"] == "credit_card") & (df["amount"] > 0))]

    # if above 0 then income if below then expense
    df["income_or_expense"] = df["amount"].apply(
        lambda x: "income" if x > 0 else "expense"
    )

    pprint_df(df.head(50))
    pprint_df(df.tail(50))
    print(df.columns.tolist())
    print(f"Shape after filtering: {df.shape}")

    return df


def write_transcations_to_sheet(df):
    WriteToSheets(
        bookName="2025 Profit and Loss",
        sheetName="Transactions",
        df=df,
        indexes=False,
        set_note=None,
        retries=3,
    )


df_tran_form = get_formatted_transactions()
# write_transcations_to_sheet(df_tran_form)


# %%
# Main #

if __name__ == "__main__":

    print("pass")


# %%
