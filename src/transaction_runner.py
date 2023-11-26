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


# %%
## Check for files ##


def check_for_files():
    folder_path = os.path.join(data_dir, "laura_transactions")
    ls_files = glob.glob(os.path.join(folder_path, "*.csv"))
    if len(ls_files) == 0:
        print_logger(f"No files found in {data_dir}, exiting.")
        return False
    else:
        print_logger(f"{len(ls_files)} Files found in {data_dir}, continuing.")
        return True


if check_for_files():
    from transaction_parser import update_and_get_transactions

    df_transactions = update_and_get_transactions()

    print_logger("Updated Transactions")


# %%
