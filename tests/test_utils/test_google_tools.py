# %%
# Imports #

import os
import sys
import pytest
import pandas as pd
import numpy as np

import config_test_utils

from src.utils.google_tools import get_book_sheet_df

from src.utils.display_tools import pprint_ls, pprint_df, print_logger

# %%
# Tests #


def test_get_book_sheet_df():
    df = get_book_sheet_df("TestApp", "TestApp")
    pprint_df(df.head(20))
    assert isinstance(df, pd.DataFrame)


# %%
# Main #

if __name__ == "__main__":
    test_get_book_sheet_df()


# %%
