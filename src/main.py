# %%
# Running Imports #

import os
import warnings

import pandas as pd
from dotenv import load_dotenv

from config import parent_dir
from utils.display_tools import pprint_df, print_logger  # noqa F401
from utils.google_tools import WriteToSheets, get_book_sheet_df

warnings.filterwarnings("ignore")


# %%
# Environment #

dotenv_path = os.path.join(parent_dir, ".env")
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)


# %%
# Functions #


# %%
