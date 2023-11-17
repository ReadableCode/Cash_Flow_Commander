# %%
## Imports ##

if __name__ != "__main__":
    print(f"Importing {__name__}")

import os, glob
import matplotlib.pyplot as plt
from tabulate import tabulate
import sys
import re
from datetime import datetime
import warnings

warnings.filterwarnings("ignore")

from config_scripts import (
    home_dir,
    log_dir,
    file_dir,
    parent_dir,
    grandparent_dir,
    great_grandparent_dir,
    gdrive_path,
    data_dir,
    docs_dir,
)

sys.path.append(file_dir)
sys.path.append(parent_dir)
sys.path.append(grandparent_dir)


# %%
## Read Logs ##

log_file_locs = glob.glob(os.path.join(log_dir, "*.txt")) + glob.glob(
    os.path.join(log_dir, "*.log")
)

# get week in format "2023-W32"
current_week = datetime.now().strftime("%Y-W%W")

for file_loc in log_file_locs:
    file_name = os.path.basename(file_loc)
    print(f"file_name: {file_name}")
    file_path = os.path.dirname(file_loc)
    print(f"file_path: {file_path}")
    file_name_with_week = current_week + "_" + file_name
    print(f"new file name: {file_name_with_week}")
    archive_folder_name = re.sub(r"\.(txt|log)$", "", file_name)
    os.makedirs(os.path.join(file_path, archive_folder_name), exist_ok=True, mode=0o777)
    os.rename(
        file_loc, os.path.join(file_path, archive_folder_name, file_name_with_week)
    )
    open(file_loc, "a").close()
    os.chmod(file_loc, 0o777)


# %%
