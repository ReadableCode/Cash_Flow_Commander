# %%
## Imports ##

if __name__ != "__main__":
    print(f"Importing {__name__}")

import datetime
import os
from async_timeout import timeout
import looker_sdk
import csv
import time
import pandas as pd
from looker_sdk import methods40, models40
import sys
from dotenv import load_dotenv
import tempfile
import json

# append grandparent
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config_utils import (
    file_dir,
    parent_dir,
    grandparent_dir,
    great_grandparent_dir,
    data_dir,
)

from utils.google_tools import gc, WriteToSheets
from utils.display_tools import print_logger, pprint_df, pprint_ls


# %%
## Define SDK ##

dotenv_path = os.path.join(grandparent_dir, ".env")
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)

looker_api_credentials = json.loads(os.environ["LOOKER_INI"])

header = looker_api_credentials["header"]
client_id = looker_api_credentials["client_id"]
base_url = looker_api_credentials["base_url"]
client_secret = looker_api_credentials["client_secret"]
verify_ssl = looker_api_credentials.get("verify_ssl", True)

# use a temp file to inject ini config
temp_ini_file = tempfile.NamedTemporaryFile(mode="w", delete=False)
temp_ini_file.write(
    f"""
[{header}]
base_url={base_url}
client_id={client_id}
client_secret={client_secret}
verify_ssl={verify_ssl}
"""
)
temp_ini_file.close()

# create sdk
sdk = looker_sdk.init40(config_file=temp_ini_file.name)
my_user = sdk.me()


# %%
## Querying ##


def get_result_from_query(dashboard_id, dashboard_title, limit="50000"):
    tile = sdk.search_dashboard_elements(
        dashboard_id=dashboard_id, title=dashboard_title
    )
    tile_query = tile[0].query_id

    result = sdk.run_query(
        tile_query,
        result_format="csv",
        limit=limit,
        cache="true",
        apply_formatting="true",
        apply_vis="true",
        server_table_calcs="true",
        transport_options={"timeout": 900},
    )
    return result


# %%
## Test ##

if __name__ == "__main__":
    result = get_result_from_query(
        dashboard_id="1123", dashboard_title="Wonolo Requests (All Jobs)", limit="10"
    )
    print(result)


# %%
