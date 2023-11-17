# %%
## Imports ##

import jaydebeapi
import json
import os
import pandas as pd
import sys
from dotenv import load_dotenv

# append grandparent
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.config_utils import great_grandparent_dir, grandparent_dir, file_dir

from utils.display_tools import print_logger, pprint_df, pprint_ls


# %%
## Environment ##

dotenv_path = os.path.join(grandparent_dir, ".env")
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)

user_env_path = os.path.join(grandparent_dir, "user.env")
if os.path.exists(user_env_path):
    load_dotenv(user_env_path)


# %%
## Queries ##


def query_cdp(query):
    print_logger(f"Running query: {query}", level="info")
    dwh_host = os.getenv("CDP_HOST")
    dwh_user = os.getenv("CDP_USERNAME")
    dwh_password = os.getenv("CDP_PASSWORD")

    impala_driver = "ImpalaJDBC41.jar"
    conn = jaydebeapi.connect(
        "com.cloudera.impala.jdbc41.Driver",
        dwh_host,
        [dwh_user, dwh_password],
        os.path.join(file_dir, impala_driver),
    )

    dwh_cursor = conn.cursor()
    dwh_cursor.execute(query)
    dwh_data = dwh_cursor.fetchall()
    col_names = [v[0] for v in dwh_cursor.description]
    return_data = [v for v in dwh_data]
    dwh_cursor.close()
    conn.close()
    df = pd.DataFrame(return_data)
    df.columns = col_names
    return df


def execute_cdp_script_from_file(filename):
    fd = open(filename, "r")
    sqlFile = fd.read()
    fd.close()
    sqlCommands = sqlFile.split(";")

    for command in sqlCommands:
        result = query_cdp(command)

    return result


# %%
