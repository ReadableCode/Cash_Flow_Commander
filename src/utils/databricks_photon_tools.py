# %%
## Imports ##

if __name__ != "__main__":
    print(f"Importing {__name__}")

from databricks import sql
import os
import json
import sys
from dotenv import load_dotenv
import pandas as pd
import subprocess

# append grandparent
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config_utils import (
    file_dir,
    parent_dir,
    grandparent_dir,
    great_grandparent_dir,
    data_dir,
    query_dir,
)

from utils.display_tools import pprint_df, print_logger


# %%
## Secrets ##

dotenv_path = os.path.join(grandparent_dir, ".env")
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)

user_dotenv_path = os.path.join(grandparent_dir, "user.env")
if os.path.exists(user_dotenv_path):
    load_dotenv(user_dotenv_path)


TOKEN_TYPE = "service"
if TOKEN_TYPE == "service":
    HTTP_PATH = "/sql/1.0/warehouses/b0736ed943ca57c5"
    DATABRICKS_TOKEN = os.getenv("DATABRICKS_NA_FINOPS_SERVICE_TOKEN")
elif TOKEN_TYPE == "user":
    HTTP_PATH = "/sql/1.0/warehouses/96c9036f3d79463f"
    DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")


# %%
## Functions ##


def check_vpn_connected():
    try:
        result = subprocess.run(["pgrep", "openvpn3"], stdout=subprocess.PIPE)
        return bool(result.stdout)
    except Exception as e:
        return False


def connect_vpn():
    home = os.path.expanduser("~")
    config_path = os.path.join(home, "hellofresh.ovpn")
    subprocess.run(["openvpn3", "session-start", "--config", config_path])


if not check_vpn_connected():
    print_logger("VPN not connected. Connecting...")
    try:
        connect_vpn()
        print_logger("VPN connected.")
    except Exception as e:
        print_logger(f"Error connecting to VPN: {e}")
else:
    print_logger("VPN already connected.")


# %%
## Functions ##


def query_databricks_photon(query: str) -> pd.DataFrame:
    """
    Query Databricks Photon using the Databricks SQL endpoint.
    """
    print_logger(f"Running query: {query}", level="info")
    try:
        connection = sql.connect(
            server_hostname="hf-query-engine.cloud.databricks.com",
            http_path=HTTP_PATH,
            access_token=DATABRICKS_TOKEN,
        )
    except EOFError as e:
        print_logger(f"EOFError: {e}\nMake sure you are connected to VPN")
        raise e

    cursor = connection.cursor()

    cursor.execute(query)

    # Fetch column names separately
    column_names = [desc[0] for desc in cursor.description]

    # Fetch data rows
    data_rows = cursor.fetchall()[1:]

    cursor.close()
    connection.close()

    # Create DataFrame with column names and data rows
    df = pd.DataFrame(data_rows, columns=column_names)

    return df


def execute_sql_script_databricks_photon(filename: str) -> pd.DataFrame:
    """
    Execute a SQL script on Databricks Photon using the Databricks SQL endpoint.
    """
    try:
        connection = sql.connect(
            server_hostname="hf-query-engine.cloud.databricks.com",
            http_path=HTTP_PATH,
            access_token=DATABRICKS_TOKEN,
        )
    except EOFError as e:
        print_logger(f"EOFError: {e}\nMake sure you are connected to VPN")
        raise e

    cursor = connection.cursor()

    # Open and read the file as a single buffer
    fd = open(os.path.join(query_dir, filename), "r")
    sqlFile = fd.read()
    fd.close()

    # Execute every command from the input file
    df = pd.DataFrame()
    sqlCommands = sqlFile.split(";")
    for command in sqlCommands:
        # Skip if command is empty (which can happen after splitting by ";")
        if command.strip() == "":
            continue

        cursor.execute(command)

        # Fetch column names from the cursor
        columns = [column[0] for column in cursor.description]

        # Fetch data rows
        data_rows = cursor.fetchall()

        # Create DataFrame for the current command
        df_current = pd.DataFrame(data_rows, columns=columns)

        # If 'df' is empty, initialize it with 'df_current'
        if df.empty:
            df = df_current
        else:
            # Otherwise, concatenate 'df_current' with 'df'
            df = pd.concat([df, df_current])

    cursor.close()
    connection.close()

    return df


def list_databricks_databases():
    """
    List DATABASES in Databricks Photon using the Databricks SQL endpoint.
    """
    query = "SHOW DATABASES"

    df = query_databricks_photon(query)
    df.columns = ["database"]

    return df


def list_databricks_schemas():
    """
    List SCHEMAS in Databricks Photon using the Databricks SQL endpoint.
    """
    query = "SHOW SCHEMAS"

    df = query_databricks_photon(query)
    df.columns = ["schema"]

    return df


def list_databricks_tables(schema="uploads"):
    """
    List tables in Databricks Photon using the Databricks SQL endpoint.
    """
    query = f"SHOW TABLES IN {schema}"

    df = query_databricks_photon(query)
    df.columns = ["database", "table", "isTemporary"]

    return df


# %%
