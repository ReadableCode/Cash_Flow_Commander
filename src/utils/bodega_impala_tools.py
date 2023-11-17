# %%
## Get Raw Data from Bodega Impala ##

if __name__ != "__main__":
    print(f"Importing {__name__}")

import psycopg2
from impala.dbapi import connect
import os
import pandas as pd
import json
import sys
from dotenv import load_dotenv

# append grandparent
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.config_utils import great_grandparent_dir, grandparent_dir, file_dir


# %%
## Credentials ##

dotenv_path = os.path.join(grandparent_dir, ".env")
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)

cred_env_key = "BODEGA_IMPALA_CREDENTIALS"
creds = json.loads(os.getenv(cred_env_key), strict=False)


# %%
## Get Raw Data ##


def query_bodega_impala(query):
    print(f"Running query: {query}")
    ##  Creating SQL Connections  ##
    conn = psycopg2.connect(
        host=creds["host"],
        database=creds["database"],
        user=creds["user"],
        password=creds["password"],
    )

    query_cursor = conn.cursor()
    query_cursor.execute(query)
    data = query_cursor.fetchall()
    col_names = [v[0] for v in query_cursor.description]
    return_data = [v for v in data]
    query_cursor.close()
    conn.close()
    df = pd.DataFrame(return_data)
    df.columns = col_names
    return df


def execute_bodega_impala_script_from_file(filename):
    print(f"Running query from file: {filename}")
    fd = open(filename, "r")
    sqlFile = fd.read()
    fd.close()
    sqlCommands = sqlFile.split(";")

    for command in sqlCommands:
        result = query_bodega_impala(command)

    return result


# %%
