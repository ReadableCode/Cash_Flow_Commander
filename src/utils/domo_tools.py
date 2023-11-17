# %%
## Imports ##

import json
import time
from pydomo import Domo
import pandas as pd
import os
import sys
import datetime
from dotenv import load_dotenv

import warnings

warnings.filterwarnings("ignore")

# append grandparent
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config_utils import (
    file_dir,
    parent_dir,
    grandparent_dir,
    great_grandparent_dir,
    data_dir,
)

from utils.display_tools import print_logger, pprint_df, pprint_ls


# %%
## User Password Auth ##

# """
# To create Domo Developer Credentials:
# Go To https://developer.domo.com/
# Click My Account
# Click New Client
# Fill In Name and Description and check at least ['Data', 'Dashboard']
# Click Create
# Note: ~ signifies your home folder, on windows this is usually C:\Users\your_username


dotenv_path = os.path.join(grandparent_dir, ".env")
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)

creds = json.loads(os.getenv("DOMO_ACCOUNT"))

client_id = creds["client_id"]
client_secret = creds["client_secret"]


# %%
## Connections ##

domo = Domo(
    client_id,
    client_secret,
    api_host="api.domo.com",
)


# %%
## Functions ##


def create_data_set_in_domo(name, description, df):
    """
    This function will create a data set in domo using the APIa d initialize it with the data in the dataframe
    :param name: name of the data set
    :param description: description of the data set
    :param df: dataframe to initialize the data set with
    :return: the id of the data set and the response from the update
    """
    print_logger(f"Creating Data Set in Domo: {name} with df of size {df.shape}")
    start_time = datetime.datetime.now()
    created_table_id = domo.ds_create(df, name, description)
    dict_update_response = domo.ds_update(created_table_id, df)
    print_logger(
        f"Finished Creating Data Set in Domo: id is {created_table_id} with df of size {df.shape}, after {datetime.datetime.now() - start_time}"
    )
    return created_table_id, dict_update_response


def get_data_set_from_domo(id):
    """
    This function will get a data set from domo using the API
    :param id: id of the data set
    :return: the dataframe of the data set
    """
    df = domo.ds_get(id)
    return df


def update_data_set_in_domo(
    id,
    df,
    retries=3,
):
    """
    This function will update a data set in domo using the API
    :param id: id of the data set
    :param df: dataframe to update the data set with
    :param retries: number of retries to attempt
    :return: the response from the update
    """
    print_logger(f"Updating Data Set in Domo: {id} with df of size {df.shape}")
    start_time = datetime.datetime.now()
    for try_num in range(0, retries):
        try:
            df_return = domo.ds_update(id, df)
            break
        except Exception as e:
            print_logger(f"Error: {e}, retrying {try_num} of {retries} in 5 seconds")
            # wait 5 seconds
            time.sleep(5)
            print_logger(f"Retrying: {try_num} of {retries}")
            pass

    print_logger(
        f"Finished writing to Domo: {id} with df of size {df.shape}, after {datetime.datetime.now() - start_time} on try {try_num} of {retries}"
    )
    return df_return


# %%
## Main ##

if __name__ == "__main__":
    print_logger(get_data_set_from_domo("18e90612-ed66-4a5d-984e-cf64abaa6471"))

    update_data_set_in_domo(
        "18e90612-ed66-4a5d-984e-cf64abaa6471",
        pd.DataFrame(
            {
                "make": ["Honda", "Honda", "Honda", "Honda", "Honda", "Honda"],
                "model": [
                    "Civic",
                    "Civic",
                    "Civic",
                    "Civic",
                    "Civic",
                    "Civic",
                ],
                "dollars": [100, 200, 300, 400, 500, 600],
            }
        ),
    )

    print_logger(get_data_set_from_domo("18e90612-ed66-4a5d-984e-cf64abaa6471"))


# %%
## Main Append Test ##

if __name__ == "__main__":
    import numpy as np
    import time

    for i in range(0, 10):
        # get current data
        df = get_data_set_from_domo("18e90612-ed66-4a5d-984e-cf64abaa6471")

        for i in range(0, 3):
            # make a random price
            price = np.random.randint(100, 1000)

            # append new data
            df = df.append(
                {
                    "make": "Honda",
                    "model": "Civic",
                    "dollars": price,
                },
                ignore_index=True,
            )

        update_data_set_in_domo("18e90612-ed66-4a5d-984e-cf64abaa6471", df)

        # sleep for 5 seconds
        time.sleep(1)


# %%
