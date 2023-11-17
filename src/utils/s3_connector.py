# %%
## Imports ##

if __name__ != "__main__":
    print(f"Importing {__name__}")


import os
import json
import pandas as pd
import boto3
import datetime
import sys
from dotenv import load_dotenv

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

file_dir = os.path.dirname(os.path.realpath(__file__))


# %%
## Load json creds ##

dotenv_path = os.path.join(grandparent_dir, ".env")
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)

dict_account_types = {
    "s3_bi_developer": "S3_CREDENTIALS_BI_DEVELOPER",
    "s3_uploader": "S3_CREDENTIALS_UPLOADER",
    "s3_uploader_2": "S3_CREDENTIALS_UPLOADER_2",
    "limesync": "S3_CREDENTIALS_LIMESYNC",
}


def get_credentials(account_type):
    cred_env_key = dict_account_types[account_type]

    creds = json.loads(os.getenv(cred_env_key), strict=False)

    return creds


def get_s3_object(account_type):
    creds = get_credentials(account_type)

    if account_type == "s3_bi_developer":
        s3 = boto3.client(
            "s3",
            aws_access_key_id=creds["aws_access_key_id"],
            aws_secret_access_key=creds["aws_secret_access_key"],
            aws_session_token=creds["aws_session_token"],
        )

    elif (
        account_type == "s3_uploader"
        or account_type == "s3_uploader_2"
        or account_type == "limesync"
    ):
        s3 = boto3.client(
            "s3",
            aws_access_key_id=creds["aws_access_key_id"],
            aws_secret_access_key=creds["aws_secret_access_key"],
        )

    return s3


# %%
## Functions ##


def list_buckets(account_type="s3_bi_developer"):
    s3 = get_s3_object(account_type)

    response = s3.list_buckets()
    buckets = [bucket["Name"] for bucket in response["Buckets"]]
    return buckets


def upload_df_to_bucket(
    df,
    file_name,
    save_path=None,
    bucket_name="hf-bi-dwh-uploader",
    account_type="s3_uploader_2",
):
    start_time = datetime.datetime.now()
    print(
        f"Uploading {file_name} to S3 bucket {bucket_name} as {account_type}, with size {df.shape}"
    )

    if save_path is None:
        save_path = f"sfp__{file_name}/{file_name}.csv"

    creds = get_credentials(account_type)
    write_path = f"s3a://{bucket_name}/{save_path}"
    print(f"Writing df of size {df.shape} to {write_path}")

    df.to_csv(
        write_path,
        index=False,
        storage_options={
            "key": creds["aws_access_key_id"],
            "secret": creds["aws_secret_access_key"],
        },
    )

    print(
        f"Uploaded to {save_path} with size {df.shape}, after {datetime.datetime.now() - start_time}"
    )


# %%
## Test ##

if __name__ == "__main__":
    print("Testing s3_bi_developer")
    print(get_s3_object(account_type="s3_bi_developer"))

    print("Testing s3_uploader")
    print(get_s3_object(account_type="s3_uploader"))

    print("Testing s3_uploader_2")
    print(get_s3_object(account_type="s3_uploader_2"))

    print("Testing limesync")
    print(list_buckets("limesync"))


# %%
## Notes ##

"""
Current Statuses:

list_buckets : account_type="s3_bi_developer" : works with new credentials each hour
list_buckets : account_type="s3_uploader" : might not work because only upload
list_buckets : account_type="limesync" : works
upload_df_to_bucket : account_type="s3_bi_developer" " works with new credentials each hour
upload_df_to_bucket : account_type="s3_uploader" : doesnt work because no token
upload_df_to_bucket : account_type="s3_uploader_2" : works
"""

"""
Go to:
https://myapplications.microsoft.com/
and then open AWS SSO sign in, BIDeveloper, programatic access, and get new credentials for json file at root of directoy or parent dir
"""


# %%
