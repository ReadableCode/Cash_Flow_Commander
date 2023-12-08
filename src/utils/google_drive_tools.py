# %%
# Imports #

import pygsheets
import os
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import pandas as pd
import datetime
import sys
import time
from google.auth.exceptions import TransportError
from googleapiclient.errors import HttpError
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
import json
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import io
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload
from dotenv import load_dotenv
import yaml

# append grandparent
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config_utils import (
    file_dir,
    parent_dir,
    grandparent_dir,
    great_grandparent_dir,
    data_dir,
    drive_download_cache_dir,
    temp_upload_dir,
)

from utils.display_tools import print_logger, pprint_dict, pprint_df, pprint_ls


# %%
# Google #

dotenv_path = os.path.join(grandparent_dir, ".env")
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)

if os.path.exists(os.path.join(file_dir, "google_config_override.yaml")):
    with open(os.path.join(file_dir, "google_config_override.yaml"), "r") as outfile:
        dict_google_config_override = yaml.load(outfile, Loader=yaml.FullLoader)
    google_service_account_var = dict_google_config_override["GOOGLE_SERVICE_ACCOUNT"]
    print_logger(
        f"Google Config Overridden with: {google_service_account_var}", level="debug"
    )
else:
    google_service_account_var = "GOOGLE_SERVICE_ACCOUNT_DIGITAL_PROTON"
    print_logger(
        f"Google Config NOT Overridden, using: GOOGLE_SERVICE_ACCOUNT_DIGITAL_PROTON",
        level="debug",
    )


credentials = service_account.Credentials.from_service_account_info(
    json.loads(os.getenv(google_service_account_var), strict=False),
    scopes=["https://www.googleapis.com/auth/drive"],
)
# Create a Google Drive API client
drive_service = build("drive", "v3", credentials=credentials)

ls_files_downloaded_this_run = []


# %%
# Get Functions #


def get_drive_file_id_from_folder_id_path(folder_id, ls_file_path, is_folder=False):
    """
    Given a folder ID and a list of folder and file names, returns the ID of the file with the specified name that
    is located within the final folder in the specified path.

    Args:
        folder_id (str): The ID of the top-level folder to start the search from.
        ls_file_path (List[str]): A list of folder and file names that make up the path to the desired file. The
        final item in the list should be the name of the desired file.

    Returns:
        str: The ID of the desired file.

    Raises:
        ValueError: If the specified folder or file cannot be found in the specified path.
    """
    curr_dir_id = folder_id
    for folder_name in ls_file_path[:-1]:
        print_logger(
            f"Scanning folder {folder_name} with ID {curr_dir_id}", level="debug"
        )

        file_found = False  # Initialize a flag variable to False

        while not file_found:
            # Retrieve a list of files in the specified folder
            # search for the folder by name and within the current directory
            query = f"name='{folder_name}' and '{curr_dir_id}' in parents and trashed=false and mimeType='application/vnd.google-apps.folder'"
            results = (
                drive_service.files().list(q=query, fields="files(id, name)").execute()
            )

            # Iterate through the files on the current page
            for file in results.get("files", []):
                if file.get("name") == folder_name:
                    curr_dir_id = file["id"]
                    file_found = True  # Set the flag to True when the file is found
                    break  # Exit the loop if the file is found

            page_token = results.get("nextPageToken", None)

            if page_token is None or file_found:
                break  # Exit the loop if the file is found or if there are no more pages

        if not results:
            raise ValueError(f"Folder not found: {folder_name}")

    # we've traversed to the final parent dir, now look for folder or file
    filename = ls_file_path[-1]
    if is_folder:
        query = f"name='{filename}' and '{curr_dir_id}' in parents and trashed=false and mimeType='application/vnd.google-apps.folder'"
    else:
        query = f"name='{filename}' and '{curr_dir_id}' in parents and trashed=false and mimeType!='application/vnd.google-apps.folder'"
    results = (
        drive_service.files()
        .list(q=query, fields="files(id, name)")
        .execute()
        .get("files", [])
    )

    if not results:
        raise ValueError(f"File not found: {filename}")

    return results[0]["id"]


def get_file_list_from_folder_id(folder_id):
    files = []
    page_token = None

    while True:
        # Retrieve a list of files in the specified folder
        results = (
            drive_service.files()
            .list(
                q=f"'{folder_id}' in parents and trashed=false",
                fields="nextPageToken, files(id, name)",
                pageToken=page_token,
            )
            .execute()
        )
        files.extend(results.get("files", []))
        page_token = results.get("nextPageToken", None)
        if page_token is None:
            break

    if not files:
        print_logger("No files found.")
        return None
    else:
        return files


def get_file_list_from_folder_id_file_path(root_folder_id, ls_file_path):
    ls_directory_path = ls_file_path

    folder_id = get_drive_file_id_from_folder_id_path(
        root_folder_id, ls_directory_path, is_folder=True
    )

    ls_files_dict = get_file_list_from_folder_id(folder_id)

    return ls_files_dict


def download_file_by_id(id, path, max_retries=3):
    retries = 0
    while retries < max_retries:
        try:
            # download the file
            request = drive_service.files().get_media(fileId=id)
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while done is False:
                status, done = downloader.next_chunk()

            os.makedirs(os.path.dirname(path), exist_ok=True)

            # save the downloaded file to disk
            with io.open(path, "wb") as f:
                fh.seek(0)
                f.write(fh.read())

            # If the download is successful, break out of the loop
            break
        except TimeoutError as e:
            print(f"Download attempt {retries + 1} failed: {e}")
            retries += 1
            if retries < max_retries:
                print(f"Retrying in 5 seconds...")
                time.sleep(5)  # Wait for 5 seconds before retrying
            else:
                print("Max retries reached. Download failed.")
                raise

    # Check if the download was successful
    if retries < max_retries:
        print("Download successful!")
    else:
        print("Max retries reached. Download failed.")


def download_and_get_drive_file_path(
    root_folder_id, ls_file_path, force_download=False, dest_root_dir_override=None
):
    if dest_root_dir_override is not None:
        drive_download_cache_dir_to_use = dest_root_dir_override
    else:
        drive_download_cache_dir_to_use = os.path.join(data_dir, "drive_download_cache")

    # create folders if they dont exist
    if not os.path.exists(drive_download_cache_dir_to_use):
        os.makedirs(drive_download_cache_dir_to_use)

    dest_file_path = os.path.join(
        os.path.join(drive_download_cache_dir_to_use, root_folder_id, *ls_file_path)
    )
    if dest_file_path in ls_files_downloaded_this_run:
        print_logger(f"File already downloaded this run: {ls_file_path}")
        return dest_file_path
    print_logger(f"dest_file_path: {dest_file_path}")

    if not os.path.exists(dest_file_path) or force_download:
        print_logger(f"Downloading file: {ls_file_path}")
        drive_file_id = get_drive_file_id_from_folder_id_path(
            root_folder_id, ls_file_path
        )

        # make dest dirs if they dont exist
        dest_dir = os.path.dirname(dest_file_path)
        print_logger(f"dest_dir: {dest_dir}")
        if not os.path.exists(dest_dir):
            print_logger(f"Making dir: {dest_dir}")
            os.makedirs(dest_dir)

        # download the file from google drive
        download_file_by_id(drive_file_id, dest_file_path)
        print_logger(f"Downloaded file: {ls_file_path}")
    else:
        print_logger(f"File already exists: {ls_file_path}")

    return dest_file_path


# %%
# Put Functions #


def create_folder_in_drive(drive_service, parent_id, folder_name):
    folder_metadata = {
        "name": folder_name,
        "parents": [parent_id],
        "mimeType": "application/vnd.google-apps.folder",
    }
    folder = drive_service.files().create(body=folder_metadata, fields="id").execute()
    parent_id = folder["id"]
    return parent_id


def upload_file_to_drive(drive_id, file_path, ls_folder_path=[]):
    # Start with the root folder ID
    parent_id = drive_id

    for folder_name in ls_folder_path:
        print_logger(f"Looking for folder: {folder_name}", level="info")
        folder_exists = False
        page_token = None

        while True:
            # Retrieve a list of files in the specified folder
            results = (
                drive_service.files()
                .list(
                    q=f"'{parent_id}' in parents and trashed=false",
                    fields="nextPageToken, files(id, name)",
                    pageToken=page_token,
                )
                .execute()
            )
            for file in results.get("files", []):
                if file.get("name") == folder_name:
                    folder_exists = True
                    parent_id = file["id"]
                    break

            page_token = results.get("nextPageToken", None)
            if page_token is None or folder_exists:
                break

        if not folder_exists:
            print_logger(
                f"Folder doesn't exist, creating folder: {folder_name}", level="info"
            )
            parent_id = create_folder_in_drive(drive_service, parent_id, folder_name)
            print(f"Folder: {folder_name} created with ID: {parent_id}")
        else:
            print_logger(
                f"Folder: {folder_name} exists, navigating to folder with id {parent_id}",
                level="info",
            )

    # Check if a file with the same name exists in the folder
    file_name = os.path.basename(file_path)
    existing_files = (
        drive_service.files()
        .list(
            q=f"'{parent_id}' in parents and name='{file_name}' and trashed=false",
            fields="files(id)",
        )
        .execute()
    )

    if existing_files.get("files"):
        # File with the same name exists, delete it
        existing_file_id = existing_files["files"][0]["id"]
        drive_service.files().delete(fileId=existing_file_id).execute()
        print(f"Existing file with the same name deleted (ID: {existing_file_id})")

    # Upload the new file to Google Drive within the specified folder
    file_metadata = {"name": file_name, "parents": [parent_id]}
    media = MediaFileUpload(file_path, mimetype="application/octet-stream")
    uploaded_file = (
        drive_service.files()
        .create(body=file_metadata, media_body=media, fields="id")
        .execute()
    )

    print(f'File uploaded with ID: {uploaded_file["id"]}')


# TODO URGENT move report id to config file
def upload_report(df, ls_folder_file_path=[]):
    report_folder_id = "1g5DAEIFGjwIf04dkjw0U5d72xp5xYUrm"

    # if list longer than just filename, makedirs
    if len(ls_folder_file_path) > 1:
        folder_path = os.path.join(temp_upload_dir, *ls_folder_file_path[:-1])
        os.makedirs(folder_path, exist_ok=True)
        file_path = os.path.join(folder_path, ls_folder_file_path[-1])
        drive_file_path = ls_folder_file_path[:-1]
    else:
        file_path = os.path.join(temp_upload_dir, *ls_folder_file_path)
        drive_file_path = []

    print(f"temp save file path: {file_path}")
    print(f"drive file path: {drive_file_path}")
    df.to_csv(
        file_path,
        index=False,
    )

    upload_file_to_drive(report_folder_id, file_path, drive_file_path)


# %%
