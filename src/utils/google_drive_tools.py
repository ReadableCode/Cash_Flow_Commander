# %%
# Imports #

import io
import json
import os
import sys
import time

import pandas as pd
from dotenv import load_dotenv
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload, MediaIoBaseDownload

# append grandparent
if __name__ == "__main__":
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config_utils import data_dir, grandparent_dir, temp_upload_dir
from utils.display_tools import print_logger

# %%
# Load Environment #

# source .env file
dotenv_path = os.path.join(grandparent_dir, ".env")
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)


GOOGLE_DRIVE_FOLDER_ID_REPORT = os.getenv("GOOGLE_DRIVE_FOLDER_ID_REPORT", None)
if GOOGLE_DRIVE_FOLDER_ID_REPORT is not None:
    print_logger(
        f"Found environment variable for google drive folder id with key: {GOOGLE_DRIVE_FOLDER_ID_REPORT}"
    )


# %%
# Google Credentials #

service_account_env_key = "GOOGLE_SERVICE_ACCOUNT"
json_file_path = os.path.join(
    grandparent_dir,
    "service_account_credentials.json",
)

if os.getenv(service_account_env_key) is not None:
    print_logger(
        f"Found environment variable for service account with key: {service_account_env_key}"
    )
    raw_json = os.getenv(service_account_env_key)

    try:
        service_account_email = json.loads(raw_json)["client_email"]
        fixed_json = raw_json
    except json.JSONDecodeError as e:
        print_logger(
            f"JSONDecodeError: {e} with reading json from environment variable, trying to repair"
        )
        fixed_json = raw_json.replace("\n", "\\n")
        service_account_email = json.loads(fixed_json)["client_email"]

        # fix environment variable without modifying the .env file
        os.environ[service_account_env_key] = fixed_json

elif os.path.exists(json_file_path):
    print_logger(
        f"No environment varible with key: {service_account_env_key}, Found json credentails at: {json_file_path}"
    )
    fixed_json = open(json_file_path).read()
    service_account_email = json.loads(fixed_json)["client_email"]

    # add environment variable without modifying the .env file
    os.environ[service_account_env_key] = fixed_json

print_logger(f"google_service_account email: {service_account_email}")

print_logger("Using service account credentials from environment variable")
# create credentials from google service account info
google_service_account_credentials = (
    service_account.Credentials.from_service_account_info(
        json.loads(os.getenv(service_account_env_key), strict=False),
        scopes=["https://www.googleapis.com/auth/drive"],
    )
)

# Create a Google Drive API client
drive_service = build("drive", "v3", credentials=google_service_account_credentials)
# increase timeout
drive_service._http.timeout = 600


# %%
# Variables #

ls_files_downloaded_this_run = []

google_drive_folder_id_report = os.getenv("GOOGLE_DRIVE_FOLDER_ID_REPORT", None)


# %%
# Get Functions #


def check_storage_space_service_account():
    """
    Retrieves and prints the storage quota information
    for the Google Drive service account.

    This function uses the Google Drive API to get the storage quota
    information for the service account
    and prints the storage quota, used storage, total storage,
    and the percentage of storage used.

    Note: This function assumes that the `drive_service`
    object has already been initialized.

    Example usage:
    check_storage_space_service_account()
    """
    # Get the about resource, which includes storage quota information
    about = drive_service.about().get(fields="storageQuota").execute()
    print(f"Storage quota: {about['storageQuota']}")
    used_storage = int(about["storageQuota"]["usage"])
    total_storage = int(about["storageQuota"]["limit"])
    used_storage_gb = used_storage / 1e9
    total_storage_gb = total_storage / 1e9
    percent_used = (used_storage / total_storage) * 100
    print(f"Using {used_storage_gb} of {total_storage_gb} GB")
    print(f"Percent Used: {percent_used} %")
    return percent_used


def get_top_storage_use_files(num_files=20, parent_folder_id=None):
    """
    Retrieves the top storage usage files from Google Drive.

    Args:
        num_files (int): The number of files to retrieve. Default is 20.
        parent_folder_id (str, optional): The ID of the parent folder to search within. Default is None.

    Returns:
        list: A list of dictionaries containing file details.
    """
    query = "'me' in owners"
    if parent_folder_id:
        query += f" and '{parent_folder_id}' in parents"

    # List files
    results = (
        drive_service.files()
        .list(
            q=query,
            pageSize=num_files,
            fields="nextPageToken, files(id, name, mimeType, size)",
            orderBy="quotaBytesUsed desc",  # Order by size, descending
        )
        .execute()
    )
    items = results.get("files", [])

    if not items:
        print("No files found.")
    else:
        print("Highest Storage Files:")
        for item in items:
            # get parent folder id
            file_id = item["id"]
            file = drive_service.files().get(fileId=file_id, fields="parents").execute()
            parent_id = file.get("parents")[0] if "parents" in file else "No parent"
            file_size = item.get("size", 0)
            file_size_MB = int(file_size) / 1e6 if file_size else 0
            # Print files name and size
            print(
                (
                    f"{file_size_MB:.2f} MB, Name: {item['name']}, "
                    f"ID: {item['id']}, Parent ID: {parent_id}"
                )
            )

    return items


def get_drive_file_id_from_folder_id_path(folder_id, ls_file_path, is_folder=False):
    """
    Given a folder ID and a list of folder and file names, returns
    the ID of the file with the specified name that
    is located within the final folder in the specified path.

    Args:
        folder_id (str): The ID of the top-level folder to start the search from.
        ls_file_path (List[str] or str): A list of folder and file names
        that make up the path to the desired file. The
        final item in the list should be the name of the desired file.

    Returns:
        str: The ID of the desired file.

    Raises:
        ValueError: If the specified folder or file cannot
        be found in the specified path.
    """

    # if ls_file_path is a string, convert to list
    if isinstance(ls_file_path, str):
        ls_file_path = [ls_file_path]

    curr_dir_id = folder_id
    for folder_name in ls_file_path[:-1]:
        print_logger(
            f"Scanning folder {folder_name} with ID {curr_dir_id}", level="debug"
        )

        file_found = False  # Initialize a flag variable to False

        while not file_found:
            # Retrieve a list of files in the specified folder
            # search for the folder by name and within the current directory
            query = (
                f"name='{folder_name}' and "
                f"'{curr_dir_id}' in parents and "
                "trashed=false and "
                "mimeType='application/vnd.google-apps.folder'"
            )

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
                # Exit the loop if the file is found or if there are no more pages
                break

        if not results:
            raise ValueError(f"Folder not found: {folder_name}")

    # we've traversed to the final parent dir, now look for folder or file
    filename = ls_file_path[-1]
    if is_folder:
        query = (
            f"name='{filename}' and '{curr_dir_id}' in parents and "
            "trashed=false and mimeType='application/vnd.google-apps.folder'"
        )
    else:
        query = (
            f"name='{filename}' and '{curr_dir_id}' in parents and trashed=false"
            "and mimeType!='application/vnd.google-apps.folder'"
        )
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
    """
    Retrieves a list of files from the specified folder ID.

    Args:
        folder_id (str): The ID of the folder.

    Returns:
        list: A list of files in the folder, each represented as
            a dictionary with 'id' and 'name' keys.
              Returns None if no files are found.
    """

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
    """
    Retrieves a list of files from a given folder ID.

    Args:
        root_folder_id (str): The ID of the root folder.
        ls_file_path (str): The path of the folder containing the files.

    Returns:
        dict: A dictionary containing the list of files in the folder.
    """

    ls_directory_path = ls_file_path

    folder_id = get_drive_file_id_from_folder_id_path(
        root_folder_id, ls_directory_path, is_folder=True
    )

    ls_files_dict = get_file_list_from_folder_id(folder_id)

    return ls_files_dict


def download_file_by_id(id, path, max_retries=3):
    """
    Downloads a file from Google Drive by its ID and saves it to the specified path.

    Args:
        id (str): The ID of the file to download.
        path (str): The path where the downloaded file will be saved.
        max_retries (int, optional): The maximum number of download
            retries in case of failure. Defaults to 3.

    Raises:
        TimeoutError: If the download fails after the maximum number of retries.

    Returns:
        None
    """
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
                print("Retrying in 5 seconds...")
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
    """
    Downloads a file from Google Drive and returns the file path.

    Args:
        root_folder_id (str): The ID of the root folder in Google Drive.
        ls_file_path (list): The list of file path components.
        force_download (bool, optional): Whether to force download the
            file even if it already exists. Defaults to False.
        dest_root_dir_override (str, optional): The destination root
            directory override. Defaults to None.

    Returns:
        str: The file path of the downloaded file.
    """
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
    """
    Creates a new folder in Google Drive.

    Args:
        drive_service (googleapiclient.discovery.Resource):
            The Google Drive service object.
        parent_id (str): The ID of the parent folder where
            the new folder will be created.
        folder_name (str): The name of the new folder.

    Returns:
        str: The ID of the newly created folder.
    """

    folder_metadata = {
        "name": folder_name,
        "parents": [parent_id],
        "mimeType": "application/vnd.google-apps.folder",
    }
    folder = drive_service.files().create(body=folder_metadata, fields="id").execute()
    parent_id = folder["id"]
    return parent_id


def upload_file_to_drive(initial_folder_id, file_path, ls_folder_path=[]):
    """
    Uploads a file to Google Drive within the specified folder path.

    Args:
        initial_folder_id (str): The ID of the initial folder
            where the file will be uploaded.
        file_path (str): The path of the file to be uploaded.
        ls_folder_path (list, optional): The list of folder names
            representing the folder path. Defaults to [].

    Returns:
        None
    """

    # Start with the root folder ID
    parent_id = initial_folder_id

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
                f"Folder: {folder_name} exists, navigating to folder id {parent_id}",
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
        # replace contents of existing file
        existing_file_id = existing_files["files"][0]["id"]
        media = MediaFileUpload(file_path, mimetype="application/octet-stream")
        drive_service.files().update(
            fileId=existing_file_id, media_body=media, fields="id"
        ).execute(num_retries=10)
        print(f"Replaced existing file with ID: {existing_file_id}")

    else:
        # Upload the new file to Google Drive within the specified folder
        file_metadata = {"name": file_name, "parents": [parent_id]}
        media = MediaFileUpload(file_path, mimetype="application/octet-stream")
        uploaded_file = (
            drive_service.files()
            .create(body=file_metadata, media_body=media, fields="id")
            .execute()
        )

        print(f'File uploaded with ID: {uploaded_file["id"]}')


# %%
# Rename Functions #


def rename_file(file_id, new_name):
    drive_service.files().update(fileId=file_id, body={"name": new_name}).execute()


# %%
# Delete Functions #


def delete_file_by_id(file_id):
    """
    Deletes a file from Google Drive by its ID.

    Args:
        file_id (str): The ID of the file to be deleted.

    Returns:
        None
    """

    # Delete the file
    drive_service.files().delete(fileId=file_id).execute()
    print(f"File with ID {file_id} deleted")


# %%
# Ownership Functions #


def download_top_used_files_for_reupload_as_user(
    num_files=30, parent_folder_id=None, actually_delete_files=False
):
    initial_percent_used = check_storage_space_service_account()
    print(f"Initial percent used: {initial_percent_used}")

    dict_items = get_top_storage_use_files(
        num_files=num_files, parent_folder_id=parent_folder_id
    )
    print(dict_items)

    base_path = os.path.join(
        os.path.dirname(os.path.dirname(grandparent_dir)), "temp_drive_reupload"
    )
    os.makedirs(base_path, exist_ok=True)
    print(base_path)

    for dict_item in dict_items:
        file_id = dict_item["id"]
        file_name = dict_item["name"]
        print(f"File Name: {file_name}")
        download_file_by_id(file_id, os.path.join(base_path, file_name))

    if actually_delete_files:
        for dict_item in dict_items:
            file_id = dict_item["id"]
            file_name = dict_item["name"]
            print(f"File Name: {file_name}")
            delete_file_by_id(file_id)

    final_percent_used = check_storage_space_service_account()
    print(f"Final percent used: {final_percent_used}")
    print(
        f"Do not forget to reupload the donwloaded files as yourself to the same folder from {base_path}"
    )


# %%
# Upload Functions #


def upload_report_csv(df, ls_folder_file_path):
    """
    Uploads a Pandas DataFrame to Google Drive.

    Args:
        df (pd.DataFrame): The DataFrame to upload.
        ls_folder_file_path (List[str] or str): A list of folder and
            file names that make up the path to the desired file.
            The final item in the list should be the name of the desired file.
            If a single string is provided, it will be
            treated as the file name. Default is an empty list.

    Raises:
        ValueError: If `google_drive_folder_id_report` is None. Configure
            it in the environment or env file.

    Notes:
        - If the `ls_folder_file_path` is a string, it will be converted
            to a list with one element.
        - If the list `ls_folder_file_path` is longer than just a filename,
            the necessary folders will be created in the temporary
          upload directory.

    Example:
        To upload a DataFrame to a specific folder on Google Drive:

        >>> df = pd.DataFrame({'Column1': [1, 2, 3], 'Column2': ['A', 'B', 'C']})
        >>> upload_report(df, ['FolderName', 'FileName.csv'])

        This will upload the DataFrame as 'FileName.csv' inside
            the 'FolderName' folder on Google Drive.

    """
    if google_drive_folder_id_report is None:
        raise ValueError(
            "google_drive_folder_id_report is None, configure in yaml file"
        )

    # if ls_folder_file_path is a string, convert to list
    if isinstance(ls_folder_file_path, str):
        ls_folder_file_path = [ls_folder_file_path]

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

    upload_file_to_drive(google_drive_folder_id_report, file_path, drive_file_path)


def upload_report_html(df, ls_folder_file_path):
    """
    Uploads a Pandas DataFrame to Google Drive.

    Args:
        df (pd.DataFrame): The DataFrame to upload.
        ls_folder_file_path (List[str] or str): A list of folder and
            file names that make up the path to the desired file.
            The final item in the list should be the name of the desired file.
            If a single string is provided, it will be
            treated as the file name. Default is an empty list.

    Raises:
        ValueError: If `google_drive_folder_id_report` is None. Configure
            it in the environment or env file.

    Notes:
        - If the `ls_folder_file_path` is a string, it will be converted
            to a list with one element.
        - If the list `ls_folder_file_path` is longer than just a filename,
            the necessary folders will be created in the temporary
          upload directory.

    Example:
        To upload a DataFrame to a specific folder on Google Drive:

        >>> df = pd.DataFrame({'Column1': [1, 2, 3], 'Column2': ['A', 'B', 'C']})
        >>> upload_report(df, ['FolderName', 'FileName.html'])

        This will upload the DataFrame as 'FileName.html' inside the
            'FolderName' folder on Google Drive.

    """
    if google_drive_folder_id_report is None:
        raise ValueError(
            "google_drive_folder_id_report is None, configure in yaml file"
        )

    # if ls_folder_file_path is a string, convert to list
    if isinstance(ls_folder_file_path, str):
        ls_folder_file_path = [ls_folder_file_path]

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
    html_style = """
    <style>
        table {
            border-collapse: collapse;
            width: 100%;
        }
        th {
            background-color: #4CAF50;
            color: white;
        }
        th, td {
            text-align: left;
            padding: 8px;
        }
        tr:nth-child(even) {
            background-color: #f2f2f2;
        }
        tr:hover {
            background-color: #ddd;
        }
    </style>
    """
    html_table = df.to_html()
    html = (
        f"<!DOCTYPE html><html><head>{html_style}"
        f"</head><body>{html_table}</body></html>"
    )
    # write to file path file_path
    with open(file_path, "w") as f:
        f.write(html)

    upload_file_to_drive(google_drive_folder_id_report, file_path, drive_file_path)


def upload_report_excel(ls_dfs, ls_tab_names, ls_folder_file_path):
    """
    Uploads a list of Pandas DataFrames to Google Drive as an Excel file.

    Args:
        ls_dfs (List[pd.DataFrame]): A list of DataFrames to upload.
        ls_tab_names (List[str]): A list of tab names for the Excel file.
            Must be the same length as `ls_dfs`.
        ls_folder_file_path (List[str] or str): A list of folder and file names
            that make up the path to the desired file.
            The final item in the list should be the name of the desired file.
                If a single string is provided, it will be
            treated as the file name. Default is an empty list.

    Raises:
        ValueError: If `google_drive_folder_id_report` is None. Configure it
            in the environment or env file.

    Notes:
        - If the `ls_folder_file_path` is a string, it will be converted to
            a list with one element.
        - If the list `ls_folder_file_path` is longer than just a filename,
            the necessary folders will be created in the temporary
          upload directory.

    Example:
        To upload a DataFrame to a specific folder on Google Drive:

        >>> df = pd.DataFrame({'Column1': [1, 2, 3], 'Column2': ['A', 'B', 'C']})
        >>> upload_report(df, ['FolderName', 'FileName.xlsx'])

        This will upload the DataFrame as 'FileName.xlsx' inside the
            'FolderName' folder on Google Drive.

    """
    if google_drive_folder_id_report is None:
        raise ValueError(
            "google_drive_folder_id_report is None, configure in yaml file"
        )

    # if ls_folder_file_path is a string, convert to list
    if isinstance(ls_folder_file_path, str):
        ls_folder_file_path = [ls_folder_file_path]

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
    with pd.ExcelWriter(file_path) as writer:
        for df, tab_name in zip(ls_dfs, ls_tab_names):
            df.to_excel(writer, sheet_name=tab_name, index=False)

    upload_file_to_drive(google_drive_folder_id_report, file_path, drive_file_path)


# %%
