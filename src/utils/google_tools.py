# %%
## Imports ##

if __name__ != "__main__":
    print(f"Importing {__name__}")

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
from googleapiclient.discovery import build
import json
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import io
from googleapiclient.http import MediaIoBaseDownload
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
)

from utils.display_tools import print_logger, pprint_dict, pprint_df, pprint_ls


# %%
## Google ##

dotenv_path = os.path.join(grandparent_dir, ".env")
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)

with open(os.path.join(file_dir, "sheet_ids.yaml"), "r") as outfile:
    dict_hardcoded_book_ids = yaml.load(outfile, Loader=yaml.FullLoader)

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


# %%

gc = pygsheets.authorize(
    service_account_env_var=google_service_account_var,
)

dot_env_service_account_email = json.loads(os.getenv(google_service_account_var))[
    "client_email"
]
print_logger(
    f"Google Authenticated with service account: {dot_env_service_account_email}",
    level="debug",
)

try:
    gc_oauth = pygsheets.authorize(
        client_secret=os.path.join(
            great_grandparent_dir,
            "credentials",
            "personal",
            "gsheets_auth_oauth",
            "oauth.json",
        ),
        credentials_directory=os.path.join(
            great_grandparent_dir, "credentials", "personal", "gsheets_auth_oauth"
        ),
    )
except Exception as e:
    gc_oauth = None
    pass


# %%
## Frequently Used Functions ##

dict_connected_books = {}
dict_connected_sheets = {}


def get_book_from_id(id, retry=True):
    global dict_connected_books

    if id in dict_connected_books.keys():
        Workbook = dict_connected_books[id]
        print_logger(f"Using cached connection to {id}", level="debug")
        return Workbook

    try:
        book_from_id = gc.open_by_key(id)
        dict_connected_books[id] = book_from_id
        print_logger(f"Opening new connection to {id}", level="debug")
        return book_from_id
    except TransportError as e:
        print_logger(
            f"Error opening connection to {id}, Trying again in 5 seconds, error: {e}",
            level="warning",
        )
        if retry:
            time.sleep(30)
            return get_book_from_id(id, retry=False)
        else:
            print_logger(
                "Failed to connect to Google Sheets even after retrying",
                level="warning",
            )
            raise Exception(
                f"Failed to connect to Google Sheets even after retrying because of TransportError {e}"
            )
    except HttpError as e:
        if e.resp.status == 429:
            print_logger(
                f"Error HttpError 429, rate limited, opening connection to {id}, Trying again in 20 seconds, error: {e}",
                level="warning",
            )
            if retry:
                time.sleep(20)
                return get_book_from_id(id, retry=False)
            else:
                print_logger(
                    f"Failed to connect to Google Sheets even after retrying because of HttpError 429: {e}",
                    level="warning",
                )
                raise Exception(
                    f"Failed to connect to Google Sheets even after retrying because of HttpError 429: {e}"
                )
        elif e.resp.status == 500:
            print_logger(
                f"Error HttpError 500, internal server error, opening connection to {id}, Trying again in 120 seconds, error: {e}",
                level="warning",
            )
            if retry:
                time.sleep(120)
                return get_book_from_id(id, retry=False)
            else:
                print_logger(
                    f"Failed to connect to Google Sheets even after retrying because of HttpError 500: {e}",
                    level="warning",
                )
                raise Exception(
                    f"Failed to connect to Google Sheets even after retrying because of HttpError 500: {e}"
                )
        elif e.resp.status == 503:
            print_logger(
                f"Error HttpError 503, internal server error, opening connection to {id}, Trying again in 120 seconds, error: {e}",
                level="warning",
            )
            if retry:
                time.sleep(120)
                return get_book_from_id(id, retry=False)
            else:
                print_logger(
                    f"Failed to connect to Google Sheets even after retrying because of HttpError 503: {e}",
                    level="warning",
                )
                raise Exception(
                    f"Failed to connect to Google Sheets even after retrying because of HttpError 503: {e}"
                )
        elif e.resp.status == 404:
            print_logger(
                f"HttpError 404 opening connection to {id}, Trying again in 5 seconds, error: {e}",
                level="warning",
            )
            if retry:
                time.sleep(5)
                return get_book_from_id(id, retry=False)
            else:
                print_logger(
                    f"Failed to connect to Google Sheets even after retrying because of HttpError 404: {e}",
                    level="warning",
                )
                raise Exception(
                    f"Failed to connect to Google Sheets even after retrying because of HttpError 404: {e}"
                )

        else:
            raise Exception(
                f"Failed to connect to Google Sheets because of other HttpError {e}"
            )


def get_book(bookName, retry=True):
    global dict_connected_books

    if bookName in dict_hardcoded_book_ids.keys():
        print_logger(
            f"Book {bookName} in hardcoded book ids, using id: {dict_hardcoded_book_ids[bookName]}",
            level="debug",
        )
        return get_book_from_id(dict_hardcoded_book_ids[bookName])
    else:
        print_logger(
            f"Book {bookName} not in hardcoded book ids, trying to open by name",
            level="warning",
        )

    if bookName in dict_connected_books.keys():
        Workbook = dict_connected_books[bookName]
        print_logger(f"Using cached connection to {bookName}", level="debug")
        return Workbook
    else:
        try:
            print_logger(f"Opening new connection to {bookName}", level="debug")
            Workbook = gc.open(bookName)

            # print out what should add
            workbook_id_to_add_to_dict = Workbook.id
            print_logger(
                f'Consider adding this to dict hardcoded book ids: "{bookName}": "{workbook_id_to_add_to_dict}"',
                level="warning",
            )
            # write to file what should add
            with open(
                os.path.join(data_dir, "dict_hardcoded_book_ids_to_add.txt"), "a"
            ) as f:
                f.write(f'"{bookName}": "{workbook_id_to_add_to_dict}"\n')

            dict_connected_books[bookName] = Workbook
            return Workbook
        except TransportError as e:
            print_logger(
                f"Error opening connection to {bookName}, Trying again in 5 seconds, error: {e}",
                level="warning",
            )
            time.sleep(5)
            if retry:
                return get_book(bookName, retry=False)
            else:
                print_logger(
                    "Failed to connect to Google Sheets even after retrying",
                    level="warning",
                )
                raise e
        except HttpError as e:
            if e.resp.status == 429:
                print_logger(
                    f"Error HttpError 429, rate limited, opening connection to {bookName}, Trying again in 20 seconds, error: {e}",
                    level="warning",
                )
                time.sleep(20)
                if retry:
                    return get_book(bookName, retry=False)
                else:
                    print_logger(
                        "Failed to connect to Google Sheets even after retrying",
                        level="warning",
                    )
                    raise e
            else:
                raise e


def get_book_with_create(bookName, parent_folder_id=None):
    """
    This function will create a new google sheet with the name bookName and return a Workbook object.

    Parameters:
    -----------
    bookName: str, the name of the Google Sheet.
    parent_folder_id: str, the id of the parent folder to create the sheet in (default is None).

    Returns:
    -----------
    a Workbook object.

    """
    global dict_connected_books

    # if already in dict_hardcoded_book_ids[bookName], then just get from there
    if bookName in dict_hardcoded_book_ids.keys():
        print_logger(
            f"Book {bookName} in hardcoded book ids, using id: {dict_hardcoded_book_ids[bookName]}",
            level="info",
        )
        return get_book_from_id(dict_hardcoded_book_ids[bookName])
    else:
        try:
            print_logger(
                f"Book {bookName} not in hardcoded book ids, trying to open by name",
                level="info",
            )
            Workbook = gc.open(bookName)
            print_logger(
                f"Book {bookName} already exists, using existing connection",
                level="info",
            )
            return Workbook
        except:
            pass

    print_logger(f"Creating book: {bookName}", level="info")
    Workbook = gc.create(bookName, folder=parent_folder_id)
    Workbook.share("jason.christiansen@hellofresh.com", role="writer")
    dict_connected_books[bookName] = Workbook
    # add sheet id to yaml with open(os.path.join(file_dir, "sheet_ids.yaml"), "r") as outfile:
    # dict_hardcoded_book_ids = yaml.load(outfile, Loader=yaml.FullLoader)
    dict_hardcoded_book_ids[bookName] = Workbook.id
    # append new sheet id to yaml
    with open(os.path.join(file_dir, "sheet_ids.yaml"), "a") as outfile:
        yaml.dump({bookName: Workbook.id}, outfile, default_flow_style=False)

    return Workbook


def get_book_sheet(bookName, sheetName, retries=3):
    """
    Returns a Worksheet object from a Google Sheet using the sheet name and the spreadsheet name.
    If a cached connection exists, it will be used instead of creating a new one.

    Parameters:
    -----------
    bookName: str, the name of the Google Sheet.
    sheetName: str, the name of the sheet within the Google Sheet.
    retries: int, the maximum number of retries in case of failure (default is 3).

    Returns:
    -----------
    a Worksheet object.

    Raises:
    -----------
    Exception if the maximum number of retries is exceeded.
    """
    global dict_connected_sheets

    retries_left = retries

    while retries_left > 0:
        if f"{bookName} : {sheetName}" in dict_connected_sheets.keys():
            Worksheet = dict_connected_sheets[f"{bookName} : {sheetName}"]
            print_logger(
                f"Using cached connection to {bookName} : {sheetName}", level="debug"
            )
            return Worksheet
        else:
            try:
                Workbook = get_book(bookName)
                Worksheet = Workbook.worksheet_by_title(sheetName)
                dict_connected_sheets[f"{bookName} : {sheetName}"] = Worksheet
                print_logger(
                    f"Opening new connection to {bookName} : {sheetName}", level="debug"
                )
                return Worksheet
            except Exception as e:
                retries_left -= 1
                if retries_left > 0:
                    print_logger(
                        f"Error: {e}. Retrying {retries_left} more time(s).",
                        level="warning",
                    )
                else:
                    raise e


def get_book_sheet_df(
    bookName,
    sheetName,
    start=None,
    end=None,
    index_column=None,
    value_render="FORMATTED_VALUE",
    numerize=True,
    max_retries=3,
):
    """
    Returns a pandas DataFrame object from a Google Sheet using the sheet name and the spreadsheet name.
    If a cached connection exists, it will be used instead of creating a new one.

    Parameters:
    -----------
    bookName: str, the name of the Google Sheet.
    sheetName: str, the name of the sheet within the Google Sheet.
    start: str, the top left cell of the range to retrieve data from (default is None).
    end: str, the bottom right cell of the range to retrieve data from (default is None).
    index_column: int, the index of the column to use as the DataFrame index (default is None).
    value_render: str, the value render option to use (default is "FORMATTED_VALUE").
    numerize: bool, whether to convert numeric values to float (default is True).
    max_retries: int, the maximum number of retries in case of failure (default is 3).

    Returns:
    -----------
    a pandas DataFrame object.

    Raises:
    -----------
    Exception if the maximum number of retries is exceeded.
    """
    retries_left = max_retries

    while retries_left > 0:
        try:
            worksheet = get_book_sheet(bookName, sheetName, max_retries)

            df = worksheet.get_as_df(
                start=start,
                end=end,
                index_column=index_column,
                value_render=value_render,
                numerize=numerize,
            )

            return df
        except Exception as e:
            retries_left -= 1
            if retries_left > 0:
                print_logger(
                    f"Error: {e}. Retrying {retries_left} more time(s).", level="error"
                )
            else:
                raise e


def get_book_sheet_values(
    bookName,
    sheetName,
    start=None,
    end=None,
):
    """
    Returns a list of lists of values from a Google Sheet using the sheet name and the spreadsheet name.
    If a cached connection exists, it will be used instead of creating a new one.

    Parameters:
    -----------
    bookName: str, the name of the Google Sheet.
    sheetName: str, the name of the sheet within the Google Sheet.
    start: str, the top left cell of the range to retrieve data from (default is None).
    end: str, the bottom right cell of the range to retrieve data from (default is None).

    Returns:
    -----------
    a list of lists of values.
    """

    workbook = get_book(bookName)
    worksheet = get_book_sheet(bookName, sheetName)

    values = worksheet.get_values(start=start, end=end)

    return values


def get_book_sheet_from_id_name(id, sheetName, retries=3):
    """
    This function will return a Worksheet object from a google sheet using the spreadsheet id and the sheet name, will return a cached connection if it exists
    :param id: the id of the google spreadsheet
    :param sheetName: the name of the sheet within the google spreadsheet
    :param retries: the maximum number of retries in case of failure (default is 3)
    :return: a Worksheet object
    """
    global dict_connected_sheets

    retries_left = retries

    while retries_left > 0:
        if f"{id} : {sheetName}" in dict_connected_sheets.keys():
            Worksheet = dict_connected_sheets[f"{id} : {sheetName}"]
            print_logger(
                f"Using cached connection to {id} : {sheetName}", level="debug"
            )
            return Worksheet
        else:
            try:
                Workbook = get_book_from_id(id)
                Worksheet = Workbook.worksheet_by_title(sheetName)
                dict_connected_sheets[f"{id} : {sheetName}"] = Worksheet
                print_logger(
                    f"Opening new connection to {id} : {sheetName}", level="debug"
                )
                return Worksheet
            except Exception as e:
                retries_left -= 1
                if retries_left > 0:
                    print_logger(
                        f"Error: {e}. Retrying {retries_left} more time(s).",
                        level="warning",
                    )
                else:
                    raise e


def get_book_sheet_df_from_id_name(
    id,
    sheetName,
    start=None,
    end=None,
    index_column=None,
    value_render="FORMATTED_VALUE",
    numerize=True,
    retries=3,
):
    try:
        worksheet = get_book_sheet_from_id_name(id, sheetName)

        df = worksheet.get_as_df(
            start=start,
            end=end,
            index_column=index_column,
            value_render=value_render,
            numerize=numerize,
        )

    except Exception as e:
        if retries == 0:
            print_logger(
                f"Error getting sheet from id after max retries: {id}, sheetName: {sheetName}, error: {e}",
                level="error",
            )
            raise Exception(
                f"Error getting sheet from id after max retries: {id}, sheetName: {sheetName}, error: {e}"
            )
        else:
            print_logger(
                f"Error getting sheet from id: {id}, sheetName: {sheetName}, retrying in 5 seconds, error: {e}",
                level="error",
            )
            time.sleep(5)
            return get_book_sheet_df_from_id_name(
                id,
                sheetName,
                start=start,
                end=end,
                index_column=index_column,
                value_render=value_render,
                numerize=numerize,
                retries=retries - 1,
            )

    return df


def get_book_sheet_values_from_id_name(
    id,
    sheetName,
    start=None,
    end=None,
    include_tailing_empty=True,
):
    workbook = get_book_from_id(id)
    worksheet = get_book_sheet_from_id_name(id, sheetName)

    values = worksheet.get_values(
        start=start,
        end=end,
        include_tailing_empty=include_tailing_empty,
    )

    return values


def WriteToSheets(
    bookName,
    sheetName,
    df,
    indexes=False,
    set_note=None,
    retries=3,
):
    """
    This function will write a dataframe to a google sheet, will create the sheet if it doesnt exist
    :param bookName: the name of the google spreadsheet
    :param sheetName: the name of the sheet within the google spreadsheet
    :param df: the dataframe to write to the sheet
    :param indexes: whether to write the index column to the sheet
    :param set_note: the note to set on the sheet, None for no note, "DT" for date time, string for custom note
    :param retries: the number of times to retry if the connection fails
    :return: None
    """

    # global dict_connected_books
    # global dict_connected_sheets
    df = df.copy()

    start_time = datetime.datetime.now()
    print_logger(
        f"Writing to Google Sheet: {bookName} - {sheetName} with size {df.shape}"
    )
    if isinstance(df, pd.Series):
        print_logger(
            "# Found a series when writing to sheets, converting to dataframe #",
            level="warning",
        )
        df = df.reset_index()

    for i in range(retries):
        try:
            Workbook = get_book(bookName)

            try:
                Worksheet = get_book_sheet(bookName, sheetName)
            except pygsheets.WorksheetNotFound:
                Workbook.add_worksheet(sheetName)
                Worksheet = get_book_sheet(bookName, sheetName)

            if indexes == False:
                Worksheet.set_dataframe(df, (1, 1), fit=True, nan="")
            else:
                Worksheet.set_dataframe(df, (1, 1), fit=True, nan="", copy_index=True)
            try:
                if set_note != None:
                    if set_note == "DT":
                        Worksheet.cell((1, 1)).note = "Data updated at: " + str(
                            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        )
                    else:
                        Worksheet.cell((1, 1)).note = set_note
            except Exception as e:
                print_logger(
                    f"Failed to set note when writing, error: {e}, trying one more time",
                    level="warning",
                )
                try:
                    if set_note != None:
                        if set_note == "DT":
                            Worksheet.cell((1, 1)).note = "Data updated at: " + str(
                                datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            )
                        else:
                            Worksheet.cell((1, 1)).note = set_note
                except:
                    pass
                pass

            print_logger(
                f"Finished writing to Google Sheet: {bookName} - {sheetName} with size {df.shape}, after {datetime.datetime.now() - start_time}\nLink: https://docs.google.com/spreadsheets/d/{Workbook.id}"
            )

            return
        except Exception as e:
            print_logger(
                f"Failed to write to sheets with name {bookName} and sheet name {sheetName} and df of size {df.shape}, error: {e}",
                level="warning",
            )
            print_logger(
                f"Retrying {i+1} of {retries} times after {i * 20} seconds",
                level="warning",
            )
            time.sleep(i * 20)
            print_logger("Retrying now", level="warning")
            pass

    print_logger(f"Failed to write to sheet after {retries} retries", level="warning")
    raise Exception(
        f"Failed to write to sheets with name {bookName} and sheet name {sheetName} and df of size {df.shape}"
    )


def ClearSheet(book_name, sheet_name, start_range, end_range):
    """
    This function will clear a range of cells on a sheet
    :param book_name: the name of the google spreadsheet
    :param sheet_name: the name of the sheet within the google spreadsheet
    :param start_range: the start range of the cells to clear in the format "A1"
    :param end_range: the end range of the cells to clear in the format "A1"
    :return: None
    """

    Worksheet = get_book_sheet(book_name, sheet_name)
    Worksheet.clear(start_range, end_range)


def clear_range_of_sheet_obj(sheet_obj, start, end, retries=3):
    """
    This function will clear a range of cells on a sheet
    :param sheet_obj: the sheet object to write to
    :param start: the start range of the cells to clear in the format "A1"
    :param end: the end range of the cells to clear in the format "A1"
    :return: None
    """

    for i in range(retries):
        try:
            sheet_obj.clear(start, end)
            return
        except Exception as e:
            print_logger(f"Failed to clear range, error: {e}", level="warning")
            print_logger(
                f"Retrying {i+1} of {retries} times after {i * 20} seconds",
                level="warning",
            )
            time.sleep(i * 10)
            print_logger("Retrying now", level="warning")
            pass

    print_logger(f"Failed to clear range after {retries} retries", level="warning")
    raise Exception(f"Failed to clear range after {retries} retries")


def write_df_to_range_of_sheet_obj(
    sheet_obj,
    df,
    start,
    fit,
    nan="",
    copy_head=False,
    retries=3,
):
    """
    This function will write a dataframe to a range on a sheet
    :param sheet_obj: the sheet object to write to
    :param df: the dataframe to write to the sheet
    :param start: the start range of the cells to clear in the format "A1"
    :param fit: whether to fit the dataframe to the range
    :param nan: the value to use for nan
    :param copy_head: whether to copy the header
    :return: None
    """

    for i in range(retries):
        try:
            sheet_obj.set_dataframe(
                df=df, start=start, fit=fit, nan=nan, copy_head=copy_head
            )
            return
        except Exception as e:
            print_logger(
                f"Failed to write to range with error: {e}, retrying {i+1} of {retries} times",
                level="warning",
            )
            time.sleep(i * 10)
            pass

    print_logger(f"Failed to write to range after {retries} retries", level="warning")
    raise Exception(f"Failed to write to range with error: {e}")


# %%
## Entire Sheet Operations ##


def copy_sheet_book_to_book(source_book, ls_source_sheets, ls_dest_books):
    Workbook_src = gc.open(source_book)
    src_book_id = Workbook_src.id

    for dest_book in ls_dest_books:
        for source_sheet in ls_source_sheets:
            sheet_src = Workbook_src.worksheet_by_title(source_sheet)
            src_sheet_id = sheet_src.id
            src_tup = (src_book_id, src_sheet_id)

            Workbook_dest = gc.open(dest_book)

            try:
                Workbook_dest.del_worksheet(
                    Workbook_dest.worksheet_by_title(source_sheet)
                )
            except:
                pass

            Workbook_dest.add_worksheet(source_sheet, src_tuple=src_tup)


# %%
## Auth and gets ##


def get_google_authentication():
    gauth = GoogleAuth()
    # Try to load saved client credentials
    gauth.LoadCredentialsFile(
        os.path.join(
            great_grandparent_dir,
            "credentials",
            "personal",
            "google_auth",
            "google_auth_creds.txt",
        )
    )
    if gauth.credentials is None:
        # Authenticate if they're not there
        gauth.LocalWebserverAuth()
    elif gauth.access_token_expired:
        # Refresh them if expired
        gauth.Refresh()
    else:
        # Initialize the saved creds
        gauth.Authorize()
    # Save the current credentials to a file
    gauth.SaveCredentialsFile(
        os.path.join(
            great_grandparent_dir,
            "credentials",
            "personal",
            "google_auth",
            "google_auth_creds.txt",
        )
    )
    return gauth


def get_google_drive_obj():
    drive = GoogleDrive(get_google_authentication())
    return drive


def get_file_list_from_folder_id_oauth(folder_id):
    parent_folder_files = (
        get_google_drive_obj()
        .ListFile({"q": f"'{folder_id}' in parents and trashed=false"})
        .GetList()
    )

    return parent_folder_files


def get_file_list_from_folder_id_file_path(root_folder_id, ls_file_path):
    ls_directory_path = ls_file_path

    folder_id = get_drive_file_id_from_folder_id_path(
        root_folder_id, ls_directory_path, is_folder=True
    )

    ls_files_dict = get_file_list_from_folder_id(folder_id)

    return ls_files_dict


def get_book_id_from_parent_folder_id_oauth(parent_folder_id, book_name):
    print_logger(
        f"Getting sheet ID for book named {book_name} inside parent folder ID {parent_folder_id}"
    )

    parent_folder_files = get_file_list_from_folder_id_oauth(parent_folder_id)

    for file in parent_folder_files:
        if file["title"] == book_name:
            file_id = file["id"]
            print_logger(
                f"Found sheet ID {file_id} for book named {book_name} inside parent folder ID {parent_folder_id}"
            )
            return file_id


def get_file_list_from_folder_id(folder_id):
    # authenticate with the Google Drive API using your service account credentials
    credentials = service_account.Credentials.from_service_account_info(
        json.loads(os.getenv(google_service_account_var), strict=False),
        scopes=["https://www.googleapis.com/auth/drive"],
    )
    service = build("drive", "v3", credentials=credentials)

    # retrieve a list of files in the specified folder
    results = (
        service.files()
        .list(
            q=f"'{folder_id}' in parents and trashed=false",
            fields="nextPageToken, files(id, name)",
        )
        .execute()
    )
    files = results.get("files", [])

    if not files:
        print_logger("No files found.")
        return None
    else:
        return files


def get_file_list_from_folder_id_with_type(folder_id):
    # authenticate with the Google Drive API using your service account credentials
    credentials = service_account.Credentials.from_service_account_info(
        json.loads(os.getenv(google_service_account_var), strict=False),
        scopes=["https://www.googleapis.com/auth/drive"],
    )
    service = build("drive", "v3", credentials=credentials)

    # retrieve a list of files in the specified folder
    results = (
        service.files()
        .list(
            q=f"'{folder_id}' in parents and trashed=false",
            fields="nextPageToken, files(id, name, mimeType)",
        )
        .execute()
    )
    files = results.get("files", [])

    if not files:
        print_logger("No files found.")
        return None
    else:
        return files


def check_if_ignored(ls_ignore_patterns, file_name):
    for ignore_pattern in ls_ignore_patterns:
        if ignore_pattern in file_name:
            return True
    return False


def get_file_tree_from_folder_id_for_file_type(
    folder_id,
    indent=0,
    ls_file_exts=None,
    folders_already_scanned=[],
    curr_folder_path=[],
    dict_cur_files={},
    ls_ignore_folders=[],
):
    # authenticate with the Google Drive API using your service account credentials
    credentials = service_account.Credentials.from_service_account_info(
        json.loads(os.getenv(google_service_account_var), strict=False),
        scopes=["https://www.googleapis.com/auth/drive"],
    )
    service = build("drive", "v3", credentials=credentials)

    # retrieve a list of files in the specified folder that end in ".yxwz"
    results = (
        service.files()
        .list(
            q=f"'{folder_id}' in parents and trashed=false",
            fields="nextPageToken, files(id, name, mimeType, parents)",
        )
        .execute()
    )
    files = results.get("files", [])

    if not files:
        # print_logger("No files found.")
        return None
    else:
        for file in files:
            if not ls_file_exts or (
                file["name"].split(".")[-1] in ls_file_exts
                and file["mimeType"] != "application/vnd.google-apps.folder"
            ):
                print(" " * indent + "├── " + file["name"])
                path_with_filename = curr_folder_path.copy()
                path_with_filename.append(file["name"])
                dict_cur_files[file["id"]] = path_with_filename
            if (
                file["mimeType"] == "application/vnd.google-apps.folder"
                and file["id"] not in folders_already_scanned
                and not check_if_ignored(ls_ignore_folders, file["name"])
            ):
                print(" " * indent + "├── " + file["name"])
                subfolder_file_path = curr_folder_path.copy()
                subfolder_file_path.append(file["name"])
                for attempt in range(6):
                    try:
                        subfolder_files = get_file_tree_from_folder_id_for_file_type(
                            file["id"],
                            indent + 4,
                            folders_already_scanned=folders_already_scanned,
                            curr_folder_path=subfolder_file_path,
                            dict_cur_files=dict_cur_files,
                            ls_file_exts=ls_file_exts,
                            ls_ignore_folders=ls_ignore_folders,
                        )
                        break
                    except:
                        print_logger(
                            f"###### Failed to get file tree for folder {file['name']} with ID {file['id']}. Trying again in {5 * attempt} seconds. ######",
                            level="error",
                        )
                        time.sleep(5 * attempt)

                if subfolder_files:
                    dict_cur_files.update(subfolder_files)
                folders_already_scanned.append(file["id"])
    return dict_cur_files


def get_file_list_from_folder_id_recursive(folder_id, level=1):
    # authenticate with the Google Drive API using your service account credentials
    credentials = service_account.Credentials.from_service_account_info(
        json.loads(os.getenv(google_service_account_var), strict=False),
        scopes=["https://www.googleapis.com/auth/drive"],
    )
    service = build("drive", "v3", credentials=credentials)

    # retrieve a list of files in the specified folder
    file_list = (
        service.files()
        .list(
            q=f"'{folder_id}' in parents and trashed=false",
            fields="nextPageToken, files(id, name, parents, mimeType)",
        )
        .execute()
    )

    files = []

    for file in file_list["files"]:
        if (
            "mimeType" not in file
            or file["mimeType"] == "application/vnd.google-apps.folder"
        ):
            files.append(
                {
                    "id": file["id"],
                    "name": file["name"],
                    "parents": file["parents"],
                    "type": "folder",
                }
            )

            # recursively call the function for each subfolder
            files += get_file_list_from_folder_id_recursive(file["id"], level + 1)

        else:
            files.append(
                {
                    "id": file["id"],
                    "name": file["name"],
                    "parents": file["parents"],
                    "type": "file",
                    "mimeType": file["mimeType"],
                }
            )

    return files


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
    # authenticate with the Google Drive API using your service account credentials
    credentials = service_account.Credentials.from_service_account_info(
        json.loads(os.getenv(google_service_account_var), strict=False),
        scopes=["https://www.googleapis.com/auth/drive"],
    )
    service = build("drive", "v3", credentials=credentials)

    curr_dir_id = folder_id
    for folder_name in ls_file_path[:-1]:
        print_logger(
            f"Scanning folder {folder_name} with ID {curr_dir_id}", level="debug"
        )
        # search for the folder by name and within the current directory
        query = f"name='{folder_name}' and '{curr_dir_id}' in parents and trashed=false and mimeType='application/vnd.google-apps.folder'"
        results = (
            service.files()
            .list(q=query, fields="files(id, name)")
            .execute()
            .get("files", [])
        )

        if not results:
            raise ValueError(f"Folder not found: {folder_name}")

        curr_dir_id = results[0]["id"]
        print_logger(f"Found folder {folder_name} with ID {curr_dir_id}")

    # we've traversed all the folders, now search for the file by name within the current directory
    filename = ls_file_path[-1]
    if is_folder:
        query = f"name='{filename}' and '{curr_dir_id}' in parents and trashed=false and mimeType='application/vnd.google-apps.folder'"
    else:
        query = f"name='{filename}' and '{curr_dir_id}' in parents and trashed=false and mimeType!='application/vnd.google-apps.folder'"
    results = (
        service.files()
        .list(q=query, fields="files(id, name)")
        .execute()
        .get("files", [])
    )

    if not results:
        raise ValueError(f"File not found: {filename}")

    return results[0]["id"]


# %%


def get_file_by_id(file_id: str) -> bytes:
    """
    Given a Google Drive file ID, downloads the file's contents and returns them as a bytes object.

    Args:
        file_id (str): The ID of the file to download.

    Returns:
        bytes: The contents of the downloaded file.
    """
    # authenticate with the Google Drive API using your service account credentials
    credentials = service_account.Credentials.from_service_account_info(
        json.loads(os.getenv(google_service_account_var), strict=False),
        scopes=["https://www.googleapis.com/auth/drive"],
    )
    service = build("drive", "v3", credentials=credentials)

    # download the file
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while done is False:
        status, done = downloader.next_chunk()

    # return the downloaded file contents
    fh.seek(0)
    return fh.read()


def download_file_by_id(id, path, max_retries=3):
    retries = 0
    while retries < max_retries:
        try:
            # authenticate with the Google Drive API using your service account credentials
            credentials = service_account.Credentials.from_service_account_info(
                json.loads(os.getenv(google_service_account_var), strict=False),
                scopes=["https://www.googleapis.com/auth/drive"],
            )
            service = build("drive", "v3", credentials=credentials)

            # download the file
            request = service.files().get_media(fileId=id)
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


ls_files_downloaded_this_run = []


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


def get_book_id_from_parent_folder_id(parent_folder_id, book_name):
    print_logger(
        f"Getting sheet ID for book named {book_name} inside parent folder ID {parent_folder_id}"
    )

    parent_folder_files = get_file_list_from_folder_id(parent_folder_id)

    for file in parent_folder_files:
        file_id = file["id"]
        file_title = file["name"]
        print_logger(f"Processing file ID {file_id} with the name {file_title}")
        if file_title == book_name:
            return file_id


def list_files_recursively(folder_id, level):
    file_list = (
        get_google_drive_obj()
        .ListFile({"q": f"'{folder_id}' in parents and trashed=false"})
        .GetList()
    )
    for file in file_list:
        if not (file["mimeType"] == "application/vnd.google-apps.folder") or (
            file["mimeType"] == "application/vnd.google-apps.shortcut"
        ):
            print(
                "\t" * level
                + "title: %s" % file["title"]
                + " - ID: %s" % file["id"]
                + " - Type: %s" % file["mimeType"]
            )

    for file in file_list:
        if (
            file["mimeType"] == "application/vnd.google-apps.folder"
            or file["mimeType"] == "application/vnd.google-apps.shortcut"
        ):
            print(
                "\t" * level
                + "title: %s" % file["title"]
                + " - ID: %s" % file["id"]
                + " - Type: %s" % file["mimeType"]
            )
            list_files_recursively(file["id"], level + 2)


def get_book_from_file_name(file_name):  # need to remove, aliased to new one for now
    book_from_file_name = get_book(file_name)
    return book_from_file_name


def get_df_from_sheet_id(
    id, sheet_name, start_range, end_range, include_tailing_empty=False, retry=True
):
    try:
        data_from_book = get_book_sheet_from_id_name(id, sheet_name).get_as_df(
            start=start_range,
            end=end_range,
            include_tailing_empty=include_tailing_empty,
        )
        return data_from_book
    except Exception as e:
        if retry:
            print_logger(
                f"Failed to get df from sheet id {id}, sheet_name: {sheet_name}, error: {e}, retrying",
                level="warning",
            )
            return get_df_from_sheet_id(
                id,
                sheet_name,
                start_range,
                end_range,
                include_tailing_empty,
                retry=False,
            )
        else:
            print_logger(
                f"Failed to get df even after retry from sheet id {id}, sheet_name: {sheet_name}, error: {e}",
                level="warning",
            )
            raise Exception(
                f"Failed to get df even after retry from sheet id {id}, sheet_name: {sheet_name}, error: {e}"
            )


def get_df_from_file_name(
    file_name, sheet_name, start_range, end_range, include_tailing_empty=False
):
    book_from_file_name = get_book_from_file_name(file_name)
    sheet_from_book = book_from_file_name.worksheet_by_title(sheet_name)
    data_from_book = sheet_from_book.get_as_df(
        start=start_range, end=end_range, include_tailing_empty=include_tailing_empty
    )
    return data_from_book


def get_df_and_id_from_file_name(
    file_name, sheet_name, start_range, end_range, include_tailing_empty=False
):
    book_from_file_name = get_book_from_file_name(file_name)
    sheet_id = book_from_file_name.id
    sheet_from_book = book_from_file_name.worksheet_by_title(sheet_name)
    data_from_book = sheet_from_book.get_as_df(
        start=start_range, end=end_range, include_tailing_empty=include_tailing_empty
    )
    return data_from_book, sheet_id


def get_book_from_id_oauth(id):
    book_from_id = gc_oauth.open_by_key(id)
    return book_from_id


def get_book_from_file_name_oauth(file_name):
    book_from_file_name = gc_oauth.open(file_name)
    return book_from_file_name


def get_df_from_sheet_id_oauth(
    id, sheet_name, start_range, end_range, include_tailing_empty=False
):
    book_from_id = get_book_from_id_oauth(id)
    sheet_from_book = book_from_id.worksheet_by_title(sheet_name)
    data_from_book = sheet_from_book.get_as_df(
        start=start_range, end=end_range, include_tailing_empty=include_tailing_empty
    )
    return data_from_book


def get_df_from_file_name_oauth(
    file_name, sheet_name, start_range, end_range, include_tailing_empty=False
):
    book_from_file_name = get_book_from_file_name_oauth(file_name)
    sheet_from_book = book_from_file_name.worksheet_by_title(sheet_name)
    data_from_book = sheet_from_book.get_as_df(
        start=start_range, end=end_range, include_tailing_empty=include_tailing_empty
    )
    return data_from_book


def copy_formulas_range_to_range(
    book_name, copy_sheet_name, copy_range, paste_sheet_name, paste_range_string
):
    copy_sheet = get_book_sheet(book_name, copy_sheet_name)
    paste_sheet = get_book_sheet(book_name, paste_sheet_name)
    paste_sheet.update_values(
        paste_range_string,
        copy_sheet.get_values(
            start=copy_range[0],
            end=copy_range[1],
            returnas="matrix",
            include_tailing_empty=True,
            include_tailing_empty_rows=True,
            value_render="FORMULA",
        ),
    )


def get_sheet_link(sheet_id):
    if (sheet_id == "") or (sheet_id == None) or (len(sheet_id) != 44):
        return ""
    return f"https://docs.google.com/spreadsheets/d/{sheet_id}"


def get_sheet_link_formula(sheet_id):
    sheet_link = get_sheet_link(sheet_id)
    if sheet_link == "":
        return ""
    return f'=hyperlink("{sheet_link}","Link")'


def convert_tab_name_to_hyperlink(book_obj, tab_name, link_text):
    if tab_name == "":
        return ""
    ss_id = book_obj.id
    sheet_id = book_obj.worksheet_by_title(tab_name).id

    hyperlink = f'=HYPERLINK("https://docs.google.com/spreadsheets/d/{ss_id}/edit#gid={sheet_id}","{link_text}")'
    return hyperlink


# %%
## Permission Management ##


def get_editors_from_spreadsheet(sheet_id, print_output=True):
    book = get_book_from_id(sheet_id)
    spreadsheet_name = book.title
    permissions = book.permissions
    editor_emails = []
    for permission in permissions:
        # if email address esixts, and role is not writer, skip
        if ("emailAddress" not in permission) or (permission["role"] != "writer"):
            continue
        editor_emails.append(permission["emailAddress"])
    if print_output:
        pprint_ls(editor_emails, f"{spreadsheet_name} Editor Emails: ")
    return editor_emails


def check_for_editor(sheet_id, email):
    editor_emails = get_editors_from_spreadsheet(sheet_id, print_output=False)
    is_editor = email in editor_emails
    print_logger(f"{email} is editor: {is_editor}")
    return is_editor


def share_to_email(sheet_id, email, role="writer"):
    book = get_book_from_id(sheet_id)
    book.share(
        email,
        role=role,
    )


def share_list_sheets_to_email(sheet_id_list, email, role="writer", test_mode=True):
    df_statuses = pd.DataFrame(columns=["sheet_id", "sheet_name", "status"])
    for sheet_id in sheet_id_list:
        # get sheet name from dict_hardcoded_book_ids values
        sheet_name = list(dict_hardcoded_book_ids.keys())[
            list(dict_hardcoded_book_ids.values()).index(sheet_id)
        ]
        status = ""
        try:
            if check_for_editor(sheet_id, email):
                status = "Already Editor"
            elif test_mode:
                status = "Would Have Shared"
            else:
                share_to_email(sheet_id, email, role=role)
                status = "Shared"
        except Exception as e:
            print_logger(f"Failed to share {sheet_id} to {email}.", level="error")
            print_logger(e)
            status = "Failed"
        df_statuses = df_statuses.append(
            {
                "sheet_id": sheet_id,
                "sheet_name": sheet_name,
                "status": status,
            },
            ignore_index=True,
        )

    pprint_df(df_statuses)
    print_logger("Failed Sheets:", level="error")
    pprint_df(df_statuses[df_statuses["status"] == "Failed"])


# %%
## Google Docs ##


def authenticate_for_google_docs():
    """
    Authenticate and create a service object for Google Docs API.
    Returns:
        googleapiclient.discovery.Resource: The authenticated service object for the Google Docs API.
    """
    credentials = service_account.Credentials.from_service_account_info(
        json.loads(os.getenv(google_service_account_var), strict=False),
        scopes=["https://www.googleapis.com/auth/documents"],
    )

    service = build("docs", "v1", credentials=credentials)

    return service


def get_google_doc_from_id(id):
    """
    Get a Google Doc object by its ID.
    Args:
        id (str): The ID of the Google Doc.

    Returns:
        dict: The Google Doc object, or None if an error occurred.
    """
    service = authenticate_for_google_docs()

    try:
        document = service.documents().get(documentId=id).execute()
        return document
    except HttpError as e:
        print_logger(f"Error: {e}", level="warning")
        return None


def print_contents_of_doc_by_id(id):
    """
    Print the contents of a Google Doc by its ID.
    Args:
        id (str): The ID of the Google Doc.
    """
    document = get_google_doc_from_id(id)
    if document == None:
        return
    pprint_dict(document["body"]["content"])
    return document["body"]["content"]


def append_text_to_doc_by_id(id, text):
    """
    Append text to a Google Doc by its ID.
    Args:
        id (str): The ID of the Google Doc.
        text (str): The text to append.

    Returns:
        dict: The result of the batch update operation.
    """
    service = authenticate_for_google_docs()

    # Get the current length of the document
    document = service.documents().get(documentId=id).execute()
    current_length = document["body"]["content"][-1]["endIndex"] - 1

    requests = [
        {
            "insertText": {
                "location": {"index": current_length},
                "text": text,
            }
        }
    ]
    result = (
        service.documents()
        .batchUpdate(documentId=id, body={"requests": requests})
        .execute()
    )
    return result


# %%
