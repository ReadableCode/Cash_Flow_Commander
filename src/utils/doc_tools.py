# %%
## Imports ##

import os
import pandas
import datetime
import sys
import json
import numpy as np
from tabulate import tabulate

# append grandparent
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config_utils import (
    file_dir,
    parent_dir,
    grandparent_dir,
    great_grandparent_dir,
    data_dir,
)

from utils.display_tools import print_logger


# %%
## Variables ##

file_dir = os.path.dirname(os.path.realpath(__file__))
docs_dir = os.path.join(grandparent_dir, "docs")


# %%
## Link Formatting Tools ##


def get_git_link(script_path, repo_owner, repo_name, branch_name):
    """
    Get the link to the git repository
    :return: link to the git repository
    """

    return (
        f"https://github.com/{repo_owner}/{repo_name}/blob/{branch_name}/{script_path}"
    )


def get_git_link_formula(script_path, repo_owner, repo_name, branch_name):
    git_link = get_git_link(
        script_path, repo_owner=repo_owner, repo_name=repo_name, branch_name=branch_name
    )
    return f'=hyperlink("{git_link}","Link")'


def get_sheet_link(sheet_id):
    if (sheet_id == "") or (sheet_id == None) or (len(sheet_id) != 44):
        return ""
    return f"https://docs.google.com/spreadsheets/d/{sheet_id}"


def get_sheet_link_formula(sheet_id):
    sheet_link = get_sheet_link(sheet_id)
    if sheet_link == "":
        return ""
    return f'=hyperlink("{sheet_link}","Link")'


def get_google_drive_folder_link(folder_id):
    if (folder_id == "") or (folder_id == None) or (len(folder_id) != 33):
        return ""
    return f"https://drive.google.com/drive/folders/{folder_id}"


def get_domo_link(domo_table_id):
    if (domo_table_id == "") or (domo_table_id == None):
        return ""
    return f"https://hellofresh.domo.com/datasources/{domo_table_id}/details/data/table"


def convert_to_domo_link(domo_table_id):
    domo_link = get_domo_link(domo_table_id)
    if domo_link == "":
        return ""
    return f'=hyperlink("{domo_link}","Link")'


def get_link_from_resource_type(
    resource_type,
    resource_path="",
    resource_id="",
):
    if resource_type == "google_sheet":
        link = get_sheet_link(resource_id)
    elif resource_type == "google_drive_folder":
        link = get_google_drive_folder_link(resource_id)
    elif resource_type == "domo_table":
        link = get_domo_link(resource_id)
    elif resource_type == "git":
        link = get_git_link(resource_path)
    else:
        link = ""

    if link == "":
        return ""

    return link


# %%
## Markdown Tools ##

dict_markdown_levels = {
    1: {
        "newlines_before": "\n",
        "header": "# ",
        "newlines_after": "\n",
    },
    2: {
        "newlines_before": "\n",
        "header": "## ",
        "newlines_after": "\n",
    },
    3: {
        "newlines_before": "\n",
        "header": "- ### ",
        "newlines_after": "\n",
    },
    4: {
        "newlines_before": "\n",
        "header": "  - ",
        "newlines_after": "\n",
    },
    5: {
        "newlines_before": "\n",
        "header": "    - ",
        "newlines_after": "\n",
    },
}


def get_markdown_line(level, name, link=""):
    if link != "":
        name = f"[{name}]"
        link = f"({link})"

    newlines_before = dict_markdown_levels[level]["newlines_before"]
    header = dict_markdown_levels[level]["header"]
    newlines_after = dict_markdown_levels[level]["newlines_after"]

    return f"{newlines_before}{header}{name}{link}{newlines_after}"


# %%
## Logging Tools ##


def log_data_pipeline_unified(
    action_method="python_script",
    script_path="",
    function_name="",
    input_output="",
    resource_type="",
    resource_path="",
    resource_sub_name="",
    resource_id="",
    datestamp="",
):
    """
    Logs data pipeline actions to a unified google sheet
    :param action_method: python_script, google_sheet, etc
    :param script_path: GoogleSheet: DC Labor 2.0 if google sheet with apps script
    :param function_name: name of function in python script or google script if nested in a function
    :param input_output: input or output
    :param resource_type: google_sheet, domo_table, file, database_table_path, s3_bucket etc
    :param resource_path: spreadsheet_name, domo_table_name, file_path, database_table, s3_bucket, etc
    :param resource_sub_name: sheet_name
    :param resource_id: domo_table_id, spreadsheet_id, etc
    :return None
    """
    if script_path == "":
        print_logger("script_path is empty, not logging", level="debug")
        return
    if datestamp == "":
        datestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # spreadsheet_name, file_name, etc
    resource_name = os.path.basename(resource_path)

    # link to github or google sheet containing script
    script_link = get_link_from_resource_type(
        resource_type="git", resource_path=script_path
    )

    # link to resource
    resource_link = get_link_from_resource_type(
        resource_type=resource_type,
        resource_path=resource_path,
        resource_id=resource_id,
    )

    dict_to_append = {
        "datestamp": datestamp,
        "action_method": action_method,
        "script_path": script_path,
        "script_link": script_link,
        "function_name": function_name,
        "input_output": input_output,
        "resource_type": resource_type,
        "resource_path": resource_path,
        "resource_name": resource_name,
        "resource_sub_name": resource_sub_name,
        "resource_id": resource_id,
        "resource_link": resource_link,
    }

    # if file does not exist write header
    if not os.path.isfile(os.path.join(data_dir, "data_pipelines_unified.csv")):
        with open(os.path.join(data_dir, "data_pipelines_unified.csv"), "w") as f:
            f.write(",".join(dict_to_append.keys()) + "\n")

    # write row
    with open(os.path.join(data_dir, "data_pipelines_unified.csv"), "a") as f:
        f.write(",".join(dict_to_append.values()) + "\n")


# %%

