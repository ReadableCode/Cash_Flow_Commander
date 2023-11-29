# %%
## Imports ##

import os
import sys
from tabulate import tabulate
import datetime
import json
from pprint import pprint
import numpy as np
from dotenv import load_dotenv

# append grandparent
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config_utils import (
    file_dir,
    parent_dir,
    grandparent_dir,
    great_grandparent_dir,
    data_dir,
)

# %%
## Variables ##

dotenv_path = os.path.join(grandparent_dir, ".env")
if os.path.exists(dotenv_path):
    load_dotenv(dotenv_path)


# %%
## Functions ##


def pprint_df(dframe, showindex=False, num_cols=None, num_decimals=2):
    """
    Pretty print a pandas dataframe
    :param dframe: pandas dataframe
    :param showindex: boolean to show the index
    :param num_cols: number of columns to print
    :param num_decimals: number of decimal places to print for float values
    :return: None
    """
    floatfmt_str = f".{num_decimals}f"

    if num_cols is not None:
        print(
            tabulate(
                dframe.iloc[:, :num_cols],
                headers="keys",
                tablefmt="psql",
                showindex=showindex,
                floatfmt=floatfmt_str,
            )
        )
    else:
        print(
            tabulate(
                dframe,
                headers="keys",
                tablefmt="psql",
                showindex=showindex,
                floatfmt=floatfmt_str,
            )
        )


def df_to_string(df):
    # Convert dataframe to markdown table
    markdown_table = tabulate(df, headers="keys", tablefmt="pipe", showindex=False)

    return markdown_table


def print_logger(message, level="info", as_break=False):
    """
    Print a message with a timestamp
    :param message: message to print
    :param level: level of the message
    :return: None
    """
    dict_levels = {
        "debug": 5,
        "info": 4,
        "warning": 3,
        "error": 2,
        "critical": 1,
    }
    if dict_levels[level.lower()] <= dict_levels[os.environ["LOG_LEVEL"]]:
        print_message = f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {level.upper()} - {message}"
        if not as_break:
            print(print_message)
        else:
            len_total_message = len(print_message)
            padding_text = ((100 - len_total_message - 2) // 2) * "#"
            print("#" * 100)
            print("#" * 100)
            print(f"{padding_text} {print_message} {padding_text}")
            print("#" * 100)
            print("#" * 100)


def pprint_ls(ls, ls_title="List"):
    """
    Pretty print a list
    :param ls: list to print
    :param ls_title: title of the list
    :return: None
    """
    # print a title box and a box that centers the title and left aligns each item of the list on a new line

    # if list is empty return
    if len(ls) == 0:
        item_max_len = 0
    else:
        item_max_len = 0
        for item in ls:
            try:
                this_length = len(str(item))
            except:
                this_length = 0
            if this_length > item_max_len:
                item_max_len = this_length

    # get the longest item in the list
    max_len = max(item_max_len, len(ls_title)) + 8

    # print the top of the box
    print(f"{'-' * (max_len + 4)}")

    # print the title with padding
    print(f"| {ls_title.center(max_len)} |")

    # print the bottom of the title box
    print(f"{'-' * (max_len + 4)}")

    # print each item in the list
    for item in ls:
        if isinstance(item, str):
            print(f"| {item.ljust(max_len)} |")
        else:
            print(f"| {str(item).ljust(max_len)} |")

    # print the bottom of the list box
    print(f"{'-' * (max_len + 4)}")


def pprint_dict(data, indent=0):
    if isinstance(data, dict):
        for key, value in data.items():
            print(" " * indent + str(key) + ": ", end="")
            if isinstance(value, dict):
                print("DICTIONARY {")
                pprint_dict(value, indent + 8)
                print(" " * indent + "}")
            elif isinstance(value, list):
                print("LIST [")
                for item in value:
                    if isinstance(item, dict):
                        pprint_dict(item, indent + 8)
                        print("," + " " * (indent + 8))
                    else:
                        print(" " * (indent + 8) + str(item) + ",")
                print(" " * indent + "]")
            else:
                print(str(value))
    elif isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                pprint_dict(item, indent)
                print("," + " " * indent)
            else:
                print(" " * indent + str(item) + ",")
    else:
        print(" " * indent + str(data))


def print_nested_dict(data, indent=0):
    if isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, (dict, list)):
                print("  " * indent + str(key) + ":")
                print_nested_dict(value, indent + 1)
            else:
                print("  " * indent + str(key) + ": " + str(value))
    elif isinstance(data, list):
        for item in data:
            if isinstance(item, (dict, list)):
                print_nested_dict(item, indent)
            else:
                print("  " * indent + str(item))
    else:
        print("  " * indent + str(data))


def check_name_against_ignore_patterns(name, ls_ignore_patterns):
    for pattern in ls_ignore_patterns:
        if pattern in name:
            return True
    return False


def display_file_tree(root_dir, indent=0, ls_ignore_patterns=[]):
    ls_unignored_file_paths = []

    root_base = os.path.basename(root_dir)

    print(" " * (indent) + "├── " + root_base + "/")

    for i, name in enumerate(os.listdir(root_dir)):
        path = os.path.join(root_dir, name)
        if os.path.isdir(path):
            if not check_name_against_ignore_patterns(name, ls_ignore_patterns):
                ls_unignored_file_paths.extend(
                    display_file_tree(path, indent + 8, ls_ignore_patterns)
                )
        elif os.path.isfile(path):
            if not check_name_against_ignore_patterns(name, ls_ignore_patterns):
                print(" " * (indent + 8) + "├── " + name)
                ls_unignored_file_paths.append(path)

    return ls_unignored_file_paths

