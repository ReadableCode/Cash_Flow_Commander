# %%
# Imports #

import os
import argparse
import sys
import pandas as pd

# append grandparent
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config_utils import (
    file_dir,
    parent_dir,
    grandparent_dir,
    great_grandparent_dir,
    data_dir,
)

from utils.display_tools import print_logger, pprint_df


# %%
# Functions #


def merge_and_return_unmerged(df1, df2, merge_cols, how="left"):
    """
    Merge two dataframes on specified columns and return the merged dataframe
    and a dataframe with unmerged rows from the first dataframe.

    Parameters:
    - df1 (DataFrame): First DataFrame to merge.
    - df2 (DataFrame): Second DataFrame to merge.
    - merge_cols (list of str): List of columns to merge on.
    - how (str, optional): Type of merge to be performed.
        - 'left' (default): Use keys from left frame only, similar to a SQL left outer join.
        - 'right': Use keys from right frame only, similar to a SQL right outer join.
        - 'outer': Use union of keys from both frames, similar to a SQL full outer join.
        - 'inner': Use intersection of keys from both frames, similar to a SQL inner join.

    Returns:
    - merged_df (DataFrame): The merged DataFrame.
    - unmerged_df (DataFrame): Rows from df1 that did not merge.
    """

    # Merging the two dataframes
    merged_df = pd.merge(df1, df2, on=merge_cols, how=how, indicator=True)

    # Finding the rows from df1 that did not merge
    unmerged_df = merged_df.loc[merged_df["_merge"] == "left_only"].drop(
        columns=["_merge"]
    )

    # Drop the indicator column from merged dataframe
    merged_df = merged_df.drop(columns=["_merge"])

    return merged_df, unmerged_df


def read_csv_with_decode_error_handling(
    file_path, run_byte_and_symbol_replacement=False
):
    if run_byte_and_symbol_replacement:
        # Read the file in binary mode ('rb')
        with open(file_path, "rb") as f:
            file_content_bytes = f.read()

        dict_bytes_to_replace = {
            b"\x96": b"",
            b"\x92": b"",
            b"\x93": b"",
            b"\x94": b"",
            b"\xcf": b"",
            b"\xc8": b"",
            b"\xd1": b"",
            b"\xd9": b"",
            b"\xc0": b"",
            b"\xae": b"",
            b"\xc7": b"",
        }

        print("replaced bytes")

        for byte_to_replace, replacement in dict_bytes_to_replace.items():
            file_content_bytes = file_content_bytes.replace(
                byte_to_replace, replacement
            )

        # Open the file in write binary mode ('wb') to overwrite its content
        with open(file_path, "wb") as f:
            f.write(file_content_bytes)

        with open(file_path, "r") as f:
            file_contents = f.read()

        dict_strings_to_replace = {
            "Ñ": "N",
            "’": "",
            "Ù": "U",
            "À": "A",
            "®": "",
            "Ç": "C",
        }

        for string_to_replace, replacement in dict_strings_to_replace.items():
            file_contents = file_contents.replace(string_to_replace, replacement)

        # Write the modified contents to a new file
        with open(file_path, "w") as f:
            f.write(file_contents)

    try:
        df = pd.read_csv(file_path, low_memory=False)
    except Exception as e:
        print_logger(f"Failed to read file: {file_path} because {e}")
        if run_byte_and_symbol_replacement:
            raise Exception(f"Failed to read file: {file_path} because {e}")

        return read_csv_with_decode_error_handling(
            file_path, run_byte_and_symbol_replacement=True
        )

    return df


# %%
# Main #


if __name__ == "__main__":
    # Sample DataFrames
    data1 = {"A": [1, 2, 3, 4], "B": [5, 6, 7, 8], "key": ["a", "b", "c", "d"]}

    data2 = {"C": [10, 20, 30], "D": [50, 60, 70], "key": ["a", "b", "x"]}

    df1 = pd.DataFrame(data1)
    df2 = pd.DataFrame(data2)

    pprint_df(df1)
    pprint_df(df2)

    # Using the function
    merged_df, unmerged_df = merge_and_return_unmerged(df1, df2, merge_cols=["key"])

    # Displaying results
    print("Merged DataFrame:")
    pprint_df(merged_df)
    print("Unmerged DataFrame:")
    pprint_df(unmerged_df)


# %%
