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
