# %%
## Imports ##

if __name__ != "__main__":
    print(f"Importing {__name__}")

import pandas as pd
import os
import sys
import matplotlib.pyplot as plt

# append grandparent
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config_utils import (
    file_dir,
    parent_dir,
    grandparent_dir,
    great_grandparent_dir,
    data_dir,
)

from utils.display_tools import print_logger, pprint_dict, pprint_df, pprint_ls


# %%
## Variables ##


def plot_dataframe_with_time(
    df,
    x_column,
    ls_y_columns,
    graph_for_each_unique_of_col=None,
):
    # Sort the DataFrame by the time column in ascending order
    df_sorted = df.sort_values(by=x_column)

    # Extract the x-axis values (time_column) and convert to datetime
    x_values = df_sorted[x_column]

    # Create a figure and axis
    fig, ax = plt.subplots()

    # Plot each y-column as a line
    for y_column in ls_y_columns:
        y_values = df_sorted[y_column]
        ax.plot(x_values, y_values, label=y_column)

    # Customize the plot
    ax.set_xlabel(x_column)
    ax.set_ylabel("Y Values")
    ax.set_title("Line Chart of Y Values over Time")
    ax.legend()

    # Rotate x-axis labels for better visibility
    plt.xticks(rotation=45)

    # Display the plot
    plt.tight_layout()
    plt.show()


# %%
## Main ##

if __name__ == "__main__":
    # Sample DataFrame with time column and y-columns
    data = {
        "Time": ["2023-W12", "2023-W15", "2023-W10", "2023-W14", "2023-W13"],
        "Category": ["A", "B", "A", "B", "A"],
        "Y1": [10, 15, 8, 12, 14],
        "Y2": [5, 9, 6, 11, 7],
        "Y3": [3, 8, 4, 10, 6],
    }
    df = pd.DataFrame(data)

    # Call the function to plot the DataFrame
    x_column = "Time"
    ls_y_columns = ["Y1", "Y2", "Y3"]
    pprint_df(df)
    plot_dataframe_with_time(df, x_column, ls_y_columns)


# %%
