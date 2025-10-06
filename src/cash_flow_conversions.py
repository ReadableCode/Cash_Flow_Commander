# %%
# Running Imports #

import pandas as pd

from data_storage import SheetsStorage
from utils.display_tools import pprint_df, print_logger  # noqa F401

# %%
# Main #

if __name__ == "__main__":
    # Create instance of SheetsStorage
    ss = SheetsStorage()

    # Test individual getter methods
    df_monthly = ss.get_monthly_budgets(force_update=False)
    print_logger("Monthly Budgets:")
    pprint_df(df_monthly.head())

    df_yearly = ss.get_yearly_budgets(force_update=False)
    print_logger("Yearly Budgets:")
    pprint_df(df_yearly.head())

    df_one_time = ss.get_one_time_budgets(force_update=False)
    print_logger("One Time Budgets:")
    pprint_df(df_one_time.head())

    df_bi_weekly = ss.get_bi_weekly_budgets(force_update=False)
    print_logger("Bi-Weekly Budgets:")
    pprint_df(df_bi_weekly.head(50))

    df_calendar = ss.get_full_calendar()
    print_logger("Full Calendar:")
    pprint_df(df_calendar.head())

    # Test the main planned budgets method
    df_planned = ss.get_planned_budgets(force_update=False)

    start_print_date = pd.to_datetime("2025-10-01").date()
    end_print_date = pd.to_datetime("2025-10-31").date()

    print_logger("Planned Budgets for October 2025:")
    pprint_df(
        df_planned[
            (df_planned["Date"] >= start_print_date)
            & (df_planned["Date"] <= end_print_date)
        ]
    )


# %%
