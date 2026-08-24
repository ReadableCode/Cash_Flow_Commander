"""Turn a series' schedule into concrete due dates.

Pure date math — no database, no pandas. Each function answers one question:
"on which dates between start and end does this series expect money to move?"

Schedule vocabulary (matches the old Income_Expense sheet's Type column):

- monthly:        day_of_month             e.g. the 6th of every month
- yearly:         month_of_year + day_of_month
- biweekly:       anchor_date, every 14 days
- every_x_days:   anchor_date + interval_days
- every_x_months: anchor_date + interval_months (a 5-month subscription)
- once:           no generated dates; occurrences are entered directly
- irregular:      recurring but on no fixed schedule (a contract paid 35-47
                  days after each invoice); occurrences are entered directly
"""

# %%
# Imports #

import calendar
import datetime

# %%
# Constants #

BIWEEKLY_INTERVAL_DAYS = 14


# %%
# Functions #


def clamp_day_to_month(year: int, month: int, day: int) -> datetime.date:
    """Return the date, pulling the day back to the month's last day if needed.

    A series on the 31st still happens in February — on the 28th (or 29th),
    the way autopay systems behave.
    """
    last_day = calendar.monthrange(year, month)[1]
    return datetime.date(year, month, min(day, last_day))


def monthly_due_dates(
    day_of_month: int, start: datetime.date, end: datetime.date
) -> list:
    """One date per month, on day_of_month, from start through end inclusive."""
    due_dates = []
    year = start.year
    month = start.month
    while (year, month) <= (end.year, end.month):
        due_date = clamp_day_to_month(year, month, day_of_month)
        if start <= due_date <= end:
            due_dates.append(due_date)
        if month == 12:
            year = year + 1
            month = 1
        else:
            month = month + 1
    return due_dates


def yearly_due_dates(
    month_of_year: int, day_of_month: int, start: datetime.date, end: datetime.date
) -> list:
    """One date per year, on month_of_year/day_of_month, from start through end."""
    due_dates = []
    for year in range(start.year, end.year + 1):
        due_date = clamp_day_to_month(year, month_of_year, day_of_month)
        if start <= due_date <= end:
            due_dates.append(due_date)
    return due_dates


def every_x_days_due_dates(
    anchor_date: datetime.date,
    interval_days: int,
    start: datetime.date,
    end: datetime.date,
) -> list:
    """Every interval_days counted from anchor_date, from start through end.

    The anchor can be years in the past (a paycheck's first pay date); we jump
    straight to the first occurrence at or after start instead of walking
    day by day from the anchor.
    """
    if interval_days <= 0:
        return []
    days_from_anchor = (start - anchor_date).days
    # Round UP to the next multiple of interval_days at or after start.
    intervals_elapsed = max(0, -(-days_from_anchor // interval_days))
    due_date = anchor_date + datetime.timedelta(days=intervals_elapsed * interval_days)

    due_dates = []
    while due_date <= end:
        due_dates.append(due_date)
        due_date = due_date + datetime.timedelta(days=interval_days)
    return due_dates


def _is_missing(value) -> bool:
    """True for None, NaN, and NaT.

    Series rows arrive through pandas, which hands back NaN for a NULL integer
    column and NaT for a NULL date — neither of which is None. The NaN/NaT
    check works because those values are famously not equal to themselves.
    """
    return value is None or value != value


def every_x_months_due_dates(
    anchor_date: datetime.date,
    interval_months: int,
    start: datetime.date,
    end: datetime.date,
) -> list:
    """Every interval_months counted from anchor_date, from start through end.

    Keeps the anchor's day of month (clamped in short months), so a 5-month
    subscription anchored on the 17th stays on the 17th.
    """
    if interval_months <= 0:
        return []
    due_dates = []
    year = anchor_date.year
    month = anchor_date.month
    while True:
        due_date = clamp_day_to_month(year, month, anchor_date.day)
        if due_date > end:
            break
        if due_date >= start:
            due_dates.append(due_date)
        month = month + interval_months
        while month > 12:
            month = month - 12
            year = year + 1
    return due_dates


def due_dates_for_series(
    series_row: dict, start: datetime.date, end: datetime.date
) -> list:
    """Due dates for one expected_series row (as a dict), clipped to its active window.

    Rows whose schedule fields are incomplete (imported closed series we only
    know history for) generate nothing — their occurrences were imported
    directly and there is no future to generate.
    """
    active_from = series_row["active_from"]
    active_until = series_row["active_until"]

    window_start = max(start, active_from)
    window_end = end
    if not _is_missing(active_until):
        window_end = min(end, active_until)
    if window_start > window_end:
        return []

    schedule_type = series_row["schedule_type"]

    # A NULL integer column comes back from pandas as a float (6.0), which
    # datetime.date refuses — hence the int() casts below.
    if schedule_type == "monthly":
        if _is_missing(series_row["day_of_month"]):
            return []
        return monthly_due_dates(
            int(series_row["day_of_month"]), window_start, window_end
        )

    if schedule_type == "yearly":
        if _is_missing(series_row["month_of_year"]) or _is_missing(
            series_row["day_of_month"]
        ):
            return []
        return yearly_due_dates(
            int(series_row["month_of_year"]),
            int(series_row["day_of_month"]),
            window_start,
            window_end,
        )

    if schedule_type == "biweekly":
        if _is_missing(series_row["anchor_date"]):
            return []
        return every_x_days_due_dates(
            series_row["anchor_date"], BIWEEKLY_INTERVAL_DAYS, window_start, window_end
        )

    if schedule_type == "every_x_days":
        if _is_missing(series_row["anchor_date"]) or _is_missing(
            series_row["interval_days"]
        ):
            return []
        return every_x_days_due_dates(
            series_row["anchor_date"],
            int(series_row["interval_days"]),
            window_start,
            window_end,
        )

    if schedule_type == "every_x_months":
        if _is_missing(series_row["anchor_date"]) or _is_missing(
            series_row["interval_months"]
        ):
            return []
        return every_x_months_due_dates(
            series_row["anchor_date"],
            int(series_row["interval_months"]),
            window_start,
            window_end,
        )

    if schedule_type in ("once", "irregular"):
        return []

    raise ValueError(f"Unknown schedule_type: {schedule_type!r}")


# %%
