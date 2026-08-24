# %%
# Imports #

import datetime as dt
import os
import sys

import pytest

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if os.path.join(_ROOT, "src") not in sys.path:
    sys.path.insert(0, os.path.join(_ROOT, "src"))

import expected_schedule  # noqa: E402

# %%
# Helpers #


def series_row(**overrides):
    """A complete series dict with every schedule field present (None by default)."""
    row = {
        "schedule_type": "monthly",
        "day_of_month": None,
        "month_of_year": None,
        "anchor_date": None,
        "interval_days": None,
        "active_from": dt.date(2020, 1, 1),
        "active_until": None,
    }
    row.update(overrides)
    return row


# %%
# Tests #


def test_monthly_hits_the_day_each_month():
    dates = expected_schedule.monthly_due_dates(
        6, dt.date(2026, 1, 1), dt.date(2026, 3, 31)
    )
    assert dates == [dt.date(2026, 1, 6), dt.date(2026, 2, 6), dt.date(2026, 3, 6)]


def test_monthly_day_31_clamps_to_short_months():
    dates = expected_schedule.monthly_due_dates(
        31, dt.date(2026, 1, 1), dt.date(2026, 4, 30)
    )
    assert dates == [
        dt.date(2026, 1, 31),
        dt.date(2026, 2, 28),
        dt.date(2026, 3, 31),
        dt.date(2026, 4, 30),
    ]


def test_monthly_respects_window_edges():
    # Window starts after the January due date and ends before March's.
    dates = expected_schedule.monthly_due_dates(
        6, dt.date(2026, 1, 10), dt.date(2026, 3, 5)
    )
    assert dates == [dt.date(2026, 2, 6)]


def test_yearly():
    dates = expected_schedule.yearly_due_dates(
        7, 13, dt.date(2025, 1, 1), dt.date(2026, 12, 31)
    )
    assert dates == [dt.date(2025, 7, 13), dt.date(2026, 7, 13)]


def test_every_x_days_stays_aligned_to_a_distant_anchor():
    # Anchored years back; occurrences must land on exact 14-day multiples.
    anchor = dt.date(2019, 7, 19)
    dates = expected_schedule.every_x_days_due_dates(
        anchor, 14, dt.date(2026, 8, 1), dt.date(2026, 8, 31)
    )
    assert dates == [dt.date(2026, 8, 7), dt.date(2026, 8, 21)]
    for date in dates:
        assert (date - anchor).days % 14 == 0


def test_every_x_days_start_on_an_occurrence_includes_it():
    anchor = dt.date(2026, 8, 1)
    dates = expected_schedule.every_x_days_due_dates(
        anchor, 7, dt.date(2026, 8, 1), dt.date(2026, 8, 15)
    )
    assert dates == [dt.date(2026, 8, 1), dt.date(2026, 8, 8), dt.date(2026, 8, 15)]


def test_series_window_is_clipped_by_active_dates():
    row = series_row(
        schedule_type="monthly",
        day_of_month=15,
        active_from=dt.date(2026, 2, 1),
        active_until=dt.date(2026, 3, 31),
    )
    dates = expected_schedule.due_dates_for_series(
        row, dt.date(2026, 1, 1), dt.date(2026, 12, 31)
    )
    assert dates == [dt.date(2026, 2, 15), dt.date(2026, 3, 15)]


def test_once_and_incomplete_schedules_generate_nothing():
    assert (
        expected_schedule.due_dates_for_series(
            series_row(schedule_type="once"), dt.date(2026, 1, 1), dt.date(2026, 12, 31)
        )
        == []
    )
    # An imported closed series knows its type but not its parameters.
    assert (
        expected_schedule.due_dates_for_series(
            series_row(schedule_type="monthly", day_of_month=None),
            dt.date(2026, 1, 1),
            dt.date(2026, 12, 31),
        )
        == []
    )


def test_unknown_schedule_type_raises():
    with pytest.raises(ValueError):
        expected_schedule.due_dates_for_series(
            series_row(schedule_type="fortnightly"),
            dt.date(2026, 1, 1),
            dt.date(2026, 12, 31),
        )


def test_every_x_months_keeps_the_anchor_day():
    anchor = dt.date(2026, 7, 17)
    dates = expected_schedule.every_x_months_due_dates(
        anchor, 5, dt.date(2026, 7, 1), dt.date(2027, 12, 31)
    )
    assert dates == [
        dt.date(2026, 7, 17),
        dt.date(2026, 12, 17),
        dt.date(2027, 5, 17),
        dt.date(2027, 10, 17),
    ]


def test_every_x_months_clamps_short_months():
    # Anchored on the 31st; a landing in June pulls back to the 30th.
    dates = expected_schedule.every_x_months_due_dates(
        dt.date(2026, 1, 31), 5, dt.date(2026, 1, 1), dt.date(2026, 12, 31)
    )
    assert dates == [dt.date(2026, 1, 31), dt.date(2026, 6, 30), dt.date(2026, 11, 30)]
