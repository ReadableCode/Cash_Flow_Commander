# %%
# Imports #

import datetime as dt
from typing import Any

import pytest

import coverage

# %%
# Fixtures #

TODAY = dt.date(2026, 8, 3)
ACCOUNT = "ACCT-TEST-COV"


def _counts(spec: list[tuple[str, int]]) -> list[tuple[dt.date, int]]:
    """Build daily_counts output from (iso_date, reading_count) pairs."""
    return [(dt.date.fromisoformat(day), count) for day, count in spec]


def _run(start: str, days: int, readings: int = 96, skip: set[str] | None = None) -> list[tuple[dt.date, int]]:
    """Build a contiguous run of daily counts, optionally omitting some dates.

    Fixtures need realistic *continuous* coverage: a spec listing only a few
    scattered dates implies multi-year holes between them, which swamps
    whatever the test is actually about.
    """
    skip = skip or set()
    first = dt.date.fromisoformat(start)
    out = []
    for offset in range(days):
        day = first + dt.timedelta(days=offset)
        if day.isoformat() not in skip:
            out.append((day, readings))
    return out


def _analyze(monkeypatch: pytest.MonkeyPatch, counts: list[tuple[dt.date, int]], **kwargs: Any) -> dict[str, Any]:
    """Run analyze_series against canned daily counts, bypassing the database."""
    monkeypatch.setattr(coverage, "daily_counts", lambda *a, **k: counts)
    return coverage.analyze_series(
        engine=None,  # unused once daily_counts is stubbed
        account_id=ACCOUNT,
        granularity="15min",
        metric="production",
        tz="America/Chicago",
        today=TODAY,
        **kwargs,
    )


# %%
# Empty series #


def test_empty_series_asks_for_full_history(monkeypatch: pytest.MonkeyPatch) -> None:
    """No stored rows means there is nothing to be incremental about."""
    report = _analyze(monkeypatch, [])

    assert report["readings"] == 0
    assert report["first_day"] is None
    assert report["fetch_from"] is None
    assert report["fetch_to"] == TODAY.isoformat()


# %%
# Gap detection #


def test_interior_gap_is_found_and_drives_the_window(monkeypatch: pytest.MonkeyPatch) -> None:
    """A missing day older than the restatement lookback pulls fetch_from back to it."""
    # 40 days of coverage with one hole well behind the trailing edge, so the
    # gap — not the routine lookback — is what sets the window.
    counts = _run("2026-06-25", 40, skip={"2026-06-30"})
    report = _analyze(monkeypatch, counts)

    assert report["missing_ranges"] == [["2026-06-30", "2026-06-30"]]
    assert report["fetch_from"] == "2026-06-30"


def test_window_never_narrows_past_the_restatement_lookback(monkeypatch: pytest.MonkeyPatch) -> None:
    """A gap newer than the lookback must not shrink the window.

    fetch_from is a minimum over candidates, so a recent hole is already
    inside the routine re-pull window; erring wide is free (sha256 dedup at
    ingest, natural-key upsert at parse).
    """
    counts = _run("2026-07-01", 33, skip={"2026-08-01"})
    report = _analyze(monkeypatch, counts)

    assert report["missing_ranges"] == [["2026-08-01", "2026-08-01"]]
    assert report["fetch_from"] < "2026-08-01"


def test_absence_before_first_day_is_not_a_gap(monkeypatch: pytest.MonkeyPatch) -> None:
    """The system did not exist yet; that is not a hole to backfill."""
    report = _analyze(monkeypatch, _counts([("2026-08-01", 96), ("2026-08-02", 96)]))

    assert report["missing_ranges"] == []
    assert report["persistent_gap_ranges"] == []


def test_consecutive_missing_days_collapse_into_one_range(monkeypatch: pytest.MonkeyPatch) -> None:
    """Adjacent holes report as a single range, not one entry per day."""
    counts = _run(
        "2026-06-25", 40, skip={"2026-06-28", "2026-06-29", "2026-06-30"}
    )
    report = _analyze(monkeypatch, counts)

    assert report["missing_ranges"] == [["2026-06-28", "2026-06-30"]]


# %%
# Persistent-gap bounding — the incremental-workflow guard #


def test_old_gap_does_not_drag_the_window_back(monkeypatch: pytest.MonkeyPatch) -> None:
    """A permanent 2022 hole must not force a full re-pull on every run.

    This is the property that keeps a re-runnable workflow incremental: some
    gaps are facts about the source (the gateway was offline) and can never be
    filled, so they are reported but not fetched.
    """
    # Continuous coverage since 2022 with one permanently-absent day in 2022 —
    # the real shape of the solar series (gateway offline for a day).
    counts = _run("2022-11-01", 1372, skip={"2022-11-11"})
    report = _analyze(monkeypatch, counts)

    assert report["persistent_gap_ranges"] == [["2022-11-11", "2022-11-11"]]
    assert report["missing_ranges"] == []
    # Window stays near the trailing edge rather than reaching back to 2022.
    assert report["fetch_from"] >= "2026-07-01"


def test_full_flag_retries_every_known_gap(monkeypatch: pytest.MonkeyPatch) -> None:
    """--full deliberately reaches back to the oldest hole."""
    counts = _run("2022-11-01", 1372, skip={"2022-11-11"})
    report = _analyze(monkeypatch, counts, full=True)

    assert report["persistent_gap_ranges"] == []
    assert report["fetch_from"] == "2022-11-11"


def test_lookback_horizon_is_configurable(monkeypatch: pytest.MonkeyPatch) -> None:
    """A wider horizon reclassifies a persistent gap as fetchable."""
    counts = _run("2026-04-01", 124, skip={"2026-05-02"})

    narrow = _analyze(monkeypatch, counts, max_lookback_days=30)
    wide = _analyze(monkeypatch, counts, max_lookback_days=180)

    assert narrow["missing_ranges"] == []
    assert narrow["persistent_gap_ranges"] == [["2026-05-02", "2026-05-02"]]
    assert wide["missing_ranges"] == [["2026-05-02", "2026-05-02"]]
    assert wide["fetch_from"] == "2026-05-02"


# %%
# Thin-day detection #


def test_partial_day_is_flagged_as_thin(monkeypatch: pytest.MonkeyPatch) -> None:
    """An interrupted capture leaves a present-but-incomplete day."""
    counts = _run("2026-06-25", 40)
    counts[10] = (counts[10][0], 40)  # one interrupted capture, well behind the edge
    report = _analyze(monkeypatch, counts)

    assert report["thin_days"] == [["2026-07-05", 40]]
    assert report["fetch_from"] == "2026-07-05"


def test_dst_short_day_is_not_thin(monkeypatch: pytest.MonkeyPatch) -> None:
    """92 intervals on a spring-forward day is complete, not a partial capture."""
    counts = _counts(
        [("2026-07-30", 96), ("2026-07-31", 96), ("2026-08-01", 92), ("2026-08-02", 96)]
    )
    report = _analyze(monkeypatch, counts)

    assert report["thin_days"] == []


def test_fall_back_day_does_not_skew_the_median(monkeypatch: pytest.MonkeyPatch) -> None:
    """A 100-interval day must not make normal days look thin."""
    counts = _counts([("2026-08-01", 100), ("2026-08-02", 96)])
    report = _analyze(monkeypatch, counts)

    assert report["thin_days"] == []


# %%
# Restatement lookback #


def test_clean_series_still_re_pulls_the_trailing_window(monkeypatch: pytest.MonkeyPatch) -> None:
    """Providers restate recent intervals, so the newest days are re-fetched."""
    counts = _counts([(f"2026-07-{day:02d}", 96) for day in range(20, 32)])
    report = _analyze(monkeypatch, counts)

    expected = (dt.date(2026, 7, 31) - dt.timedelta(days=coverage.RESTATEMENT_LOOKBACK_DAYS))
    assert report["missing_ranges"] == []
    assert report["fetch_from"] == expected.isoformat()


# %%
# Config resolution #


def test_account_id_resolves_from_provider_config() -> None:
    """Provider slug -> account_number, the same key parse_raw.py uses."""
    config = {"enphase_enlighten": {"account_number": "SYS-1"}}

    assert coverage.account_id_for_provider("enphase_enlighten", config) == "SYS-1"


@pytest.mark.parametrize(
    "config",
    [{}, {"p": {}}, {"p": {"account_number": ""}}, {"p": {"account_number": "   "}}, {"p": "junk"}],
)
def test_missing_account_number_is_none_never_invented(config: dict[str, Any]) -> None:
    """An absent id must surface as None rather than a fabricated value."""
    assert coverage.account_id_for_provider("p", config) is None
