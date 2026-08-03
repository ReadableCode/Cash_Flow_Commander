# %%
# Imports #

import datetime as dt
import json
from decimal import Decimal
from typing import Any

import pytest

from providers import get_parser, enphase_enlighten

# %%
# Synthetic fixtures (never a real system id or account) #

ACCOUNT_ID = "ACCT-TEST-SOLAR"
CTX: dict[str, Any] = {"account_id": ACCOUNT_ID}

UTC = dt.timezone.utc

# 2026-03-08 is a US spring-forward date: the local day is 23 hours, so
# Enlighten emits 92 fifteen-minute intervals instead of 96. start_time is the
# epoch instant of local midnight (CST, UTC-6) that morning.
SPRING_FORWARD_START = int(dt.datetime(2026, 3, 8, 6, 0, tzinfo=UTC).timestamp())

# 2026-11-01 is a fall-back date: 25 local hours, 100 intervals. Local midnight
# is CDT (UTC-5).
FALL_BACK_START = int(dt.datetime(2026, 11, 1, 5, 0, tzinfo=UTC).timestamp())

# An ordinary 96-interval day, local midnight CDT.
NORMAL_START = int(dt.datetime(2026, 7, 2, 5, 0, tzinfo=UTC).timestamp())


def _day(start_time: int, production: list[Any], interval_length: int = 900) -> dict[str, Any]:
    """Build one stats entry with the empty non-production channels Enphase ships."""
    entry: dict[str, Any] = {
        "start_time": start_time,
        "interval_length": interval_length,
        "production": production,
        "totals": {"production": sum(v for v in production if v)} if production else {},
    }
    for channel in enphase_enlighten._NON_PRODUCTION_CHANNELS:
        entry[channel] = []
    return entry


def _payload(stats: list[dict[str, Any]]) -> bytes:
    return json.dumps({"system_id": 1, "stats": stats}).encode()


# %%
# daily_energy parsing #


def test_parses_wh_to_kwh_and_derives_timestamps() -> None:
    """Values convert Wh -> kWh; timestamps step by interval_length from start_time."""
    rows = enphase_enlighten.parse_daily_energy_json(
        _payload([_day(NORMAL_START, [0, 250, 1500])]), CTX
    )

    assert [row["value"] for row in rows] == [Decimal("0"), Decimal("0.25"), Decimal("1.5")]
    assert [row["ts"] for row in rows] == [
        dt.datetime(2026, 7, 2, 5, 0, tzinfo=UTC),
        dt.datetime(2026, 7, 2, 5, 15, tzinfo=UTC),
        dt.datetime(2026, 7, 2, 5, 30, tzinfo=UTC),
    ]
    assert {row["granularity"] for row in rows} == {"15min"}
    assert {row["metric"] for row in rows} == {"production"}
    assert {row["unit"] for row in rows} == {"kwh"}
    assert {row["account_id"] for row in rows} == {ACCOUNT_ID}


def test_metric_is_production_not_generation() -> None:
    """Gross inverter output must not collide with the retailer's metered export."""
    rows = enphase_enlighten.parse_daily_energy_json(
        _payload([_day(NORMAL_START, [100])]), CTX
    )
    assert rows[0]["metric"] == "production"
    assert rows[0]["metric"] != "generation"


@pytest.mark.parametrize(
    "start_time,count,expected_span_hours",
    [
        (SPRING_FORWARD_START, 92, 23),
        (NORMAL_START, 96, 24),
        (FALL_BACK_START, 100, 25),
    ],
)
def test_dst_days_span_their_true_length(
    start_time: int, count: int, expected_span_hours: int
) -> None:
    """92/96/100-interval days must produce 23/24/25 hours of timestamps.

    Indexing off a fixed 96 would silently shift every interval after a DST
    boundary; deriving from start_time + i*interval_length cannot.
    """
    rows = enphase_enlighten.parse_daily_energy_json(
        _payload([_day(start_time, [10] * count)]), CTX
    )

    assert len(rows) == count
    span = rows[-1]["ts"] - rows[0]["ts"]
    assert span == dt.timedelta(hours=expected_span_hours) - dt.timedelta(minutes=15)


def test_consecutive_dst_days_do_not_overlap_or_gap() -> None:
    """A short day followed by a normal day must remain strictly contiguous."""
    next_day_start = SPRING_FORWARD_START + 23 * 3600
    rows = enphase_enlighten.parse_daily_energy_json(
        _payload([_day(SPRING_FORWARD_START, [1] * 92), _day(next_day_start, [1] * 96)]),
        CTX,
    )

    stamps = [row["ts"] for row in rows]
    assert len(stamps) == len(set(stamps)), "duplicate timestamps across the DST boundary"
    deltas = {b - a for a, b in zip(stamps, stamps[1:])}
    assert deltas == {dt.timedelta(minutes=15)}


def test_empty_production_day_is_skipped_not_zeroed() -> None:
    """A non-reporting day yields no rows — absence is not a zero measurement."""
    rows = enphase_enlighten.parse_daily_energy_json(
        _payload([_day(NORMAL_START, []), _day(NORMAL_START + 86400, [500])]), CTX
    )

    assert len(rows) == 1
    assert rows[0]["value"] == Decimal("0.5")


def test_null_interval_inside_a_day_is_skipped() -> None:
    """A null reading mid-day is a gap, not a zero."""
    rows = enphase_enlighten.parse_daily_energy_json(
        _payload([_day(NORMAL_START, [100, None, 300])]), CTX
    )

    assert [row["value"] for row in rows] == [Decimal("0.1"), Decimal("0.3")]
    assert [row["ts"].minute for row in rows] == [0, 30]


def test_populated_extra_channel_raises() -> None:
    """Added consumption CTs must fail loudly, not drop real measurements."""
    day = _day(NORMAL_START, [100])
    day["consumption"] = [42]

    with pytest.raises(ValueError, match="non-production channels are populated"):
        enphase_enlighten.parse_daily_energy_json(_payload([day]), CTX)


def test_unexpected_interval_length_raises() -> None:
    """An unrecognized granularity must not be mislabeled as 15min."""
    with pytest.raises(ValueError, match="unexpected interval_length"):
        enphase_enlighten.parse_daily_energy_json(
            _payload([_day(NORMAL_START, [100], interval_length=1800)]), CTX
        )


def test_five_minute_payload_is_labeled_correctly() -> None:
    """If Enphase ever serves 5-minute data, granularity must follow."""
    rows = enphase_enlighten.parse_daily_energy_json(
        _payload([_day(NORMAL_START, [10, 20], interval_length=300)]), CTX
    )

    assert {row["granularity"] for row in rows} == {"5min"}
    assert rows[1]["ts"] - rows[0]["ts"] == dt.timedelta(minutes=5)


# %%
# lifetime_energy parsing #


def test_lifetime_energy_is_daily_and_skips_leading_nulls() -> None:
    """Daily rollups land at granularity 'day', nulls before first report skipped."""
    content = json.dumps(
        {"system_id": 1, "start_date": "2021-12-16", "production": [None, 0, 25292]}
    ).encode()

    rows = enphase_enlighten.parse_lifetime_energy_json(content, CTX)

    assert [row["granularity"] for row in rows] == ["day", "day"]
    assert [row["ts"].date() for row in rows] == [dt.date(2021, 12, 17), dt.date(2021, 12, 18)]
    assert rows[-1]["value"] == Decimal("25.292")


def test_day_and_interval_rows_share_no_natural_key() -> None:
    """Both parsers write 'production'; differing granularity keeps them distinct.

    usage_intervals upserts on (account_id, ts, granularity, metric), so a
    shared granularity would make the daily rollup overwrite an interval.
    """
    interval_rows = enphase_enlighten.parse_daily_energy_json(
        _payload([_day(NORMAL_START, [100])]), CTX
    )
    daily_rows = enphase_enlighten.parse_lifetime_energy_json(
        json.dumps({"start_date": "2026-07-02", "production": [55013]}).encode(), CTX
    )

    def key(row: dict[str, Any]) -> tuple[Any, ...]:
        return (row["account_id"], row["ts"], row["granularity"], row["metric"])

    assert not {key(r) for r in interval_rows} & {key(r) for r in daily_rows}


# %%
# Registry wiring #


@pytest.mark.parametrize(
    "name,expected",
    [
        (
            "enphase_enlighten_api_usage_daily_energy_2021-12-16_2026-08-03.json",
            enphase_enlighten.parse_daily_energy_json,
        ),
        (
            "enphase_enlighten_api_usage_lifetime_energy_2026-08-03.json",
            enphase_enlighten.parse_lifetime_energy_json,
        ),
    ],
)
def test_registry_routes_by_filename(name: str, expected: Any) -> None:
    """Both Enlighten captures are api_usage_json; the filename picks the parser."""
    resolved = get_parser("enphase_enlighten", "api_usage_json", name)

    assert resolved is not None
    parse_fn, version = resolved
    assert parse_fn is expected
    assert version == enphase_enlighten.PARSER_VERSION


def test_registry_returns_none_for_unknown_enphase_document() -> None:
    """An unrecognized capture stays pending rather than being parsed wrongly."""
    assert get_parser("enphase_enlighten", "other", "enphase_enlighten_api_system_today.json") is None
