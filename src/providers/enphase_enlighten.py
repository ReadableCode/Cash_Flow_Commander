# %%
# Imports #

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo


# %%
# Constants #

PARSER_VERSION = "enphase-production/1.1.0"

# The site's local zone. Enlighten keys its daily rollup by local calendar
# date, so deciding which rollup is the capture's own day needs it.
CENTRAL = ZoneInfo("America/Chicago")

# Gross AC production measured at the inverters. Deliberately NOT 'generation':
# the electricity retailer's 'generation' metric is metered EXPORT, a different
# physical quantity. Self-consumed solar is the difference between the two, and
# naming them alike would make that reconciliation compare a series to itself.
METRIC = "production"

# Enphase reports energy per interval in watt-hours; usage_intervals is kWh.
_WH_PER_KWH = Decimal(1000)

# Interval lengths Enphase is known to emit, in seconds. 900 (15 min) is what
# the web portal serves; the guard exists so a silent granularity change is
# loud rather than mislabeled as '15min'.
_GRANULARITY_BY_INTERVAL_SECONDS = {
    300: "5min",
    900: "15min",
    3600: "hour",
}

# Energy channels Enphase ships in every stats entry alongside 'production'.
# All empty on a production-only site; see _reject_populated_extra_channels.
_NON_PRODUCTION_CHANNELS = (
    "consumption",
    "generator",
    "import",
    "export",
    "grid_import",
    "grid_home",
    "grid_battery",
    "solar_home",
    "solar_battery",
    "solar_grid",
    "generator_home",
    "generator_battery",
    "generator_grid",
    "battery_home",
    "battery_grid",
)


# %%
# Functions #


def _granularity_for(interval_length: Any) -> str:
    """Map an interval_length in seconds to a granularity label.

    Raises on an unrecognized length: mislabeling 5-minute data as '15min'
    would corrupt the natural key silently, and loud beats silent.
    """
    try:
        seconds = int(interval_length)
    except (TypeError, ValueError):
        raise ValueError(f"non-numeric interval_length {interval_length!r}")
    granularity = _GRANULARITY_BY_INTERVAL_SECONDS.get(seconds)
    if granularity is None:
        raise ValueError(f"unexpected interval_length {seconds} seconds")
    return granularity


def _reject_populated_extra_channels(day: dict[str, Any]) -> None:
    """Raise if a non-production energy channel carries readings.

    On a production-only site every one of these is an empty array — the
    schema is present, the data is not. A populated one means consumption CTs,
    a battery, or a generator was added, which this parser is not written to
    handle; failing loudly beats dropping real measurements on the floor.
    """
    populated = sorted(
        name
        for name in _NON_PRODUCTION_CHANNELS
        if isinstance(day.get(name), list) and day[name]
    )
    if populated:
        raise ValueError(
            "non-production channels are populated "
            f"({', '.join(populated)}) — metering hardware changed; "
            "extend enphase_enlighten parser to handle them"
        )


def _capture_time(ctx: dict[str, Any]) -> datetime:
    """Return the capture instant from ctx['fetched_at'] as an aware UTC datetime.

    fetched_at is the capture file's mtime at ingest (ingest_raw._read_file),
    not the HTTP fetch time. `mv` into raw_dir keeps the mtime; a `cp` without
    `-p` stamps the copy later and lets a few unelapsed zero slots through, so
    file captures with `mv`. SQLite hands the column back naive; a naive value
    is UTC, which is how raw_store.ingest_bytes stored it.
    """
    fetched_at = ctx["fetched_at"]
    if fetched_at.tzinfo is None:
        return fetched_at.replace(tzinfo=timezone.utc)
    return fetched_at.astimezone(timezone.utc)


def parse_daily_energy_json(content: bytes, ctx: dict[str, Any]) -> list[dict[str, Any]]:
    """Parse an Enlighten daily_energy payload into usage interval row dicts.

    Shape: {'stats': [{'start_time': <epoch seconds>, 'interval_length': 900,
    'production': [<Wh>, ...], ...}, ...]} — one stats entry per local day.

    Timestamps are derived as start_time + index * interval_length and stored
    as UTC. start_time is the epoch instant of site-local midnight, so this
    arithmetic is DST-correct without any local-time handling: the number of
    intervals per day is 92/96/100 on transition days rather than a fixed 96,
    and indexing off a fixed day length would shift every interval after a
    time change. Never assume the array length.

    A day the system did not report yields `production: []` — an empty array,
    not zeros. Those days are skipped entirely rather than written as zero
    production, which would be a measurement claim the data does not make.

    The current day is different: once the panels are producing, Enlighten
    serves all of today's slots, with the ones that have not happened yet as
    zeros. Those are placeholders, not measurements, so every interval that
    starts at or after the capture time (ctx['fetched_at'], see _capture_time)
    is dropped. A later capture's overlap writes the real values.

    Non-production channels (consumption/import/export/battery/generator) carry
    the full Enphase schema but are empty arrays on a production-only site, and
    this parser reads only `production`. If any of them ever arrives populated
    it means metering hardware was added, so the parse RAISES rather than
    silently discarding real measurements — extend this parser deliberately at
    that point. account_id always comes from ctx, never from the document.
    """
    account_id = ctx["account_id"]
    captured_at = _capture_time(ctx)
    payload = json.loads(content)
    rows: list[dict[str, Any]] = []

    for day in payload.get("stats") or []:
        _reject_populated_extra_channels(day)
        production = day.get("production") or []
        if not production:
            continue  # no reading for this day — not a zero-production claim
        granularity = _granularity_for(day["interval_length"])
        step = timedelta(seconds=int(day["interval_length"]))
        day_start = datetime.fromtimestamp(int(day["start_time"]), timezone.utc)

        for index, watt_hours in enumerate(production):
            if watt_hours is None:
                continue  # gap inside a reporting day
            ts = day_start + index * step
            if ts >= captured_at:
                continue  # slot had not happened at capture time — zero padding
            rows.append(
                {
                    "account_id": account_id,
                    "ts": ts,
                    "granularity": granularity,
                    "metric": METRIC,
                    "value": Decimal(str(watt_hours)) / _WH_PER_KWH,
                    "unit": "kwh",
                    "rate": None,
                    "cost": None,
                    "estimated": False,
                }
            )
    return rows


def parse_lifetime_energy_json(content: bytes, ctx: dict[str, Any]) -> list[dict[str, Any]]:
    """Parse an Enlighten lifetime_energy payload into daily usage row dicts.

    Shape: {'start_date': 'YYYY-MM-DD', 'production': [<Wh per day>, ...]} —
    one value per day counting forward from start_date. Stored at granularity
    'day', so these rows never collide with the interval rows from
    parse_daily_energy_json on the natural key.

    This is a coarse cross-check of the interval series, not a substitute for
    it. Leading nulls (days before the system reported) are skipped.

    The last element is the capture's own local day, carrying the running
    total so far rather than the day's production. That row, and any dated
    after the capture's local day, is dropped; the local day is the capture
    time (ctx['fetched_at'], see _capture_time) in CENTRAL.

    Day rows are timestamped at UTC midnight of the local calendar date. That
    is a label for the day, not a physical instant — do not difference these
    against interval rows without re-aggregating.
    """
    account_id = ctx["account_id"]
    capture_day = _capture_time(ctx).astimezone(CENTRAL).date()
    payload = json.loads(content)
    start = datetime.fromisoformat(payload["start_date"]).replace(tzinfo=timezone.utc)

    rows: list[dict[str, Any]] = []
    for index, watt_hours in enumerate(payload.get("production") or []):
        if watt_hours is None:
            continue  # system had not reported yet
        ts = start + timedelta(days=index)
        if ts.date() >= capture_day:
            continue  # the capture's own day: a running total, not the day's production
        rows.append(
            {
                "account_id": account_id,
                "ts": ts,
                "granularity": "day",
                "metric": METRIC,
                "value": Decimal(str(watt_hours)) / _WH_PER_KWH,
                "unit": "kwh",
                "rate": None,
                "cost": None,
                "estimated": False,
            }
        )
    return rows
