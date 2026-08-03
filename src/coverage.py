"""Report interval-series coverage and emit the window a provider should fetch.

Step 1 of every acquisition workflow. Acquisition commands are re-runnable, so
they must not blindly re-pull everything (slow) or blindly pull "since last
run" (loses interior gaps a provider backfilled late). This answers, per
account/metric/granularity: what range is covered, which interior days are
missing or thin, and therefore what date window is worth fetching.

Overlap is a non-issue by construction and this tool leans on that: raw
documents dedup by sha256 at ingest, and usage_intervals upserts on
(account_id, ts, granularity, metric). Re-fetching a period already held costs
bandwidth and nothing else — so the recommended window always errs wide.

Usage:

    uv run python src/coverage.py --account 12345 --metric production
    uv run python src/coverage.py --provider rhythm --json
"""

# %%
# Imports #

import argparse
import datetime
import json
import os
import sys
from typing import Any

import yaml
from sqlalchemy import func, select
from sqlalchemy.engine import Engine

_SRC_DIR = os.path.dirname(os.path.abspath(__file__))
if _SRC_DIR not in sys.path:
    sys.path.insert(0, _SRC_DIR)

import db  # noqa: E402

# %%
# Constants #

_REPO_ROOT = os.path.dirname(_SRC_DIR)
PROVIDERS_YAML_PATH = os.path.join(_REPO_ROOT, "providers.local.yaml")

# A local day is "thin" when it holds fewer readings than this share of the
# median day for its series. Catches partial captures (a provider that served
# half a day, or a run interrupted mid-fetch) which a pure min/max check reads
# as covered. Not 1.0: DST short days are legitimately 92/96 of normal.
THIN_DAY_RATIO = 0.9

# Re-pull this many days behind the covered maximum even when no gap is found.
# Providers restate recent intervals — meter data gets revised, and an Enphase
# gateway backfills after a comms outage — so the newest days are the least
# trustworthy ones already stored.
RESTATEMENT_LOOKBACK_DAYS = 7

# Interior gaps older than this stop driving the fetch window. Some gaps are
# permanent facts about the source, not capture failures: a day the solar
# gateway was offline has no data to fetch and never will. Without this bound
# one such day in 2022 would pull the fetch window back to 2022 on every run,
# forever, turning an incremental workflow into a full re-pull that can never
# succeed. Old gaps are still REPORTED (as persistent_gap_ranges) so they stay
# visible; retry them deliberately with --full.
MAX_LOOKBACK_DAYS = 60


# %%
# Config #


def _load_providers_config(path: str) -> dict[str, Any]:
    """Load providers.local.yaml; {} when missing or not a mapping."""
    if not os.path.isfile(path):
        return {}
    with open(path, "r", encoding="utf-8") as handle:
        loaded = yaml.safe_load(handle)
    return loaded if isinstance(loaded, dict) else {}


def account_id_for_provider(provider: str, config: dict[str, Any]) -> str | None:
    """Resolve a provider slug to its account id, or None when unset."""
    entry = config.get(provider)
    if not isinstance(entry, dict):
        return None
    account = entry.get("account_number")
    if account is None or not str(account).strip():
        return None
    return str(account)


# %%
# Coverage #


def series_keys(
    engine: Engine, account_id: str | None = None
) -> list[tuple[str, str, str]]:
    """List distinct (account_id, granularity, metric) series, optionally filtered."""
    stmt = select(
        db.usage_intervals.c.account_id,
        db.usage_intervals.c.granularity,
        db.usage_intervals.c.metric,
    ).distinct()
    if account_id is not None:
        stmt = stmt.where(db.usage_intervals.c.account_id == account_id)
    stmt = stmt.order_by(
        db.usage_intervals.c.account_id,
        db.usage_intervals.c.granularity,
        db.usage_intervals.c.metric,
    )
    with engine.connect() as conn:
        return [tuple(row) for row in conn.execute(stmt)]


def daily_counts(
    engine: Engine, account_id: str, granularity: str, metric: str, tz: str
) -> list[tuple[datetime.date, int]]:
    """Return [(local_date, reading_count), ...] ascending for one series.

    Bucketing uses the site-local date, not UTC: a gap means "a day with no
    readings" in the sense a person means it, and UTC bucketing would split
    every local day across two buckets for a US site.
    """
    local_ts = func.timezone(tz, db.usage_intervals.c.ts)
    local_day = func.date(local_ts).label("local_day")
    stmt = (
        select(local_day, func.count().label("readings"))
        .where(
            db.usage_intervals.c.account_id == account_id,
            db.usage_intervals.c.granularity == granularity,
            db.usage_intervals.c.metric == metric,
        )
        .group_by(local_day)
        .order_by(local_day)
    )
    with engine.connect() as conn:
        rows = conn.execute(stmt).all()

    out: list[tuple[datetime.date, int]] = []
    for day, count in rows:
        out.append((day if isinstance(day, datetime.date) else datetime.date.fromisoformat(str(day)), int(count)))
    return out


def _median(values: list[int]) -> float:
    """Median of a non-empty list of ints."""
    ordered = sorted(values)
    middle = len(ordered) // 2
    if len(ordered) % 2:
        return float(ordered[middle])
    return (ordered[middle - 1] + ordered[middle]) / 2.0


def _contiguous_ranges(days: list[datetime.date]) -> list[tuple[datetime.date, datetime.date]]:
    """Collapse a sorted date list into [(start, end), ...] inclusive runs."""
    ranges: list[tuple[datetime.date, datetime.date]] = []
    for day in days:
        if ranges and day == ranges[-1][1] + datetime.timedelta(days=1):
            ranges[-1] = (ranges[-1][0], day)
        else:
            ranges.append((day, day))
    return ranges


def analyze_series(
    engine: Engine,
    account_id: str,
    granularity: str,
    metric: str,
    *,
    tz: str,
    today: datetime.date,
    max_lookback_days: int = MAX_LOOKBACK_DAYS,
    full: bool = False,
) -> dict[str, Any]:
    """Describe one series: covered range, interior gaps, thin days, fetch window.

    Interior gaps are missing days strictly inside the covered range —
    absence before first_day is not a gap (the system did not exist yet) and
    absence after last_day is the trailing edge, handled by the fetch window.

    Gaps are split by age against max_lookback_days: recent ones drive
    fetch_from, older ones are reported as persistent_gap_ranges and ignored
    when choosing the window (see MAX_LOOKBACK_DAYS). full=True removes the
    bound, so every known gap is retried.
    """
    counts = daily_counts(engine, account_id, granularity, metric, tz)
    if not counts:
        return {
            "account_id": account_id,
            "granularity": granularity,
            "metric": metric,
            "readings": 0,
            "first_day": None,
            "last_day": None,
            "missing_ranges": [],
            "thin_days": [],
            "fetch_from": None,
            "fetch_to": today.isoformat(),
            "note": "no rows — fetch full history",
        }

    present = {day for day, _ in counts}
    first_day, last_day = counts[0][0], counts[-1][0]
    median_readings = _median([count for _, count in counts])

    missing: list[datetime.date] = []
    day = first_day
    while day <= last_day:
        if day not in present:
            missing.append(day)
        day += datetime.timedelta(days=1)

    thin = [
        (day, count)
        for day, count in counts
        if median_readings and count < median_readings * THIN_DAY_RATIO
    ]

    # Split gaps and thin days by age. Anything older than the horizon is a
    # known fact about the source rather than a fetchable hole.
    horizon = today - datetime.timedelta(days=max_lookback_days)
    if full:
        horizon = datetime.date.min

    recent_missing = [day for day in missing if day >= horizon]
    persistent_missing = [day for day in missing if day < horizon]
    recent_thin = [(day, count) for day, count in thin if day >= horizon]

    # Fetch window: earliest thing worth re-pulling through today. Interior
    # holes win over the restatement lookback when they are older.
    lookback_start = last_day - datetime.timedelta(days=RESTATEMENT_LOOKBACK_DAYS)
    candidates = [lookback_start]
    if recent_missing:
        candidates.append(recent_missing[0])
    if recent_thin:
        candidates.append(recent_thin[0][0])
    fetch_from = min(candidates)

    return {
        "account_id": account_id,
        "granularity": granularity,
        "metric": metric,
        "readings": sum(count for _, count in counts),
        "first_day": first_day.isoformat(),
        "last_day": last_day.isoformat(),
        "median_readings_per_day": median_readings,
        "missing_ranges": [
            [start.isoformat(), end.isoformat()]
            for start, end in _contiguous_ranges(recent_missing)
        ],
        "persistent_gap_ranges": [
            [start.isoformat(), end.isoformat()]
            for start, end in _contiguous_ranges(persistent_missing)
        ],
        "thin_days": [[day.isoformat(), count] for day, count in recent_thin],
        "fetch_from": fetch_from.isoformat(),
        "fetch_to": today.isoformat(),
    }


# %%
# Reporting #


def _print_report(reports: list[dict[str, Any]]) -> None:
    """Print a human-readable coverage report."""
    if not reports:
        print("No matching series in usage_intervals.")
        return
    for report in reports:
        print(
            f"\n{report['account_id']} / {report['metric']} / {report['granularity']}"
        )
        if not report["readings"]:
            print("  no rows stored — fetch full history")
            print(f"  FETCH: earliest available .. {report['fetch_to']}")
            continue
        print(
            f"  covered: {report['first_day']} .. {report['last_day']}"
            f"  ({report['readings']:,} readings,"
            f" median {report['median_readings_per_day']:g}/day)"
        )
        missing = report["missing_ranges"]
        if missing:
            shown = ", ".join(
                start if start == end else f"{start}..{end}" for start, end in missing[:8]
            )
            more = f" (+{len(missing) - 8} more)" if len(missing) > 8 else ""
            total = sum(
                (datetime.date.fromisoformat(end) - datetime.date.fromisoformat(start)).days + 1
                for start, end in missing
            )
            print(f"  MISSING {total} day(s) inside range: {shown}{more}")
        else:
            print("  no recent interior gaps")
        persistent = report.get("persistent_gap_ranges") or []
        if persistent:
            shown = ", ".join(
                start if start == end else f"{start}..{end}" for start, end in persistent[:8]
            )
            more = f" (+{len(persistent) - 8} more)" if len(persistent) > 8 else ""
            print(
                f"  persistent gaps (not fetched; --full to retry): {shown}{more}"
            )
        thin = report["thin_days"]
        if thin:
            shown = ", ".join(f"{day}({count})" for day, count in thin[:6])
            more = f" (+{len(thin) - 6} more)" if len(thin) > 6 else ""
            print(f"  THIN {len(thin)} day(s): {shown}{more}")
        print(f"  FETCH: {report['fetch_from']} .. {report['fetch_to']}")


# %%
# CLI #


def build_arg_parser() -> argparse.ArgumentParser:
    """Build the coverage CLI argument parser."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--provider",
        default=None,
        help="provider slug; resolves account id via providers.local.yaml",
    )
    parser.add_argument("--account", default=None, help="account id, bypassing providers.local.yaml")
    parser.add_argument("--metric", default=None, help="only this metric, e.g. production")
    parser.add_argument("--granularity", default=None, help="only this granularity, e.g. 15min")
    parser.add_argument(
        "--tz",
        default="America/Chicago",
        help="local timezone for day bucketing (default: America/Chicago)",
    )
    parser.add_argument(
        "--today",
        default=None,
        help="override today's date (YYYY-MM-DD), for testing",
    )
    parser.add_argument(
        "--max-lookback-days",
        type=int,
        default=MAX_LOOKBACK_DAYS,
        help=(
            "gaps older than this stop driving the fetch window "
            f"(default: {MAX_LOOKBACK_DAYS})"
        ),
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="ignore the lookback bound and retry every known gap",
    )
    parser.add_argument("--json", action="store_true", help="emit JSON instead of a text report")
    return parser


def main(argv: list[str] | None = None) -> int:
    """Entry point; returns a process exit code."""
    args = build_arg_parser().parse_args(argv)

    account_id = args.account
    if account_id is None and args.provider:
        account_id = account_id_for_provider(
            args.provider, _load_providers_config(PROVIDERS_YAML_PATH)
        )
        if account_id is None:
            print(
                f"No account_number for provider '{args.provider}' in providers.local.yaml",
                file=sys.stderr,
            )
            return 2

    today = (
        datetime.date.fromisoformat(args.today)
        if args.today
        else datetime.datetime.now().date()
    )

    engine = db.get_engine()
    reports = [
        analyze_series(
            engine,
            acct,
            granularity,
            metric,
            tz=args.tz,
            today=today,
            max_lookback_days=args.max_lookback_days,
            full=args.full,
        )
        for acct, granularity, metric in series_keys(engine, account_id)
        if (args.metric is None or metric == args.metric)
        and (args.granularity is None or granularity == args.granularity)
    ]

    if args.json:
        print(json.dumps(reports, indent=2))
    else:
        _print_report(reports)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
