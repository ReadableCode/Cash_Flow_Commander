"""Report transaction coverage and emit the windows to download.

Step 1 of the acquisition workflow for any transaction source (--provider,
default chase), the transaction-shaped analogue of `src/coverage.py`. It
answers, per account: which months are already captured, where the holes are,
and therefore exactly which date windows this run should ask the portal for.

Why transactions need their own planner rather than reusing coverage.py: that
tool infers gaps from *missing readings*, which works because an interval series
emits a fixed number of readings per day. Transactions have no such cadence — a
card can legitimately go a week with no activity, so "no rows for that day" says
nothing. Coverage here is therefore tracked by *requested window*, recorded at
capture time, not by what came back. A month fetched and found empty is covered;
a month never fetched is not.

Two rules drive the window (both lean on overlap being free — see LANDING 0.1):

- **The current month is always re-fetched.** It is by definition incomplete.
- **So is everything back to `newest transaction - overlap-days`.** Chase posts
  pending transactions late, so the newest days already stored are the least
  trustworthy ones — the same reasoning as RESTATEMENT_LOOKBACK_DAYS in
  coverage.py, expressed in transaction time.

Windows come out month-aligned so each capture files as one clean partition.

Usage:

    uv run python transaction_downloader/plan.py
    uv run python transaction_downloader/plan.py --json
    uv run python transaction_downloader/plan.py --full
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

_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
if _THIS_DIR not in sys.path:
    sys.path.insert(0, _THIS_DIR)
_SRC_DIR = os.path.join(os.path.dirname(_THIS_DIR), "src")
if _SRC_DIR not in sys.path:
    sys.path.insert(0, _SRC_DIR)

import store  # noqa: E402
import user_paths  # noqa: E402

# %%
# Constants #

_REPO_ROOT = os.path.dirname(_THIS_DIR)
PROVIDERS_YAML_PATH = os.path.join(_REPO_ROOT, "providers.local.yaml")

DEFAULT_PROVIDER = store.DEFAULT_PROVIDER

# Re-fetch this many days behind the newest stored transaction, on top of the
# current month. Portals post pending activity late and revise descriptions and
# amounts after the fact, so the newest days already stored are the least
# trustworthy ones. Cheap insurance: overlap dedups at ingest.
OVERLAP_DAYS = 5

# How far back each source's export reaches lives in store.PROVIDERS
# ("retention_months"), with the per-source verification notes. This is a HARD
# limit, not a preference: months older than the floor cannot be fetched from
# the portal at all; they are reported as unreachable so they can be filled
# from an archive of previously-downloaded exports instead. Chase's floor is a
# rolling daily one; Citi's is a statement-cycle boundary approximated
# conservatively by the same rolling computation.
RETENTION_MONTHS = store.provider_def(DEFAULT_PROVIDER)["retention_months"]

DEFAULT_ACCOUNT_KIND = "bank"

# With nothing stored and no backfill_start configured, go back this far rather
# than guessing at account-opening dates.
DEFAULT_BACKFILL_MONTHS = 24

# Months per download window. 1 keeps each capture to a clean month, which is
# right for incremental runs. A first backfill is far cheaper in wide windows:
# 24 months of one account is ~2-6 downloads instead of 25. The ceiling is
# Chase's 1,000-row report cap, which truncates SILENTLY -- so pick a width that
# keeps a window under it. src/providers/chase.py refuses any file that comes
# back with exactly 1,000 rows, so guessing too wide fails loudly, not quietly.
DEFAULT_WINDOW_MONTHS = "auto"

# Rows to aim for per download, against Chase's hard 1,000-row cap. The margin
# absorbs a busier-than-usual month; overshooting means a refused download and a
# re-pull, which is cheap, while the cap itself truncates silently.
TARGET_ROWS_PER_WINDOW = 700

# Never merge more than this into one window, however quiet an account looks —
# an unbounded window would make one refusal expensive to narrow.
MAX_WINDOW_MONTHS = 12

# Window to use for an account with nothing stored, where density is unknown.
# Deliberately cautious: the cost of guessing small is one extra download.
UNKNOWN_DENSITY_WINDOW_MONTHS = 3

# Most month-windows one run will ask for. A first run against years of history
# would otherwise emit a hundred downloads and stall halfway; the remainder is
# reported as deferred and picked up by the next run, newest first.
MAX_MONTHS_PER_RUN = 24


# %%
# Config #


def _load_providers_config(path: str) -> dict[str, Any]:
    """Load providers.local.yaml; {} when missing, unreadable, or not a mapping."""
    user_paths.check_config_readable(path)
    if not os.path.isfile(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as handle:
            loaded = yaml.safe_load(handle)
    except (OSError, yaml.YAMLError):
        return {}
    user_paths.check_not_desymlinked(path, loaded)
    return loaded if isinstance(loaded, dict) else {}


def _resolve_repo_relative(path: str) -> str:
    """Expand a config path, resolving a relative one against the repo root.

    Goes through user_paths so a config written as `${ONEDRIVE_DOCS}/...`
    resolves to whichever machine this is running on.
    """
    return user_paths.expand_config_path(path, _REPO_ROOT)


def provider_entry(config: dict[str, Any], provider: str = DEFAULT_PROVIDER) -> dict[str, Any]:
    """The provider's entry from providers.local.yaml; {} when absent."""
    entry = config.get(provider)
    return entry if isinstance(entry, dict) else {}


def configured_accounts(entry: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Map of account last-4 -> {label, kind}, from external_ids.accounts.

    Two config shapes are accepted, because the kind only started mattering once
    the retention floor turned out to differ between banks and cards:

        accounts: {"0000": "Checking"}                        # label only
        accounts: {"0000": {label: Checking, kind: bank}}     # explicit kind

    A bare label defaults to the bank kind, which is the conservative choice:
    it assumes the 24-month floor rather than planning windows Chase will refuse.
    """
    external = entry.get("external_ids")
    if not isinstance(external, dict):
        return {}
    accounts = external.get("accounts")
    if not isinstance(accounts, dict):
        return {}

    out: dict[str, dict[str, Any]] = {}
    for key, value in accounts.items():
        if isinstance(value, dict):
            out[str(key)] = {
                "label": str(value.get("label") or key),
                "kind": str(value.get("kind") or DEFAULT_ACCOUNT_KIND),
            }
        else:
            out[str(key)] = {"label": str(value), "kind": DEFAULT_ACCOUNT_KIND}
    return out


def retention_floor(today: datetime.date, months: int = RETENTION_MONTHS) -> datetime.date:
    """Earliest date the portal will export: exactly `months` back, plus one day.

    Day precision matters. Chase refuses a From equal to the boundary date and
    accepts the day after, so a month-aligned floor would generate windows whose
    first days are rejected.
    """
    year = today.year - months // 12
    month = today.month - months % 12
    if month <= 0:
        year -= 1
        month += 12
    day = min(today.day, _days_in_month(year, month))
    return datetime.date(year, month, day) + datetime.timedelta(days=1)


def _days_in_month(year: int, month: int) -> int:
    """Number of days in a month, so a Feb 29 anniversary does not explode."""
    if month == 12:
        return 31
    return (datetime.date(year, month + 1, 1) - datetime.timedelta(days=1)).day


# %%
# Coverage source #


def captures_from_database(provider: str = DEFAULT_PROVIDER) -> list[dict[str, Any]] | None:
    """Rebuild coverage from raw_documents, or None when the database is unreachable.

    This is the durable source. On-disk staging is transient by design — once a
    capture is ingested, `raw_documents` holds the bytes and the file can be
    deleted — so coverage must not depend on the file still being there.

    The requested window is recoverable because it is encoded in the capture
    filename, which ingest stores as `original_name`.
    """
    src_dir = os.path.join(_REPO_ROOT, "src")
    if src_dir not in sys.path:
        sys.path.insert(0, src_dir)
    try:
        import db  # noqa: PLC0415 - optional dependency of this tool
        from sqlalchemy import func, select  # noqa: PLC0415
    except ImportError:
        return None

    try:
        engine = db.get_engine()
        with engine.connect() as conn:
            names = [
                row[0]
                for row in conn.execute(
                    select(db.raw_documents.c.original_name).where(
                        db.raw_documents.c.provider == provider,
                        # Both marker kinds are coverage evidence, for the
                        # same reason: neither fetch can leave a csv_export row
                        # behind. empty_window records a window the portal
                        # served no file for; refetched_window records one it
                        # served bytes already filed, which content_sha256
                        # dedup then collapses onto the existing row.
                        db.raw_documents.c.doc_type.in_(
                            ("csv_export", "empty_window", "refetched_window")
                        ),
                    )
                )
            ]
            stats = {
                str(row[0]): {
                    "rows": int(row[1]),
                    "min_date": row[2].isoformat() if row[2] else None,
                    "max_date": row[3].isoformat() if row[3] else None,
                }
                for row in conn.execute(
                    select(
                        db.transactions.c.account_id,
                        func.count(),
                        func.min(db.transactions.c.post_date),
                        func.max(db.transactions.c.post_date),
                    ).group_by(db.transactions.c.account_id)
                )
            }
    except Exception:  # noqa: BLE001 - no database is a fallback, not a failure
        return None

    captures: list[dict[str, Any]] = []
    seen_accounts: set[str] = set()
    for name in names:
        meta = store.parse_capture_name(name)
        if meta is None:
            continue
        entry = dict(meta)
        entry["file"] = name
        account = str(entry["account"])
        # Row counts and the transaction span are per-account facts, so attach
        # them to the account's first capture only; the analyzer sums rows and
        # takes min/max across captures.
        if account not in seen_accounts:
            seen_accounts.add(account)
            entry.update(stats.get(account, {"rows": 0, "min_date": None, "max_date": None}))
        else:
            entry.update({"rows": 0, "min_date": None, "max_date": None})
        captures.append(entry)
    return captures


# %%
# Date helpers #


def month_start(day: datetime.date) -> datetime.date:
    """First day of the month containing `day`."""
    return day.replace(day=1)


def month_end(day: datetime.date) -> datetime.date:
    """Last day of the month containing `day`."""
    if day.month == 12:
        return day.replace(day=31)
    return day.replace(month=day.month + 1, day=1) - datetime.timedelta(days=1)


def add_months(day: datetime.date, count: int) -> datetime.date:
    """Shift a date by whole months, anchored to the first of the month."""
    total = (day.year * 12 + day.month - 1) + count
    return datetime.date(total // 12, total % 12 + 1, 1)


def months_between(first: datetime.date, last: datetime.date) -> list[datetime.date]:
    """Every month start from `first`'s month through `last`'s month, inclusive."""
    out: list[datetime.date] = []
    cursor = month_start(first)
    limit = month_start(last)
    while cursor <= limit:
        out.append(cursor)
        cursor = add_months(cursor, 1)
    return out


def merge_ranges(
    ranges: list[tuple[datetime.date, datetime.date]]
) -> list[tuple[datetime.date, datetime.date]]:
    """Merge overlapping or adjacent inclusive date ranges into a minimal set."""
    if not ranges:
        return []
    ordered = sorted(ranges)
    merged = [ordered[0]]
    for start, end in ordered[1:]:
        last_start, last_end = merged[-1]
        if start <= last_end + datetime.timedelta(days=1):
            merged[-1] = (last_start, max(last_end, end))
        else:
            merged.append((start, end))
    return merged


def month_is_covered(
    month: datetime.date,
    covered: list[tuple[datetime.date, datetime.date]],
    today: datetime.date,
    floor: datetime.date | None = None,
) -> bool:
    """True when every fetchable day of `month` up to today falls inside a covered range.

    The current month is judged only through today — a month cannot be faulted
    for missing days that have not happened yet. Likewise days before the
    retention `floor`: Chase will never serve them, so a capture that covers a
    month from the floor onward covers everything that month can ever hold —
    without the clip, the floor month re-reports as a gap forever.
    """
    need_start = month if floor is None else max(month, floor)
    need_end = min(month_end(month), today)
    if need_end < need_start:
        return False
    for start, end in covered:
        if start <= need_start and end >= need_end:
            return True
    return False


# %%
# Analysis #


def auto_window_months(rows: int, covered: list[tuple[datetime.date, datetime.date]]) -> int:
    """Choose a window width from how busy this account actually is.

    A quiet savings account can be pulled a year at a time; a busy card cannot.
    Rather than making that a flag someone has to reason about, measure it: rows
    already held, divided by the months they span, gives rows per month, and the
    window is whatever keeps a download under TARGET_ROWS_PER_WINDOW.

    This is what makes catching up after a long gap the same operation as a
    routine monthly run — the width adapts, the command does not change.
    """
    if not rows or not covered:
        return UNKNOWN_DENSITY_WINDOW_MONTHS
    days = sum((end - start).days + 1 for start, end in covered)
    if days <= 0:
        return UNKNOWN_DENSITY_WINDOW_MONTHS
    per_month = rows / (days / 30.44)
    if per_month <= 0:
        return MAX_WINDOW_MONTHS
    return max(1, min(MAX_WINDOW_MONTHS, int(TARGET_ROWS_PER_WINDOW / per_month)))


def _chunk_months(
    months: list[datetime.date], window_months: int
) -> list[list[datetime.date]]:
    """Group newest-first months into runs of consecutive months, capped at window_months.

    Only genuinely adjacent months merge. A gap month sitting on its own stays
    its own window rather than dragging in everything between it and the next
    wanted month, which would turn one hole into a year-wide download.
    """
    if window_months < 1:
        window_months = 1
    chunks: list[list[datetime.date]] = []
    for month in months:  # newest first
        if (
            chunks
            and len(chunks[-1]) < window_months
            and add_months(month, 1) == chunks[-1][-1]
        ):
            chunks[-1].append(month)
        else:
            chunks.append([month])
    return chunks


def _combined_reason(reasons: list[str]) -> str:
    """Describe why a merged window is being fetched."""
    unique = list(dict.fromkeys(reasons))
    if len(unique) == 1:
        return unique[0]
    return f"{unique[0]} +{len(unique) - 1} other reason(s)"


def analyze_account(
    account: str,
    captures: list[dict[str, Any]],
    *,
    today: datetime.date,
    label: str | None = None,
    kind: str = DEFAULT_ACCOUNT_KIND,
    overlap_days: int = OVERLAP_DAYS,
    backfill_start: datetime.date | None = None,
    max_months: int = MAX_MONTHS_PER_RUN,
    window_months: int | str = DEFAULT_WINDOW_MONTHS,
    full: bool = False,
    retention_months: int = RETENTION_MONTHS,
) -> dict[str, Any]:
    """Describe one account's coverage and the month windows worth downloading.

    Returns a report whose `requests` list is what the acquisition agent acts on,
    newest month first so an interrupted run still made the most valuable
    downloads.
    """
    covered = merge_ranges(
        [
            (
                datetime.date.fromisoformat(capture["requested_start"]),
                datetime.date.fromisoformat(capture["requested_end"]),
            )
            for capture in captures
            if capture.get("requested_start") and capture.get("requested_end")
        ]
    )

    inferred_ranges = merge_ranges(
        [
            (
                datetime.date.fromisoformat(capture["requested_start"]),
                datetime.date.fromisoformat(capture["requested_end"]),
            )
            for capture in captures
            if capture.get("window_source") == "inferred"
            and capture.get("requested_start")
            and capture.get("requested_end")
        ]
    )

    txn_dates = [capture["max_date"] for capture in captures if capture.get("max_date")]
    min_dates = [capture["min_date"] for capture in captures if capture.get("min_date")]
    newest_txn = max((datetime.date.fromisoformat(day) for day in txn_dates), default=None)
    oldest_txn = min((datetime.date.fromisoformat(day) for day in min_dates), default=None)
    total_rows = sum(int(capture.get("rows") or 0) for capture in captures)

    # Where history starts for this account. An explicit backfill_start wins;
    # otherwise anchor on what we already hold, and fall back to a bounded
    # lookback when there is nothing at all.
    if backfill_start is not None:
        horizon = month_start(backfill_start)
    elif covered:
        horizon = month_start(covered[0][0])
    else:
        horizon = add_months(month_start(today), -DEFAULT_BACKFILL_MONTHS)

    # Two always-refetch rules, kept deliberately separate:
    #
    #   1. the current month, which is incomplete by definition
    #   2. the tail of what we already hold -- the days from
    #      (newest transaction - overlap) THROUGH the newest transaction --
    #      because Chase restates recent activity after it posts
    #
    # Rule 2 is bounded by the newest transaction, NOT extended to today. An
    # earlier version ran it through to today, so an account whose newest stored
    # transaction was two years old reported two years of months as "refresh
    # window". Those months are simply uncovered, and saying otherwise hides a
    # real gap behind a routine-sounding label.
    refresh_months: set[datetime.date] = {month_start(today)}
    if newest_txn is not None:
        refresh_months.update(
            months_between(newest_txn - datetime.timedelta(days=overlap_days), newest_txn)
        )

    floor = retention_floor(today, retention_months)
    floor_month = month_start(floor)

    reasons: dict[datetime.date, str] = {}
    for month in months_between(horizon, today):
        if month in refresh_months:
            reasons[month] = (
                "current month"
                if month == month_start(today)
                else f"refresh window (newest txn -{overlap_days}d)"
            )
        elif not month_is_covered(month, covered, today, floor=floor):
            reasons[month] = "never captured" if not covered else "gap"

    # Split off what Chase will not serve at all before doing anything else --
    # a month past the retention floor is not deferred work, it is work that has
    # to come from an archive of older exports instead.
    resolved_window = (
        auto_window_months(total_rows, covered)
        if window_months == "auto"
        else max(1, int(window_months))
    )
    wanted_all = sorted(reasons, reverse=True)
    # A month is reachable when any part of it is at or after the floor; the
    # month the floor falls inside is reachable but only from the floor onward.
    wanted = [month for month in wanted_all if month >= floor_month]
    unreachable = [month for month in wanted_all if month < floor_month]

    # Everything from here is ordering and bounding: newest first, then trim.
    selected = wanted if full else wanted[:max_months]
    deferred = [] if full else wanted[max_months:]

    requests = [
        {
            "account": account,
            "label": label or account,
            "month": chunk[-1].strftime("%Y-%m"),
            "months": [m.strftime("%Y-%m") for m in reversed(chunk)],
            "start": max(chunk[-1], floor).isoformat(),
            "end": min(month_end(chunk[0]), today).isoformat(),
            "reason": _combined_reason([reasons[m] for m in chunk]),
        }
        for chunk in _chunk_months(selected, resolved_window)
    ]

    # Months whose only evidence of coverage is a window we guessed at.
    inferred_months = [
        month.strftime("%Y-%m")
        for month in months_between(horizon, today)
        if month not in reasons and month_is_covered(month, inferred_ranges, today, floor=floor)
    ]

    return {
        "account": account,
        "label": label or account,
        "kind": kind,
        "window_months": resolved_window,
        "retention_floor": floor.isoformat(),
        "captures": len(captures),
        "rows": total_rows,
        "oldest_transaction": oldest_txn.isoformat() if oldest_txn else None,
        "newest_transaction": newest_txn.isoformat() if newest_txn else None,
        "covered_ranges": [[start.isoformat(), end.isoformat()] for start, end in covered],
        "requests": requests,
        "deferred_months": [month.strftime("%Y-%m") for month in deferred],
        "unreachable_months": [month.strftime("%Y-%m") for month in unreachable],
        "inferred_months": sorted(inferred_months, reverse=True),
    }


def analyze_all(
    captures: list[dict[str, Any]],
    accounts: dict[str, dict[str, Any]],
    *,
    today: datetime.date,
    **kwargs: Any,
) -> list[dict[str, Any]]:
    """Analyze every configured account plus any account found only on disk."""
    by_account: dict[str, list[dict[str, Any]]] = {name: [] for name in accounts}
    for capture in captures:
        by_account.setdefault(str(capture.get("account")), []).append(capture)

    reports = []
    for account in sorted(by_account):
        config = accounts.get(account) or {}
        if isinstance(config, str):  # bare label, same leniency as configured_accounts
            config = {"label": config, "kind": DEFAULT_ACCOUNT_KIND}
        reports.append(
            analyze_account(
                account,
                by_account[account],
                today=today,
                label=config.get("label"),
                kind=str(config.get("kind") or DEFAULT_ACCOUNT_KIND),
                **kwargs,
            )
        )
    return reports


# %%
# Reporting #


def _print_report(reports: list[dict[str, Any]], today: datetime.date, provider: str) -> None:
    """Print a human-readable coverage-and-plan report."""
    if not reports:
        print(f"No {provider} accounts configured and no captures on disk.")
        print(f"Add a `{provider}` entry to providers.local.yaml (see template_providers.yaml).")
        return

    total_requests = 0
    for report in reports:
        print(
            f"\n{report['label']}  (account {report['account']})"
            f"   [windows of {report['window_months']} month(s)]"
        )
        if not report["captures"]:
            print("  nothing captured yet")
        else:
            print(
                f"  {report['captures']} capture(s), {report['rows']:,} transaction row(s), "
                f"{report['oldest_transaction']} .. {report['newest_transaction']}"
            )
            covered = report["covered_ranges"]
            shown = ", ".join(f"{start}..{end}" for start, end in covered[:4])
            more = f" (+{len(covered) - 4} more)" if len(covered) > 4 else ""
            print(f"  covered windows: {shown or '(none)'}{more}")

        for request in report["requests"]:
            total_requests += 1
            span = f" ({len(request['months'])} months)" if len(request["months"]) > 1 else ""
            print(f"  FETCH {request['start']} .. {request['end']}{span}   [{request['reason']}]")
        if not report["requests"]:
            print("  nothing to fetch")
        if report["deferred_months"]:
            months = report["deferred_months"]
            print(f"  deferred to a later run ({len(months)}): {months[0]} .. {months[-1]}")
        if report["inferred_months"]:
            months = report["inferred_months"]
            print(
                f"  covered by imported archives, window inferred ({len(months)}): "
                f"{months[-1]} .. {months[0]}"
            )
        if report["unreachable_months"]:
            months = report["unreachable_months"]
            print(
                f"  BEYOND {provider.upper()} RETENTION ({len(months)}): {months[-1]} .. {months[0]}"
                f"  (floor {report['retention_floor']})"
            )
            print(f"    ^ {provider} will not export these. Fill them from archived exports:")
            print(f"      capture.py --provider {provider} import-legacy <files>")

    print(f"\n{total_requests} window(s) to download, as of {today.isoformat()}.")


# %%
# CLI #


def build_arg_parser() -> argparse.ArgumentParser:
    """Build the plan CLI argument parser."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--provider",
        default=DEFAULT_PROVIDER,
        choices=sorted(store.PROVIDERS),
        help=f"transaction source slug (default: {DEFAULT_PROVIDER})",
    )
    parser.add_argument("--raw-dir", default=None, help="capture directory; overrides providers.local.yaml")
    parser.add_argument("--account", default=None, help="only this account (last 4 digits)")
    parser.add_argument(
        "--kind",
        default=DEFAULT_ACCOUNT_KIND,
        choices=("bank", "credit"),
        help="account kind for an unconfigured --account; sets the retention floor",
    )
    parser.add_argument("--today", default=None, help="override today's date (YYYY-MM-DD), for testing")
    parser.add_argument(
        "--overlap-days",
        type=int,
        default=OVERLAP_DAYS,
        help=f"re-fetch this far behind the newest stored transaction (default: {OVERLAP_DAYS})",
    )
    parser.add_argument(
        "--backfill-start",
        default=None,
        help="earliest month to consider, YYYY-MM or YYYY-MM-DD; overrides providers.local.yaml",
    )
    parser.add_argument(
        "--max-months",
        type=int,
        default=MAX_MONTHS_PER_RUN,
        help=f"cap month-windows emitted per run (default: {MAX_MONTHS_PER_RUN})",
    )
    parser.add_argument(
        "--window-months",
        default=DEFAULT_WINDOW_MONTHS,
        help=(
            "months per download window. Default 'auto' sizes each account from its own "
            "observed rows-per-month so a download stays under Chase's 1,000-row cap; "
            "pass an integer to override"
        ),
    )
    parser.add_argument("--full", action="store_true", help="emit every wanted month, ignoring --max-months")
    parser.add_argument(
        "--from-disk",
        action="store_true",
        help="read coverage from staging on disk instead of the database",
    )
    parser.add_argument(
        "--rescan",
        action="store_true",
        help="rebuild coverage by reading every capture file instead of trusting the manifest",
    )
    parser.add_argument("--json", action="store_true", help="emit JSON instead of a text report")
    return parser


def _parse_backfill(raw: str | None) -> datetime.date | None:
    """Accept YYYY-MM or YYYY-MM-DD; None when unset."""
    if not raw:
        return None
    text = str(raw).strip()
    if len(text) == 7:
        text += "-01"
    return datetime.date.fromisoformat(text)


def main(argv: list[str] | None = None) -> int:
    """Entry point; returns a process exit code."""
    args = build_arg_parser().parse_args(argv)

    provider = args.provider
    entry = provider_entry(_load_providers_config(PROVIDERS_YAML_PATH), provider)
    raw_dir = args.raw_dir or entry.get("raw_dir") or ""
    if not raw_dir:
        print(
            f"No raw_dir for provider '{provider}'. Add a `{provider}` entry to "
            "providers.local.yaml (shape in template_providers.yaml) or pass --raw-dir.",
            file=sys.stderr,
        )
        return 2
    raw_dir = _resolve_repo_relative(raw_dir)

    # The database is the durable record: staging is cleared once ingested, so
    # asking the disk alone would report months as missing that are safely
    # landed. Disk is the fallback for a first run, or when there is no database.
    source = "database"
    captures = None if args.from_disk else captures_from_database(provider)
    if captures is None:
        source = "disk"
        captures = (
            store.scan_captures(raw_dir, provider)
            if args.rescan
            else store.read_manifest(raw_dir, provider)
        )
        if not captures and not args.rescan:
            captures = store.scan_captures(raw_dir, provider)

    accounts = configured_accounts(entry)
    if args.account:
        accounts = {args.account: accounts.get(args.account, {"label": args.account, "kind": args.kind})}
        captures = [c for c in captures if str(c.get("account")) == args.account]

    today = datetime.date.fromisoformat(args.today) if args.today else datetime.date.today()

    try:
        backfill_start = _parse_backfill(args.backfill_start or entry.get("backfill_start"))
    except ValueError:
        print("--backfill-start must be YYYY-MM or YYYY-MM-DD", file=sys.stderr)
        return 2

    reports = analyze_all(
        captures,
        accounts,
        today=today,
        overlap_days=args.overlap_days,
        backfill_start=backfill_start,
        max_months=args.max_months,
        window_months=args.window_months if args.window_months == "auto" else int(args.window_months),
        full=args.full,
        retention_months=store.provider_def(provider)["retention_months"],
    )

    if args.json:
        print(json.dumps(
            {
                "today": today.isoformat(),
                "provider": provider,
                "coverage_source": source,
                "raw_dir": raw_dir,
                "accounts": reports,
            },
            indent=2,
        ))
    else:
        print(f"coverage from: {source}")
        _print_report(reports, today, provider)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


# %%
