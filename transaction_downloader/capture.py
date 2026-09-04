"""File a downloaded transaction CSV into the capture store, verbatim.

Step 2 of the acquisition workflow for any transaction source: the agent
downloads a window from the portal, then hands the file here with the window it
asked for. This module validates the file really is that provider's export,
copies it byte-for-byte into the provider's raw_dir under the canonical capture
name, and records the requested window in the manifest.

Raw-first (LANDING 1): the bytes are never rewritten, reordered, or normalized.
Nothing is ever overwritten either — every capture keeps its own capture-date
suffix, so re-fetching a month adds a file rather than replacing one, and a
restated transaction stays traceable to the day it appeared.

Usage:

    uv run python transaction_downloader/capture.py file \
        --account 7676 --start 2026-08-01 --end 2026-08-22 <download_dir>/Chase7676_Activity.CSV
    uv run python transaction_downloader/capture.py --provider citi file \
        --account 0000 --start 2026-08-01 --end 2026-08-24 "Date range.CSV"
    uv run python transaction_downloader/capture.py reindex
    uv run python transaction_downloader/capture.py status
"""

# %%
# Imports #

import argparse
import datetime
import os
import shutil
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


class TruncatedExport(Exception):
    """Raised when an export sits on its provider's row cap and is probably incomplete."""


# %%
# Config #


def _provider_entry(provider: str) -> dict[str, Any]:
    """The provider's entry from providers.local.yaml; {} when missing or unreadable."""
    user_paths.check_config_readable(PROVIDERS_YAML_PATH)
    if not os.path.isfile(PROVIDERS_YAML_PATH):
        return {}
    try:
        with open(PROVIDERS_YAML_PATH, "r", encoding="utf-8") as handle:
            loaded = yaml.safe_load(handle)
    except (OSError, yaml.YAMLError):
        return {}
    user_paths.check_not_desymlinked(PROVIDERS_YAML_PATH, loaded)
    if not isinstance(loaded, dict):
        return {}
    entry = loaded.get(provider)
    return entry if isinstance(entry, dict) else {}


def _resolve_repo_relative(path: str) -> str:
    """Expand a config path, resolving a relative one against the repo root.

    Staging lives inside the repo (data/, gitignored), so the configured value is
    relative. Resolving against the repo rather than the cwd means the tools work
    from anywhere, and nothing outside the repo is ever written.

    Goes through user_paths so a config written as `${ONEDRIVE_DOCS}/...`
    resolves to whichever machine this is running on.
    """
    return user_paths.expand_config_path(path, _REPO_ROOT)


def _load_raw_dir(override: str | None, provider: str) -> str | None:
    """Resolve raw_dir from the CLI flag or the provider's entry in providers.local.yaml."""
    if override:
        return _resolve_repo_relative(override)
    raw_dir = _provider_entry(provider).get("raw_dir")
    return _resolve_repo_relative(raw_dir) if raw_dir else None


def resolve_download(path: str, provider: str = DEFAULT_PROVIDER) -> str:
    """Resolve a download path, falling back to the configured download_dir.

    Browsers do not always save where you expect — Chrome here drops exports in
    a cloud-synced Documents folder, not ~/Downloads. Recording that once in
    providers.local.yaml means a bare filename is enough, and writing it as
    `${ONEDRIVE_DOCS}/...` keeps it correct on every machine that syncs it.
    """
    expanded = os.path.expanduser(path)
    if os.path.isfile(expanded):
        return expanded
    download_dir = _provider_entry(provider).get("download_dir")
    if download_dir and not os.path.isabs(expanded):
        candidate = os.path.join(user_paths.expand_config_path(download_dir), path)
        if os.path.isfile(candidate):
            return candidate
    return expanded


# %%
# Filing #


def _record_refetched_window(
    raw_dir: str,
    *,
    account: str,
    start: datetime.date,
    end: datetime.date,
    captured: datetime.date,
    held_file: str,
    provider: str,
) -> str | None:
    """Record that `start..end` was fetched and returned bytes already filed.

    A dormant account re-downloaded on the overlap rule comes back byte-identical,
    and `raw_documents` dedups on content_sha256 ALONE — so those bytes can never
    produce a second row, and the newly-requested window would go unrecorded no
    matter what the file were named. Coverage is rebuilt from recorded windows,
    so without this the window stays "never fetched" and the planner asks for it
    forever even though the answer is already on disk.

    Returns the marker filename, or None when this exact window is already
    recorded for this account (by a capture or a marker) and there is nothing
    new to say.
    """
    for entry in store.read_manifest(raw_dir, provider):
        if (
            str(entry.get("account")) == str(account)
            and entry.get("requested_start") == start.isoformat()
            and entry.get("requested_end") == end.isoformat()
        ):
            return None

    name = store.refetched_name(account, start, end, captured, provider)
    path = os.path.join(raw_dir, name)
    os.makedirs(raw_dir, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(
            f"{provider} served this window as an export whose bytes were already filed.\n"
            f"account (last 4): {account}\n"
            f"requested window: {start.isoformat()} .. {end.isoformat()}\n"
            f"observed: {captured.isoformat()}\n"
            f"identical to: {held_file}\n"
            "The window was fetched; its contents are recorded under that capture.\n"
        )
    store.append_manifest(
        raw_dir,
        {
            "file": name,
            "provider": provider,
            "account": account,
            "requested_start": start.isoformat(),
            "requested_end": end.isoformat(),
            "captured_at": captured.isoformat(),
            "sha256": store.sha256_file(path),
            "window_source": "requested",
            "refetched": True,
            "identical_to": held_file,
            "rows": 0,
            "min_date": None,
            "max_date": None,
        },
        provider,
    )
    return name


def file_capture(
    source_path: str,
    raw_dir: str,
    *,
    account: str,
    start: datetime.date,
    end: datetime.date,
    captured: datetime.date,
    window_source: str = "requested",
    move: bool = True,
    provider: str = DEFAULT_PROVIDER,
) -> dict[str, Any]:
    """Copy one downloaded export into raw_dir and record it. Returns the manifest entry.

    Raises store.UnrecognizedExport when the file is not a recognizable export
    for this provider, which is the guard against filing a stray download and
    silently marking a month covered by a file that holds nothing.

    Raises TruncatedExport when the file sits on the provider's row cap (Chase:
    1,000, silently truncated; Citi: no cap observed, so never raised). That
    check belongs HERE rather than in the parser: once a file is filed it is
    ingested into raw_documents, and a truncated one then fails to parse on
    every run forever. Refusing before it enters the pipeline leaves nothing to
    clean up — you just re-download a narrower window.
    """
    described = store.read_export_file(source_path, provider)
    row_cap = store.provider_def(provider)["row_cap"]
    if row_cap is not None and described["rows"] >= row_cap:
        raise TruncatedExport(
            f"{described['rows']} rows hits {provider}'s {row_cap}-row report cap, so this export "
            "is almost certainly truncated. Re-download this window in smaller pieces "
            "(try --window-months 2)."
        )

    os.makedirs(raw_dir, exist_ok=True)
    digest = store.sha256_file(source_path)

    already = [
        entry for entry in store.read_manifest(raw_dir, provider) if entry.get("sha256") == digest
    ]
    # The manifest is a cache; the filed capture is the truth. Only treat this
    # download as redundant when the capture the manifest names is STILL on disk
    # and still holds these bytes. A stale entry pointing at a deleted capture
    # must fall through and file normally — otherwise it would both suppress
    # filing and (below) discard the download, leaving the bytes stored nowhere.
    for entry in already:
        held = os.path.join(raw_dir, str(entry.get("file") or ""))
        if not os.path.isfile(held) or store.sha256_file(held) != digest:
            continue
        # A download is consumed exactly once, whether or not it turned out to
        # be new: filing MOVES it, and an identical re-download is discarded
        # here. That is what lets a successful run leave the browser's folder
        # empty, so a leftover there always means real unfiled work.
        # Archives (move=False) are the user's own kept files — never removed.
        if move:
            os.remove(source_path)
        # The bytes are already held, but the WINDOW just requested may reach
        # past anything recorded. Coverage is tracked by requested window, so
        # record the fetch or the planner re-asks for it forever.
        marker = _record_refetched_window(
            raw_dir, account=account, start=start, end=end, captured=captured,
            held_file=str(entry.get("file") or ""), provider=provider,
        )
        return {
            **entry,
            "status": "duplicate",
            "source_removed": bool(move),
            "window_recorded": marker,
        }

    name = store.capture_name(account, start, end, captured, provider)
    target = os.path.join(raw_dir, name)
    suffix = 1
    while os.path.exists(target):
        stem, ext = os.path.splitext(name)
        target = os.path.join(raw_dir, f"{stem}({suffix}){ext}")
        suffix += 1

    # Move by default: a download is pulled out of the browser's folder and into
    # repo staging exactly once, so nothing re-scans that folder on later runs.
    # Archived exports are copied instead — they are someone's kept files.
    if move:
        shutil.move(source_path, target)
    else:
        shutil.copy2(source_path, target)

    entry: dict[str, Any] = {
        "file": os.path.basename(target),
        "provider": provider,
        "account": account,
        "requested_start": start.isoformat(),
        "requested_end": end.isoformat(),
        "captured_at": captured.isoformat(),
        "sha256": digest,
        "window_source": window_source,
        "source_name": os.path.basename(source_path),
        **described,
    }
    store.append_manifest(raw_dir, entry, provider)
    return {**entry, "status": "filed"}


# %%
# Commands #


def cmd_file(args: argparse.Namespace, raw_dir: str, provider: str) -> int:
    """File one or more downloaded exports for a single requested window."""
    try:
        start = datetime.date.fromisoformat(args.start)
        end = datetime.date.fromisoformat(args.end)
    except ValueError:
        print("--start and --end must be YYYY-MM-DD", file=sys.stderr)
        return 2
    if end < start:
        print("--end is before --start", file=sys.stderr)
        return 2

    captured = datetime.date.fromisoformat(args.captured) if args.captured else datetime.date.today()

    failures = 0
    for source in args.paths:
        source = resolve_download(source, provider)
        if not os.path.isfile(source):
            print(f"  ! not found: {source}", file=sys.stderr)
            failures += 1
            continue

        account = args.account or store.account_hint_from_name(source, provider)
        if not account:
            print(
                f"  ! {os.path.basename(source)}: no account. Pass --account "
                "(the download filename carries no usable last 4).",
                file=sys.stderr,
            )
            failures += 1
            continue

        try:
            entry = file_capture(
                source, raw_dir, account=account, start=start, end=end, captured=captured,
                provider=provider,
            )
        except store.UnrecognizedExport as exc:
            print(f"  ! {os.path.basename(source)}: not a {provider} export ({exc})", file=sys.stderr)
            failures += 1
            continue
        except TruncatedExport as exc:
            print(f"  ! {os.path.basename(source)}: {exc}", file=sys.stderr)
            failures += 1
            continue
        except OSError as exc:
            print(f"  ! {os.path.basename(source)}: {exc}", file=sys.stderr)
            failures += 1
            continue

        if entry["status"] == "duplicate":
            note = ", download discarded" if entry.get("source_removed") else ""
            if entry.get("window_recorded"):
                note += f"; window {args.start}..{args.end} recorded as fetched"
            print(
                f"  = {os.path.basename(source)}: identical to {entry['file']}, "
                f"not filed again{note}"
            )
            continue
        print(
            f"  + {entry['file']}  ({entry['layout']}, {entry['rows']} row(s), "
            f"{entry['min_date'] or 'empty'} .. {entry['max_date'] or 'empty'})"
        )
        print(f"    moved out of {os.path.dirname(source) or '.'} into repo staging")

    return 1 if failures else 0


def _month_bounds(first: datetime.date, last: datetime.date) -> tuple[datetime.date, datetime.date]:
    """Expand a date span out to whole month boundaries."""
    start = first.replace(day=1)
    if last.month == 12:
        end = last.replace(day=31)
    else:
        end = last.replace(month=last.month + 1, day=1) - datetime.timedelta(days=1)
    return start, end


def cmd_import_legacy(args: argparse.Namespace, raw_dir: str, provider: str) -> int:
    """File exports downloaded before this tool existed.

    These carry no record of the window that was requested, which is the one
    thing the coverage model actually needs. The window is therefore INFERRED
    from the transaction dates inside the file and stamped `window_source:
    inferred`, so `plan.py` can report which coverage rests on an assumption.

    By default the inferred window is widened to whole month boundaries: a file
    whose first transaction is the 5th almost certainly covers the 1st-4th too,
    they were just quiet days. `--exact` uses the content dates verbatim instead,
    which is stricter but leaves every partial edge month looking uncovered.

    The capture date stamped on a legacy file is its newest transaction date,
    not today: the capture date orders restatement authority between overlapping
    captures, and an archived export must never outrank a live re-download.
    """
    failures = 0
    filed = 0
    for source in args.paths:
        source = resolve_download(source, provider)
        if not os.path.isfile(source):
            print(f"  ! not found: {source}", file=sys.stderr)
            failures += 1
            continue

        account = args.account or store.account_hint_from_name(source, provider)
        if not account:
            print(f"  ! {os.path.basename(source)}: no account; pass --account", file=sys.stderr)
            failures += 1
            continue

        try:
            described = store.read_export_file(source, provider)
        except store.UnrecognizedExport as exc:
            print(f"  ! {os.path.basename(source)}: not a {provider} export ({exc})", file=sys.stderr)
            failures += 1
            continue

        if not described["min_date"] or not described["max_date"]:
            print(
                f"  ! {os.path.basename(source)}: no dated rows, so no window can be inferred. "
                "File it with `capture.py file --start --end` if you know the window.",
                file=sys.stderr,
            )
            failures += 1
            continue

        first = datetime.date.fromisoformat(described["min_date"])
        last = datetime.date.fromisoformat(described["max_date"])
        start, end = (first, last) if args.exact else _month_bounds(first, last)

        try:
            # The capture date doubles as restatement authority: a newer capture
            # of a window supersedes an older one's rows. An archive's true
            # download date is unknown, but it cannot precede its own newest
            # transaction — stamping that date (not today) guarantees an
            # imported archive never outranks a live re-download of the same
            # window, which reflects Chase's later restatements.
            entry = file_capture(
                source, raw_dir, account=account, start=start, end=end,
                captured=last, window_source="inferred", move=False, provider=provider,
            )
        except OSError as exc:
            print(f"  ! {os.path.basename(source)}: {exc}", file=sys.stderr)
            failures += 1
            continue

        if entry["status"] == "duplicate":
            print(f"  = {os.path.basename(source)}: identical to {entry['file']}, not filed again")
            continue
        filed += 1
        print(f"  + {entry['file']}  ({entry['rows']} row(s), window inferred {start} .. {end})")

    if filed:
        print(f"\n{filed} legacy capture(s) filed with INFERRED windows.")
        print("Run `plan.py` to see which months now rest on inference.")
    return 1 if failures else 0


def cmd_record_empty(args: argparse.Namespace, raw_dir: str, provider: str) -> int:
    """Record a window the source reported as having no activity.

    Both known sources serve NO file for an empty window (Chase: "We couldn't
    find any activity..."; Citi: the export icon silently does nothing), so
    without this the coverage model cannot tell an empty month from a
    never-fetched month and the planner asks for it forever. The marker file
    records the observation; it flows through ingest like any capture so the
    database — the durable coverage source — carries it after staging is
    cleared.
    """
    try:
        start = datetime.date.fromisoformat(args.start)
        end = datetime.date.fromisoformat(args.end)
    except ValueError:
        print("--start and --end must be YYYY-MM-DD", file=sys.stderr)
        return 2
    if end < start:
        print("--end is before --start", file=sys.stderr)
        return 2
    captured = datetime.date.fromisoformat(args.captured) if args.captured else datetime.date.today()

    for entry in store.read_manifest(raw_dir, provider):
        if (
            entry.get("empty")
            and str(entry.get("account")) == args.account
            and entry.get("requested_start") == start.isoformat()
            and entry.get("requested_end") == end.isoformat()
        ):
            print(f"  = already recorded as empty: {entry['file']}")
            return 0

    name = store.empty_name(args.account, start, end, captured, provider)
    path = os.path.join(raw_dir, name)
    os.makedirs(raw_dir, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(
            f"{provider} reported no activity for the requested window.\n"
            f"account (last 4): {args.account}\n"
            f"requested window: {start.isoformat()} .. {end.isoformat()}\n"
            f"observed: {captured.isoformat()}\n"
            f'Portal message: "{store.provider_def(provider)["empty_message"]}"\n'
        )
    entry = {
        "file": name,
        "provider": provider,
        "account": args.account,
        "requested_start": start.isoformat(),
        "requested_end": end.isoformat(),
        "captured_at": captured.isoformat(),
        "sha256": store.sha256_file(path),
        "window_source": "requested",
        "empty": True,
        "rows": 0,
        "min_date": None,
        "max_date": None,
    }
    store.append_manifest(raw_dir, entry, provider)
    print(f"  + {name}  (empty window recorded)")
    return 0


def cmd_reindex(raw_dir: str, provider: str) -> int:
    """Rebuild the manifest from the capture files on disk."""
    entries = store.scan_captures(raw_dir, provider)
    store.write_manifest(raw_dir, entries, provider)
    broken = [entry for entry in entries if entry.get("error")]
    print(f"Reindexed {len(entries)} {provider} capture(s) in {raw_dir}")
    for entry in broken:
        print(f"  ! {entry['file']}: {entry['error']}", file=sys.stderr)
    return 1 if broken else 0


def cmd_status(raw_dir: str, provider: str) -> int:
    """Summarize what the capture store holds, per account."""
    entries = store.read_manifest(raw_dir, provider)
    if not entries:
        print(f"No captures recorded in {raw_dir}")
        return 0

    by_account: dict[str, list[dict[str, Any]]] = {}
    for entry in entries:
        by_account.setdefault(str(entry.get("account")), []).append(entry)

    print(f"{len(entries)} capture(s) in {raw_dir}\n")
    for account in sorted(by_account):
        group = by_account[account]
        rows = sum(int(entry.get("rows") or 0) for entry in group)
        dates = [entry.get("max_date") for entry in group if entry.get("max_date")]
        newest = max(dates) if dates else "—"
        print(f"  {account}  {len(group):>3} capture(s)  {rows:>6,} row(s)  newest txn {newest}")
    return 0


# %%
# CLI #


def build_arg_parser() -> argparse.ArgumentParser:
    """Build the capture CLI argument parser."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--provider",
        default=DEFAULT_PROVIDER,
        choices=sorted(store.PROVIDERS),
        help=f"transaction source slug (default: {DEFAULT_PROVIDER})",
    )
    parser.add_argument("--raw-dir", default=None, help="capture directory; overrides providers.local.yaml")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_file = sub.add_parser("file", help="file downloaded export(s) for one requested window")
    p_file.add_argument("paths", nargs="+", help="downloaded CSV file(s)")
    p_file.add_argument("--account", default=None, help="account last 4; inferred from the filename when omitted")
    p_file.add_argument("--start", required=True, help="requested window start, YYYY-MM-DD")
    p_file.add_argument("--end", required=True, help="requested window end, YYYY-MM-DD")
    p_file.add_argument("--captured", default=None, help="capture date override, YYYY-MM-DD (testing)")

    p_legacy = sub.add_parser(
        "import-legacy", help="file pre-existing Chase exports, inferring their window"
    )
    p_legacy.add_argument("paths", nargs="+", help="previously-downloaded Chase CSV file(s)")
    p_legacy.add_argument("--account", default=None, help="account last 4; inferred from the filename when omitted")
    p_legacy.add_argument(
        "--exact",
        action="store_true",
        help="use the file's own first/last transaction dates instead of whole months",
    )

    p_empty = sub.add_parser(
        "record-empty", help="record a window Chase reported as having no activity"
    )
    p_empty.add_argument("--account", required=True, help="account last 4")
    p_empty.add_argument("--start", required=True, help="requested window start, YYYY-MM-DD")
    p_empty.add_argument("--end", required=True, help="requested window end, YYYY-MM-DD")
    p_empty.add_argument("--captured", default=None, help="observation date override, YYYY-MM-DD (testing)")

    sub.add_parser("reindex", help="rebuild the manifest from files on disk")
    sub.add_parser("status", help="summarize the capture store")
    return parser


def main(argv: list[str] | None = None) -> int:
    """Entry point; returns a process exit code."""
    args = build_arg_parser().parse_args(argv)

    raw_dir = _load_raw_dir(args.raw_dir, args.provider)
    if not raw_dir:
        print(
            f"No raw_dir for provider '{args.provider}'. Add a `{args.provider}` entry to "
            "providers.local.yaml (shape in template_providers.yaml) or pass --raw-dir.",
            file=sys.stderr,
        )
        return 2

    if args.cmd == "file":
        return cmd_file(args, raw_dir, args.provider)
    if args.cmd == "import-legacy":
        return cmd_import_legacy(args, raw_dir, args.provider)
    if args.cmd == "record-empty":
        return cmd_record_empty(args, raw_dir, args.provider)
    if args.cmd == "reindex":
        return cmd_reindex(raw_dir, args.provider)
    return cmd_status(raw_dir, args.provider)


if __name__ == "__main__":
    raise SystemExit(main())


# %%
