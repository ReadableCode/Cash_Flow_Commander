"""Tell a provider run which files in the browser's download folder are its own.

Every portal command downloads into the same folder, and most portals name
their export without saying whose it is (`Date range.CSV`, `IntervalData.csv`).
"The newest file since I clicked" is only true while one run is downloading.
With two runs at once it hands one run the other's file, and the mistake is
quiet: the file is filed and ingested under the wrong provider.

A run claims a download on two facts together: the file is newer than a marker
taken just before the click, and its name has the shape this provider's portal
gives its downloads. The shapes do not overlap between providers, so runs can
download at the same time. A new file whose name fits no provider is reported,
never guessed at: that is a portal that changed its naming, and the pattern
here needs the new shape.

Usage:

    uv run python src/downloads.py mark
    uv run python src/downloads.py claim --provider elan --since <marker>
    uv run python src/downloads.py leftovers --provider elan
    uv run python src/downloads.py arrived --provider newbank --since <marker>

`mark` prints the marker. `claim` waits for the download to finish and prints
its path; it exits 1 when nothing of this provider's arrived and 2 when more
files arrived than expected. `leftovers` lists every file of this provider's
still in the download folder, whenever it landed, and exits 1 when there is
one: a clean run leaves none. `arrived` lists everything newer than the marker
with whose it is, for learning how a new portal names its downloads before it
has a pattern here.
"""

# %%
# Imports #

import argparse
import os
import re
import sys
import time
from typing import Any

import yaml

_SRC_DIR = os.path.dirname(os.path.abspath(__file__))
if _SRC_DIR not in sys.path:
    sys.path.insert(0, _SRC_DIR)

import user_paths  # noqa: E402

# %%
# Constants #

_REPO_ROOT = os.path.dirname(_SRC_DIR)
PROVIDERS_YAML_PATH = os.path.join(_REPO_ROOT, "providers.local.yaml")

# Chrome appends ` (1)` when the name is taken.
_DUP = r"(?: \(\d+\))?"

# The name each provider's download arrives under. Matched against the whole
# basename, case-insensitively. A portal names its own exports; where a command
# saves a response itself (Blob + a.download), and once a capture is renamed
# for filing, the name starts with the provider's slug.
DOWNLOAD_NAME_PATTERNS: dict[str, tuple[str, ...]] = {
    # `Chase1234_Activity_20260822.CSV`, `Chase1234_Activity20260701_20260822_20260822.CSV`
    "chase": (r"chase\d{4}_activity.*\.csv", r"chase_.+"),
    # The scope label, never the account or the window.
    "citi": (rf"date range{_DUP}\.csv", r"since .+\.csv", r"citi_.+"),
    # `<account label> - <last4>_<MM-DD-YYYY>_<MM-DD-YYYY>.csv`
    "elan": (rf".+ - \d{{4}}_\d{{2}}-\d{{2}}-\d{{4}}_\d{{2}}-\d{{2}}-\d{{4}}{_DUP}\.csv", r"elan_.+"),
    "smt": (rf"intervaldata{_DUP}\.csv", r"smt_.+"),
    "rhythm": (r"rhythm_.+",),
    "gexa": (r"gexa_.+",),
    "enphase_enlighten": (r"enphase_enlighten_.+",),
}

# A download Chrome has not finished writing.
_PARTIAL_SUFFIXES = (".crdownload", ".download", ".part", ".tmp")

DEFAULT_TIMEOUT_SECONDS = 30.0
POLL_SECONDS = 0.5


# %%
# Config #


def _provider_entry(provider: str) -> dict[str, Any]:
    """The provider's entry from providers.local.yaml; {} when missing."""
    user_paths.check_config_readable(PROVIDERS_YAML_PATH)
    if not os.path.isfile(PROVIDERS_YAML_PATH):
        return {}
    with open(PROVIDERS_YAML_PATH, "r", encoding="utf-8") as handle:
        loaded = yaml.safe_load(handle)
    user_paths.check_not_desymlinked(PROVIDERS_YAML_PATH, loaded)
    if not isinstance(loaded, dict):
        return {}
    entry = loaded.get(provider)
    return entry if isinstance(entry, dict) else {}


def download_dir_for(provider: str) -> str:
    """The provider's download_dir, expanded for this machine. Raises when unset."""
    configured = _provider_entry(provider).get("download_dir")
    if not configured:
        raise user_paths.SetupIncomplete(
            f"no download_dir for provider '{provider}' in providers.local.yaml. "
            + user_paths.setup_hint(provider)
        )
    return user_paths.expand_config_path(str(configured), _REPO_ROOT)


# %%
# Matching #


def owner_of(name: str) -> str | None:
    """The provider whose portal names a download like this, or None."""
    for provider, patterns in DOWNLOAD_NAME_PATTERNS.items():
        if any(re.fullmatch(pattern, name, re.IGNORECASE) for pattern in patterns):
            return provider
    return None


def _is_partial(name: str) -> bool:
    return name.lower().endswith(_PARTIAL_SUFFIXES)


def _files(download_dir: str) -> list[tuple[str, float, int]]:
    """(name, landed, size) for every visible file directly in the folder.

    landed is the later of the file's creation and modification times. Chrome
    stamps a download's modification time from the server's Last-Modified
    header when there is one, which can be long before the click; the creation
    time is always the click.
    """
    out: list[tuple[str, float, int]] = []
    for name in os.listdir(download_dir):
        if name.startswith("."):
            continue
        path = os.path.join(download_dir, name)
        try:
            stat_result = os.stat(path)
        except OSError:
            continue  # renamed away between the listing and the stat
        if os.path.isfile(path):
            landed = max(stat_result.st_mtime, getattr(stat_result, "st_birthtime", 0.0))
            out.append((name, landed, stat_result.st_size))
    return out


def look(download_dir: str, provider: str, since: float) -> dict[str, list[tuple[str, float, int]]]:
    """Sort the files newer than the marker into mine, theirs, unknown and partial."""
    found: dict[str, list[tuple[str, float, int]]] = {"mine": [], "theirs": [], "unknown": [], "partial": []}
    for entry in _files(download_dir):
        name, landed, _ = entry
        if landed <= since:
            continue
        if _is_partial(name):
            found["partial"].append(entry)
            continue
        owner = owner_of(name)
        found["mine" if owner == provider else "unknown" if owner is None else "theirs"].append(entry)
    for entries in found.values():
        entries.sort(key=lambda entry: (entry[1], entry[0]))
    return found


def claim(
    download_dir: str,
    provider: str,
    since: float,
    expect: int = 1,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
) -> tuple[list[str], dict[str, list[tuple[str, float, int]]]]:
    """Wait for this provider's download(s) newer than the marker; return their names.

    Done means `expect` of them are present, none is still growing and no
    partial download is in flight. Returns whatever is there at the timeout,
    which may be fewer, plus the last look for the caller to report from.
    """
    deadline = time.monotonic() + timeout
    previous: list[tuple[str, float, int]] = []
    while True:
        found = look(download_dir, provider, since)
        mine = found["mine"]
        settled = mine == previous and not found["partial"]
        if len(mine) >= expect and settled:
            return [name for name, _, _ in mine], found
        if time.monotonic() >= deadline:
            return [name for name, _, _ in mine], found
        previous = mine
        time.sleep(POLL_SECONDS)


def arrived(download_dir: str, since: float) -> list[tuple[str, str | None]]:
    """(name, owner) for every finished file newer than the marker, oldest first."""
    entries = sorted(
        (entry for entry in _files(download_dir) if entry[1] > since and not _is_partial(entry[0])),
        key=lambda entry: (entry[1], entry[0]),
    )
    return [(name, owner_of(name)) for name, _, _ in entries]


def leftovers(download_dir: str, provider: str) -> list[str]:
    """Every file of this provider's in the download folder, whenever it landed."""
    return sorted(name for name, _, _ in _files(download_dir) if owner_of(name) == provider)


# %%
# CLI #


def build_arg_parser() -> argparse.ArgumentParser:
    """Build the downloads CLI argument parser."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("mark", help="print a marker; take it just before clicking download")

    p_claim = sub.add_parser("claim", help="wait for this provider's download and print its path")
    p_claim.add_argument("--provider", required=True, choices=sorted(DOWNLOAD_NAME_PATTERNS))
    p_claim.add_argument("--since", required=True, type=float, help="the marker printed by `mark`")
    p_claim.add_argument("--expect", type=int, default=1, help="how many files this click downloads (default 1)")
    p_claim.add_argument(
        "--timeout", type=float, default=DEFAULT_TIMEOUT_SECONDS, help="seconds to wait (default 30)"
    )

    p_left = sub.add_parser("leftovers", help="list this provider's files still in the download folder")
    p_left.add_argument("--provider", required=True, choices=sorted(DOWNLOAD_NAME_PATTERNS))

    p_arrived = sub.add_parser("arrived", help="list every file newer than the marker, with whose it is")
    p_arrived.add_argument("--provider", required=True, help="any configured slug; only its download_dir is used")
    p_arrived.add_argument("--since", required=True, type=float, help="the marker printed by `mark`")
    return parser


def _report_strangers(found: dict[str, list[tuple[str, float, int]]]) -> None:
    """Say what else arrived since the marker, on stderr."""
    if found["partial"]:
        print("still downloading: " + ", ".join(name for name, _, _ in found["partial"]), file=sys.stderr)
    if found["theirs"]:
        print(
            "arrived since the marker but another provider's, left alone: "
            + ", ".join(f"{name} ({owner_of(name)})" for name, _, _ in found["theirs"]),
            file=sys.stderr,
        )
    if found["unknown"]:
        print(
            "arrived since the marker with a name no provider's pattern fits: "
            + ", ".join(name for name, _, _ in found["unknown"])
            + ". If the portal changed how it names downloads, add the shape to "
            "DOWNLOAD_NAME_PATTERNS in src/downloads.py, with a test.",
            file=sys.stderr,
        )


def main(argv: list[str] | None = None) -> int:
    """Entry point; returns a process exit code."""
    args = build_arg_parser().parse_args(argv)
    if args.command == "mark":
        print(f"{time.time():.6f}")
        return 0

    download_dir = download_dir_for(args.provider)
    if args.command == "arrived":
        for name, owner in arrived(download_dir, args.since):
            print(f"{os.path.join(download_dir, name)}\t{owner or 'no pattern'}")
        return 0
    if args.command == "leftovers":
        names = leftovers(download_dir, args.provider)
        for name in names:
            print(os.path.join(download_dir, name))
        return 1 if names else 0

    names, found = claim(download_dir, args.provider, args.since, args.expect, args.timeout)
    for name in names:
        print(os.path.join(download_dir, name))
    if len(names) == args.expect and not found["partial"]:
        return 0
    if len(names) > args.expect:
        print(
            f"{len(names)} files of {args.provider}'s arrived since the marker, expected {args.expect}. "
            "A double click downloads twice; file the one you want and move the rest aside.",
            file=sys.stderr,
        )
        _report_strangers(found)
        return 2
    print(
        f"{len(names)} of {args.expect} download(s) of {args.provider}'s arrived within "
        f"{args.timeout:g} s in {download_dir}.",
        file=sys.stderr,
    )
    _report_strangers(found)
    return 1


# %%
# Main #

if __name__ == "__main__":
    raise SystemExit(user_paths.run_entry_point(main, PROVIDERS_YAML_PATH))


# %%
