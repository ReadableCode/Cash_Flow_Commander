"""Resolve the OneDrive sync path, which differs per machine.

Bills and statements land in a OneDrive folder that syncs to several of the
owner's Macs. Writing one machine's absolute path into `providers.local.yaml`
pinned the acquisition workflow to that machine: anywhere else the configured
`raw_dir` simply does not exist, so a run either creates an empty folder in the
wrong place or reports nothing to ingest.

Config paths may therefore be written with a variable:

    raw_dir: "${ONEDRIVE_DOCS}/FinancialLegal/Utilities/Rythm/raw"

`expand_config_path` fills that in with this machine's root. Plain absolute and
repo-relative paths keep working exactly as before, so nothing had to migrate.

macOS only, matching the rest of the acquisition workflow (the browser
automation it feeds is AppleScript-driven). On any other platform this refuses
to guess; set $ONEDRIVE_DOCS explicitly if you have a synced copy elsewhere.

Resolution order:

  1. $ONEDRIVE_DOCS - explicit override. Must exist; trusted as-is.
  2. ~/Library/CloudStorage/OneDrive-Personal*/Documents
  3. ~/OneDrive/Documents  (older client layout, or a symlink to the above)

Business tenants are deliberately not candidates - personal finance data must
never be written into a work account.
"""

# %%
# Imports #

import os
import platform

# %%
# Constants #

ONEDRIVE_DOCS_VAR = "ONEDRIVE_DOCS"

# A resolved root must contain at least one of these. Without the check, an
# empty or still-initialising sync folder resolves happily and a run reports
# "nothing to ingest" instead of failing.
MARKERS = ("FinancialLegal", "Health", "Scans", "Scanned Documents")

_cached_root: str | None = None


# %%
# Discovery #


def onedrive_candidates() -> list[str]:
    """Every path worth probing on this Mac, best guess first."""
    home = os.path.expanduser("~")
    out: list[str] = []

    cloud = os.path.join(home, "Library", "CloudStorage")
    if os.path.isdir(cloud):
        for entry in sorted(os.listdir(cloud)):
            if entry.startswith("OneDrive-Personal"):
                out.append(os.path.join(cloud, entry, "Documents"))

    # Older client layout. Usually a symlink to the CloudStorage path above,
    # which the realpath dedupe in onedrive_documents collapses.
    out.append(os.path.join(home, "OneDrive", "Documents"))

    seen: set[str] = set()
    unique = []
    for path in out:
        norm = os.path.normpath(path)
        if norm not in seen:
            seen.add(norm)
            unique.append(norm)
    return unique


def _has_marker(root: str) -> bool:
    """True when the root looks like the real Documents tree, not an empty mount."""
    return any(os.path.isdir(os.path.join(root, marker)) for marker in MARKERS)


def onedrive_documents(use_cache: bool = True) -> str:
    """This machine's OneDrive personal Documents root.

    Raises FileNotFoundError with an actionable message rather than returning a
    plausible-but-wrong root - that is how a month of captures ends up written
    somewhere nobody looks.
    """
    global _cached_root
    if use_cache and _cached_root is not None:
        return _cached_root

    override = os.environ.get(ONEDRIVE_DOCS_VAR)
    if override:
        # A typo in an explicit override is an error, not a reason to guess.
        expanded = os.path.normpath(os.path.expanduser(override))
        if not os.path.isdir(expanded):
            raise FileNotFoundError(f"{ONEDRIVE_DOCS_VAR} is set to {expanded!r}, which is not a directory")
        _cached_root = expanded
        return expanded

    if platform.system() != "Darwin":
        raise FileNotFoundError(
            f"OneDrive path discovery is macOS-only (running on {platform.system()}). "
            f"Set {ONEDRIVE_DOCS_VAR} to the synced Documents folder on this machine."
        )

    existing = [path for path in onedrive_candidates() if os.path.isdir(path)]
    if not existing:
        raise FileNotFoundError(
            "No OneDrive personal Documents folder found on this Mac. Check that OneDrive is "
            f"signed in, or set {ONEDRIVE_DOCS_VAR}. Probed: " + ", ".join(onedrive_candidates())
        )

    # Collapse symlinked duplicates - ~/OneDrive usually links to the
    # CloudStorage path, and those are one answer, not two.
    by_real: dict[str, str] = {}
    for path in existing:
        by_real.setdefault(os.path.realpath(path), path)

    usable = {real: shown for real, shown in by_real.items() if _has_marker(real)}
    if not usable:
        raise FileNotFoundError(
            f"Found {', '.join(by_real.values())}, but none contains any of {', '.join(MARKERS)}. "
            f"OneDrive is probably still syncing. Wait, or set {ONEDRIVE_DOCS_VAR}."
        )
    if len(usable) > 1:
        raise FileNotFoundError(
            "More than one folder on this Mac looks like the Documents tree ("
            + ", ".join(sorted(usable.values()))
            + f"). They are different directories, so picking one is a guess. Set {ONEDRIVE_DOCS_VAR}."
        )

    _cached_root = next(iter(usable.values()))
    return _cached_root


# %%
# Config path expansion #


def expand_config_path(value: str, base_dir: str | None = None) -> str:
    """Expand a configured path: ~, environment variables, then relative-to-base.

    `${ONEDRIVE_DOCS}` resolves even when the variable is not exported, by
    discovering this machine's root on demand. Any variable left unexpanded is
    an error rather than a literal path component, because "$" in a directory
    name would otherwise create a folder called `${ONEDRIVE_DOCS}` and quietly
    succeed.
    """
    text = str(value)

    if ONEDRIVE_DOCS_VAR in text and ONEDRIVE_DOCS_VAR not in os.environ:
        # expandvars only reads os.environ, so seed it for this call only.
        env = dict(os.environ)
        env[ONEDRIVE_DOCS_VAR] = onedrive_documents()
        original = os.environ
        try:
            os.environ = env  # type: ignore[assignment]
            expanded = os.path.expandvars(text)
        finally:
            os.environ = original  # type: ignore[assignment]
    else:
        expanded = os.path.expandvars(text)

    expanded = os.path.expanduser(expanded)

    if "$" in expanded:
        raise ValueError(
            f"unresolved variable in configured path {value!r} (expanded to {expanded!r}); "
            "export it, or write the path out in full"
        )

    if os.path.isabs(expanded):
        return os.path.normpath(expanded)
    if base_dir is None:
        return expanded
    return os.path.normpath(os.path.join(base_dir, expanded))


# %%
# Config file integrity #


def check_config_readable(path: str) -> None:
    """Fail loudly when a config symlink is present but points at nothing.

    `.env` and `providers.local.yaml` are symlinks into the sibling
    personal_credentials clone. If that clone is missing, renamed, or moved,
    the link dangles: `os.path.isfile` is False, every loader here falls back
    to `{}`, and the run continues with no provider config at all - reporting
    nothing to do rather than saying the config is gone. `db.py` degrades the
    same way, silently using the local dev SQLite file instead of the
    configured database.
    """
    if os.path.islink(path) and not os.path.exists(path):
        raise RuntimeError(
            f"{path} is a symlink to {os.readlink(path)!r}, which does not exist. "
            "The personal_credentials clone is missing or moved - without it there is no "
            "provider config and no database URL, and a run would silently do nothing."
        )


def check_not_desymlinked(path: str, loaded: object) -> None:
    """Raise when a config file turns out to be a checked-out symlink's text.

    A copied-not-cloned repo (or a checkout on a filesystem without symlink
    support) materializes these links as ordinary files whose entire contents
    are the target path. YAML then parses to a string and every loader falls
    back to `{}`, exactly as above.
    """
    if isinstance(loaded, str):
        raise RuntimeError(
            f"{path} is not config, it is the text {loaded.strip()!r} - the symlink was replaced "
            "by a plain file holding its target path. Restore the symlink, or put a real copy "
            "of the target there."
        )


# %%
# Main #

if __name__ == "__main__":
    for candidate in onedrive_candidates():
        real = os.path.realpath(candidate)
        suffix = f"  -> {real}" if real != candidate else ""
        print(f"{'OK     ' if os.path.isdir(candidate) else 'missing'}  {candidate}{suffix}")
    print()
    try:
        print(f"resolved: {onedrive_documents()}")
    except FileNotFoundError as exc:
        raise SystemExit(str(exc))
