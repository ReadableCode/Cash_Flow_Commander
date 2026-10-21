# %%
# Imports #

import os
import subprocess

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LAND_SH = os.path.join(REPO, "transaction_downloader", "land.sh")

# Resolving directories needs the gitignored local config, so those tests are
# skipped where it is absent rather than failing on a fresh clone.
needs_local_config = pytest.mark.skipif(
    not os.path.isfile(os.path.join(REPO, "providers.local.yaml")),
    reason="providers.local.yaml not present",
)


# %%
# Helpers #


def _run(*args: str) -> subprocess.CompletedProcess[str]:
    """Run land.sh from a directory that is not the repo, as a real caller might."""
    return subprocess.run(
        ["bash", LAND_SH, *args],
        capture_output=True,
        text=True,
        cwd=os.path.dirname(REPO),
    )


# %%
# Tests #


def test_land_sh_is_syntactically_valid() -> None:
    assert subprocess.run(["bash", "-n", LAND_SH], capture_output=True).returncode == 0


def test_provider_flag_requires_a_value() -> None:
    """A bare --provider must fail loudly rather than silently landing as chase."""
    result = _run("--provider")

    assert result.returncode != 0
    assert "--provider needs a slug" in result.stderr


@pytest.mark.parametrize("flag", ["--provider rhythm", "--provider=rhythm"])
def test_bill_providers_are_rejected_with_guidance(flag: str) -> None:
    """Both flag spellings reach the registry check, and it names the way out."""
    result = _run(*flag.split())
    combined = result.stdout + result.stderr

    assert result.returncode != 0
    assert "unknown transaction provider" in combined
    assert "/bills-<slug>" in combined


@needs_local_config
def test_default_provider_is_still_chase() -> None:
    """Callers predating --provider pass no flag and must keep getting chase.

    --dry-run stops before anything is filed, so this asserts the resolution
    without touching the database.
    """
    result = _run("--dry-run")

    assert result.returncode == 0
    assert "provider: chase" in result.stdout


@needs_local_config
@pytest.mark.parametrize("flag", ["--provider elan", "--provider=elan"])
def test_named_provider_overrides_the_default(flag: str) -> None:
    result = _run(*flag.split(), "--dry-run")

    assert result.returncode == 0
    assert "provider: elan" in result.stdout
    assert "provider: chase" not in result.stdout


@needs_local_config
def test_dry_run_lands_nothing() -> None:
    """The point of --dry-run is that no pipeline stage runs."""
    result = _run("--dry-run")

    assert "dry run — nothing filed, ingested or parsed." in result.stdout
    for stage in ("1. filing downloads", "2. ingest", "3. parse"):
        assert stage not in result.stdout


def test_ingest_walks_raw_dir_only() -> None:
    """archive_dir and the folders around it are filed by hand; only raw_dir holds captures."""
    with open(LAND_SH, encoding="utf-8") as handle:
        script = handle.read()
    ingest_lines = [line.strip() for line in script.splitlines() if "src/ingest_raw.py" in line and "uv run" in line]

    assert ingest_lines == ['uv run python src/ingest_raw.py --provider "$PROVIDER" "$RAW_DIR"']
    assert 'entry["archive_dir"]' not in script
