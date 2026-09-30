# %%
# Imports #

import time
from typing import Any

import pytest

import downloads

# %%
# Fixtures #

# One download name per provider, as the portal (or the command's own
# a.download) names it. Invented accounts and dates.
SAMPLES: dict[str, list[str]] = {
    "chase": [
        "Chase1234_Activity_20260822.CSV",
        "Chase5678_Activity20260701_20260822_20260822.CSV",
        "chase_csv_export_1234_20260801_20260822_captured20260822.csv",
    ],
    "citi": [
        "Date range.CSV",
        "Date range (1).CSV",
        "Since Aug 06, 2026.CSV",
        "citi_csv_export_9012_20260801_20260822_captured20260822.csv",
    ],
    "elan": [
        "Synthetic Rewards Card - 9012_07-01-2026_07-31-2026.csv",
        "Card - 9012_07-01-2026_07-31-2026 (1).csv",
        "elan_empty_window_9012_20260801_20260822_captured20260822.txt",
    ],
    "smt": ["IntervalData.csv", "IntervalData (2).csv", "smt_IntervalData_2026-09-01_2026-09-15.csv"],
    "rhythm": ["rhythm_api_orders_INV00000001.json", "rhythm_bill_INV00000001_2026-08-28.pdf"],
    "gexa": ["gexa_api_invoice-history_20260928.json", "gexa_bill_90000002_2026-09-28.pdf"],
    "enphase_enlighten": ["enphase_enlighten_api_usage_daily_energy_2026-09-01_2026-09-28.json"],
}

MARKER = 1000.0


@pytest.fixture()
def download_dir(tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> Any:
    """A throwaway download folder that every provider's config points at."""
    folder = tmp_path / "downloads"
    folder.mkdir()
    config = tmp_path / "providers.local.yaml"
    config.write_text(
        "".join(f'{slug}:\n  download_dir: "{folder}"\n' for slug in [*SAMPLES, "nodir_but_configured"])
        + "nodir:\n  raw_dir: x\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(downloads, "PROVIDERS_YAML_PATH", str(config))
    return folder


def _fake_folder(monkeypatch: pytest.MonkeyPatch, looks: list[list[tuple[str, float, int]]]) -> None:
    """Make the folder show each listing in turn, holding on the last, with no real waiting."""
    remaining = list(looks)

    def files(_: str) -> list[tuple[str, float, int]]:
        return remaining.pop(0) if len(remaining) > 1 else remaining[0]

    monkeypatch.setattr(downloads, "_files", files)
    monkeypatch.setattr(downloads.time, "sleep", lambda _: None)


# %%
# Whose file is it #


@pytest.mark.parametrize(("provider", "name"), [(p, n) for p, names in SAMPLES.items() for n in names])
def test_each_download_name_belongs_to_exactly_one_provider(provider: str, name: str) -> None:
    """The shapes must not overlap, or two runs at once could claim the same file."""
    matching = [
        slug
        for slug, patterns in downloads.DOWNLOAD_NAME_PATTERNS.items()
        if any(downloads.re.fullmatch(pattern, name, downloads.re.IGNORECASE) for pattern in patterns)
    ]
    assert matching == [provider]
    assert downloads.owner_of(name) == provider


@pytest.mark.parametrize("name", ["statement.pdf", "export.csv", "Chaser.CSV", "IntervalData.xlsx"])
def test_a_name_no_portal_uses_has_no_owner(name: str) -> None:
    assert downloads.owner_of(name) is None


# %%
# Claiming #


def test_two_runs_downloading_at_once_each_claim_only_their_own(monkeypatch: pytest.MonkeyPatch) -> None:
    """The case 'newest file since my marker' gets wrong."""
    folder = [
        ("Date range.CSV", MARKER + 2, 500),
        ("Synthetic Rewards Card - 9012_07-01-2026_07-31-2026.csv", MARKER + 3, 700),
    ]
    _fake_folder(monkeypatch, [folder])

    citi, _ = downloads.claim("d", "citi", MARKER)
    elan, _ = downloads.claim("d", "elan", MARKER)

    assert citi == ["Date range.CSV"]
    assert elan == ["Synthetic Rewards Card - 9012_07-01-2026_07-31-2026.csv"]


def test_a_leftover_from_before_the_marker_is_never_claimed(monkeypatch: pytest.MonkeyPatch) -> None:
    """An earlier session's file looks exactly like this run's download."""
    _fake_folder(monkeypatch, [[("Date range.CSV", MARKER - 60, 500), ("Date range (1).CSV", MARKER + 1, 500)]])

    names, _ = downloads.claim("d", "citi", MARKER)

    assert names == ["Date range (1).CSV"]


def test_claim_waits_for_a_download_still_being_written(monkeypatch: pytest.MonkeyPatch) -> None:
    """A partial file, or one still growing, is not done."""
    _fake_folder(
        monkeypatch,
        [
            [("Unconfirmed 123.crdownload", MARKER + 1, 10)],
            [("IntervalData.csv", MARKER + 2, 100)],
            [("IntervalData.csv", MARKER + 2, 900)],
            [("IntervalData.csv", MARKER + 2, 900)],
        ],
    )

    names, found = downloads.claim("d", "smt", MARKER)

    assert names == ["IntervalData.csv"]
    assert found["mine"] == [("IntervalData.csv", MARKER + 2, 900)]


def test_claim_gives_up_at_the_timeout_and_says_what_else_arrived(monkeypatch: pytest.MonkeyPatch) -> None:
    _fake_folder(monkeypatch, [[("Date range.CSV", MARKER + 1, 500), ("mystery export.csv", MARKER + 1, 5)]])

    names, found = downloads.claim("d", "smt", MARKER, timeout=0)

    assert names == []
    assert [name for name, _, _ in found["theirs"]] == ["Date range.CSV"]
    assert [name for name, _, _ in found["unknown"]] == ["mystery export.csv"]


# %%
# CLI, against a real folder #


def test_mark_then_claim_finds_the_new_file_and_not_the_old_one(download_dir: Any, capsys: Any) -> None:
    (download_dir / "Chase1234_Activity_20260701.CSV").write_text("old")
    assert downloads.main(["mark"]) == 0
    marker = float(capsys.readouterr().out)
    assert marker == pytest.approx(time.time(), abs=5)
    time.sleep(0.05)
    (download_dir / "Chase1234_Activity_20260822.CSV").write_text("new")
    (download_dir / "IntervalData.csv").write_text("another run's")
    (download_dir / ".hidden").write_text("x")

    code = downloads.main(["claim", "--provider", "chase", "--since", f"{marker:.6f}", "--timeout", "5"])

    assert code == 0
    assert capsys.readouterr().out.splitlines() == [str(download_dir / "Chase1234_Activity_20260822.CSV")]


def test_claim_exits_1_when_nothing_of_this_providers_arrived(download_dir: Any, capsys: Any) -> None:
    marker = time.time() - 1
    (download_dir / "Date range.CSV").write_text("citi's")
    (download_dir / "mystery export.csv").write_text("?")

    code = downloads.main(["claim", "--provider", "elan", "--since", f"{marker:.6f}", "--timeout", "0"])

    captured = capsys.readouterr()
    assert code == 1
    assert captured.out == ""
    assert "Date range.CSV (citi)" in captured.err
    assert "mystery export.csv" in captured.err and "DOWNLOAD_NAME_PATTERNS" in captured.err


def test_claim_exits_2_when_more_files_arrived_than_the_click_should_give(download_dir: Any, capsys: Any) -> None:
    marker = time.time() - 1
    (download_dir / "IntervalData.csv").write_text("a")
    (download_dir / "IntervalData (1).csv").write_text("a")

    code = downloads.main(["claim", "--provider", "smt", "--since", f"{marker:.6f}", "--timeout", "2"])

    captured = capsys.readouterr()
    assert code == 2
    assert len(captured.out.splitlines()) == 2
    assert "expected 1" in captured.err


def test_leftovers_lists_this_providers_files_whenever_they_landed(download_dir: Any, capsys: Any) -> None:
    assert downloads.main(["leftovers", "--provider", "citi"]) == 0
    (download_dir / "Since Aug 06, 2026.CSV").write_text("x")
    (download_dir / "gexa_bill_90000002_2026-09-28.pdf").write_text("x")
    capsys.readouterr()

    assert downloads.main(["leftovers", "--provider", "citi"]) == 1

    assert capsys.readouterr().out.splitlines() == [str(download_dir / "Since Aug 06, 2026.CSV")]


def test_arrived_lists_everything_new_with_its_owner_for_a_portal_not_yet_known(
    download_dir: Any, capsys: Any
) -> None:
    """Onboarding: see how a new portal names its download before it has a pattern."""
    (download_dir / "Date range.CSV").write_text("old")
    marker = time.time()
    time.sleep(0.05)
    (download_dir / "NewBank export 2026.csv").write_text("new")
    (download_dir / "IntervalData.csv").write_text("new")

    assert downloads.main(["arrived", "--provider", "nodir_but_configured", "--since", f"{marker:.6f}"]) == 0

    lines = sorted(capsys.readouterr().out.splitlines())
    assert lines == [
        f"{download_dir / 'IntervalData.csv'}\tsmt",
        f"{download_dir / 'NewBank export 2026.csv'}\tno pattern",
    ]


def test_a_provider_with_no_download_dir_is_an_error(download_dir: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setitem(downloads.DOWNLOAD_NAME_PATTERNS, "nodir", (r"nodir_.+",))
    with pytest.raises(ValueError, match="no download_dir"):
        downloads.download_dir_for("nodir")


# %%
