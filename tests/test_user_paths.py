# %%
# Imports #

import os
import sys

import pytest

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_SRC = os.path.join(_ROOT, "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

import user_paths  # noqa: E402

# %%
# Fixtures #


@pytest.fixture
def fake_mac(monkeypatch, tmp_path):
    """A pretend Mac home directory, with discovery pointed at it."""
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.delenv("ONEDRIVE_DOCS", raising=False)
    monkeypatch.setattr(user_paths.platform, "system", lambda: "Darwin")
    monkeypatch.setattr(user_paths.os.path, "expanduser", lambda p: str(home) if p == "~" else p)
    return home


def make_tree(base, marker="FinancialLegal"):
    """A folder that looks like the real Documents tree."""
    (base / marker).mkdir(parents=True)
    return base


# %%
# Root discovery #


def test_explicit_override_wins(tmp_path, monkeypatch):
    monkeypatch.setenv("ONEDRIVE_DOCS", str(tmp_path))
    assert user_paths.onedrive_documents(use_cache=False) == os.path.normpath(str(tmp_path))


def test_override_that_does_not_exist_raises(tmp_path, monkeypatch):
    """A typo in the override must fail loudly, not fall back to a real folder."""
    monkeypatch.setenv("ONEDRIVE_DOCS", str(tmp_path / "not-here"))
    with pytest.raises(FileNotFoundError):
        user_paths.onedrive_documents(use_cache=False)


def test_non_mac_refuses_to_guess(monkeypatch):
    monkeypatch.delenv("ONEDRIVE_DOCS", raising=False)
    monkeypatch.setattr(user_paths.platform, "system", lambda: "Linux")
    with pytest.raises(FileNotFoundError, match="macOS-only"):
        user_paths.onedrive_documents(use_cache=False)


def test_finds_the_personal_cloudstorage_tree(fake_mac):
    docs = make_tree(fake_mac / "Library" / "CloudStorage" / "OneDrive-Personal" / "Documents")
    assert user_paths.onedrive_documents(use_cache=False) == str(docs)


def test_business_account_is_never_used(fake_mac):
    """Personal finance data must not land in a work tenant."""
    make_tree(fake_mac / "Library" / "CloudStorage" / "OneDrive-Contoso" / "Documents")
    with pytest.raises(FileNotFoundError, match="No OneDrive personal Documents folder"):
        user_paths.onedrive_documents(use_cache=False)


def test_symlinked_duplicate_collapses_to_one_answer(fake_mac):
    """~/OneDrive is usually a link to the CloudStorage path — one answer, not two."""
    docs = make_tree(fake_mac / "Library" / "CloudStorage" / "OneDrive-Personal" / "Documents")
    (fake_mac / "OneDrive").symlink_to(docs.parent)
    assert user_paths.onedrive_documents(use_cache=False) == str(docs)


def test_two_real_trees_refuse_to_be_guessed_between(fake_mac):
    """Sorting into a replica leaves the live tree untouched. Never pick blind."""
    make_tree(fake_mac / "Library" / "CloudStorage" / "OneDrive-Personal" / "Documents")
    make_tree(fake_mac / "OneDrive" / "Documents")
    with pytest.raises(FileNotFoundError, match="More than one folder"):
        user_paths.onedrive_documents(use_cache=False)


def test_root_without_markers_is_rejected(fake_mac):
    """A still-syncing mount resolves happily and then finds nothing."""
    (fake_mac / "Library" / "CloudStorage" / "OneDrive-Personal" / "Documents").mkdir(parents=True)
    with pytest.raises(FileNotFoundError, match="still syncing"):
        user_paths.onedrive_documents(use_cache=False)


# %%
# Config path expansion #


def test_absolute_path_is_unchanged(tmp_path):
    """Configs written before this existed must keep resolving identically."""
    assert user_paths.expand_config_path(str(tmp_path)) == os.path.normpath(str(tmp_path))


def test_relative_path_resolves_against_base_dir():
    resolved = user_paths.expand_config_path("data/chase/incoming", "/repo")
    assert resolved == os.path.normpath("/repo/data/chase/incoming")


def test_braced_variable_expands_from_environment(tmp_path, monkeypatch):
    monkeypatch.setenv("ONEDRIVE_DOCS", str(tmp_path))
    resolved = user_paths.expand_config_path("${ONEDRIVE_DOCS}/FinancialLegal/Utilities")
    assert resolved == os.path.normpath(str(tmp_path / "FinancialLegal" / "Utilities"))


def test_unbraced_variable_expands_too(tmp_path, monkeypatch):
    """personal.env has to use $VAR, because dotenv eats an unset ${VAR}."""
    monkeypatch.setenv("ONEDRIVE_DOCS", str(tmp_path))
    resolved = user_paths.expand_config_path("$ONEDRIVE_DOCS/FinancialLegal")
    assert resolved == os.path.normpath(str(tmp_path / "FinancialLegal"))


def test_variable_expands_without_being_exported(tmp_path, monkeypatch):
    """The var is normally never exported; discovery has to fill it in."""
    monkeypatch.delenv("ONEDRIVE_DOCS", raising=False)
    monkeypatch.setattr(user_paths, "onedrive_documents", lambda *a, **k: str(tmp_path))
    resolved = user_paths.expand_config_path("${ONEDRIVE_DOCS}/Utilities")
    assert resolved == os.path.normpath(str(tmp_path / "Utilities"))
    assert "ONEDRIVE_DOCS" not in os.environ, "expansion must not leak the var into the process"


def test_unresolved_variable_raises_rather_than_making_a_dollar_folder(monkeypatch):
    monkeypatch.delenv("SOME_UNSET_THING", raising=False)
    with pytest.raises(ValueError):
        user_paths.expand_config_path("${SOME_UNSET_THING}/Utilities")


# %%
# Config file integrity #


def test_dangling_symlink_raises(tmp_path):
    """A moved personal_credentials clone must not degrade to an empty config."""
    link = tmp_path / "providers.local.yaml"
    link.symlink_to(tmp_path / "gone" / "providers.yaml")
    with pytest.raises(RuntimeError, match="does not exist"):
        user_paths.check_config_readable(str(link))


def test_working_symlink_passes(tmp_path):
    target = tmp_path / "real.yaml"
    target.write_text("chase: {}\n")
    link = tmp_path / "providers.local.yaml"
    link.symlink_to(target)
    user_paths.check_config_readable(str(link))


def test_plain_missing_file_still_passes(tmp_path):
    """Not every missing config is an error; only a broken link is."""
    user_paths.check_config_readable(str(tmp_path / "never-existed.yaml"))


def test_desymlinked_config_raises():
    """A copied-not-cloned repo yields the target path as the file's contents."""
    with pytest.raises(RuntimeError, match="symlink"):
        user_paths.check_not_desymlinked(
            "providers.local.yaml", "../personal_credentials/cash_flow_commander_providers.yaml"
        )


def test_real_mapping_passes():
    user_paths.check_not_desymlinked("providers.local.yaml", {"chase": {"raw_dir": "data/chase"}})


def test_empty_file_still_passes():
    """yaml.safe_load of an empty file is None; that stays a soft {} fallback."""
    user_paths.check_not_desymlinked("providers.local.yaml", None)


# %%
# First-run config #


def _repo_with_template(tmp_path):
    (tmp_path / user_paths.TEMPLATE_PROVIDERS_NAME).write_text("chase:\n  raw_dir: \"\"\n")
    return tmp_path / "providers.local.yaml"


def test_missing_config_is_created_from_the_template(tmp_path, capsys):
    path = _repo_with_template(tmp_path)
    assert user_paths.ensure_providers_file(str(path)) is True
    assert path.read_text() == (tmp_path / user_paths.TEMPLATE_PROVIDERS_NAME).read_text()
    assert not path.is_symlink()
    message = capsys.readouterr().err
    assert "Created" in message and "private repo" in message
    assert "/bills-add-company" in message and "Run this command again" in message


def test_existing_config_is_left_alone(tmp_path, capsys):
    path = _repo_with_template(tmp_path)
    path.write_text("chase:\n  raw_dir: mine\n")
    assert user_paths.ensure_providers_file(str(path)) is False
    assert path.read_text() == "chase:\n  raw_dir: mine\n"
    assert capsys.readouterr().err == ""


def test_working_symlink_is_left_alone(tmp_path):
    path = _repo_with_template(tmp_path)
    target = tmp_path / "private" / "providers.yaml"
    target.parent.mkdir()
    target.write_text("chase: {}\n")
    path.symlink_to(target)
    assert user_paths.ensure_providers_file(str(path)) is False
    assert path.is_symlink() and target.read_text() == "chase: {}\n"


def test_dangling_symlink_is_never_replaced(tmp_path):
    """A moved private config must stay visible as a broken link, not become a blank file."""
    path = _repo_with_template(tmp_path)
    path.symlink_to(tmp_path / "gone" / "providers.yaml")
    with pytest.raises(RuntimeError, match="does not exist"):
        user_paths.ensure_providers_file(str(path))
    assert path.is_symlink()


# %%
# Entry points #


def test_first_run_stops_before_the_work(tmp_path, capsys):
    """A just-created config is all blanks, so the work is not attempted."""
    path = _repo_with_template(tmp_path)
    ran = []
    code = user_paths.run_entry_point(lambda: ran.append(True) or 0, str(path))
    assert code == user_paths.SETUP_EXIT_CODE
    assert ran == []
    assert path.exists()
    assert "first run" in capsys.readouterr().err


def test_second_run_does_the_work(tmp_path, capsys):
    path = _repo_with_template(tmp_path)
    path.write_text("chase:\n  raw_dir: mine\n")
    assert user_paths.run_entry_point(lambda: 7, str(path)) == 7
    assert capsys.readouterr().err == ""


def test_setup_incomplete_is_one_message_not_a_traceback(tmp_path, capsys):
    path = _repo_with_template(tmp_path)
    path.write_text("chase:\n  raw_dir: \"\"\n")

    def work():
        raise user_paths.SetupIncomplete("no raw_dir for provider 'chase'. " + user_paths.setup_hint("chase"))

    assert user_paths.run_entry_point(work, str(path)) == user_paths.SETUP_EXIT_CODE
    message = capsys.readouterr().err
    assert message.startswith("no raw_dir for provider 'chase'")
    assert "the `chase` entry" in message and "Traceback" not in message


def test_other_errors_still_raise(tmp_path):
    """Only a setup gap is softened; a real bug keeps its traceback."""
    path = _repo_with_template(tmp_path)
    path.write_text("chase: {}\n")

    def work():
        raise KeyError("a bug")

    with pytest.raises(KeyError):
        user_paths.run_entry_point(work, str(path))
