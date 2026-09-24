"""parse_raw's end-of-run forecast rebuild: once per real run, never on a dry run."""

import importlib
import os
import sys
from typing import Any

import pytest

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if os.path.join(_ROOT, "src") not in sys.path:
    sys.path.insert(0, os.path.join(_ROOT, "src"))

import db  # noqa: E402
import parse_raw  # noqa: E402


def _restore_env(name: str, value: str | None) -> None:
    if value is None:
        os.environ.pop(name, None)
    else:
        os.environ[name] = value


@pytest.fixture()
def engine(tmp_path: Any) -> Any:
    """Engine on a throwaway SQLite file, with CFC_DB_SCHEMA forced off."""
    saved_url = os.environ.get("CFC_DATABASE_URL")
    saved_schema = os.environ.get("CFC_DB_SCHEMA")
    os.environ["CFC_DATABASE_URL"] = f"sqlite:///{tmp_path / 'parse_raw_test.db'}"
    os.environ["CFC_DB_SCHEMA"] = ""
    try:
        importlib.reload(db)
        eng = db.get_engine()
        db.create_tables(eng)
        yield eng
    finally:
        _restore_env("CFC_DATABASE_URL", saved_url)
        _restore_env("CFC_DB_SCHEMA", saved_schema)
        importlib.reload(db)


def _capture_rebuilds(monkeypatch) -> list[str]:
    events: list[str] = []
    monkeypatch.setattr(
        parse_raw.expected_forecast,
        "rebuild_after",
        lambda eng, event, config=None: events.append(event) or 0,
    )
    return events


def test_a_real_run_rebuilds_the_forecast_once(engine, monkeypatch):
    events = _capture_rebuilds(monkeypatch)
    assert parse_raw.main(["--account-id", "ACCT-TEST-1"]) == 0
    assert events == ["parse"]


def test_a_dry_run_leaves_the_forecast_alone(engine, monkeypatch):
    events = _capture_rebuilds(monkeypatch)
    assert parse_raw.main(["--account-id", "ACCT-TEST-1", "--dry-run"]) == 0
    assert events == []
