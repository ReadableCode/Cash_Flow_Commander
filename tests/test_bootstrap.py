# %%
# Imports #

import datetime as dt
import importlib
import os
import sys
from typing import Any

import pytest
from sqlalchemy import inspect, select

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if os.path.join(_ROOT, "src") not in sys.path:
    sys.path.insert(0, os.path.join(_ROOT, "src"))

import bootstrap  # noqa: E402
import db  # noqa: E402

# %%
# Fixtures #


def _reload_modules() -> None:
    """Reload db and the modules holding references to its Table objects."""
    importlib.reload(db)
    importlib.reload(bootstrap)


def _restore_env(name: str, value: str | None) -> None:
    """Put an environment variable back the way it was."""
    if value is None:
        os.environ.pop(name, None)
    else:
        os.environ[name] = value


@pytest.fixture()
def engine(tmp_path: Any) -> Any:
    """Engine on a throwaway SQLite file, with CFC_DB_SCHEMA forced off.

    Deliberately does NOT create tables - these tests are about the thing that
    creates them.
    """
    saved_url = os.environ.get("CFC_DATABASE_URL")
    saved_schema = os.environ.get("CFC_DB_SCHEMA")
    os.environ["CFC_DATABASE_URL"] = f"sqlite:///{tmp_path / 'bootstrap_test.db'}"
    os.environ["CFC_DB_SCHEMA"] = ""
    try:
        _reload_modules()
        yield db.get_engine()
    finally:
        _restore_env("CFC_DATABASE_URL", saved_url)
        _restore_env("CFC_DB_SCHEMA", saved_schema)
        _reload_modules()


# %%
# Convergence #


def test_ensure_schema_creates_every_table_from_nothing(engine: Any) -> None:
    """A fresh clone gets a usable database with no manual step."""
    assert bootstrap.applied_version(engine) is None
    assert bootstrap.ensure_schema(engine) is True

    tables = set(inspect(engine).get_table_names())
    assert set(db.metadata.tables) <= tables
    # The tables the app actually reads, not just whatever metadata happens to hold.
    for expected in ("raw_documents", "transactions", "bills", "expected_series", "forecast_days"):
        assert expected in tables


def test_ensure_schema_stamps_the_version(engine: Any) -> None:
    bootstrap.ensure_schema(engine)
    assert bootstrap.applied_version(engine) == bootstrap.SCHEMA_VERSION

    with engine.connect() as conn:
        rows = conn.execute(select(db.deploy_meta)).all()
    assert len(rows) == 1, "deploy_meta must hold exactly one row"


def test_ensure_schema_is_a_noop_once_converged(engine: Any) -> None:
    """The steady state on 11 entry points: called every time, does nothing."""
    assert bootstrap.ensure_schema(engine) is True
    assert bootstrap.ensure_schema(engine) is False
    assert bootstrap.ensure_schema(engine) is False


def test_ensure_schema_force_reapplies_without_duplicating_the_stamp(engine: Any) -> None:
    bootstrap.ensure_schema(engine)
    assert bootstrap.ensure_schema(engine, force=True) is True
    with engine.connect() as conn:
        assert len(conn.execute(select(db.deploy_meta)).all()) == 1


def test_ensure_schema_preserves_existing_rows(engine: Any) -> None:
    """Convergence is additive: re-running it must never touch data.

    This is the property that makes it safe to call on every process start
    against a database holding real financial records.
    """
    bootstrap.ensure_schema(engine)
    with engine.begin() as conn:
        conn.execute(
            db.expected_series.insert(),
            [
                {
                    "name": "synthetic series",
                    "category": "Expense",
                    "schedule_type": "monthly",
                    "day_of_month": 1,
                    "amount": -10,
                    "active_from": dt.date(2026, 1, 1),
                }
            ],
        )

    assert bootstrap.ensure_schema(engine, force=True) is True

    with engine.connect() as conn:
        names = [r[0] for r in conn.execute(select(db.expected_series.c.name))]
    assert names == ["synthetic series"]


def test_ensure_schema_defaults_to_the_configured_engine(engine: Any) -> None:
    """Called with no engine it builds one from CFC_DATABASE_URL."""
    assert bootstrap.ensure_schema() is True
    assert bootstrap.applied_version(engine) == bootstrap.SCHEMA_VERSION


# %%
# Backend resolution #


def test_env_file_present_without_url_is_a_hard_failure(monkeypatch: Any) -> None:
    """The failure this guards: a configured checkout silently reading an empty
    SQLite file and looking like it worked."""
    monkeypatch.delenv("CFC_DATABASE_URL", raising=False)
    monkeypatch.setattr(db, "_ENV_FILE_PRESENT", True)
    with pytest.raises(RuntimeError, match="CFC_DATABASE_URL is unset"):
        db._resolve_database_url()


def test_no_env_file_falls_back_to_sqlite_with_a_warning(
    monkeypatch: Any, capsys: Any
) -> None:
    """A fresh clone runs on SQLite, but is told so and told to back it up."""
    monkeypatch.delenv("CFC_DATABASE_URL", raising=False)
    monkeypatch.setattr(db, "_ENV_FILE_PRESENT", False)

    assert db._resolve_database_url() == db.DEFAULT_SQLITE_URL

    warning = capsys.readouterr().err
    assert db.DEFAULT_SQLITE_URL in warning
    assert "back it up" in warning.lower()


@pytest.mark.parametrize("env_file_present", [True, False])
def test_configured_url_always_wins(monkeypatch: Any, env_file_present: bool) -> None:
    monkeypatch.setenv("CFC_DATABASE_URL", "postgresql+psycopg://u:p@example.invalid:5432/apps")
    monkeypatch.setattr(db, "_ENV_FILE_PRESENT", env_file_present)
    assert db._resolve_database_url() == "postgresql+psycopg://u:p@example.invalid:5432/apps"


def test_blank_url_is_treated_as_unset(monkeypatch: Any) -> None:
    """An empty CFC_DATABASE_URL= line in the env file must not read as configured."""
    monkeypatch.setenv("CFC_DATABASE_URL", "   ")
    monkeypatch.setattr(db, "_ENV_FILE_PRESENT", True)
    with pytest.raises(RuntimeError, match="CFC_DATABASE_URL is unset"):
        db._resolve_database_url()


# %%
