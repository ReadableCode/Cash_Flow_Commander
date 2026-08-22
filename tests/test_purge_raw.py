# %%
# Imports #

import datetime as dt
import importlib
import os
import sys
from typing import Any

import pytest

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if os.path.join(_ROOT, "src") not in sys.path:
    sys.path.insert(0, os.path.join(_ROOT, "src"))

import db  # noqa: E402
import purge_raw  # noqa: E402

# %%
# Fixtures #


def _reload_modules() -> None:
    """Reload db and the modules holding references to its Table objects."""
    importlib.reload(db)
    importlib.reload(purge_raw)


def _restore_env(name: str, value: str | None) -> None:
    """Put an environment variable back the way it was."""
    if value is None:
        os.environ.pop(name, None)
    else:
        os.environ[name] = value


@pytest.fixture()
def engine(tmp_path: Any) -> Any:
    """Engine on a throwaway SQLite file, with CFC_DB_SCHEMA forced off."""
    saved_url = os.environ.get("CFC_DATABASE_URL")
    saved_schema = os.environ.get("CFC_DB_SCHEMA")
    os.environ["CFC_DATABASE_URL"] = f"sqlite:///{tmp_path / 'purge_test.db'}"
    os.environ["CFC_DB_SCHEMA"] = ""
    try:
        _reload_modules()
        eng = db.get_engine()
        db.create_tables(eng)
        yield eng
    finally:
        _restore_env("CFC_DATABASE_URL", saved_url)
        _restore_env("CFC_DB_SCHEMA", saved_schema)
        _reload_modules()


def _add_doc(eng: Any, provider: str, doc_type: str, name: str) -> int:
    """Insert one raw document; returns its id."""
    with eng.begin() as conn:
        result = conn.execute(
            db.raw_documents.insert(),
            [{
                "provider": provider, "doc_type": doc_type, "source": "manual",
                "original_name": name, "mime": "application/pdf", "byte_size": 10,
                "content": b"x" * 10, "content_sha256": name, "fetched_at": dt.datetime.now(),
            }],
        )
        return int(result.inserted_primary_key[0])


def _count_docs(eng: Any) -> int:
    from sqlalchemy import func, select
    with eng.connect() as conn:
        return int(conn.execute(select(func.count()).select_from(db.raw_documents)).scalar_one())


# %%
# Safety #


def test_dry_run_deletes_nothing(engine: Any, capsys: Any) -> None:
    """Default is to report, never to write — raw is the source of truth."""
    _add_doc(engine, "chase", "other", "some-letter.pdf")

    assert purge_raw.main(["--provider", "chase", "--doc-type", "other"]) == 0

    assert _count_docs(engine) == 1
    assert "Nothing deleted" in capsys.readouterr().out


def test_refuses_when_rows_were_parsed_from_the_document(engine: Any, capsys: Any) -> None:
    """A document real rows came from is provenance, not clutter."""
    doc_id = _add_doc(engine, "chase", "csv_export", "chase_csv_export_0001_20260801_20260822_captured20260822.csv")
    with engine.begin() as conn:
        conn.execute(db.transactions.insert(), [{
            "account_id": "0001", "account_kind": "bank",
            "txn_date": dt.date(2026, 8, 1), "post_date": dt.date(2026, 8, 1),
            "description": "X", "amount": -1, "occurrence": 0,
            "raw_document_id": doc_id, "parser_version": "t",
        }])

    assert purge_raw.main(["--provider", "chase", "--doc-type", "csv_export", "--yes"]) == 2

    assert _count_docs(engine) == 1
    assert "REFUSING" in capsys.readouterr().out


def test_removes_an_unparsed_document_when_confirmed(engine: Any) -> None:
    """The actual job: clear a document nothing was built from."""
    _add_doc(engine, "chase", "other", "some-letter.pdf")

    assert purge_raw.main(["--provider", "chase", "--doc-type", "other", "--yes"]) == 0

    assert _count_docs(engine) == 0


def test_only_touches_the_named_provider_and_doc_type(engine: Any) -> None:
    """A filter is required precisely so nothing collateral is removed."""
    _add_doc(engine, "chase", "other", "letter.pdf")
    _add_doc(engine, "rhythm", "other", "rhythm-thing.pdf")
    _add_doc(engine, "chase", "csv_export", "chase_csv_export_0001_20260801_20260822_captured20260822.csv")

    purge_raw.main(["--provider", "chase", "--doc-type", "other", "--yes"])

    assert _count_docs(engine) == 2


def test_reports_cleanly_when_nothing_matches(engine: Any, capsys: Any) -> None:
    """No match is a normal outcome, not an error."""
    assert purge_raw.main(["--provider", "chase", "--doc-type", "other"]) == 0
    assert "Nothing matches" in capsys.readouterr().out


# %%
