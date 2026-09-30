# %%
# Imports #

import datetime as dt
import importlib
import os
import sys
from typing import Any

import pytest
from sqlalchemy import func, select

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if os.path.join(_ROOT, "src") not in sys.path:
    sys.path.insert(0, os.path.join(_ROOT, "src"))

import db  # noqa: E402
import purge_raw  # noqa: E402
import relabel_raw  # noqa: E402

# %%
# Fixtures #

SMT_NAME = "IntervalData_2026-09-01_2026-09-15.csv"


def _reload_modules() -> None:
    """Reload db and the modules holding references to its Table objects."""
    importlib.reload(db)
    importlib.reload(purge_raw)
    importlib.reload(relabel_raw)


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
    os.environ["CFC_DATABASE_URL"] = f"sqlite:///{tmp_path / 'relabel_test.db'}"
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


def _add_doc(eng: Any, provider: str, doc_type: str, name: str, parse_status: str = "pending") -> int:
    """Insert one raw document; returns its id."""
    with eng.begin() as conn:
        result = conn.execute(
            db.raw_documents.insert(),
            [{
                "provider": provider, "doc_type": doc_type, "source": "manual",
                "original_name": name, "mime": "text/csv", "byte_size": 10,
                "content": b"x" * 10, "content_sha256": name, "fetched_at": dt.datetime.now(),
                "extra": {"provider_from": "cli"}, "parse_status": parse_status,
                "parser_version": "t" if parse_status == "ok" else None,
            }],
        )
        return int(result.inserted_primary_key[0])


def _add_usage_row(eng: Any, doc_id: int, account_id: str, hour: int) -> None:
    """Insert one usage row parsed from doc_id."""
    with eng.begin() as conn:
        conn.execute(db.usage_intervals.insert(), [{
            "account_id": account_id, "ts": dt.datetime(2026, 9, 1, hour, tzinfo=dt.timezone.utc),
            "granularity": "15min", "metric": "consumption", "value": 1,
            "raw_document_id": doc_id, "parser_version": "t",
        }])


def _doc(eng: Any, doc_id: int) -> Any:
    with eng.connect() as conn:
        return conn.execute(select(db.raw_documents).where(db.raw_documents.c.id == doc_id)).mappings().one()


def _usage_count(eng: Any) -> int:
    with eng.connect() as conn:
        return int(conn.execute(select(func.count()).select_from(db.usage_intervals)).scalar_one())


# %%
# Safety #


def test_dry_run_writes_nothing(engine: Any, capsys: Any) -> None:
    """Default is to report, never to write."""
    doc_id = _add_doc(engine, "elan", "bill_pdf", "Rythm 2026-08.pdf")

    assert relabel_raw.main(["--from", "elan", "--to", "rhythm", "--doc-type", "bill_pdf"]) == 0

    assert _doc(engine, doc_id)["provider"] == "elan"
    assert "Nothing written" in capsys.readouterr().out


def test_refuses_parsed_documents_without_rebuild(engine: Any, capsys: Any) -> None:
    """Rows parsed under the old provider's account must not be left behind."""
    doc_id = _add_doc(engine, "rhythm", "smt_export", SMT_NAME, parse_status="ok")
    _add_usage_row(engine, doc_id, "OLD-ACCT", 0)

    assert relabel_raw.main(["--from", "rhythm", "--to", "smt", "--doc-type", "smt_export", "--yes"]) == 2

    assert _doc(engine, doc_id)["provider"] == "rhythm"
    assert _usage_count(engine) == 1
    assert "REFUSING" in capsys.readouterr().out


def test_refuses_a_name_that_matches_nothing(engine: Any, capsys: Any) -> None:
    """A mistyped name must not turn into a smaller relabel that reads as done."""
    doc_id = _add_doc(engine, "elan", "bill_pdf", "Rythm 2026-08.pdf")

    code = relabel_raw.main([
        "--from", "elan", "--to", "rhythm", "--doc-type", "bill_pdf",
        "--name", "Rythm 2026-08.pdf", "--name", "Rythm 2026-09.pdf", "--yes",
    ])

    assert code == 2
    assert _doc(engine, doc_id)["provider"] == "elan"
    assert "Rythm 2026-09.pdf" in capsys.readouterr().out


def test_same_provider_is_a_usage_error(engine: Any) -> None:
    with pytest.raises(SystemExit):
        relabel_raw.main(["--from", "elan", "--to", "elan", "--doc-type", "bill_pdf"])


# %%
# The job #


def test_relabels_a_pending_document_when_confirmed(engine: Any) -> None:
    """The mislabel case: nothing parsed yet, only the label is wrong."""
    doc_id = _add_doc(engine, "elan", "bill_pdf", "Rythm 2026-08.pdf")

    assert relabel_raw.main(["--from", "elan", "--to", "rhythm", "--doc-type", "bill_pdf", "--yes"]) == 0

    row = _doc(engine, doc_id)
    assert row["provider"] == "rhythm"
    assert row["parse_status"] == "pending"
    assert row["extra"] == {"provider_from": "cli", "relabelled_from": "elan"}


def test_rebuild_deletes_parsed_rows_and_resets_to_pending(engine: Any, capsys: Any) -> None:
    """The re-key case: the document moves and its projection is dropped for a reparse."""
    doc_id = _add_doc(engine, "rhythm", "smt_export", SMT_NAME, parse_status="ok")
    other_id = _add_doc(engine, "rhythm", "api_usage_json", "rhythm_api_usage_INV00000000.json", parse_status="ok")
    _add_usage_row(engine, doc_id, "OLD-ACCT", 0)
    _add_usage_row(engine, doc_id, "OLD-ACCT", 1)
    _add_usage_row(engine, other_id, "OLD-ACCT", 2)

    code = relabel_raw.main(["--from", "rhythm", "--to", "smt", "--doc-type", "smt_export", "--rebuild", "--yes"])

    assert code == 0
    row = _doc(engine, doc_id)
    assert row["provider"] == "smt"
    assert row["parse_status"] == "pending"
    assert row["parser_version"] is None
    # Only the rows parsed from the relabelled document go.
    assert _usage_count(engine) == 1
    assert _doc(engine, other_id)["provider"] == "rhythm"
    assert "parse_raw.py --provider smt --doc-type smt_export" in capsys.readouterr().out


def test_only_touches_the_named_documents(engine: Any) -> None:
    """--name narrows a doc_type that also holds correctly filed documents."""
    wrong = _add_doc(engine, "elan", "csv_export", "hourly_usage.csv")
    right = _add_doc(engine, "elan", "csv_export", "elan_csv_export_0001_20260801_20260822_captured20260822.csv")

    relabel_raw.main([
        "--from", "elan", "--to", "rhythm", "--doc-type", "csv_export", "--name", "hourly_usage.csv", "--yes",
    ])

    assert _doc(engine, wrong)["provider"] == "rhythm"
    assert _doc(engine, right)["provider"] == "elan"


def test_notes_when_the_new_provider_has_no_parser(engine: Any, capsys: Any) -> None:
    """A relabel into a provider that cannot parse the document is said up front."""
    _add_doc(engine, "rhythm", "other", "letter.pdf")

    relabel_raw.main(["--from", "rhythm", "--to", "chase", "--doc-type", "other"])

    assert "no parser is registered for chase/other" in capsys.readouterr().out


def test_reports_cleanly_when_nothing_matches(engine: Any, capsys: Any) -> None:
    """No match is a normal outcome, not an error."""
    assert relabel_raw.main(["--from", "elan", "--to", "rhythm", "--doc-type", "bill_pdf"]) == 0
    assert "Nothing matches" in capsys.readouterr().out


# %%
