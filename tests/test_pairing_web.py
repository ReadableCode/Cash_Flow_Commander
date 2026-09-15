# %%
# Imports #

import datetime as dt
import importlib
import json
import os
import sys
import threading
import urllib.request
from typing import Any

import pytest
from sqlalchemy import insert, select

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if os.path.join(_ROOT, "src") not in sys.path:
    sys.path.insert(0, os.path.join(_ROOT, "src"))

import db  # noqa: E402
import expected_store  # noqa: E402
import pairing_web  # noqa: E402

# %%
# Fixtures #

TODAY = dt.date(2026, 9, 14)
START = dt.date(2026, 8, 1)
END = dt.date(2026, 9, 30)


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
    os.environ["CFC_DATABASE_URL"] = f"sqlite:///{tmp_path / 'pairing_web_test.db'}"
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


def add_transaction(engine, account_id, post_date, description, amount, occurrence=0):
    with engine.begin() as conn:
        conn.execute(
            insert(db.transactions).values(
                account_id=account_id,
                account_kind="bank",
                txn_date=post_date,
                post_date=post_date,
                description=description,
                amount=amount,
                occurrence=occurrence,
                parser_version="test-1",
            )
        )


def seed(engine):
    """A mortgage and a PMI line due Sep 6, one payment on Sep 5 that covers both,
    a gym due Sep 15 with nothing yet, and an August mortgage paid in August."""
    mortgage = expected_store.add_series(
        engine,
        {
            "name": "Mortgage",
            "category": "Debt",
            "schedule_type": "monthly",
            "day_of_month": 6,
            "amount": -2294.00,
            "auto_pay_account_id": "1234",
            "match_pattern": "MTG",
            "active_from": dt.date(2026, 1, 1),
        },
    )
    pmi = expected_store.add_series(
        engine,
        {
            "name": "PMI",
            "category": "Debt",
            "schedule_type": "monthly",
            "day_of_month": 6,
            "amount": -150.00,
            "auto_pay_account_id": "1234",
            "active_from": dt.date(2026, 1, 1),
        },
    )
    expected_store.add_series(
        engine,
        {
            "name": "Gym",
            "category": "Leisure",
            "schedule_type": "monthly",
            "day_of_month": 15,
            "amount": -40.00,
            "auto_pay_account_id": "5678",
            "active_from": dt.date(2026, 1, 1),
        },
    )
    expected_store.generate_occurrences(
        engine, start=dt.date(2026, 8, 1), horizon_days=90
    )
    add_transaction(engine, "1234", dt.date(2026, 8, 5), "CHASE MTG PAYMENT", -2444.00)
    add_transaction(engine, "1234", dt.date(2026, 9, 5), "CHASE MTG PAYMENT", -2444.00)
    add_transaction(engine, "5678", dt.date(2026, 7, 15), "PLANET FITNESS", -40.00)
    return mortgage, pmi


def occurrence_id(engine, series_name, due_date):
    df = expected_store.get_occurrence_status_df(engine, due_date, due_date)
    return int(df[df["series_name"] == series_name].iloc[0]["occurrence_id"])


def txn_payload(account_id, post_date, description, amount, occurrence=0):
    return {
        "account_id": account_id,
        "post_date": post_date.isoformat(),
        "description": description,
        "amount": amount,
        "occurrence": occurrence,
    }


# %%
# State #


def test_state_lists_window_and_suggestions(engine):
    seed(engine)
    state = pairing_web.build_state(engine, START, END, {"1234": "Checking"})

    assert [o["series"] for o in state["occurrences"]] == [
        "Mortgage",
        "PMI",
        "Gym",
        "Mortgage",
        "PMI",
        "Gym",
    ]
    # Aug 6 is the oldest open bill, so the board reaches to Jul 7 and the Jul 15
    # gym charge comes along with it
    assert [t["post_date"] for t in state["transactions"]] == [
        "2026-07-15",
        "2026-08-05",
        "2026-09-05",
    ]
    assert state["transactions"][0]["account"] == "5678"
    assert state["transactions"][1]["account"] == "Checking"
    assert state["matches"] == []

    september_mortgage = next(
        o
        for o in state["occurrences"]
        if o["due_date"] == "2026-09-06" and o["series"] == "Mortgage"
    )
    assert september_mortgage["status"] == "unpaid"
    # the Sep 5 payment is the top suggestion for the Sep 6 mortgage; the Aug one is too far
    assert [s["txn_key"] for s in september_mortgage["suggestions"]] == [
        state["transactions"][2]["key"]
    ]
    assert json.dumps(
        state, default=pairing_web._json_default
    )  # nothing unserializable


def test_open_occurrences_before_the_window_are_carried_in(engine):
    """An unpaid bill never scrolls off the board; a paid or skipped one does."""
    seed(engine)
    august_mortgage = occurrence_id(engine, "Mortgage", dt.date(2026, 8, 6))
    august_pmi = occurrence_id(engine, "PMI", dt.date(2026, 8, 6))
    august_gym = occurrence_id(engine, "Gym", dt.date(2026, 8, 15))
    expected_store.add_match(
        engine,
        august_mortgage,
        {
            "account_id": "1234",
            "post_date": dt.date(2026, 8, 5),
            "description": "CHASE MTG PAYMENT",
            "amount": -2444.00,
            "occurrence": 0,
        },
        source="manual",
    )
    expected_store.skip_occurrence(engine, august_gym, "cancelled")

    state = pairing_web.build_state(engine, dt.date(2026, 9, 1), END, {})
    by_id = {o["id"]: o for o in state["occurrences"]}
    assert august_pmi in by_id and by_id[august_pmi]["before_window"] is True
    assert by_id[august_pmi]["status"] == "unpaid"
    # the board reaches back a month before the oldest open bill (Aug 6 -> Jul 7)
    assert state["window"]["effective_start"] == "2026-07-07"
    assert state["window"]["start"] == "2026-09-01"
    # transactions come along from there; settled bills from back then do not
    assert august_mortgage not in by_id  # paid before the window
    assert august_gym not in by_id  # skipped before the window
    assert [t["post_date"] for t in state["transactions"]] == [
        "2026-07-15",
        "2026-08-05",
        "2026-09-05",
    ]
    september = [o for o in state["occurrences"] if not o["before_window"]]
    assert all(o["due_date"] >= "2026-09-01" for o in september)
    # the PMI's own payment is now a suggestion for it (it is claimed whole by
    # the mortgage, so it is not "unmatched", but it is on the board to pair)
    assert any(t["post_date"] == "2026-08-05" for t in state["transactions"])


def test_kept_occurrences_stay_on_the_board_after_they_settle(engine):
    """The page keeps what it touched: a carried-in bill that just got paid
    stays visible so a second charge can be added to it."""
    seed(engine)
    august_pmi = occurrence_id(engine, "PMI", dt.date(2026, 8, 6))
    expected_store.add_match(
        engine,
        august_pmi,
        {
            "account_id": "1234",
            "post_date": dt.date(2026, 8, 5),
            "description": "CHASE MTG PAYMENT",
            "amount": -2444.00,
            "occurrence": 0,
        },
        source="manual",
    )
    without = pairing_web.build_state(engine, dt.date(2026, 9, 1), END, {})
    assert august_pmi not in {o["id"] for o in without["occurrences"]}
    kept = pairing_web.build_state(
        engine, dt.date(2026, 9, 1), END, {}, keep_ids={august_pmi}
    )
    row = next(o for o in kept["occurrences"] if o["id"] == august_pmi)
    assert row["status"] == "paid" and row["before_window"] is True


def test_pad_applies_to_the_oldest_open_bill_even_inside_the_window(engine):
    """With nothing open before the window, the board still reaches a month
    before the oldest open bill (Sep 6 -> Aug 7), but no further."""
    seed(engine)
    for series_name, due in [
        ("Mortgage", dt.date(2026, 8, 6)),
        ("PMI", dt.date(2026, 8, 6)),
        ("Gym", dt.date(2026, 8, 15)),
    ]:
        expected_store.skip_occurrence(
            engine, occurrence_id(engine, series_name, due), "n/a"
        )
    state = pairing_web.build_state(engine, dt.date(2026, 9, 1), END, {})
    assert state["window"]["effective_start"] == "2026-08-07"
    assert all(o["due_date"] >= "2026-09-01" for o in state["occurrences"])
    assert [t["post_date"] for t in state["transactions"]] == ["2026-09-05"]


def test_state_flags_matches_whose_other_end_is_outside_the_window(engine):
    seed(engine)
    august = occurrence_id(engine, "Mortgage", dt.date(2026, 8, 6))
    expected_store.add_match(
        engine,
        august,
        {
            "account_id": "1234",
            "post_date": dt.date(2026, 8, 5),
            "description": "CHASE MTG PAYMENT",
            "amount": -2444.00,
            "occurrence": 0,
        },
        source="manual",
    )
    # a window holding the transaction but not the August occurrence
    state = pairing_web.build_state(
        engine, dt.date(2026, 8, 5), dt.date(2026, 8, 5), {}
    )
    assert len(state["occurrences"]) == 0
    assert len(state["matches"]) == 1
    match = state["matches"][0]
    assert match["txn_visible"] is True
    assert match["occurrence_visible"] is False
    assert match["series"] == "Mortgage"
    assert match["due_date"] == "2026-08-06"
    assert state["transactions"][0]["match_ids"] == [match["id"]]


def test_state_reports_broken_matches_without_a_transaction_end(engine):
    seed(engine)
    september = occurrence_id(engine, "Mortgage", dt.date(2026, 9, 6))
    expected_store.add_match(
        engine,
        september,
        {
            "account_id": "1234",
            "post_date": dt.date(2026, 9, 5),
            "description": "GONE",
            "amount": -1.00,
            "occurrence": 0,
        },
        source="import_sheet",
    )
    state = pairing_web.build_state(engine, START, END, {})
    broken = next(o for o in state["occurrences"] if o["id"] == september)
    assert broken["status"] == "broken"
    match = next(m for m in state["matches"] if m["occurrence_id"] == september)
    assert match["txn_visible"] is False and match["occurrence_visible"] is True


# %%
# Writes #


def test_apply_match_and_void_go_through_the_store(engine):
    seed(engine)
    september = occurrence_id(engine, "Mortgage", dt.date(2026, 9, 6))
    match_id = pairing_web.apply_match(
        engine,
        {
            "occurrence_id": september,
            "txn": txn_payload(
                "1234", dt.date(2026, 9, 5), "CHASE MTG PAYMENT", -2444.00
            ),
            "matched_amount": None,
            "source": "confirmed_suggestion",
        },
    )
    df = expected_store.get_active_matches_df(engine)
    assert list(df["id"]) == [match_id]
    assert df.iloc[0]["source"] == "confirmed_suggestion"
    assert (
        expected_store.get_occurrence_status_df(
            engine, dt.date(2026, 9, 6), dt.date(2026, 9, 6)
        )
        .set_index("series_name")
        .loc["Mortgage", "status"]
        == "paid"
    )

    pairing_web.apply_void(
        engine, {"match_id": match_id, "note": "unpaired on the pairing board"}
    )
    assert len(expected_store.get_active_matches_df(engine)) == 0
    with engine.connect() as conn:
        row = (
            conn.execute(
                select(db.expected_matches).where(db.expected_matches.c.id == match_id)
            )
            .mappings()
            .one()
        )
    assert (
        row["voided_at"] is not None and row["note"] == "unpaired on the pairing board"
    )


def test_apply_match_rejects_unknown_source(engine):
    seed(engine)
    september = occurrence_id(engine, "Mortgage", dt.date(2026, 9, 6))
    with pytest.raises(ValueError, match="source"):
        pairing_web.apply_match(
            engine,
            {
                "occurrence_id": september,
                "txn": txn_payload(
                    "1234", dt.date(2026, 9, 5), "CHASE MTG PAYMENT", -2444.00
                ),
                "source": "import_sheet",
            },
        )


def test_split_rules_are_the_stores(engine):
    """A whole claim blocks a second claim; two stated shares coexist."""
    seed(engine)
    mortgage = occurrence_id(engine, "Mortgage", dt.date(2026, 9, 6))
    pmi = occurrence_id(engine, "PMI", dt.date(2026, 9, 6))
    txn = txn_payload("1234", dt.date(2026, 9, 5), "CHASE MTG PAYMENT", -2444.00)

    whole = pairing_web.apply_match(
        engine, {"occurrence_id": mortgage, "txn": txn, "matched_amount": None}
    )
    with pytest.raises(ValueError, match="already matched"):
        pairing_web.apply_match(
            engine, {"occurrence_id": pmi, "txn": txn, "matched_amount": None}
        )
    with pytest.raises(ValueError, match="claims this whole transaction"):
        pairing_web.apply_match(
            engine, {"occurrence_id": pmi, "txn": txn, "matched_amount": -150.00}
        )

    pairing_web.apply_void(engine, {"match_id": whole})
    pairing_web.apply_match(
        engine, {"occurrence_id": mortgage, "txn": txn, "matched_amount": -2294.00}
    )
    pairing_web.apply_match(
        engine, {"occurrence_id": pmi, "txn": txn, "matched_amount": -150.00}
    )
    state = pairing_web.build_state(engine, START, END, {})
    statuses = {(o["series"], o["due_date"]): o["status"] for o in state["occurrences"]}
    assert statuses[("Mortgage", "2026-09-06")] == "paid"
    assert statuses[("PMI", "2026-09-06")] == "paid"
    september_payment = next(
        t for t in state["transactions"] if t["post_date"] == "2026-09-05"
    )
    assert len(september_payment["match_ids"]) == 2


def test_apply_skip_needs_a_note_and_unskip_reverses_it(engine):
    seed(engine)
    gym = occurrence_id(engine, "Gym", dt.date(2026, 9, 15))
    with pytest.raises(ValueError, match="note"):
        pairing_web.apply_skip(engine, {"occurrence_id": gym, "note": "  "})

    pairing_web.apply_skip(
        engine, {"occurrence_id": gym, "note": "cancelled in august"}
    )
    state = pairing_web.build_state(engine, START, END, {})
    skipped = next(o for o in state["occurrences"] if o["id"] == gym)
    assert skipped["status"] == "skipped"
    assert skipped["skip_note"] == "cancelled in august"

    pairing_web.apply_unskip(engine, {"occurrence_id": gym})
    state = pairing_web.build_state(engine, START, END, {})
    assert next(o for o in state["occurrences"] if o["id"] == gym)["status"] == "unpaid"


def test_apply_share_turns_a_whole_claim_into_two_shares(engine):
    seed(engine)
    mortgage = occurrence_id(engine, "Mortgage", dt.date(2026, 9, 6))
    pmi = occurrence_id(engine, "PMI", dt.date(2026, 9, 6))
    txn = txn_payload("1234", dt.date(2026, 9, 5), "CHASE MTG PAYMENT", -2444.00)
    whole = pairing_web.apply_match(
        engine, {"occurrence_id": mortgage, "txn": txn, "matched_amount": None}
    )

    result = pairing_web.apply_share(
        engine,
        {
            "match_id": whole,
            "keep_amount": -2294.00,
            "occurrence_id": pmi,
            "matched_amount": -150.00,
        },
    )
    active = expected_store.get_active_matches_df(engine).set_index("id")
    assert whole not in active.index
    assert float(active.loc[result["kept_id"], "matched_amount"]) == -2294.00
    assert int(active.loc[result["kept_id"], "occurrence_id"]) == mortgage
    assert float(active.loc[result["match_id"], "matched_amount"]) == -150.00
    assert int(active.loc[result["match_id"], "occurrence_id"]) == pmi
    statuses = {
        (o["series"], o["due_date"]): o["status"]
        for o in pairing_web.build_state(engine, START, END, {})["occurrences"]
    }
    assert statuses[("Mortgage", "2026-09-06")] == "paid"
    assert statuses[("PMI", "2026-09-06")] == "paid"

    # a share cannot be re-split this way; and a voided claim is refused
    with pytest.raises(ValueError, match="already a share"):
        pairing_web.apply_share(
            engine,
            {
                "match_id": result["kept_id"],
                "keep_amount": -1,
                "occurrence_id": pmi,
                "matched_amount": -1,
            },
        )
    with pytest.raises(ValueError, match="not an active match"):
        pairing_web.apply_share(
            engine,
            {
                "match_id": whole,
                "keep_amount": -1,
                "occurrence_id": pmi,
                "matched_amount": -1,
            },
        )


# %%
# Server #


@pytest.fixture()
def server(engine):
    """A real PairingServer on a free localhost port, stopped after the test."""
    srv = pairing_web.PairingServer(("127.0.0.1", 0), engine, {"1234": "Checking"})
    thread = threading.Thread(
        target=srv.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True
    )
    thread.start()
    try:
        yield srv
    finally:
        srv.shutdown()
        srv.server_close()


def call(srv, path, body=None):
    url = f"http://127.0.0.1:{srv.server_address[1]}{path}"
    if body is None:
        request = urllib.request.Request(url)
    else:
        request = urllib.request.Request(
            url,
            data=json.dumps(body).encode(),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
    try:
        with urllib.request.urlopen(request) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as error:
        return error.code, json.loads(error.read())


def test_server_serves_page_state_and_writes(engine, server):
    seed(engine)
    window = {"start": START.isoformat(), "end": END.isoformat()}

    with urllib.request.urlopen(
        f"http://127.0.0.1:{server.server_address[1]}/"
    ) as response:
        page = response.read().decode()
    assert "pairing.map" in page and "/api/state" in page

    status, state = call(
        server, f"/api/state?start={window['start']}&end={window['end']}"
    )
    assert status == 200 and len(state["occurrences"]) == 6

    september = occurrence_id(engine, "Mortgage", dt.date(2026, 9, 6))
    status, reply = call(
        server,
        "/api/match",
        {
            "occurrence_id": september,
            "txn": txn_payload(
                "1234", dt.date(2026, 9, 5), "CHASE MTG PAYMENT", -2444.00
            ),
            "matched_amount": None,
            "source": "manual",
            "window": window,
        },
    )
    assert status == 200 and reply["ok"] is True
    # the answer IS the database re-read: the line is there and the occurrence is paid
    paid = next(o for o in reply["state"]["occurrences"] if o["id"] == september)
    assert paid["status"] == "paid" and paid["match_ids"] == [reply["match_id"]]
    assert len(expected_store.get_active_matches_df(engine)) == 1

    # the page sends the ids it touched; the server keeps them on the board
    status, kept = call(
        server, f"/api/state?start=2026-09-07&end={window['end']}&keep={september}"
    )
    assert status == 200 and any(o["id"] == september for o in kept["occurrences"])
    status, reply = call(
        server, "/api/void", {"match_id": reply["match_id"], "window": window}
    )
    assert status == 200
    assert (
        next(o for o in reply["state"]["occurrences"] if o["id"] == september)["status"]
        == "unpaid"
    )
    assert len(expected_store.get_active_matches_df(engine)) == 0


def test_server_skips_and_unskips(engine, server):
    seed(engine)
    window = {"start": START.isoformat(), "end": END.isoformat()}
    gym = occurrence_id(engine, "Gym", dt.date(2026, 9, 15))
    status, reply = call(
        server, "/api/skip", {"occurrence_id": gym, "note": "waived", "window": window}
    )
    assert status == 200
    assert (
        next(o for o in reply["state"]["occurrences"] if o["id"] == gym)["status"]
        == "skipped"
    )
    status, reply = call(
        server, "/api/skip", {"occurrence_id": gym, "note": "", "window": window}
    )
    assert status == 400 and "note" in reply["error"]
    status, reply = call(
        server, "/api/unskip", {"occurrence_id": gym, "window": window}
    )
    assert status == 200
    assert (
        next(o for o in reply["state"]["occurrences"] if o["id"] == gym)["status"]
        == "unpaid"
    )


def test_server_answers_store_errors_as_400(engine, server):
    seed(engine)
    window = {"start": START.isoformat(), "end": END.isoformat()}
    mortgage = occurrence_id(engine, "Mortgage", dt.date(2026, 9, 6))
    pmi = occurrence_id(engine, "PMI", dt.date(2026, 9, 6))
    txn = txn_payload("1234", dt.date(2026, 9, 5), "CHASE MTG PAYMENT", -2444.00)
    call(
        server, "/api/match", {"occurrence_id": mortgage, "txn": txn, "window": window}
    )
    status, reply = call(
        server, "/api/match", {"occurrence_id": pmi, "txn": txn, "window": window}
    )
    assert status == 400 and "already matched" in reply["error"]
    status, reply = call(server, "/api/state?start=2026-09-30&end=2026-08-01")
    assert status == 400 and "before start" in reply["error"]


def test_server_exits_when_the_page_says_bye(engine):
    srv = pairing_web.PairingServer(("127.0.0.1", 0), engine, {})
    thread = threading.Thread(
        target=srv.serve_forever, kwargs={"poll_interval": 0.05}, daemon=True
    )
    thread.start()
    watchdog = threading.Thread(
        target=pairing_web._watchdog, args=(srv, 60.0, 0.2), daemon=True
    )
    watchdog.start()
    call(srv, "/api/heartbeat", {})
    call(srv, "/api/bye", {})
    thread.join(timeout=5)
    assert not thread.is_alive()
    assert srv.exit_reason == "the page closed"
    srv.server_close()


# %%
