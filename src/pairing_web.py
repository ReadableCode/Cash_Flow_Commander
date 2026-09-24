"""Two-column pairing board, spawned from the CLI into the browser.

    uv run python src/pairing_web.py               # 45 days back, 45 forward
    uv run python src/pairing_web.py --days-back 90 --days-forward 30
    uv run python src/pairing_web.py --no-browser  # print the URL only

Expected occurrences on the left, real transactions on the right, both laid
out on one date axis so a bill and the payment that covered it sit on the
same line. Every active match is a line across the gutter; drag from a row
on one side to a row on the other to draw a new one. Many-to-many is the
normal case (two gym charges on one occurrence, one mortgage payment paying
the mortgage and the PMI lines) and every line is its own match row.

Every line is written the moment it is confirmed: a drop opens a small card
(whole transaction, or a stated share), Enter writes the match through
expected_store.add_match, and the board reloads from the database. Removing
a line voids the match the same way, and skipping an occurrence (with a
note) or unskipping it is one more write. There is no save button and no
timer; undo is the inverse write (void what was added, re-add what was
voided, unskip what was skipped).

The server is a stdlib HTTP server bound to 127.0.0.1 on a free port. It
serves one self-contained page and a tiny JSON API, and exits by itself when
the tab goes away (the page heartbeats; silence ends the process) or on
Ctrl-C. Nothing here decides anything about matching: writes go through
expected_store, suggestions come from expected_suggest, and both keep their
rules (a whole-transaction claim never coexists with a split, nothing is
matched without a person choosing it).
"""

# %%
# Imports #

import argparse
import datetime
import json
import os
import sys
import threading
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import pandas as pd

_SRC_DIR = os.path.dirname(os.path.abspath(__file__))
if _SRC_DIR not in sys.path:
    sys.path.insert(0, _SRC_DIR)

import bootstrap  # noqa: E402
import db  # noqa: E402
import expected_forecast  # noqa: E402
import expected_store  # noqa: E402
import expected_suggest  # noqa: E402
from cfc_tui import load_account_labels  # noqa: E402

# %%
# Constants #

_REPO_ROOT = os.path.dirname(_SRC_DIR)
TEMPLATE_PATH = os.path.join(_REPO_ROOT, "templates", "pairing_map.html")

DAYS_BACK = 45
DAYS_FORWARD = 45

# Suggestions shown as dashed candidate lines when an unpaid occurrence is
# hovered; the ranking is expected_suggest's, this only caps the count.
SUGGESTIONS_PER_OCCURRENCE = 5

# The page heartbeats every HEARTBEAT_SECONDS; once a page has connected,
# this much silence means the tab is gone and the process exits.
HEARTBEAT_SECONDS = 5
IDLE_EXIT_SECONDS = 20
# A reload sends 'bye' then a fresh heartbeat within a second or two; this
# grace keeps a reload from killing the server.
BYE_GRACE_SECONDS = 3

# Occurrence statuses that still need a person: these are carried onto the
# board from before the window, however old they are.
OPEN_STATUSES = ("unpaid", "broken", "net_zero")
# Lower bound for the carried-in scan; nothing in this system predates it.
EARLIEST_DATE = datetime.date(2000, 1, 1)
# How far before the oldest open occurrence the board reaches, so the
# payment that covered it (even one a few weeks early) is on the right-hand side.
BACKLOG_PAD_DAYS = 30

# Only these may be written as a match source from the page.
MATCH_SOURCES = ("manual", "confirmed_suggestion")


# %%
# Keys #


def txn_key_text(account_id, post_date, description, amount, occurrence) -> str:
    """The transaction natural key as one string, the page's row id.

    Uses the same normalization as expected_store._txn_key so a match row and
    a transaction row that refer to the same transaction get the same id.
    """
    parts = expected_store._txn_key(
        account_id, post_date, description, amount, occurrence
    )
    return "|".join(str(part) for part in parts)


def _json_default(value):
    if isinstance(value, (datetime.date, datetime.datetime)):
        return value.isoformat()
    if hasattr(value, "item"):  # numpy scalars
        return value.item()
    return str(value)


def _clean(value):
    """NaN/NaT -> None so the payload is valid JSON."""
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except (TypeError, ValueError):
        pass
    return value


# %%
# State #


def effective_window_start(df_occurrences, start: datetime.date) -> datetime.date:
    """The requested start, or a month before the oldest open occurrence if older."""
    is_open = df_occurrences["status"].isin(OPEN_STATUSES)
    if not is_open.any():
        return start
    oldest_open = pd.to_datetime(df_occurrences.loc[is_open, "due_date"]).min().date()
    return min(start, oldest_open - datetime.timedelta(days=BACKLOG_PAD_DAYS))


def build_state(
    engine,
    start: datetime.date,
    end: datetime.date,
    account_labels: dict,
    keep_ids: set | None = None,
) -> dict:
    """Everything the board draws for one window, fresh from the database.

    keep_ids are occurrences the page has touched this session (paired,
    skipped, unskipped): they stay on the board whatever their status or
    date, so a carried-in bill that just became paid does not vanish before
    its second charge can be added, and an undo has something to land on.

    The window the caller asked for is only a floor. An unpaid, broken, or
    net-zero bill is still owed no matter how old it is, so the board reaches
    back to the OLDEST open occurrence, minus BACKLOG_PAD_DAYS so the payment
    that usually lands a few days early is on screen too. Transactions are
    shown from that effective start. Occurrences before the requested start
    are shown ONLY if they are open: a skipped or paid bill from back then is
    settled and is not something to pair, so it stays off the board. Those
    carried-in rows are flagged `before_window` so the page can draw the
    backlog block and the divider where the requested window begins.
    Matches are every active match touching either side; a match whose other
    end falls outside the window is still listed, flagged, so the row can
    say "pays Mortgage 2026-08-06 (outside this view)" instead of looking
    unmatched.
    """
    df_occurrences = expected_store.get_occurrence_status_df(engine, EARLIEST_DATE, end)
    effective_start = effective_window_start(df_occurrences, start)
    due_dates = pd.to_datetime(df_occurrences["due_date"]).dt.date
    is_open = df_occurrences["status"].isin(OPEN_STATUSES)
    is_kept = df_occurrences["occurrence_id"].isin(keep_ids or set())
    df_occurrences = df_occurrences[
        (due_dates >= start) | is_open | is_kept
    ].reset_index(drop=True)
    df_transactions = expected_store.get_transactions_with_matches_df(
        engine, effective_start, end
    )
    df_matches = expected_store.get_active_matches_df(engine)

    occurrence_ids = set(int(value) for value in df_occurrences["occurrence_id"])
    txn_keys = set()
    transactions = []
    for _, txn_row in df_transactions.iterrows():
        key = txn_key_text(
            txn_row["account_id"],
            txn_row["post_date"],
            txn_row["description"],
            txn_row["amount"],
            txn_row["occurrence"],
        )
        txn_keys.add(key)
        transactions.append(
            {
                "key": key,
                "account_id": str(txn_row["account_id"]),
                "account": str(
                    account_labels.get(
                        str(txn_row["account_id"]), txn_row["account_id"]
                    )
                ),
                "post_date": pd.to_datetime(txn_row["post_date"]).date().isoformat(),
                "description": str(txn_row["description"]),
                "amount": float(txn_row["amount"]),
                "occurrence": int(txn_row["occurrence"]),
                "match_ids": [],
            }
        )
    txn_by_key = {txn["key"]: txn for txn in transactions}

    # Names for the far end of an out-of-window match.
    series_name_by_occurrence, due_by_occurrence = _occurrence_names(engine)

    matches = []
    for _, match_row in df_matches.iterrows():
        key = txn_key_text(
            match_row["txn_account_id"],
            match_row["txn_post_date"],
            match_row["txn_description"],
            match_row["txn_amount"],
            match_row["txn_occurrence"],
        )
        occurrence_id = int(match_row["occurrence_id"])
        occurrence_visible = occurrence_id in occurrence_ids
        txn_visible = key in txn_keys
        if not occurrence_visible and not txn_visible:
            continue
        matched_amount = _clean(match_row["matched_amount"])
        matches.append(
            {
                "id": int(match_row["id"]),
                "occurrence_id": occurrence_id,
                "txn_key": key,
                "matched_amount": (
                    None if matched_amount is None else float(matched_amount)
                ),
                "source": str(match_row["source"]),
                "matched_at": _json_default(match_row["matched_at"]),
                "note": _clean(match_row["note"]),
                "occurrence_visible": occurrence_visible,
                "txn_visible": txn_visible,
                "series": series_name_by_occurrence.get(occurrence_id, "?"),
                "due_date": _json_default(due_by_occurrence.get(occurrence_id, "")),
                "txn_post_date": pd.to_datetime(match_row["txn_post_date"])
                .date()
                .isoformat(),
                "txn_description": str(match_row["txn_description"]),
                "txn_amount": float(match_row["txn_amount"]),
                "txn_account": str(
                    account_labels.get(
                        str(match_row["txn_account_id"]), match_row["txn_account_id"]
                    )
                ),
            }
        )
        if txn_visible:
            txn_by_key[key]["match_ids"].append(int(match_row["id"]))

    df_unmatched = df_transactions[df_transactions["matched_series"] == ""]
    occurrences = []
    for _, occurrence_row in df_occurrences.iterrows():
        occurrence_id = int(occurrence_row["occurrence_id"])
        status = str(occurrence_row["status"])
        suggestions = []
        if status in ("unpaid", "broken", "net_zero") and len(df_unmatched):
            df_suggested = expected_suggest.suggest_for_occurrence(
                occurrence_row, df_unmatched, top_n=SUGGESTIONS_PER_OCCURRENCE
            )
            for _, suggested_row in df_suggested.iterrows():
                suggestions.append(
                    {
                        "txn_key": txn_key_text(
                            suggested_row["account_id"],
                            suggested_row["post_date"],
                            suggested_row["description"],
                            suggested_row["amount"],
                            suggested_row["occurrence"],
                        ),
                        "score": float(suggested_row["score"]),
                    }
                )
        occurrences.append(
            {
                "id": occurrence_id,
                "series_id": int(occurrence_row["series_id"]),
                "series": str(occurrence_row["series_name"]),
                "category": _clean(occurrence_row["category"]),
                "sub_category": _clean(occurrence_row["sub_category"]),
                "due_date": pd.to_datetime(occurrence_row["due_date"])
                .date()
                .isoformat(),
                "amount": float(occurrence_row["amount"]),
                "status": status,
                "before_window": bool(
                    pd.to_datetime(occurrence_row["due_date"]).date() < start
                ),
                "skip_note": _clean(occurrence_row["skip_note"]),
                "net_amount": float(occurrence_row["net_amount"]),
                "is_transfer": bool(occurrence_row["is_transfer"]),
                "auto_pay_account": _clean(occurrence_row["auto_pay_account_id"]),
                "match_ids": [
                    m["id"] for m in matches if m["occurrence_id"] == occurrence_id
                ],
                "suggestions": suggestions,
            }
        )

    return {
        "window": {
            "start": start.isoformat(),
            "effective_start": effective_start.isoformat(),
            "end": end.isoformat(),
            "today": datetime.date.today().isoformat(),
        },
        "occurrences": occurrences,
        "transactions": transactions,
        "matches": matches,
        "backend": engine.dialect.name,
    }


def _occurrence_names(engine):
    """occurrence_id -> series name, and occurrence_id -> due date."""
    from sqlalchemy import select

    stmt = select(
        db.expected_occurrences.c.id,
        db.expected_series.c.name,
        db.expected_occurrences.c.due_date,
    ).select_from(
        db.expected_occurrences.join(
            db.expected_series,
            db.expected_occurrences.c.series_id == db.expected_series.c.id,
        )
    )
    names, dues = {}, {}
    with engine.connect() as conn:
        for occurrence_id, name, due_date in conn.execute(stmt).all():
            names[int(occurrence_id)] = str(name)
            dues[int(occurrence_id)] = due_date
    return names, dues


# %%
# Writes #


def apply_match(engine, payload: dict) -> int:
    """One new line: payload -> expected_store.add_match. Returns the match id.

    payload: occurrence_id, txn {account_id, post_date, description, amount,
    occurrence}, matched_amount (None = whole transaction), source, note.
    Validation beyond shape (already claimed, split rules) is add_match's.
    """
    txn = payload["txn"]
    txn_dict = {
        "account_id": str(txn["account_id"]),
        "post_date": datetime.date.fromisoformat(str(txn["post_date"])),
        "description": str(txn["description"]),
        "amount": float(txn["amount"]),
        "occurrence": int(txn.get("occurrence", 0)),
    }
    source = payload.get("source", "manual")
    if source not in MATCH_SOURCES:
        raise ValueError(f"source must be one of {MATCH_SOURCES}, got {source!r}")
    matched_amount = payload.get("matched_amount")
    if matched_amount is not None:
        matched_amount = float(matched_amount)
    return expected_store.add_match(
        engine,
        int(payload["occurrence_id"]),
        txn_dict,
        source=source,
        note=payload.get("note"),
        matched_amount=matched_amount,
    )


def apply_void(engine, payload: dict) -> int:
    """One removed line: void the match. Returns the match id."""
    match_id = int(payload["match_id"])
    expected_store.void_match(engine, match_id, note=payload.get("note"))
    return match_id


def apply_skip(engine, payload: dict) -> int:
    """Mark an occurrence as not going to happen. Returns the occurrence id.

    The note is required: a skipped bill with no reason is a bill that was
    forgotten, and the forecast would silently stop counting it.
    """
    occurrence_id = int(payload["occurrence_id"])
    note = str(payload.get("note") or "").strip()
    if not note:
        raise ValueError("a skip needs a note saying why")
    expected_store.skip_occurrence(engine, occurrence_id, note)
    return occurrence_id


def apply_share(engine, payload: dict) -> dict:
    """Turn one bill's whole claim on a transaction into two stated shares.

    The mortgage payment was paired whole to the mortgage; now the PMI line
    wants its part. payload: match_id (the whole claim), keep_amount (what
    the existing bill keeps), occurrence_id + matched_amount (the new bill
    and its share), txn, source, note. The whole claim is voided and re-added
    as a share, then the new share is added — three store writes, in order,
    under the caller's lock. Returns the ids so the page can undo all three.
    """
    match_id = int(payload["match_id"])
    with engine.connect() as conn:
        from sqlalchemy import select

        old = (
            conn.execute(
                select(db.expected_matches).where(db.expected_matches.c.id == match_id)
            )
            .mappings()
            .one_or_none()
        )
    if old is None or old["voided_at"] is not None:
        raise ValueError(f"match {match_id} is not an active match")
    if old["matched_amount"] is not None:
        raise ValueError(f"match {match_id} is already a share; just add another share")
    keep_amount = float(payload["keep_amount"])
    new_amount = float(payload["matched_amount"])
    if keep_amount == 0 or new_amount == 0:
        raise ValueError("both shares must be non-zero")
    txn_dict = {
        "account_id": str(old["txn_account_id"]),
        "post_date": old["txn_post_date"],
        "description": str(old["txn_description"]),
        "amount": float(old["txn_amount"]),
        "occurrence": int(old["txn_occurrence"]),
    }
    expected_store.void_match(
        engine, match_id, note="split into shares on the pairing board"
    )
    kept_id = expected_store.add_match(
        engine,
        int(old["occurrence_id"]),
        txn_dict,
        source=str(old["source"]),
        note=old["note"],
        matched_amount=keep_amount,
    )
    new_id = expected_store.add_match(
        engine,
        int(payload["occurrence_id"]),
        txn_dict,
        source=payload.get("source", "manual"),
        note=payload.get("note"),
        matched_amount=new_amount,
    )
    return {"voided_id": match_id, "kept_id": kept_id, "match_id": new_id}


def apply_unskip(engine, payload: dict) -> int:
    """Put a skipped occurrence back in the queue. Returns the occurrence id."""
    occurrence_id = int(payload["occurrence_id"])
    expected_store.unskip_occurrence(engine, occurrence_id)
    return occurrence_id


# %%
# Server #


class PairingServer(ThreadingHTTPServer):
    """The HTTP server plus the shared state every request handler needs."""

    daemon_threads = True
    allow_reuse_address = True

    def __init__(
        self, address, engine, account_labels: dict, template_path: str = TEMPLATE_PATH
    ):
        super().__init__(address, PairingHandler)
        self.engine = engine
        self.account_labels = account_labels
        self.template_path = template_path
        # One writer at a time; the store's own checks (already claimed,
        # split rules) are read-then-write and must not interleave.
        self.write_lock = threading.Lock()
        self.last_heartbeat: float | None = None
        self.bye_at: float | None = None
        self.exit_reason: str | None = None

    def page_html(self) -> str:
        with open(self.template_path, "r", encoding="utf-8") as handle:
            return handle.read()

    def request_exit(self, reason: str) -> None:
        if self.exit_reason is None:
            self.exit_reason = reason
            threading.Thread(target=self.shutdown, daemon=True).start()


class PairingHandler(BaseHTTPRequestHandler):
    server: PairingServer

    def log_message(self, format, *args):  # noqa: A002 - stdlib signature
        return  # the CLI prints the URL and the writes; request noise is not useful

    # ---- helpers ----

    def _send(self, status: int, body: bytes, content_type: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _send_json(self, status: int, payload: dict) -> None:
        body = json.dumps(payload, default=_json_default).encode("utf-8")
        self._send(status, body, "application/json; charset=utf-8")

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        if length == 0:
            return {}
        return json.loads(self.rfile.read(length).decode("utf-8"))

    def _window(self, params: dict) -> tuple:
        start = datetime.date.fromisoformat(params["start"])
        end = datetime.date.fromisoformat(params["end"])
        if end < start:
            raise ValueError("end is before start")
        return start, end

    def _state(self, params: dict) -> dict:
        start, end = self._window(params)
        keep_text = str(params.get("keep") or "")
        keep_ids = {int(part) for part in keep_text.split(",") if part.strip()}
        return build_state(
            self.server.engine, start, end, self.server.account_labels, keep_ids
        )

    # ---- routes ----

    def do_GET(self) -> None:  # noqa: N802 - stdlib naming
        url = urlparse(self.path)
        if url.path == "/":
            self._send(
                200, self.server.page_html().encode("utf-8"), "text/html; charset=utf-8"
            )
            return
        if url.path == "/api/state":
            params = {key: values[0] for key, values in parse_qs(url.query).items()}
            try:
                self._send_json(200, self._state(params))
            except (KeyError, ValueError) as error:
                self._send_json(400, {"error": str(error)})
            return
        self._send(404, b"not found", "text/plain")

    def do_POST(self) -> None:  # noqa: N802 - stdlib naming
        url = urlparse(self.path)
        try:
            payload = self._read_json()
        except ValueError as error:
            self._send_json(400, {"error": f"bad json: {error}"})
            return

        if url.path == "/api/heartbeat":
            self.server.last_heartbeat = _monotonic()
            self.server.bye_at = None
            self._send_json(200, {"ok": True})
            return
        if url.path == "/api/bye":
            self.server.bye_at = _monotonic()
            self._send_json(200, {"ok": True})
            return
        if url.path == "/api/quit":
            self._send_json(200, {"ok": True})
            self.server.request_exit("quit from the page")
            return
        if url.path == "/api/match":
            self._write(payload, apply_match, "match_id")
            return
        if url.path == "/api/void":
            self._write(payload, apply_void, "match_id")
            return
        if url.path == "/api/skip":
            self._write(payload, apply_skip, "occurrence_id")
            return
        if url.path == "/api/unskip":
            self._write(payload, apply_unskip, "occurrence_id")
            return
        if url.path == "/api/share":
            self._write(payload, apply_share, "result")
            return
        self._send(404, b"not found", "text/plain")

    def _write(self, payload: dict, operation, result_key: str) -> None:
        """Apply one write under the lock, then answer with the fresh state."""
        try:
            with self.server.write_lock:
                result = operation(self.server.engine, payload)
            print(f"  {operation.__name__}: {result_key}={result}", flush=True)
            state = self._state(payload.get("window", {}))
        except (KeyError, ValueError, TypeError) as error:
            self._send_json(400, {"error": str(error)})
            return
        self._send_json(200, {"ok": True, result_key: result, "state": state})


def _monotonic() -> float:
    import time

    return time.monotonic()


def _watchdog(server: PairingServer, idle_seconds: float, bye_grace: float) -> None:
    """End the process when the page has gone quiet."""
    import time

    while server.exit_reason is None:
        time.sleep(0.5)
        now = _monotonic()
        if server.bye_at is not None and now - server.bye_at > bye_grace:
            server.request_exit("the page closed")
            return
        if (
            server.last_heartbeat is not None
            and now - server.last_heartbeat > idle_seconds
        ):
            server.request_exit(f"no heartbeat for {idle_seconds:.0f}s")
            return


def serve(
    engine,
    account_labels: dict,
    start: datetime.date,
    end: datetime.date,
    port: int = 0,
    open_browser: bool = True,
    idle_seconds: float = IDLE_EXIT_SECONDS,
    template_path: str = TEMPLATE_PATH,
) -> str:
    """Run the board until the page goes away or Ctrl-C. Returns the exit reason."""
    server = PairingServer(
        ("127.0.0.1", port), engine, account_labels, template_path=template_path
    )
    host, bound_port = server.server_address[:2]
    url = f"http://{host}:{bound_port}/?start={start.isoformat()}&end={end.isoformat()}"
    print(f"pairing board: {url}", flush=True)
    print("  the process exits when the tab closes, or on Ctrl-C", flush=True)

    if open_browser:
        webbrowser.open(url)
    threading.Thread(
        target=_watchdog, args=(server, idle_seconds, BYE_GRACE_SECONDS), daemon=True
    ).start()
    try:
        server.serve_forever(poll_interval=0.25)
    except KeyboardInterrupt:
        server.exit_reason = "Ctrl-C"
    finally:
        server.server_close()
    print(f"pairing board closed: {server.exit_reason}", flush=True)
    return server.exit_reason or "stopped"


# %%
# CLI #


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--days-back", type=int, default=DAYS_BACK)
    parser.add_argument("--days-forward", type=int, default=DAYS_FORWARD)
    parser.add_argument("--port", type=int, default=0, help="default: a free port")
    parser.add_argument(
        "--no-browser", action="store_true", help="print the URL, do not open it"
    )
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    engine = db.get_engine()
    bootstrap.ensure_schema(engine)
    today = datetime.date.today()
    serve(
        engine,
        load_account_labels(),
        start=today - datetime.timedelta(days=args.days_back),
        end=today + datetime.timedelta(days=args.days_forward),
        port=args.port,
        open_browser=not args.no_browser,
    )
    # One rebuild per session, after the board is gone: every pair, split,
    # skip and undo changed what the forecast counts, and Grafana reads the
    # table, not the board.
    expected_forecast.rebuild_after(engine, "the pairing board closed")
    return 0


if __name__ == "__main__":
    sys.exit(main())


# %%
