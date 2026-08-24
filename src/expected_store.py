"""Read and write the expected-transaction tables.

Three tables, one rule each:

- expected_series:      append-only. Close a series and add a new one; never edit.
- expected_occurrences: generated or imported; the only manual override is skip.
- expected_matches:     written by a person (or the sheet import), voided never deleted.

Paid status is NOT stored anywhere. get_occurrence_status_df derives it fresh
every time by joining active matches against live transactions, so a
transaction Chase restated or removed flips its occurrence back to unpaid
without anyone having to notice first.
"""

# %%
# Imports #

import datetime
import os
import sys
from typing import Any

import pandas as pd
from sqlalchemy import delete, func, insert, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import Engine

_SRC_DIR = os.path.dirname(os.path.abspath(__file__))
if _SRC_DIR not in sys.path:
    sys.path.insert(0, _SRC_DIR)

import db  # noqa: E402
import expected_schedule  # noqa: E402

# %%
# Constants #

# How far an actual may drift from the expected amount and still be flagged
# as normal, when the series does not set its own amount_tolerance.
DEFAULT_AMOUNT_TOLERANCE = 5.00

# How far ahead generate_occurrences fills in future occurrences.
DEFAULT_HORIZON_DAYS = 365 * 2


# %%
# Helpers #


def _now() -> datetime.datetime:
    return datetime.datetime.now()


def _amount_key(amount: Any) -> str:
    """Amounts as fixed 2-decimal strings so Decimal/float compare equal."""
    return f"{float(amount):.2f}"


def _txn_key(
    account_id: Any, post_date: Any, description: Any, amount: Any, occurrence: Any
):
    """The transaction natural key, normalized the same way for both sides."""
    if isinstance(post_date, str):
        date_text = post_date
    else:
        date_text = post_date.isoformat()
    return (
        str(account_id),
        date_text,
        str(description),
        _amount_key(amount),
        int(occurrence),
    )


def _occurrence_insert(engine: Engine):
    """Dialect-specific INSERT so on_conflict_do_nothing is available."""
    if engine.dialect.name == "sqlite":
        return sqlite_insert(db.expected_occurrences)
    if engine.dialect.name == "postgresql":
        return pg_insert(db.expected_occurrences)
    raise ValueError(
        f"expected_store only supports sqlite and postgresql, got {engine.dialect.name!r}"
    )


# %%
# Series #


def add_series(engine: Engine, series: dict) -> int:
    """Insert one expected_series row and return its id."""
    with engine.begin() as conn:
        result = conn.execute(insert(db.expected_series).values(**series))
        return int(result.inserted_primary_key[0])


def close_series(engine: Engine, series_id: int, active_until: datetime.date) -> int:
    """Close a series as of a date, and drop its now-wrong future occurrences.

    Closing is the ONLY edit a series ever gets. Its generated occurrences
    past the close date were machine-made from a schedule that no longer
    applies (a replacement series will generate its own), so they are deleted
    — unless a person touched one (matched, skipped, or added by hand), in
    which case it stays visible on the pairing screen. Returns the number of
    occurrences deleted.
    """
    matched_occurrence_ids = select(db.expected_matches.c.occurrence_id).where(
        db.expected_matches.c.voided_at.is_(None)
    )
    stale = select(db.expected_occurrences.c.id).where(
        db.expected_occurrences.c.series_id == series_id,
        db.expected_occurrences.c.due_date > active_until,
        db.expected_occurrences.c.source == "generated",
        db.expected_occurrences.c.skipped_at.is_(None),
        db.expected_occurrences.c.id.not_in(matched_occurrence_ids),
    )
    with engine.begin() as conn:
        conn.execute(
            update(db.expected_series)
            .where(db.expected_series.c.id == series_id)
            .values(active_until=active_until)
        )
        stale_ids = [row[0] for row in conn.execute(stale)]
        if stale_ids:
            conn.execute(
                delete(db.expected_occurrences).where(
                    db.expected_occurrences.c.id.in_(stale_ids)
                )
            )
    return len(stale_ids)


def get_series_df(engine: Engine, include_closed: bool = True) -> pd.DataFrame:
    """All series as a DataFrame, oldest first."""
    stmt = select(db.expected_series).order_by(
        db.expected_series.c.active_from, db.expected_series.c.name
    )
    with engine.connect() as conn:
        df_series = pd.read_sql(stmt, conn)

    if not include_closed:
        today = datetime.date.today()
        is_open = df_series["active_until"].isna() | (
            pd.to_datetime(df_series["active_until"]).dt.date >= today
        )
        df_series = df_series[is_open]

    return df_series


def get_last_due_dates(engine: Engine) -> dict:
    """series_id -> its latest occurrence due date.

    How the series screen decides a 'once' series is done: its last date has
    passed. A series with no occurrences yet is simply absent from the dict.
    """
    stmt = select(
        db.expected_occurrences.c.series_id,
        func.max(db.expected_occurrences.c.due_date),
    ).group_by(db.expected_occurrences.c.series_id)
    with engine.connect() as conn:
        return dict(conn.execute(stmt).all())


# %%
# Occurrences #


def generate_occurrences(
    engine: Engine,
    start: datetime.date | None = None,
    horizon_days: int = DEFAULT_HORIZON_DAYS,
) -> int:
    """Fill in future occurrences for every series, from start (default today).

    Inserts only what is missing — an occurrence that already exists (imported,
    manually added, amount-edited, matched, or skipped) is left alone. Safe to
    run any time, from anywhere.
    """
    if start is None:
        start = datetime.date.today()
    end = start + datetime.timedelta(days=horizon_days)

    df_series = get_series_df(engine)

    rows_to_insert = []
    for _, series_row in df_series.iterrows():
        due_dates = expected_schedule.due_dates_for_series(series_row, start, end)
        for due_date in due_dates:
            rows_to_insert.append(
                {
                    "series_id": int(series_row["id"]),
                    "due_date": due_date,
                    "amount": series_row["amount"],
                    "source": "generated",
                }
            )

    if not rows_to_insert:
        return 0

    insert_stmt = _occurrence_insert(engine).on_conflict_do_nothing(
        index_elements=["series_id", "due_date"]
    )
    with engine.begin() as conn:
        conn.execute(insert_stmt, rows_to_insert)
    return len(rows_to_insert)


def add_occurrence(
    engine: Engine,
    series_id: int,
    due_date: datetime.date,
    amount: float,
    source: str = "manual",
) -> None:
    """Add one occurrence directly (for 'once' series and imports)."""
    insert_stmt = _occurrence_insert(engine).on_conflict_do_nothing(
        index_elements=["series_id", "due_date"]
    )
    with engine.begin() as conn:
        conn.execute(
            insert_stmt,
            [
                {
                    "series_id": series_id,
                    "due_date": due_date,
                    "amount": amount,
                    "source": source,
                }
            ],
        )


def set_occurrence_amount(engine: Engine, occurrence_id: int, amount: float) -> None:
    """Override one occurrence's expected amount (a known statement balance)."""
    with engine.begin() as conn:
        conn.execute(
            update(db.expected_occurrences)
            .where(db.expected_occurrences.c.id == occurrence_id)
            .values(amount=amount)
        )


def skip_occurrence(engine: Engine, occurrence_id: int, note: str) -> None:
    """Mark an occurrence as not-going-to-happen (covered early, waived, etc.)."""
    with engine.begin() as conn:
        conn.execute(
            update(db.expected_occurrences)
            .where(db.expected_occurrences.c.id == occurrence_id)
            .values(skipped_at=_now(), skip_note=note)
        )


def unskip_occurrence(engine: Engine, occurrence_id: int) -> None:
    with engine.begin() as conn:
        conn.execute(
            update(db.expected_occurrences)
            .where(db.expected_occurrences.c.id == occurrence_id)
            .values(skipped_at=None, skip_note=None)
        )


# %%
# Matches #


def add_match(
    engine: Engine,
    occurrence_id: int,
    txn: dict,
    source: str,
    note: str | None = None,
    matched_amount: float | None = None,
) -> int:
    """Pair one real transaction to one occurrence.

    txn is a dict with the transaction's natural key: account_id, post_date,
    description, amount, occurrence.

    matched_amount is the portion of the transaction that belongs to this
    occurrence; None means all of it. A transaction normally belongs to one
    occurrence — but one bill can pay several expected items (the mortgage
    payment also covers the PMI line), and then EVERY match on that
    transaction must state its share, so nothing is silently double-counted.
    """
    stmt = select(db.expected_matches).where(
        db.expected_matches.c.voided_at.is_(None),
        db.expected_matches.c.txn_account_id == str(txn["account_id"]),
        db.expected_matches.c.txn_post_date == txn["post_date"],
        db.expected_matches.c.txn_description == str(txn["description"]),
        db.expected_matches.c.txn_occurrence == int(txn["occurrence"]),
    )
    with engine.connect() as conn:
        existing = [
            match_row
            for match_row in conn.execute(stmt).mappings()
            if _amount_key(match_row["txn_amount"]) == _amount_key(txn["amount"])
        ]
    if existing:
        claimed_by = existing[0]["occurrence_id"]
        if matched_amount is None:
            raise ValueError(
                f"Transaction already matched to occurrence {claimed_by}. To share one "
                "bill across several expected items, give each match a split amount; "
                "otherwise void the old match first."
            )
        for match_row in existing:
            if match_row["matched_amount"] is None:
                raise ValueError(
                    f"Occurrence {claimed_by} claims this whole transaction. Void that "
                    "match and re-add it with a split amount before sharing it."
                )

    with engine.begin() as conn:
        result = conn.execute(
            insert(db.expected_matches).values(
                occurrence_id=occurrence_id,
                txn_account_id=str(txn["account_id"]),
                txn_post_date=txn["post_date"],
                txn_description=str(txn["description"]),
                txn_amount=txn["amount"],
                txn_occurrence=int(txn["occurrence"]),
                matched_amount=matched_amount,
                source=source,
                matched_at=_now(),
                note=note,
            )
        )
        return int(result.inserted_primary_key[0])


def void_match(engine: Engine, match_id: int, note: str | None = None) -> None:
    """Retire a match without deleting it — the pairing history stays visible."""
    values: dict[str, Any] = {"voided_at": _now()}
    if note is not None:
        values["note"] = note
    with engine.begin() as conn:
        conn.execute(
            update(db.expected_matches)
            .where(db.expected_matches.c.id == match_id)
            .values(**values)
        )


def get_active_matches_df(engine: Engine) -> pd.DataFrame:
    """All non-voided matches."""
    stmt = select(db.expected_matches).where(db.expected_matches.c.voided_at.is_(None))
    with engine.connect() as conn:
        return pd.read_sql(stmt, conn)


# %%
# Derived status #


def get_occurrence_status_df(
    engine: Engine, start: datetime.date, end: datetime.date
) -> pd.DataFrame:
    """Every occurrence due in [start, end] with its status derived right now.

    Statuses:
    - skipped:  manually skipped, with a note.
    - unpaid:   no active matches.
    - broken:   has a match whose transaction no longer exists (restated or
                removed by a re-download) — needs re-pairing.
    - net_zero: its matches exist and resolve, but they cancel out (a payment
                plus its reversal) or point the wrong way — effectively unpaid.
    - paid:     matches resolve and their net moves the expected direction.

    Transfer legs (matches on the series' transfer_account_id) are excluded
    from the net: a card payoff matches -X from the bank and +X on the card,
    and the bank leg alone is what says the payoff happened.

    Also returns net_amount (what actually moved) and amount_diff
    (net - expected) so drift is visible without another query.
    """
    stmt = (
        select(
            db.expected_occurrences.c.id.label("occurrence_id"),
            db.expected_occurrences.c.series_id,
            db.expected_occurrences.c.due_date,
            db.expected_occurrences.c.amount,
            db.expected_occurrences.c.skipped_at,
            db.expected_occurrences.c.skip_note,
            db.expected_series.c.name.label("series_name"),
            db.expected_series.c.category,
            db.expected_series.c.sub_category,
            db.expected_series.c.auto_pay_account_id,
            db.expected_series.c.is_transfer,
            db.expected_series.c.transfer_account_id,
            db.expected_series.c.amount_tolerance,
            db.expected_series.c.match_pattern,
        )
        .select_from(
            db.expected_occurrences.join(
                db.expected_series,
                db.expected_occurrences.c.series_id == db.expected_series.c.id,
            )
        )
        .where(
            db.expected_occurrences.c.due_date >= start,
            db.expected_occurrences.c.due_date <= end,
        )
        .order_by(db.expected_occurrences.c.due_date, db.expected_series.c.name)
    )
    with engine.connect() as conn:
        df_occurrences = pd.read_sql(stmt, conn)

    df_matches = get_active_matches_df(engine)

    # The set of live transaction keys, so each match can be checked for resolution.
    txn_stmt = select(
        db.transactions.c.account_id,
        db.transactions.c.post_date,
        db.transactions.c.description,
        db.transactions.c.amount,
        db.transactions.c.occurrence,
    )
    with engine.connect() as conn:
        live_txn_rows = conn.execute(txn_stmt).all()
    live_txn_keys = set()
    for txn_row in live_txn_rows:
        live_txn_keys.add(_txn_key(*txn_row))

    statuses = []
    net_amounts = []
    match_counts = []
    for _, occurrence_row in df_occurrences.iterrows():
        occurrence_matches = df_matches[
            df_matches["occurrence_id"] == occurrence_row["occurrence_id"]
        ]
        status, net_amount = _derive_one_status(
            occurrence_row, occurrence_matches, live_txn_keys
        )
        statuses.append(status)
        net_amounts.append(net_amount)
        match_counts.append(len(occurrence_matches))

    df_occurrences["status"] = statuses
    df_occurrences["net_amount"] = net_amounts
    df_occurrences["match_count"] = match_counts
    df_occurrences["amount_diff"] = df_occurrences["net_amount"] - df_occurrences[
        "amount"
    ].astype(float)
    return df_occurrences


def _derive_one_status(
    occurrence_row, occurrence_matches: pd.DataFrame, live_txn_keys: set
):
    """Status and net amount for one occurrence. See get_occurrence_status_df."""
    if pd.notna(occurrence_row["skipped_at"]):
        return "skipped", 0.0

    if len(occurrence_matches) == 0:
        return "unpaid", 0.0

    net_amount = 0.0
    for _, match_row in occurrence_matches.iterrows():
        match_key = _txn_key(
            match_row["txn_account_id"],
            match_row["txn_post_date"],
            match_row["txn_description"],
            match_row["txn_amount"],
            match_row["txn_occurrence"],
        )
        if match_key not in live_txn_keys:
            return "broken", 0.0
        # Transfer legs don't count toward the net — only the cash side does.
        if pd.notna(occurrence_row["transfer_account_id"]) and str(
            match_row["txn_account_id"]
        ) == str(occurrence_row["transfer_account_id"]):
            continue
        # A split match contributes only its share of the transaction.
        if pd.notna(match_row["matched_amount"]):
            net_amount = net_amount + float(match_row["matched_amount"])
        else:
            net_amount = net_amount + float(match_row["txn_amount"])

    expected_amount = float(occurrence_row["amount"])
    if expected_amount == 0:
        return "paid", net_amount
    if abs(net_amount) < 0.005:
        return "net_zero", net_amount
    if (net_amount > 0) != (expected_amount > 0):
        return "net_zero", net_amount
    return "paid", net_amount


# %%
# Unmatched transactions #


def get_unmatched_transactions_df(
    engine: Engine, start: datetime.date, end: datetime.date
) -> pd.DataFrame:
    """Transactions in [start, end] not actively matched to any occurrence.

    This is the right-hand pane of the pairing screen: everything across all
    accounts that could still be claimed by an expected occurrence.
    """
    stmt = (
        select(
            db.transactions.c.account_id,
            db.transactions.c.account_kind,
            db.transactions.c.post_date,
            db.transactions.c.description,
            db.transactions.c.amount,
            db.transactions.c.occurrence,
        )
        .where(
            db.transactions.c.post_date >= start,
            db.transactions.c.post_date <= end,
        )
        .order_by(db.transactions.c.post_date, db.transactions.c.account_id)
    )
    with engine.connect() as conn:
        df_transactions = pd.read_sql(stmt, conn)

    df_matches = get_active_matches_df(engine)
    matched_keys = set()
    for _, match_row in df_matches.iterrows():
        matched_keys.add(
            _txn_key(
                match_row["txn_account_id"],
                match_row["txn_post_date"],
                match_row["txn_description"],
                match_row["txn_amount"],
                match_row["txn_occurrence"],
            )
        )

    is_unmatched = []
    for _, txn_row in df_transactions.iterrows():
        txn_key = _txn_key(
            txn_row["account_id"],
            txn_row["post_date"],
            txn_row["description"],
            txn_row["amount"],
            txn_row["occurrence"],
        )
        is_unmatched.append(txn_key not in matched_keys)

    return df_transactions[is_unmatched].reset_index(drop=True)


def get_transactions_with_matches_df(
    engine: Engine, start: datetime.date, end: datetime.date
) -> pd.DataFrame:
    """Every transaction in [start, end] with the series name(s) it pays.

    The transactions-browser view: date-ordered actuals across all accounts,
    with `matched_series` empty for anything still unclaimed.
    """
    stmt = (
        select(
            db.transactions.c.account_id,
            db.transactions.c.post_date,
            db.transactions.c.description,
            db.transactions.c.amount,
            db.transactions.c.occurrence,
        )
        .where(
            db.transactions.c.post_date >= start,
            db.transactions.c.post_date <= end,
        )
        .order_by(db.transactions.c.post_date, db.transactions.c.account_id)
    )
    with engine.connect() as conn:
        df_transactions = pd.read_sql(stmt, conn)

    occurrence_stmt = select(
        db.expected_occurrences.c.id, db.expected_series.c.name
    ).select_from(
        db.expected_occurrences.join(
            db.expected_series,
            db.expected_occurrences.c.series_id == db.expected_series.c.id,
        )
    )
    with engine.connect() as conn:
        series_name_by_occurrence = dict(conn.execute(occurrence_stmt).all())

    series_by_txn_key: dict = {}
    for _, match_row in get_active_matches_df(engine).iterrows():
        txn_key = _txn_key(
            match_row["txn_account_id"],
            match_row["txn_post_date"],
            match_row["txn_description"],
            match_row["txn_amount"],
            match_row["txn_occurrence"],
        )
        series_name = series_name_by_occurrence.get(match_row["occurrence_id"], "?")
        series_by_txn_key.setdefault(txn_key, []).append(series_name)

    matched_series = []
    for _, txn_row in df_transactions.iterrows():
        txn_key = _txn_key(
            txn_row["account_id"],
            txn_row["post_date"],
            txn_row["description"],
            txn_row["amount"],
            txn_row["occurrence"],
        )
        matched_series.append(", ".join(series_by_txn_key.get(txn_key, [])))
    df_transactions["matched_series"] = matched_series
    return df_transactions


# %%
