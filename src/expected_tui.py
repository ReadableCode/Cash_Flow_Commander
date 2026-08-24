"""Terminal UI for pairing expected occurrences with real transactions.

    uv run python src/expected_tui.py

One app, three screens:

- Pairing (default): every occurrence in a date window — past AND upcoming —
  with its derived status. Select one and press m to see candidate
  transactions, suggestions ranked first, and pick the one(s) that paid it.
  Nothing is ever matched without a person choosing it here. When one bill
  pays several expected items (the mortgage payment that also covers the PMI
  line), press x instead of space to claim a stated share of a transaction.
- Transactions (press t): every real transaction, date-ordered, showing which
  series each one pays — the place to spot reversals and strays.
- Series (press e): the bills themselves. New series, close a series, or
  close-and-replace. There is deliberately no edit — replacing is what keeps
  old matches pointing at the truth they were made against.

All decisions live in expected_store / expected_suggest; this file only draws
tables and asks questions.
"""

# %%
# Imports #

import datetime
import os
import sys

import pandas as pd
import yaml
from rich.text import Text
from sqlalchemy import func, select
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Vertical, VerticalScroll
from textual.screen import ModalScreen, Screen
from textual.widgets import Button, DataTable, Footer, Header, Input, Label, Static

_SRC_DIR = os.path.dirname(os.path.abspath(__file__))
if _SRC_DIR not in sys.path:
    sys.path.insert(0, _SRC_DIR)

import db  # noqa: E402
import expected_store  # noqa: E402
import expected_suggest  # noqa: E402

# %%
# Constants #

DAYS_BACK = 45
DAYS_FORWARD = 45
WINDOW_STEP_DAYS = 30

# Candidate transactions are pulled this far either side of the due date.
CANDIDATE_WINDOW_DAYS = 20

STATUS_STYLES = {
    "paid": "green",
    "unpaid": "yellow",
    "skipped": "dim",
    "broken": "red",
    "net_zero": "red",
}

_REPO_ROOT = os.path.dirname(_SRC_DIR)
PROVIDERS_YAML_PATH = os.path.join(_REPO_ROOT, "providers.local.yaml")


# %%
# Helpers #


def load_account_labels() -> dict:
    """account_id -> friendly label, from providers.local.yaml (chase section)."""
    if not os.path.isfile(PROVIDERS_YAML_PATH):
        return {}
    with open(PROVIDERS_YAML_PATH, "r", encoding="utf-8") as handle:
        config = yaml.safe_load(handle) or {}
    accounts = config.get("chase", {}).get("external_ids", {}).get("accounts", {})
    labels = {}
    for account_id, details in accounts.items():
        labels[str(account_id)] = details.get("label", str(account_id))
    return labels


def parse_date(text: str) -> datetime.date | None:
    try:
        return datetime.date.fromisoformat(text.strip())
    except ValueError:
        return None


def styled_status(status: str) -> Text:
    return Text(status, style=STATUS_STYLES.get(status, ""))


# %%
# Small modals #


class TextPromptScreen(ModalScreen):
    """Ask one line of text; dismisses with the text, or None on escape."""

    BINDINGS = [("escape", "cancel", "Cancel")]

    def __init__(self, prompt: str, initial: str = "") -> None:
        super().__init__()
        self.prompt = prompt
        self.initial = initial

    def compose(self) -> ComposeResult:
        with Vertical(classes="modal_box"):
            yield Label(self.prompt)
            yield Input(value=self.initial, id="prompt_input")

    def on_mount(self) -> None:
        self.query_one("#prompt_input", Input).focus()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        self.dismiss(event.value)

    def action_cancel(self) -> None:
        self.dismiss(None)


class ConfirmScreen(ModalScreen):
    """Yes/No question; dismisses with True or False."""

    BINDINGS = [
        ("escape", "cancel", "Cancel"),
        ("y", "yes", "Yes"),
        ("n", "cancel", "No"),
    ]

    def __init__(self, question: str) -> None:
        super().__init__()
        self.question = question

    def compose(self) -> ComposeResult:
        with Vertical(classes="modal_box"):
            yield Label(self.question)
            yield Button("Yes (y)", id="yes")
            yield Button("No (n)", id="no")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        self.dismiss(event.button.id == "yes")

    def action_yes(self) -> None:
        self.dismiss(True)

    def action_cancel(self) -> None:
        self.dismiss(False)


# %%
# Match screen #


class MatchScreen(ModalScreen):
    """Pick the transaction(s) that paid one occurrence.

    Suggestions come first with their score; below them, every other
    transaction near the due date — including already-matched ones, marked
    with the series they pay, so a bill that covers several expected items
    can be shared. Space toggles a whole-transaction claim, x claims a stated
    share (the PMI inside the mortgage payment), enter saves, escape leaves.
    """

    # priority=True: the focused DataTable also wants space and enter (row
    # selection); without priority these bindings never fire and the screen
    # looks keyless.
    BINDINGS = [
        Binding("space", "toggle", "Select", priority=True),
        Binding("x", "split_claim", "Claim a share", priority=True),
        Binding("enter", "save", "Save matches", priority=True),
        Binding("escape", "cancel", "Cancel", priority=True),
    ]

    def __init__(self, app_state, occurrence_row) -> None:
        super().__init__()
        self.app_state = app_state
        self.occurrence_row = occurrence_row
        self.candidates = []  # list of dicts: txn fields + suggested flag
        self.selected_indexes = set()

    def compose(self) -> ComposeResult:
        row = self.occurrence_row
        header = (
            f"{row['series_name']}  due {row['due_date']}  "
            f"expected {float(row['amount']):.2f}  [{row['status']}]   "
            "(select every transaction that paid this — both gym charges "
            "belong to the one occurrence)"
        )
        with Vertical(classes="match_box"):
            yield Label(header)
            yield DataTable(id="candidates_table")
        yield Footer()

    def on_mount(self) -> None:
        self.load_candidates()
        table = self.query_one("#candidates_table", DataTable)
        table.cursor_type = "row"
        table.zebra_stripes = True
        table.add_columns(
            "sel", "date", "account", "amount", "score", "pays", "description"
        )
        self.render_candidates()
        table.focus()

    def load_candidates(self) -> None:
        due_date = pd.to_datetime(self.occurrence_row["due_date"]).date()
        start = due_date - datetime.timedelta(days=CANDIDATE_WINDOW_DAYS)
        end = due_date + datetime.timedelta(days=CANDIDATE_WINDOW_DAYS)
        df_transactions = expected_store.get_transactions_with_matches_df(
            self.app_state.engine, start, end
        )
        df_unmatched = df_transactions[df_transactions["matched_series"] == ""]

        df_suggested = expected_suggest.suggest_for_occurrence(
            self.occurrence_row, df_unmatched
        )
        suggested_keys = set()
        self.candidates = []
        for _, txn_row in df_suggested.iterrows():
            self.candidates.append(self._candidate(txn_row, score=txn_row["score"]))
            suggested_keys.add(self._txn_id(txn_row))
        for _, txn_row in df_transactions.iterrows():
            if self._txn_id(txn_row) not in suggested_keys:
                self.candidates.append(self._candidate(txn_row, score=None))

    def _candidate(self, txn_row, score) -> dict:
        return {
            "account_id": str(txn_row["account_id"]),
            "post_date": pd.to_datetime(txn_row["post_date"]).date(),
            "description": str(txn_row["description"]),
            "amount": float(txn_row["amount"]),
            "occurrence": int(txn_row["occurrence"]),
            "score": score,
            "matched_series": str(txn_row.get("matched_series", "") or ""),
        }

    def _txn_id(self, txn_row) -> tuple:
        return (
            str(txn_row["account_id"]),
            str(txn_row["post_date"]),
            str(txn_row["description"]),
            f"{float(txn_row['amount']):.2f}",
            int(txn_row["occurrence"]),
        )

    def render_candidates(self) -> None:
        table = self.query_one("#candidates_table", DataTable)
        cursor = table.cursor_row
        table.clear()
        for index, candidate in enumerate(self.candidates):
            # Text, not str: a plain "[x]" string would be read as Rich markup
            # (an unknown style tag) and vanish from the cell.
            if index in self.selected_indexes:
                marker = Text("[x]", style="bold green")
            else:
                marker = Text("[ ]")
            account_label = self.app_state.account_labels.get(
                candidate["account_id"], candidate["account_id"]
            )
            score_text = (
                "" if candidate["score"] is None else f"{candidate['score']:.1f}"
            )
            table.add_row(
                marker,
                candidate["post_date"].isoformat(),
                account_label,
                f"{candidate['amount']:.2f}",
                score_text,
                candidate["matched_series"][:24],
                candidate["description"][:60],
            )
        if len(self.candidates) > 0:
            table.move_cursor(row=min(cursor, len(self.candidates) - 1))

    def action_toggle(self) -> None:
        table = self.query_one("#candidates_table", DataTable)
        index = table.cursor_row
        if index < 0 or index >= len(self.candidates):
            return
        if self.candidates[index]["matched_series"] != "":
            self.notify(
                "Already pays " + self.candidates[index]["matched_series"] + ". "
                "Use x to claim a share of it, or unpair it first.",
                severity="warning",
            )
            return
        if index in self.selected_indexes:
            self.selected_indexes.discard(index)
        else:
            self.selected_indexes.add(index)
        self.render_candidates()

    def action_split_claim(self) -> None:
        """Claim a stated share of the cursor transaction for this occurrence.

        This is how one bill pays several expected items: the mortgage
        transaction gets a full (or split) match on the mortgage occurrence
        and a split match carrying the PMI share on the PMI occurrence.
        """
        table = self.query_one("#candidates_table", DataTable)
        index = table.cursor_row
        if index < 0 or index >= len(self.candidates):
            return
        candidate = self.candidates[index]

        def on_amount(text) -> None:
            if text is None:
                return
            try:
                share = float(text)
            except ValueError:
                self.notify(f"Not a number: {text!r}", severity="error")
                return
            try:
                expected_store.add_match(
                    self.app_state.engine,
                    int(self.occurrence_row["occurrence_id"]),
                    candidate,
                    source="manual",
                    matched_amount=share,
                )
            except ValueError as error:
                self.notify(str(error), severity="error")
                return
            self.dismiss(True)

        self.app.push_screen(
            TextPromptScreen(
                f"Share of {candidate['amount']:.2f} that belongs to "
                f"{self.occurrence_row['series_name']} (signed):",
                initial=f"{float(self.occurrence_row['amount']):.2f}",
            ),
            on_amount,
        )

    def action_save(self) -> None:
        if not self.selected_indexes:
            self.notify("Nothing selected — space to select, escape to leave.")
            return
        for index in sorted(self.selected_indexes):
            candidate = self.candidates[index]
            source = "manual" if candidate["score"] is None else "confirmed_suggestion"
            try:
                expected_store.add_match(
                    self.app_state.engine,
                    int(self.occurrence_row["occurrence_id"]),
                    candidate,
                    source=source,
                )
            except ValueError as error:
                self.notify(str(error), severity="error")
        self.dismiss(True)

    def action_cancel(self) -> None:
        self.dismiss(False)


# %%
# Pairing screen #


class PairingScreen(Screen):
    """Date-ordered occurrences with derived status; the daily driver."""

    BINDINGS = [
        ("m", "open_match", "Pair"),
        ("v", "void_matches", "Unpair"),
        ("s", "skip", "Skip"),
        ("u", "unskip", "Unskip"),
        ("a", "edit_amount", "Amount"),
        ("b", "window_back", "Older"),
        ("f", "window_forward", "Newer"),
        ("r", "refresh", "Refresh"),
        ("t", "goto_transactions", "Transactions"),
        ("e", "goto_series", "Series"),
        ("escape", "back", "Menu"),
        ("q", "quit_app", "Quit"),
    ]

    def __init__(self, app_state) -> None:
        super().__init__()
        self.app_state = app_state
        self.window_end = datetime.date.today() + datetime.timedelta(days=DAYS_FORWARD)
        self.df_status = pd.DataFrame()

    def compose(self) -> ComposeResult:
        yield Header()
        yield Static("", id="window_label")
        yield DataTable(id="occurrences_table")
        yield Footer()

    def on_mount(self) -> None:
        table = self.query_one("#occurrences_table", DataTable)
        table.cursor_type = "row"
        table.zebra_stripes = True
        table.add_columns(
            "due", "series", "category", "expected", "status", "actual", "matches"
        )
        self.action_refresh()
        table.focus()

    def action_refresh(self) -> None:
        window_start = self.window_end - datetime.timedelta(
            days=DAYS_BACK + DAYS_FORWARD
        )
        self.df_status = expected_store.get_occurrence_status_df(
            self.app_state.engine, window_start, self.window_end
        )
        self.query_one("#window_label", Static).update(
            f" {window_start} → {self.window_end}   (b/f to move, m to pair)"
        )

        table = self.query_one("#occurrences_table", DataTable)
        cursor = table.cursor_row
        table.clear()
        for _, row in self.df_status.iterrows():
            actual_text = "" if row["match_count"] == 0 else f"{row['net_amount']:.2f}"
            table.add_row(
                str(row["due_date"]),
                row["series_name"],
                row["category"],
                f"{float(row['amount']):.2f}",
                styled_status(row["status"]),
                actual_text,
                str(row["match_count"]) if row["match_count"] else "",
            )
        if len(self.df_status) > 0 and cursor >= 0:
            table.move_cursor(row=min(cursor, len(self.df_status) - 1))

    def selected_row(self):
        index = self.query_one("#occurrences_table", DataTable).cursor_row
        if index < 0 or index >= len(self.df_status):
            return None
        return self.df_status.iloc[index]

    def action_open_match(self) -> None:
        row = self.selected_row()
        if row is None:
            return
        self.app.push_screen(
            MatchScreen(self.app_state, row), lambda saved: self.action_refresh()
        )

    def action_void_matches(self) -> None:
        row = self.selected_row()
        if row is None or row["match_count"] == 0:
            return

        def on_confirm(confirmed) -> None:
            if not confirmed:
                return
            df_matches = expected_store.get_active_matches_df(self.app_state.engine)
            for _, match_row in df_matches[
                df_matches["occurrence_id"] == row["occurrence_id"]
            ].iterrows():
                expected_store.void_match(self.app_state.engine, int(match_row["id"]))
            self.action_refresh()

        question = f"Unpair all {row['match_count']} match(es) on {row['series_name']} {row['due_date']}?"
        self.app.push_screen(ConfirmScreen(question), on_confirm)

    def action_skip(self) -> None:
        row = self.selected_row()
        if row is None:
            return

        def on_note(note) -> None:
            if note is None:
                return
            expected_store.skip_occurrence(
                self.app_state.engine, int(row["occurrence_id"]), note
            )
            self.action_refresh()

        self.app.push_screen(
            TextPromptScreen("Why is this occurrence skipped?"), on_note
        )

    def action_unskip(self) -> None:
        row = self.selected_row()
        if row is None or row["status"] != "skipped":
            return
        expected_store.unskip_occurrence(
            self.app_state.engine, int(row["occurrence_id"])
        )
        self.action_refresh()

    def action_edit_amount(self) -> None:
        row = self.selected_row()
        if row is None:
            return

        def on_amount(text) -> None:
            if text is None:
                return
            try:
                amount = float(text)
            except ValueError:
                self.notify(f"Not a number: {text!r}", severity="error")
                return
            expected_store.set_occurrence_amount(
                self.app_state.engine, int(row["occurrence_id"]), amount
            )
            self.action_refresh()

        self.app.push_screen(
            TextPromptScreen("Expected amount:", initial=f"{float(row['amount']):.2f}"),
            on_amount,
        )

    def action_window_back(self) -> None:
        self.window_end = self.window_end - datetime.timedelta(days=WINDOW_STEP_DAYS)
        self.action_refresh()

    def action_window_forward(self) -> None:
        self.window_end = self.window_end + datetime.timedelta(days=WINDOW_STEP_DAYS)
        self.action_refresh()

    def action_goto_transactions(self) -> None:
        self.app.push_screen(TransactionsScreen(self.app_state))

    def action_goto_series(self) -> None:
        self.app.push_screen(SeriesScreen(self.app_state))

    def action_back(self) -> None:
        self.app.pop_screen()

    def action_quit_app(self) -> None:
        self.app.exit()


# %%
# Transactions screen #


# View filters, cycled with v.
TXN_VIEWS = ["all", "matched", "unmatched"]


class TransactionsScreen(Screen):
    """Every held transaction — the whole two years at once — with the series
    it pays. Newest first.

    Read-only. Press v to cycle all / matched / unmatched: 'matched' is the
    audit view of everything the sheet import (and you) have paired so far;
    'unmatched' is money the plan doesn't know about yet, where reversals and
    stray subscriptions stand out. PageUp/PageDown and Home/End move fast.
    """

    BINDINGS = [
        ("v", "cycle_view", "All/Matched/Unmatched"),
        ("r", "refresh", "Refresh"),
        ("escape", "back", "Back"),
    ]

    def __init__(self, app_state) -> None:
        super().__init__()
        self.app_state = app_state
        self.view = "all"
        self.df_all = pd.DataFrame()

    def compose(self) -> ComposeResult:
        yield Header()
        yield Static("", id="txn_view_label")
        yield DataTable(id="transactions_table")
        yield Footer()

    def on_mount(self) -> None:
        table = self.query_one("#transactions_table", DataTable)
        table.cursor_type = "row"
        table.zebra_stripes = True
        table.add_columns("date", "account", "amount", "pays", "description")
        self.action_refresh()
        table.focus()

    def action_refresh(self) -> None:
        # All of history in one load; a few thousand rows is nothing.
        self.df_all = expected_store.get_transactions_with_matches_df(
            self.app_state.engine, datetime.date(2000, 1, 1), datetime.date(2100, 1, 1)
        )
        self.df_all = self.df_all.sort_values(
            ["post_date", "account_id"], ascending=[False, True]
        )
        self.render_transactions()

    def render_transactions(self) -> None:
        is_matched = self.df_all["matched_series"] != ""
        if self.view == "matched":
            df_view = self.df_all[is_matched]
        elif self.view == "unmatched":
            df_view = self.df_all[~is_matched]
        else:
            df_view = self.df_all
        self.query_one("#txn_view_label", Static).update(
            f" view: {self.view} ({len(df_view)} rows)   "
            f"all {len(self.df_all)} / matched {int(is_matched.sum())} / "
            f"unmatched {int((~is_matched).sum())}   "
            "(v to switch, PgUp/PgDn/Home/End to move)"
        )
        table = self.query_one("#transactions_table", DataTable)
        table.clear()
        for _, txn_row in df_view.iterrows():
            account_label = self.app_state.account_labels.get(
                str(txn_row["account_id"]), str(txn_row["account_id"])
            )
            table.add_row(
                str(txn_row["post_date"]),
                account_label,
                f"{float(txn_row['amount']):.2f}",
                str(txn_row["matched_series"])[:30],
                str(txn_row["description"])[:70],
            )

    def action_cycle_view(self) -> None:
        self.view = TXN_VIEWS[(TXN_VIEWS.index(self.view) + 1) % len(TXN_VIEWS)]
        self.render_transactions()

    def action_back(self) -> None:
        self.app.pop_screen()


# %%
# Series form #

# (field name, label, required)
SERIES_FORM_FIELDS = [
    ("name", "Name", True),
    ("category", "Category (Income/Expense/Debt/Leisure/Credit Card)", True),
    ("sub_category", "Sub category", False),
    (
        "schedule_type",
        "Schedule (monthly/yearly/biweekly/every_x_days/every_x_months/once)",
        True,
    ),
    ("day_of_month", "Day of month (monthly, yearly)", False),
    ("month_of_year", "Month of year (yearly)", False),
    ("anchor_date", "Anchor date YYYY-MM-DD (biweekly, every_x_days/months)", False),
    ("interval_days", "Interval days (every_x_days)", False),
    ("interval_months", "Interval months (every_x_months)", False),
    ("amount", "Amount (signed; negative = money out)", True),
    ("amount_tolerance", "Amount tolerance", False),
    ("auto_pay_account_id", "Pays from account (last 4)", False),
    ("is_transfer", "Is transfer? (y/n)", False),
    ("transfer_account_id", "Transfer lands on account (last 4)", False),
    ("match_pattern", "Description pattern for suggestions", False),
    ("active_from", "Active from YYYY-MM-DD", True),
    ("notes", "Notes", False),
]

INTEGER_FIELDS = ["day_of_month", "month_of_year", "interval_days", "interval_months"]
FLOAT_FIELDS = ["amount", "amount_tolerance"]
DATE_FIELDS = ["anchor_date", "active_from"]


class SeriesFormScreen(ModalScreen):
    """New-series form. Dismisses with the series dict, or None on escape."""

    BINDINGS = [("escape", "cancel", "Cancel"), ("ctrl+s", "save", "Save")]

    def __init__(self, initial: dict | None = None) -> None:
        super().__init__()
        self.initial = initial or {}

    def compose(self) -> ComposeResult:
        with VerticalScroll(classes="modal_box_wide"):
            yield Label("New series  (ctrl+s to save, escape to cancel)")
            for field_name, field_label, required in SERIES_FORM_FIELDS:
                marker = " *" if required else ""
                yield Label(field_label + marker)
                initial_value = self.initial.get(field_name)
                yield Input(
                    value="" if initial_value is None else str(initial_value),
                    id=f"field_{field_name}",
                )

    def action_save(self) -> None:
        series = {}
        for field_name, field_label, required in SERIES_FORM_FIELDS:
            text = self.query_one(f"#field_{field_name}", Input).value.strip()
            if text == "":
                if required:
                    self.notify(f"{field_label} is required", severity="error")
                    return
                continue
            if field_name in INTEGER_FIELDS:
                series[field_name] = int(text)
            elif field_name in FLOAT_FIELDS:
                series[field_name] = float(text)
            elif field_name in DATE_FIELDS:
                parsed = parse_date(text)
                if parsed is None:
                    self.notify(f"{field_label}: bad date {text!r}", severity="error")
                    return
                series[field_name] = parsed
            elif field_name == "is_transfer":
                series[field_name] = text.lower() in ("y", "yes", "true", "1")
            else:
                series[field_name] = text
        if "is_transfer" not in series:
            series["is_transfer"] = False
        if "replaces_series_id" in self.initial:
            series["replaces_series_id"] = self.initial["replaces_series_id"]
        self.dismiss(series)

    def action_cancel(self) -> None:
        self.dismiss(None)


# %%
# Series screen #


# Section titles, in display order.
SERIES_SECTIONS = [
    ("active", "ACTIVE"),
    ("onces", "ONCES  (done ones at the bottom)"),
    ("inactive", "INACTIVE"),
]


class SeriesScreen(Screen):
    """The bills themselves. New, close, or close-and-replace — never edit.

    Three sections: ACTIVE recurring series, ONCES (one-shot commitments,
    with the already-done ones sunk to the bottom of the section but not
    dimmed — they were real), and INACTIVE (closed/replaced series, dimmed).
    Each section groups by category and then amount.
    """

    BINDINGS = [
        ("n", "new_series", "New"),
        ("c", "close_series", "Close"),
        ("p", "replace_series", "Close & replace"),
        ("r", "refresh", "Refresh"),
        ("escape", "back", "Back"),
    ]

    def __init__(self, app_state) -> None:
        super().__init__()
        self.app_state = app_state
        self.df_series = pd.DataFrame()
        self.row_to_series = []

    def compose(self) -> ComposeResult:
        yield Header()
        yield Static(" series — n new, c close, p close & replace, escape back")
        yield DataTable(id="series_table")
        yield Footer()

    def on_mount(self) -> None:
        table = self.query_one("#series_table", DataTable)
        table.cursor_type = "row"
        table.zebra_stripes = True
        table.add_columns(
            "",
            "name",
            "category",
            "schedule",
            "amount",
            "account",
            "active_from",
            "active_until",
        )
        self.action_refresh()
        table.focus()

    def action_refresh(self) -> None:
        df_series = expected_store.get_series_df(self.app_state.engine)
        today = datetime.date.today()

        has_end = pd.notna(df_series["active_until"])
        df_series["is_ended"] = False
        df_series.loc[has_end, "is_ended"] = (
            pd.to_datetime(df_series.loc[has_end, "active_until"]).dt.date < today
        )
        df_series["amount"] = df_series["amount"].astype(float)

        # A 'once' series is done when its last occurrence date has passed.
        last_due = expected_store.get_last_due_dates(self.app_state.engine)
        df_series["last_due"] = df_series["id"].map(last_due)
        df_series["once_done"] = (
            (df_series["schedule_type"] == "once")
            & pd.notna(df_series["last_due"])
            & (pd.to_datetime(df_series["last_due"]).dt.date < today)
        )

        df_series["section"] = "active"
        df_series.loc[df_series["schedule_type"] == "once", "section"] = "onces"
        df_series.loc[df_series["is_ended"], "section"] = "inactive"

        self.df_series = df_series.reset_index(drop=True)
        self.render_series()

    def render_series(self) -> None:
        """Draw the three sections. Header and spacer rows carry no series, so
        row_to_series maps every table row back to a df index (or None)."""
        table = self.query_one("#series_table", DataTable)
        table.clear()
        self.row_to_series = []

        for section, title in SERIES_SECTIONS:
            block = self.df_series[self.df_series["section"] == section]
            block = block.sort_values(["once_done", "category", "amount", "name"])
            if len(block) == 0:
                continue
            if table.row_count > 0:  # a little air between sections
                table.add_row(*[""] * 8)
                self.row_to_series.append(None)
            table.add_row("", Text(f"── {title} ──", style="bold cyan"), *[""] * 6)
            self.row_to_series.append(None)

            for df_index, row in block.iterrows():
                self.row_to_series.append(df_index)
                table.add_row(*self._series_cells(row))

    def _series_cells(self, row) -> list:
        schedule = row["schedule_type"]
        if row["schedule_type"] == "monthly" and pd.notna(row["day_of_month"]):
            schedule = f"monthly d{int(row['day_of_month'])}"
        if row["schedule_type"] == "once" and pd.notna(row["last_due"]):
            schedule = f"once → {row['last_due']}"
        account_label = self.app_state.account_labels.get(
            str(row["auto_pay_account_id"]), row["auto_pay_account_id"]
        )
        active_until = "" if pd.isna(row["active_until"]) else str(row["active_until"])
        cells = [
            "done" if row["once_done"] else ("ended" if row["is_ended"] else ""),
            str(row["name"]),
            str(row["category"]),
            str(schedule),
            f"{row['amount']:.2f}",
            "" if pd.isna(row["auto_pay_account_id"]) else str(account_label),
            str(row["active_from"]),
            active_until,
        ]
        if row["is_ended"]:
            cells = [Text(cell, style="dim") for cell in cells]
        return cells

    def selected_row(self):
        index = self.query_one("#series_table", DataTable).cursor_row
        if index < 0 or index >= len(self.row_to_series):
            return None
        df_index = self.row_to_series[index]
        if df_index is None:  # a section header or spacer row
            return None
        return self.df_series.loc[df_index]

    def _save_new_series(self, series) -> None:
        if series is None:
            return
        expected_store.add_series(self.app_state.engine, series)
        expected_store.generate_occurrences(self.app_state.engine)
        self.action_refresh()

    def action_new_series(self) -> None:
        self.app.push_screen(SeriesFormScreen(), self._save_new_series)

    def action_close_series(self) -> None:
        row = self.selected_row()
        if row is None:
            return

        def on_date(text) -> None:
            if text is None:
                return
            close_date = parse_date(text)
            if close_date is None:
                self.notify(f"Bad date: {text!r}", severity="error")
                return
            removed = expected_store.close_series(
                self.app_state.engine, int(row["id"]), close_date
            )
            self.notify(
                f"{row['name']} closed {close_date}; "
                f"{removed} future occurrence(s) removed."
            )
            self.action_refresh()

        self.app.push_screen(
            TextPromptScreen(f"Close {row['name']} as of (YYYY-MM-DD):"), on_date
        )

    def action_replace_series(self) -> None:
        row = self.selected_row()
        if row is None:
            return

        def on_date(text) -> None:
            if text is None:
                return
            close_date = parse_date(text)
            if close_date is None:
                self.notify(f"Bad date: {text!r}", severity="error")
                return
            expected_store.close_series(
                self.app_state.engine, int(row["id"]), close_date
            )
            prefill = row.to_dict()
            prefill["active_from"] = close_date + datetime.timedelta(days=1)
            prefill["replaces_series_id"] = int(row["id"])
            for dropped in ("id", "active_until"):
                prefill.pop(dropped, None)
            self.app.push_screen(
                SeriesFormScreen(initial=prefill), self._save_new_series
            )

        self.app.push_screen(
            TextPromptScreen(
                f"Close {row['name']} and start its replacement after (YYYY-MM-DD):"
            ),
            on_date,
        )

    def action_back(self) -> None:
        self.app.pop_screen()


# %%
# Menu screen #

MENU_BANNER = """\
[green]$[/] ────────────────────────────────────────── [green]$[/]

     [bold]C A S H   F L O W   C O M M A N D E R[/]
            [dim]expected transactions[/]

[green]$[/] ────────────────────────────────────────── [green]$[/]"""


class MenuScreen(Screen):
    """The landing page: pick where to go.

    Deliberately cheap — a few COUNT queries and nothing else, so the app
    opens instantly. The heavy status derivation happens only when a page
    that needs it is opened.
    """

    BINDINGS = [
        ("p", "goto_pairing", "Pairing"),
        ("t", "goto_transactions", "Transactions"),
        ("e", "goto_series", "Series"),
        ("q", "quit_app", "Quit"),
    ]

    def __init__(self, app_state) -> None:
        super().__init__()
        self.app_state = app_state

    def compose(self) -> ComposeResult:
        with Vertical(classes="menu_box"):
            yield Static(MENU_BANNER, id="menu_banner")
            yield Static(self._counts_line(), id="menu_counts")
            yield Button(
                "p   Pairing — expected vs actual, month by month",
                id="goto_pairing",
            )
            yield Button(
                "t   Transactions — every actual, matched and not",
                id="goto_transactions",
            )
            yield Button("e   Series — the bills themselves", id="goto_series")
            yield Button("q   Quit", id="quit_app")
        yield Footer()

    def _counts_line(self) -> str:
        """A one-line lay of the land, from four cheap COUNT(*) queries."""
        today = datetime.date.today()
        with self.app_state.engine.connect() as conn:
            active_series = conn.execute(
                select(func.count()).where(
                    db.expected_series.c.active_until.is_(None)
                    | (db.expected_series.c.active_until >= today)
                )
            ).scalar()
            ended_series = conn.execute(
                select(func.count()).where(db.expected_series.c.active_until < today)
            ).scalar()
            matches = conn.execute(
                select(func.count()).where(db.expected_matches.c.voided_at.is_(None))
            ).scalar()
            transactions = conn.execute(
                select(func.count()).select_from(db.transactions)
            ).scalar()
        return (
            f"  [green]{active_series}[/] active series"
            f"   [dim]{ended_series} ended[/]"
            f"   [green]{matches}[/] matches"
            f"   {transactions} transactions held"
        )

    def on_button_pressed(self, event: Button.Pressed) -> None:
        getattr(self, f"action_{event.button.id}")()

    def action_goto_pairing(self) -> None:
        self.app.push_screen(PairingScreen(self.app_state))

    def action_goto_transactions(self) -> None:
        self.app.push_screen(TransactionsScreen(self.app_state))

    def action_goto_series(self) -> None:
        self.app.push_screen(SeriesScreen(self.app_state))

    def action_quit_app(self) -> None:
        self.app.exit()


# %%
# App #


class AppState:
    """Shared engine and lookups, passed to every screen."""

    def __init__(self) -> None:
        self.engine = db.get_engine()
        self.account_labels = load_account_labels()


class ExpectedApp(App):
    TITLE = "Cash Flow Commander — expected transactions"
    CSS = """
    .modal_box {
        width: 70;
        height: auto;
        max-height: 80%;
        padding: 1 2;
        background: $surface;
        border: thick $primary;
    }
    .modal_box_wide {
        width: 110;
        height: auto;
        max-height: 90%;
        padding: 1 2;
        background: $surface;
        border: thick $primary;
    }
    /* Fixed height with the table taking the leftover space, so a long
       candidate list scrolls INSIDE the box instead of shoving everything
       else (and the key hints) off screen. */
    .match_box {
        width: 90%;
        height: 90%;
        padding: 1 2;
        background: $surface;
        border: thick $primary;
    }
    .match_box Label {
        height: auto;
        margin-bottom: 1;
    }
    .match_box DataTable {
        height: 1fr;
    }
    TextPromptScreen, ConfirmScreen, MatchScreen, SeriesFormScreen {
        align: center middle;
    }
    MenuScreen {
        align: center middle;
    }
    .menu_box {
        width: 56;
        height: auto;
        padding: 1 3;
        border: double $primary;
        background: $surface;
    }
    .menu_box Static {
        text-align: center;
        margin-bottom: 1;
    }
    .menu_box Button {
        width: 100%;
        margin-bottom: 1;
    }
    """

    def on_mount(self) -> None:
        self.push_screen(MenuScreen(AppState()))


# %%
# Run #

if __name__ == "__main__":
    ExpectedApp().run()


# %%
