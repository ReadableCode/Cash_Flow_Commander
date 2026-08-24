# %%
# Imports #

import datetime as dt
import os
import sys
from typing import Any

import pytest

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_PKG = os.path.join(_ROOT, "transaction_downloader")
if _PKG not in sys.path:
    sys.path.insert(0, _PKG)

import capture  # noqa: E402
import plan  # noqa: E402
import store  # noqa: E402

_SRC = os.path.join(_ROOT, "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

import ingest_raw  # noqa: E402

# %%
# Fixtures #

TODAY = dt.date(2026, 8, 22)
ACCOUNT = "7676"

# Chase's report cap, as the provider registry records it; the capture-time and
# parser-side guards must both agree with this number.
CHASE_ROW_CAP = store.provider_def("chase")["row_cap"]

CHECKING_CSV = (
    "Details,Posting Date,Description,Amount,Type,Balance,Check or Slip #,\n"
    'DEBIT,08/19/2026,"HEB #0567 AUSTIN TX",-96.31,DEBIT_CARD,5210.09,,\n'
    'CREDIT,08/07/2026,"ACME CORP DIRECT DEP",3120.44,ACH_CREDIT,5306.40,,\n'
    "Totals,,,,,,,\n"
)

CARD_CSV = (
    "Transaction Date,Post Date,Description,Category,Type,Amount,Memo\n"
    "08/11/2026,08/12/2026,DELTA AIR LINES,Travel,Sale,-388.20,\n"
    "08/06/2026,08/07/2026,Payment Thank You - Web,,Payment,900.00,\n"
)

CARD_LEGACY_CSV = (
    "Type,Trans Date,Post Date,Description,Amount\n"
    "SALE,12/22/2024,12/23/2024,APPLE.COM/BILL,-9.99\n"
)


def _capture(start: str, end: str, *, account: str = ACCOUNT, rows: int = 5,
             min_date: str | None = None, max_date: str | None = None) -> dict[str, Any]:
    """Build a manifest entry for one requested window."""
    return {
        "account": account,
        "requested_start": start,
        "requested_end": end,
        "captured_at": end,
        "rows": rows,
        "min_date": min_date if min_date is not None else start,
        "max_date": max_date if max_date is not None else end,
    }


def _months(report: dict[str, Any]) -> list[str]:
    """The months a report asks to download."""
    return [request["month"] for request in report["requests"]]


def _analyze(captures: list[dict[str, Any]], **kwargs: Any) -> dict[str, Any]:
    """Run analyze_account with the test defaults.

    window_months pins to 1 unless a test says otherwise: most tests here are
    about which MONTHS get requested, and merging them into wider windows would
    obscure that. The auto-sizing tests pass "auto" explicitly.
    """
    kwargs.setdefault("window_months", 1)
    return plan.analyze_account(ACCOUNT, captures, today=TODAY, **kwargs)


# %%
# Export parsing #


def test_reads_checking_layout_and_ignores_junk_rows() -> None:
    """Chase appends blank and totals lines; they must not count as transactions."""
    described = store.read_export(CHECKING_CSV)

    assert described["layout"] == "checking"
    assert described["account_type"] == "bank"
    assert described["rows"] == 2
    assert (described["min_date"], described["max_date"]) == ("2026-08-07", "2026-08-19")


def test_reads_both_credit_card_layouts() -> None:
    """The current and archived card exports both parse, spanning all date columns."""
    current = store.read_export(CARD_CSV)
    legacy = store.read_export(CARD_LEGACY_CSV)

    assert (current["layout"], current["rows"]) == ("credit_card", 2)
    # Spans transaction_date min through post_date max — deliberately wide.
    assert (current["min_date"], current["max_date"]) == ("2026-08-06", "2026-08-12")
    assert (legacy["layout"], legacy["rows"]) == ("credit_card_legacy", 1)


def test_rejects_a_file_that_is_not_a_chase_export() -> None:
    """The guard that stops a stray download from marking a month covered."""
    with pytest.raises(store.NotAChaseExport):
        store.read_export("date,total\n2026-08-01,5\n")


def test_capture_name_round_trips() -> None:
    """parse_capture_name is the exact inverse of capture_name."""
    name = store.capture_name(ACCOUNT, dt.date(2026, 8, 1), dt.date(2026, 8, 22), TODAY)

    assert name == "chase_csv_export_7676_20260801_20260822_captured20260822.csv"
    assert store.parse_capture_name(name) == {
        "provider": "chase",
        "account": ACCOUNT,
        "requested_start": "2026-08-01",
        "requested_end": "2026-08-22",
        "captured_at": "2026-08-22",
    }


# %%
# Planning: the always-refetch window #


def test_current_month_is_always_requested() -> None:
    """The current month is incomplete by definition, however fresh the data is."""
    report = _analyze([_capture("2026-08-01", "2026-08-22", max_date="2026-08-21")])

    assert "2026-08" in _months(report)


def test_overlap_window_reaches_back_into_the_previous_month() -> None:
    """Newest txn early in a month means the previous month is still settling."""
    report = _analyze(
        [_capture("2026-07-01", "2026-08-02", min_date="2026-07-01", max_date="2026-08-02")],
        overlap_days=5,
    )

    # newest 2026-08-02 minus 5 days lands on 2026-07-28, so July comes too.
    assert _months(report) == ["2026-08", "2026-07"]


def test_overlap_window_stays_in_month_when_newest_is_late() -> None:
    """A newest transaction well into the month does not drag the previous one in."""
    report = _analyze(
        [_capture("2026-07-01", "2026-08-22", min_date="2026-07-01", max_date="2026-08-19")],
        overlap_days=5,
    )

    assert _months(report) == ["2026-08"]


def test_overlap_days_is_configurable() -> None:
    """A wider overlap pulls in more settled months."""
    captures = [_capture("2026-06-01", "2026-08-22", min_date="2026-06-01", max_date="2026-08-03")]

    assert _months(_analyze(captures, overlap_days=1)) == ["2026-08"]
    assert _months(_analyze(captures, overlap_days=10)) == ["2026-08", "2026-07"]


# %%
# Planning: coverage and gaps #


def test_settled_covered_months_are_not_refetched() -> None:
    """The whole point of tracking coverage: stop re-downloading finished months."""
    report = _analyze([_capture("2026-01-01", "2026-08-22", min_date="2026-01-04", max_date="2026-08-20")])

    assert _months(report) == ["2026-08"]
    assert report["deferred_months"] == []


def test_interior_gap_month_is_requested() -> None:
    """A month never captured between two that were is a hole, and gets filled."""
    report = _analyze(
        [
            _capture("2026-01-01", "2026-04-30", min_date="2026-01-04", max_date="2026-04-29"),
            # May skipped
            _capture("2026-06-01", "2026-08-22", min_date="2026-06-02", max_date="2026-08-20"),
        ]
    )

    assert _months(report) == ["2026-08", "2026-05"]
    assert next(r["reason"] for r in report["requests"] if r["month"] == "2026-05") == "gap"


def test_a_captured_month_with_no_transactions_counts_as_covered() -> None:
    """An empty month is a fact, not a hole — otherwise it is refetched forever."""
    report = _analyze(
        [
            _capture("2026-01-01", "2026-04-30", min_date="2026-01-04", max_date="2026-04-29"),
            _capture("2026-05-01", "2026-05-31", rows=0, min_date=None, max_date=None),
            _capture("2026-06-01", "2026-08-22", min_date="2026-06-02", max_date="2026-08-20"),
        ]
    )

    assert _months(report) == ["2026-08"]


def test_no_captures_backfills_a_bounded_window() -> None:
    """With nothing stored, go back a bounded distance rather than guessing forever."""
    report = _analyze([], max_months=plan.MAX_MONTHS_PER_RUN)

    assert _months(report)[0] == "2026-08"
    assert len(report["requests"]) == plan.MAX_MONTHS_PER_RUN
    assert all(r["reason"] == "never captured" for r in report["requests"] if r["month"] != "2026-08")


def test_backfill_start_extends_history_and_defers_the_overflow() -> None:
    """A long backfill is chunked across runs, newest first, with the rest reported."""
    report = _analyze([], backfill_start=dt.date(2020, 1, 1), max_months=6)

    assert _months(report) == ["2026-08", "2026-07", "2026-06", "2026-05", "2026-04", "2026-03"]
    assert report["deferred_months"][0] == "2026-02"
    # Deferral stops at the retention floor; older months are a different problem.
    assert report["deferred_months"][-1] == "2024-08"


# %%
# Retention floor #


def test_the_floor_is_measured_to_the_day_not_the_month() -> None:
    """Verified live: From 2024-08-22 was refused, 2024-08-23 accepted, on 2026-08-22."""
    assert plan.retention_floor(TODAY) == dt.date(2024, 8, 23)


def test_the_oldest_window_starts_at_the_floor_not_the_first_of_the_month() -> None:
    """A month-aligned start would ask for days Chase rejects outright."""
    report = _analyze([], backfill_start=dt.date(2020, 1, 1), max_months=30)

    oldest = report["requests"][-1]
    assert oldest["month"] == "2024-08"
    assert oldest["start"] == "2024-08-23"
    assert oldest["end"] == "2024-08-31"


def test_months_past_the_floor_are_unreachable_not_deferred() -> None:
    """Past the floor is not work to retry — it can only come from an archive."""
    report = _analyze([], backfill_start=dt.date(2020, 1, 1), max_months=6)

    assert report["retention_floor"] == "2024-08-23"
    assert report["unreachable_months"][0] == "2024-07"
    assert report["unreachable_months"][-1] == "2020-01"
    assert not set(report["unreachable_months"]) & set(report["deferred_months"])


def test_credit_cards_have_the_same_floor_as_banks() -> None:
    """A card looked unbounded until both date fields were filled; it is not."""
    bank = _analyze([], backfill_start=dt.date(2020, 1, 1), max_months=3, kind="bank")
    credit = _analyze([], backfill_start=dt.date(2020, 1, 1), max_months=3, kind="credit")

    assert bank["retention_floor"] == credit["retention_floor"] == "2024-08-23"
    assert bank["unreachable_months"] == credit["unreachable_months"]


def test_a_leap_day_anniversary_does_not_explode() -> None:
    """24 months back from 2026-02-28 lands in a February that may be short."""
    assert plan.retention_floor(dt.date(2026, 2, 28)) == dt.date(2024, 2, 29)
    assert plan.retention_floor(dt.date(2025, 2, 28)) == dt.date(2023, 3, 1)


def test_full_ignores_the_per_run_cap() -> None:
    """--full is the deliberate 'fetch everything wanted' escape hatch."""
    report = _analyze([], backfill_start=dt.date(2025, 1, 1), max_months=3, full=True)

    assert len(report["requests"]) == 20
    assert report["deferred_months"] == []


# %%
# Planning: request shape #


def test_requests_are_month_aligned_and_clipped_to_today() -> None:
    """Windows must partition cleanly, and never ask for days that have not happened."""
    report = _analyze([_capture("2026-06-01", "2026-06-30", min_date="2026-06-02", max_date="2026-06-29")])

    current = next(r for r in report["requests"] if r["month"] == "2026-08")
    july = next(r for r in report["requests"] if r["month"] == "2026-07")

    assert (current["start"], current["end"]) == ("2026-08-01", "2026-08-22")
    assert (july["start"], july["end"]) == ("2026-07-01", "2026-07-31")


def test_accounts_found_only_on_disk_are_still_planned() -> None:
    """An account captured before it was configured must not silently drop out."""
    reports = plan.analyze_all(
        [_capture("2026-08-01", "2026-08-22", account="9021")],
        {ACCOUNT: {"label": "Total Checking", "kind": "bank"}},
        today=TODAY,
    )

    assert sorted(report["account"] for report in reports) == ["7676", "9021"]


def test_account_config_accepts_a_bare_label_or_a_full_entry() -> None:
    """Both providers.local.yaml shapes normalize to the same thing."""
    short = plan.configured_accounts({"external_ids": {"accounts": {"7676": "Checking"}}})
    full = plan.configured_accounts(
        {"external_ids": {"accounts": {"0002": {"label": "A Card", "kind": "credit"}}}}
    )

    assert short["7676"] == {"label": "Checking", "kind": "bank"}
    assert full["0002"] == {"label": "A Card", "kind": "credit"}


# %%
# Filing #


def _write(tmp_path: Any, name: str, body: str) -> str:
    """Write a fake download and return its path."""
    path = os.path.join(str(tmp_path), name)
    with open(path, "w", encoding="utf-8") as handle:
        handle.write(body)
    return path


def test_filing_copies_verbatim_and_records_the_requested_window(tmp_path: Any) -> None:
    """The bytes must survive untouched, and the window must be recorded."""
    raw_dir = os.path.join(str(tmp_path), "raw")
    source = _write(tmp_path, "Chase7676_Activity_20260822.CSV", CHECKING_CSV)

    entry = capture.file_capture(
        source, raw_dir, account=ACCOUNT,
        start=dt.date(2026, 8, 1), end=dt.date(2026, 8, 22), captured=TODAY,
    )

    assert entry["status"] == "filed"
    assert entry["file"] == "chase_csv_export_7676_20260801_20260822_captured20260822.csv"
    with open(os.path.join(raw_dir, entry["file"]), "r", encoding="utf-8") as handle:
        assert handle.read() == CHECKING_CSV
    assert store.read_manifest(raw_dir)[0]["requested_end"] == "2026-08-22"


def test_filing_moves_the_download_out_of_the_browser_folder(tmp_path: Any) -> None:
    """A download is handled once and lands in the repo; nothing re-walks its origin."""
    raw_dir = os.path.join(str(tmp_path), "raw")
    source = _write(tmp_path, "Chase7676_Activity_20260822.CSV", CHECKING_CSV)

    entry = capture.file_capture(
        source, raw_dir, account=ACCOUNT,
        start=dt.date(2026, 8, 1), end=dt.date(2026, 8, 22), captured=TODAY,
    )

    assert not os.path.exists(source), "the download should have been moved, not copied"
    assert os.path.isfile(os.path.join(raw_dir, entry["file"]))


def test_refiling_identical_bytes_is_a_no_op(tmp_path: Any) -> None:
    """Overlap is free, but it should not litter the store with identical copies."""
    raw_dir = os.path.join(str(tmp_path), "raw")
    window = {"start": dt.date(2026, 8, 1), "end": dt.date(2026, 8, 22), "captured": TODAY}

    first = _write(tmp_path, "Chase7676_Activity_20260822.CSV", CHECKING_CSV)
    capture.file_capture(first, raw_dir, account=ACCOUNT, **window)
    # Downloading the same window again produces a fresh file with identical bytes.
    again = _write(tmp_path, "Chase7676_Activity_20260822 (1).CSV", CHECKING_CSV)
    second = capture.file_capture(again, raw_dir, account=ACCOUNT, **window)

    assert second["status"] == "duplicate"
    assert len(store.read_manifest(raw_dir)) == 1


def test_legacy_import_copies_rather_than_moving(tmp_path: Any) -> None:
    """Archived exports are kept files, not transient downloads — leave them alone."""
    raw_dir = os.path.join(str(tmp_path), "raw")
    source = _write(tmp_path, "Chase7676_Activity_20220301.CSV",
                    "Details,Posting Date,Description,Amount,Type,Balance,Check or Slip #,\n"
                    'DEBIT,02/05/2022,"OLD",-20.00,DEBIT_CARD,100.00,,\n')

    capture.main(["--raw-dir", raw_dir, "import-legacy", source])

    assert os.path.exists(source), "an archived export must not be moved out of its home"


def test_relative_staging_resolves_against_the_repo_not_the_cwd() -> None:
    """Config uses repo-relative staging so the tools work from anywhere."""
    resolved = capture._resolve_repo_relative("data/chase/incoming")

    assert os.path.isabs(resolved)
    assert resolved.endswith(os.path.join("Cash_Flow_Commander", "data", "chase", "incoming"))


def test_refetching_a_month_adds_a_capture_rather_than_overwriting(tmp_path: Any) -> None:
    """Re-pulling a month later must preserve the earlier capture."""
    raw_dir = os.path.join(str(tmp_path), "raw")
    first = _write(tmp_path, "aug_first.CSV", CHECKING_CSV)
    later = _write(tmp_path, "aug_later.CSV", CHECKING_CSV + 'DEBIT,08/21/2026,"NEW ROW",-1.00,DEBIT_CARD,1.00,,\n')

    capture.file_capture(first, raw_dir, account=ACCOUNT, start=dt.date(2026, 8, 1),
                         end=dt.date(2026, 8, 22), captured=dt.date(2026, 8, 22))
    capture.file_capture(later, raw_dir, account=ACCOUNT, start=dt.date(2026, 8, 1),
                         end=dt.date(2026, 8, 31), captured=dt.date(2026, 8, 31))

    files = sorted(name for name in os.listdir(raw_dir) if name.endswith(".csv"))
    assert len(files) == 2
    assert len(store.read_manifest(raw_dir)) == 2


def test_filing_refuses_a_file_that_is_not_a_chase_export(tmp_path: Any) -> None:
    """A stray download must never mark a month covered."""
    raw_dir = os.path.join(str(tmp_path), "raw")
    source = _write(tmp_path, "bank_statement.csv", "date,total\n2026-08-01,5\n")

    with pytest.raises(store.NotAChaseExport):
        capture.file_capture(source, raw_dir, account=ACCOUNT, start=dt.date(2026, 8, 1),
                             end=dt.date(2026, 8, 22), captured=TODAY)
    assert not os.path.exists(os.path.join(raw_dir, "_chase_captures.jsonl"))


def test_reindex_rebuilds_a_lost_manifest_from_the_files(tmp_path: Any) -> None:
    """The manifest is a cache; the capture files on disk are the truth."""
    raw_dir = os.path.join(str(tmp_path), "raw")
    source = _write(tmp_path, "Chase7676_Activity.CSV", CHECKING_CSV)
    capture.file_capture(source, raw_dir, account=ACCOUNT, start=dt.date(2026, 8, 1),
                         end=dt.date(2026, 8, 22), captured=TODAY)
    os.remove(store.manifest_path(raw_dir))

    rebuilt = store.scan_captures(raw_dir)

    assert len(rebuilt) == 1
    assert rebuilt[0]["requested_start"] == "2026-08-01"
    assert rebuilt[0]["rows"] == 2


def test_plan_survives_a_lost_manifest(tmp_path: Any) -> None:
    """Losing the manifest must not make the planner re-download all history."""
    raw_dir = os.path.join(str(tmp_path), "raw")
    source = _write(tmp_path, "Chase7676_Activity.CSV", CHECKING_CSV)
    capture.file_capture(source, raw_dir, account=ACCOUNT, start=dt.date(2026, 1, 1),
                         end=dt.date(2026, 8, 22), captured=TODAY)
    os.remove(store.manifest_path(raw_dir))

    recovered = store.scan_captures(raw_dir)
    report = plan.analyze_account(ACCOUNT, recovered, today=TODAY)

    assert _months(report) == ["2026-08"]


# %%
# Landing hand-off #


def test_ingest_classifies_a_chase_capture_as_a_dated_csv_export() -> None:
    """Captures must land through the existing ingest CLI, not a parallel path."""
    doc_type, period = ingest_raw.classify(
        "chase_csv_export_7676_20260801_20260822_captured20260822.csv"
    )

    assert doc_type == "csv_export"
    assert period == dt.date(2026, 8, 1)


def test_ingest_still_ignores_a_raw_chase_download() -> None:
    """Chase's own filename carries no requested window, so it is not a capture."""
    assert ingest_raw.classify("Chase7676_Activity_20260822.CSV") == ("other", None)


# %%
# Legacy archive import #


def test_legacy_import_infers_a_whole_month_window(tmp_path: Any) -> None:
    """Files predating this tool have no recorded window, so one is inferred."""
    raw_dir = os.path.join(str(tmp_path), "raw")
    source = _write(tmp_path, "Chase7676_Activity_20220301.CSV",
                    "Details,Posting Date,Description,Amount,Type,Balance,Check or Slip #,\n"
                    'DEBIT,02/05/2022,"OLD PURCHASE",-20.00,DEBIT_CARD,100.00,,\n'
                    'DEBIT,02/24/2022,"OLDER PURCHASE",-10.00,DEBIT_CARD,120.00,,\n')

    rc = capture.main(["--raw-dir", raw_dir, "import-legacy", source])

    assert rc == 0
    entry = store.read_manifest(raw_dir)[0]
    assert entry["window_source"] == "inferred"
    # Widened from 02/05..02/24 out to the whole month.
    assert (entry["requested_start"], entry["requested_end"]) == ("2022-02-01", "2022-02-28")


def test_legacy_import_exact_keeps_the_content_dates(tmp_path: Any) -> None:
    """--exact is the stricter option: claim only what the file actually proves."""
    raw_dir = os.path.join(str(tmp_path), "raw")
    source = _write(tmp_path, "Chase7676_Activity_20220301.CSV",
                    "Details,Posting Date,Description,Amount,Type,Balance,Check or Slip #,\n"
                    'DEBIT,02/05/2022,"OLD PURCHASE",-20.00,DEBIT_CARD,100.00,,\n')

    capture.main(["--raw-dir", raw_dir, "import-legacy", "--exact", source])

    entry = store.read_manifest(raw_dir)[0]
    assert (entry["requested_start"], entry["requested_end"]) == ("2022-02-05", "2022-02-05")


def test_inferred_coverage_is_reported_separately() -> None:
    """Coverage resting on a guess must be visible as such, not silently trusted."""
    captures = [
        {**_capture("2024-09-01", "2024-09-30"), "window_source": "inferred"},
        _capture("2026-08-01", "2026-08-22", max_date="2026-08-20"),
    ]

    report = _analyze(captures, backfill_start=dt.date(2024, 9, 1), kind="credit")

    assert "2024-09" in report["inferred_months"]
    assert "2026-08" not in report["inferred_months"]


def test_a_legacy_file_with_no_dated_rows_is_refused(tmp_path: Any) -> None:
    """No dates means no window can be inferred; guessing would be worse than failing."""
    raw_dir = os.path.join(str(tmp_path), "raw")
    source = _write(tmp_path, "Chase7676_Empty.CSV",
                    "Details,Posting Date,Description,Amount,Type,Balance,Check or Slip #,\n")

    assert capture.main(["--raw-dir", raw_dir, "import-legacy", source]) == 1
    assert store.read_manifest(raw_dir) == []


# %%
# Wide backfill windows #


def test_consecutive_months_merge_into_one_window() -> None:
    """A first backfill in wide windows is a fraction of the downloads."""
    report = _analyze([], backfill_start=dt.date(2025, 3, 1), max_months=12, window_months=6)

    assert len(report["requests"]) == 2          # 12 wanted months / 6 per window
    first = report["requests"][0]
    assert first["start"] == "2026-03-01"
    assert first["end"] == "2026-08-22"          # clipped to today
    assert len(first["months"]) == 6


def test_a_window_never_spans_a_settled_month() -> None:
    """Merging must not drag in months that are covered and already settled.

    June IS expected here: the newest transaction is 2026-06-29 and the overlap
    reaches back to 06-24, so June is still settling. January through May are
    covered and old enough to be left alone.
    """
    captures = [
        _capture("2026-01-01", "2026-06-30", min_date="2026-01-04", max_date="2026-06-29"),
    ]

    report = _analyze(captures, window_months=6)

    requested = {month for request in report["requests"] for month in request["months"]}
    assert requested == {"2026-06", "2026-07", "2026-08"}
    assert not requested & {"2026-01", "2026-02", "2026-03", "2026-04", "2026-05"}


def test_an_isolated_gap_stays_its_own_window() -> None:
    """One missing month must not drag a year of held data into the download."""
    captures = [
        _capture("2025-01-01", "2026-02-28", min_date="2025-01-04", max_date="2026-02-27"),
        # 2026-03 missing
        _capture("2026-04-01", "2026-08-22", min_date="2026-04-02", max_date="2026-08-20"),
    ]

    report = _analyze(captures, window_months=12)

    gap = [r for r in report["requests"] if "2026-03" in r["months"]]
    assert len(gap) == 1
    assert gap[0]["months"] == ["2026-03"]
    assert (gap[0]["start"], gap[0]["end"]) == ("2026-03-01", "2026-03-31")


def test_window_months_of_one_is_the_old_month_aligned_behaviour() -> None:
    """The incremental default must not change."""
    report = _analyze([], backfill_start=dt.date(2026, 5, 1), max_months=4, window_months=1)

    assert [r["month"] for r in report["requests"]] == ["2026-08", "2026-07", "2026-06", "2026-05"]
    assert all(len(r["months"]) == 1 for r in report["requests"])


def test_a_wide_window_still_clips_to_the_retention_floor() -> None:
    """Merging must not reintroduce start dates Chase refuses."""
    report = _analyze([], backfill_start=dt.date(2020, 1, 1), max_months=30, window_months=12)

    oldest = report["requests"][-1]
    assert oldest["start"] == "2024-08-23"


# %%
# Stale data must not masquerade as a refresh #


def test_an_old_newest_transaction_does_not_make_everything_a_refresh() -> None:
    """Regression: a 2024 capture reported two years of months as "refresh window".

    Seen on real data — Checking held only 2024-08-23..2024-09-30, and every
    month through 2026-08 came back labelled as the routine overlap refresh.
    They are uncovered months, and calling them a refresh hides a real gap.
    """
    captures = [_capture("2024-08-23", "2024-09-30", min_date="2024-08-23", max_date="2024-09-30")]

    report = _analyze(captures, backfill_start=dt.date(2024, 8, 1), max_months=40)

    reasons = {r["month"]: r["reason"] for r in report["requests"]}
    # The month holding the newest transaction is a genuine refresh.
    assert reasons["2024-09"].startswith("refresh window")
    # The current month is always refetched.
    assert reasons["2026-08"] == "current month"
    # Everything in between is a gap, not a refresh.
    for month in ("2024-10", "2025-06", "2026-01", "2026-07"):
        assert reasons[month] == "gap", f"{month} -> {reasons[month]}"


def test_the_refresh_window_still_works_when_data_is_current() -> None:
    """The normal case must be unchanged: overlap reaches back from the newest txn."""
    report = _analyze(
        [_capture("2026-07-01", "2026-08-02", min_date="2026-07-01", max_date="2026-08-02")],
        overlap_days=5,
    )

    reasons = {r["month"]: r["reason"] for r in report["requests"]}
    assert reasons["2026-08"] == "current month"
    assert reasons["2026-07"].startswith("refresh window")


# %%
# Truncated downloads never enter the pipeline #


def _capped_card_csv(rows: int) -> str:
    """A card export with `rows` data rows."""
    header = "Transaction Date,Post Date,Description,Category,Type,Amount,Memo\n"
    body = "".join(
        f"08/{i % 28 + 1:02d}/2026,08/{i % 28 + 1:02d}/2026,TXN {i},Shopping,Sale,-1.00,\n"
        for i in range(rows)
    )
    return header + body


def test_a_download_on_the_row_cap_is_refused_before_filing(tmp_path: Any) -> None:
    """Filing it would ingest a truncated month that then fails to parse forever."""
    raw_dir = os.path.join(str(tmp_path), "raw")
    source = _write(tmp_path, "Chase4242_Activity.CSV", _capped_card_csv(CHASE_ROW_CAP))

    with pytest.raises(capture.TruncatedExport):
        capture.file_capture(source, raw_dir, account="0002", start=dt.date(2026, 8, 1),
                             end=dt.date(2026, 8, 31), captured=TODAY)

    assert os.path.exists(source), "a refused download must not be moved"
    assert not os.path.exists(store.manifest_path(raw_dir)), "nothing recorded"


def test_a_download_just_under_the_cap_is_fine(tmp_path: Any) -> None:
    """The guard must not reject a legitimately busy month."""
    raw_dir = os.path.join(str(tmp_path), "raw")
    source = _write(tmp_path, "Chase4242_Activity.CSV", _capped_card_csv(CHASE_ROW_CAP - 1))

    entry = capture.file_capture(source, raw_dir, account="0002", start=dt.date(2026, 8, 1),
                                 end=dt.date(2026, 8, 31), captured=TODAY)

    assert entry["status"] == "filed"
    assert entry["rows"] == CHASE_ROW_CAP - 1


def test_the_cli_reports_a_truncated_download_and_fails(tmp_path: Any, capsys: Any) -> None:
    """The agent must see this and narrow the window, not carry on."""
    raw_dir = os.path.join(str(tmp_path), "raw")
    source = _write(tmp_path, "Chase4242_Activity.CSV", _capped_card_csv(CHASE_ROW_CAP))

    rc = capture.main(["--raw-dir", raw_dir, "file", "--account", "0002",
                       "--start", "2026-08-01", "--end", "2026-08-31", source])

    assert rc == 1
    assert "truncated" in capsys.readouterr().err


def test_capture_and_parser_agree_on_the_cap() -> None:
    """Two guards, one number — they must not drift apart."""
    import sys as _sys
    _sys.path.insert(0, os.path.join(_ROOT, "src"))
    from providers import chase as chase_parser

    assert CHASE_ROW_CAP == chase_parser.ROW_CAP


# %%
# Automatic window sizing #


def test_a_busy_account_gets_narrow_windows() -> None:
    """A card at ~133 rows/month must not be pulled 6 months at a time."""
    # 133 rows over one month.
    captures = [_capture("2026-07-01", "2026-07-31", rows=133, min_date="2026-07-01", max_date="2026-07-31")]

    report = _analyze(captures, window_months="auto")

    assert report["window_months"] == 5  # 700 / 133


def test_a_quiet_account_gets_wide_windows() -> None:
    """A savings account with almost no activity can be caught up in one pull."""
    captures = [_capture("2025-08-01", "2026-07-31", rows=24, min_date="2025-08-01", max_date="2026-07-31")]

    report = _analyze(captures, window_months="auto")

    assert report["window_months"] == plan.MAX_WINDOW_MONTHS


def test_an_unknown_account_is_sized_cautiously() -> None:
    """With nothing held there is no density to measure, so guess small."""
    report = _analyze([], window_months="auto")

    assert report["window_months"] == plan.UNKNOWN_DENSITY_WINDOW_MONTHS


def test_auto_sizing_keeps_a_window_under_the_row_cap() -> None:
    """The whole point: an auto-sized window must not be refused at capture."""
    for per_month in (10, 48, 133, 400, 900):
        captures = [_capture("2026-07-01", "2026-07-31", rows=per_month,
                             min_date="2026-07-01", max_date="2026-07-31")]
        width = _analyze(captures, window_months="auto")["window_months"]
        assert width * per_month <= 1000, f"{per_month}/mo -> {width} months would truncate"


def test_an_explicit_width_still_overrides() -> None:
    """The flag remains for when you know better than the measurement."""
    captures = [_capture("2026-07-01", "2026-07-31", rows=133, min_date="2026-07-01", max_date="2026-07-31")]

    assert _analyze(captures, window_months=2)["window_months"] == 2


def test_forgetting_for_two_years_is_the_same_operation() -> None:
    """No separate 'backfill' mode: the same run just emits more windows."""
    stale = _analyze([_capture("2024-09-01", "2024-09-30", rows=48,
                               min_date="2024-09-01", max_date="2024-09-30")],
                     window_months="auto", max_months=40)
    fresh = _analyze([_capture("2026-07-01", "2026-08-22", rows=48,
                               min_date="2026-07-01", max_date="2026-08-20")],
                     window_months="auto", max_months=40)

    # Same command, same sizing logic — only the number of windows differs.
    assert stale["window_months"] == fresh["window_months"]
    assert len(stale["requests"]) > len(fresh["requests"])


# %%
# Empty windows #
#
# Chase serves NO file for a window with no activity — "We couldn't find any
# activity that matched the date range you chose." Without a record of that
# refusal, an empty month is indistinguishable from a never-fetched month and
# the planner asks for it forever.


def test_record_empty_marks_the_window_covered(tmp_path: Any) -> None:
    raw = str(tmp_path / "raw")
    assert capture.main([
        "--raw-dir", raw, "record-empty", "--account", ACCOUNT,
        "--start", "2026-03-01", "--end", "2026-05-31", "--captured", "2026-08-22",
    ]) == 0

    captures = store.read_manifest(raw)
    assert len(captures) == 1
    assert captures[0]["empty"] is True

    report = _analyze(captures + [
        _capture("2025-12-01", "2026-02-28"),
        _capture("2026-06-01", "2026-08-22", max_date="2026-08-21"),
    ], backfill_start=dt.date(2025, 12, 1))
    for month in ("2026-03", "2026-04", "2026-05"):
        assert month not in _months(report)


def test_record_empty_is_idempotent(tmp_path: Any) -> None:
    raw = str(tmp_path / "raw")
    args = ["--raw-dir", raw, "record-empty", "--account", ACCOUNT,
            "--start", "2026-03-01", "--end", "2026-05-31", "--captured", "2026-08-22"]
    assert capture.main(args) == 0
    assert capture.main(args) == 0
    assert len(store.read_manifest(raw)) == 1


def test_scan_captures_rebuilds_empty_markers_from_disk(tmp_path: Any) -> None:
    """The marker file, like any capture, survives a lost manifest."""
    raw = str(tmp_path / "raw")
    capture.main(["--raw-dir", raw, "record-empty", "--account", ACCOUNT,
                  "--start", "2026-03-01", "--end", "2026-05-31", "--captured", "2026-08-22"])
    os.remove(store.manifest_path(raw))

    entries = store.scan_captures(raw)
    assert len(entries) == 1
    assert entries[0]["empty"] is True
    assert entries[0]["requested_start"] == "2026-03-01"
    assert entries[0]["rows"] == 0
    assert "error" not in entries[0]


def test_ingest_classifies_an_empty_window_marker() -> None:
    name = store.empty_name(ACCOUNT, dt.date(2026, 3, 1), dt.date(2026, 5, 31), TODAY)
    assert ingest_raw.classify(name) == ("empty_window", dt.date(2026, 3, 1))


def test_empty_window_documents_have_a_registered_parser() -> None:
    """A permanent no_parser alarm would train you to ignore the real one."""
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "src"))
    import providers
    name = store.empty_name(ACCOUNT, dt.date(2026, 3, 1), dt.date(2026, 5, 31), TODAY)
    parser = providers.get_parser("chase", "empty_window", name)
    assert parser is not None
    parse_fn, _version = parser
    assert parse_fn(b"whatever the marker says", {"original_name": name}) == {}


# %%
# Retention floor vs coverage #


def test_the_floor_month_covered_from_the_floor_is_not_a_gap() -> None:
    """Days Chase will never serve cannot make a covered month look uncovered.

    A capture that starts exactly at the retention floor (2024-08-23 with today
    2026-08-22) covers everything its month can ever hold; reporting 2024-08 as
    a gap would re-request the same window on every run forever.
    """
    captures = [_capture("2024-08-23", "2026-08-22", rows=1000,
                         min_date="2024-08-23", max_date="2026-08-21")]

    report = _analyze(captures)

    assert "2024-08" not in _months(report)
    assert report["unreachable_months"] == []


# %%
# Provider dimension (citi) #

CITI_CSV = (
    "Status,Date,Description,Debit,Credit,Member Name\n"
    'Cleared,08/17/2026,"BOOKSTORE SEATTLE WA",12.34,,JANE Q MEMBER\n'
    "Cleared,08/01/2026,AUTOPAY AUTO-PMT,,250.00,JANE Q MEMBER\n"
)


def test_detects_the_citi_layout() -> None:
    described = store.read_export(CITI_CSV, provider="citi")

    assert (described["layout"], described["account_type"]) == ("citi_card", "credit")
    assert described["rows"] == 2
    assert (described["min_date"], described["max_date"]) == ("2026-08-01", "2026-08-17")


def test_a_citi_file_is_not_a_chase_export_and_vice_versa() -> None:
    """Cross-provider filing must fail loudly, not misfile."""
    with pytest.raises(store.UnrecognizedExport):
        store.read_export(CITI_CSV, provider="chase")
    with pytest.raises(store.UnrecognizedExport):
        store.read_export(
            "Transaction Date,Post Date,Description,Category,Type,Amount,Memo\n"
            "08/11/2026,08/12/2026,SHOP,Travel,Sale,-1.00,\n",
            provider="citi",
        )


def test_citi_capture_name_round_trips() -> None:
    name = store.capture_name("0001", dt.date(2026, 8, 1), dt.date(2026, 8, 24), TODAY,
                              provider="citi")

    assert name == "citi_csv_export_0001_20260801_20260824_captured20260822.csv"
    parsed = store.parse_capture_name(name)
    assert parsed["provider"] == "citi"
    # A provider filter excludes the other source's captures.
    assert store.parse_capture_name(name, "chase") is None


def test_manifests_are_kept_per_provider(tmp_path: Any) -> None:
    """Two sources sharing one raw_dir must not read each other's records."""
    raw_dir = str(tmp_path)
    store.append_manifest(raw_dir, {"file": "a"}, provider="chase")
    store.append_manifest(raw_dir, {"file": "b"}, provider="citi")

    assert [e["file"] for e in store.read_manifest(raw_dir, "chase")] == ["a"]
    assert [e["file"] for e in store.read_manifest(raw_dir, "citi")] == ["b"]


def test_citi_filing_records_provider_and_ignores_the_row_cap(tmp_path: Any) -> None:
    """Citi has no observed cap, so a huge export files fine."""
    raw_dir = os.path.join(str(tmp_path), "raw")
    body = "Status,Date,Description,Debit,Credit,Member Name\n" + "".join(
        f'Cleared,08/{i % 28 + 1:02d}/2026,"TXN {i}",1.00,,JANE Q MEMBER\n'
        for i in range(CHASE_ROW_CAP + 200)
    )
    source = _write(tmp_path, "Date range.CSV", body)

    entry = capture.file_capture(
        source, raw_dir, account="0001", start=dt.date(2024, 8, 5),
        end=dt.date(2026, 8, 24), captured=TODAY, provider="citi",
    )

    assert entry["status"] == "filed"
    assert entry["provider"] == "citi"
    assert entry["rows"] == CHASE_ROW_CAP + 200
    assert entry["file"].startswith("citi_csv_export_0001_")


def test_citi_download_names_carry_no_account_hint() -> None:
    """"Date range.CSV" proves nothing; the agent must pass --account."""
    assert store.account_hint_from_name("Date range.CSV", "citi") is None
    assert store.account_hint_from_name("Chase4242_Activity.CSV", "chase") == "4242"


def test_scan_captures_filters_by_provider(tmp_path: Any) -> None:
    raw_dir = str(tmp_path)
    _write(tmp_path, "citi_csv_export_0001_20260801_20260824_captured20260824.csv", CITI_CSV)
    _write(
        tmp_path,
        "chase_csv_export_0002_20260801_20260822_captured20260822.csv",
        "Transaction Date,Post Date,Description,Category,Type,Amount,Memo\n"
        "08/11/2026,08/12/2026,SHOP,Travel,Sale,-1.00,\n",
    )

    citi_entries = store.scan_captures(raw_dir, "citi")
    chase_entries = store.scan_captures(raw_dir, "chase")

    assert [e["account"] for e in citi_entries] == ["0001"]
    assert [e["account"] for e in chase_entries] == ["0002"]
    assert citi_entries[0]["rows"] == 2


def test_citi_record_empty_quotes_the_citi_portal_message(tmp_path: Any) -> None:
    raw_dir = os.path.join(str(tmp_path), "raw")
    rc = capture.main([
        "--provider", "citi", "--raw-dir", raw_dir, "record-empty",
        "--account", "0001", "--start", "2025-04-20", "--end", "2025-05-03",
        "--captured", "2026-08-24",
    ])

    assert rc == 0
    name = "citi_empty_window_0001_20250420_20250503_captured20260824.txt"
    with open(os.path.join(raw_dir, name), "r", encoding="utf-8") as handle:
        body = handle.read()
    assert "no transactions for this time period." in body
    entries = store.read_manifest(raw_dir, "citi")
    assert entries[0]["empty"] is True and entries[0]["provider"] == "citi"


def test_unknown_provider_fails_loudly() -> None:
    with pytest.raises(KeyError, match="unknown transaction provider"):
        store.provider_def("wells_fargo")
