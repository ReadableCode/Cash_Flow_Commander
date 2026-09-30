# %%
# Imports #

from typing import Any, Callable

from . import chase, citi, elan, enphase_enlighten, gexa, rhythm, smt


# %%
# Types #

# Usage parsers return a plain list (usage_intervals rows); bill/payment
# parsers return a dict of sink name -> rows. Both shapes flow through
# get_parser unchanged — the caller dispatches on the shape.
ParseResult = list[dict[str, Any]] | dict[str, list[dict[str, Any]]]
ParseFn = Callable[[bytes, dict[str, Any]], ParseResult]
NamePredicate = Callable[[str], bool]


# %%
# Registry #


def _any_name(name: str) -> bool:
    """Match any original_name."""
    return True


def _is_hourly_usage_csv(name: str) -> bool:
    """Match only the hourly_usage.csv export."""
    return name == "hourly_usage.csv"


def _is_payments_csv(name: str) -> bool:
    """Match only the payments.csv export."""
    return name == "payments.csv"


def _is_chase_capture(name: str) -> bool:
    """Match a Chase capture filed by transaction_downloader/capture.py."""
    return chase.account_from_capture_name(name) is not None


def _is_citi_capture(name: str) -> bool:
    """Match a Citi capture filed by transaction_downloader/capture.py."""
    return citi.account_from_capture_name(name) is not None


def _is_elan_capture(name: str) -> bool:
    """Match an Elan capture filed by transaction_downloader/capture.py."""
    return elan.account_from_capture_name(name) is not None


def _is_enphase_daily_energy(name: str) -> bool:
    """Match Enlighten daily_energy captures (the 15-minute interval series)."""
    return "daily_energy" in name


def _is_enphase_lifetime_energy(name: str) -> bool:
    """Match Enlighten lifetime_energy captures (daily rollups)."""
    return "lifetime_energy" in name


def _is_enphase_system_today(name: str) -> bool:
    """Match Enlighten api_system_today captures (system metadata, not measurements)."""
    return "api_system_today" in name


def _parse_nothing(content: bytes, ctx: dict[str, Any]) -> dict[str, Any]:
    """'Parse' a document held as evidence: there is nothing to extract, by design.

    Registering it makes the document parse `ok` with no rows instead of
    surfacing as `no_parser` on every run (a permanent false alarm trains you
    to ignore the real one). An empty dict is no sinks at all, so nothing is
    stamped, counted or written. Two kinds of document use it.

    A refetched-window marker is coverage evidence: the window was requested
    and the source served bytes already filed, which content_sha256 dedup then
    collapses onto the existing capture's row. Like an empty-window marker it
    deliberately emits no transactions_window sink: it proves a window was
    fetched, never what it contained, so it must not be able to prune rows a
    real export proved existed.

    An Enlighten api_system_today capture is system metadata, kept verbatim
    beside the measurements it was captured with.
    """
    return {}


# (provider, doc_type, name predicate, parse fn, parser version): data-driven
# so step-2 parsers slot in by appending tuples.
_REGISTRY: list[tuple[str, str, NamePredicate, ParseFn, str]] = [
    ("rhythm", "api_usage_json", _any_name, rhythm.parse_api_usage_json, rhythm.PARSER_VERSION),
    ("rhythm", "csv_export", _is_hourly_usage_csv, rhythm.parse_hourly_usage_csv, rhythm.PARSER_VERSION),
    ("rhythm", "api_invoice_json", _any_name, rhythm.parse_api_invoice_json, rhythm.BILL_PARSER_VERSION),
    ("rhythm", "bill_pdf", _any_name, rhythm.parse_bill_pdf, rhythm.BILL_PARSER_VERSION),
    ("rhythm", "csv_export", _is_payments_csv, rhythm.parse_payments_csv, rhythm.BILL_PARSER_VERSION),
    ("rhythm", "api_orders_json", _any_name, rhythm.parse_api_orders_json, rhythm.PLAN_PARSER_VERSION),
    # The meter's series belongs to the meter, not to whoever sells the power.
    ("smt", "smt_export", _any_name, smt.parse_interval_csv, smt.PARSER_VERSION),
    ("gexa", "api_invoice_json", _any_name, gexa.parse_api_invoice_json, gexa.BILL_PARSER_VERSION),
    ("gexa", "bill_pdf", _any_name, gexa.parse_bill_pdf, gexa.BILL_PARSER_VERSION),
    ("gexa", "api_orders_json", _any_name, gexa.parse_api_current_plan_json, gexa.PLAN_PARSER_VERSION),
    ("chase", "csv_export", _is_chase_capture, chase.parse_transactions_csv, chase.PARSER_VERSION),
    ("chase", "empty_window", _any_name, chase.parse_empty_window, chase.PARSER_VERSION),
    ("chase", "refetched_window", _any_name, _parse_nothing, chase.PARSER_VERSION),
    ("citi", "csv_export", _is_citi_capture, citi.parse_transactions_csv, citi.PARSER_VERSION),
    ("citi", "empty_window", _any_name, citi.parse_empty_window, citi.PARSER_VERSION),
    ("citi", "refetched_window", _any_name, _parse_nothing, citi.PARSER_VERSION),
    ("elan", "csv_export", _is_elan_capture, elan.parse_transactions_csv, elan.PARSER_VERSION),
    ("elan", "empty_window", _any_name, elan.parse_empty_window, elan.PARSER_VERSION),
    ("elan", "refetched_window", _any_name, _parse_nothing, elan.PARSER_VERSION),
    (
        "enphase_enlighten",
        "api_usage_json",
        _is_enphase_daily_energy,
        enphase_enlighten.parse_daily_energy_json,
        enphase_enlighten.PARSER_VERSION,
    ),
    (
        "enphase_enlighten",
        "api_usage_json",
        _is_enphase_lifetime_energy,
        enphase_enlighten.parse_lifetime_energy_json,
        enphase_enlighten.PARSER_VERSION,
    ),
    ("enphase_enlighten", "other", _is_enphase_system_today, _parse_nothing, enphase_enlighten.PARSER_VERSION),
]


# %%
# Functions #


def get_parser(
    provider: str, doc_type: str, original_name: str
) -> tuple[ParseFn, str] | None:
    """Resolve (parse_fn, parser_version) for a raw document, or None.

    None means no parser is registered for the document, so it stays pending
    and the CLI reports it.
    """
    for reg_provider, reg_doc_type, name_predicate, parse_fn, parser_version in _REGISTRY:
        if reg_provider == provider and reg_doc_type == doc_type and name_predicate(original_name):
            return parse_fn, parser_version
    return None
