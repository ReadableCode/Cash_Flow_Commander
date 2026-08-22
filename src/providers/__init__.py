# %%
# Imports #

from typing import Any, Callable

from . import chase, enphase_enlighten, rhythm, smt


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


def _is_enphase_daily_energy(name: str) -> bool:
    """Match Enlighten daily_energy captures (the 15-minute interval series)."""
    return "daily_energy" in name


def _is_enphase_lifetime_energy(name: str) -> bool:
    """Match Enlighten lifetime_energy captures (daily rollups)."""
    return "lifetime_energy" in name


# (provider, doc_type, name predicate, parse fn, parser version) — data-driven
# so step-2 parsers slot in by appending tuples.
_REGISTRY: list[tuple[str, str, NamePredicate, ParseFn, str]] = [
    ("rhythm", "api_usage_json", _any_name, rhythm.parse_api_usage_json, rhythm.PARSER_VERSION),
    ("rhythm", "csv_export", _is_hourly_usage_csv, rhythm.parse_hourly_usage_csv, rhythm.PARSER_VERSION),
    ("rhythm", "smt_export", _any_name, smt.parse_interval_csv, smt.PARSER_VERSION),
    ("rhythm", "api_invoice_json", _any_name, rhythm.parse_api_invoice_json, rhythm.BILL_PARSER_VERSION),
    ("rhythm", "bill_pdf", _any_name, rhythm.parse_bill_pdf, rhythm.BILL_PARSER_VERSION),
    ("rhythm", "csv_export", _is_payments_csv, rhythm.parse_payments_csv, rhythm.BILL_PARSER_VERSION),
    ("chase", "csv_export", _is_chase_capture, chase.parse_transactions_csv, chase.PARSER_VERSION),
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
