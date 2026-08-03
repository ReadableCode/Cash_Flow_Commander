# %%
# Imports #

import csv
import io
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo


# %%
# Constants #

PARSER_VERSION = "smt-interval/1.0.0"

# Source timestamps are naive US/Central local times; they are localized with
# zoneinfo and converted to UTC. DST handling: spring-forward nonexistent
# local times resolve forward (zoneinfo default); fall-back ambiguous local
# times use fold=0 (the first occurrence). Both readings of a repeated local
# hour therefore map to the same UTC instant, and the natural-key upsert means
# the second overwrites the first — acceptable, documented here.
CENTRAL = ZoneInfo("America/Chicago")

_METRIC_BY_LABEL = {
    "Consumption": "consumption",
    "Surplus Generation": "generation",
}
_ESTIMATED_BY_FLAG = {
    "A": False,  # actual reading
    "E": True,  # estimated reading
}


# %%
# Functions #


def parse_interval_csv(content: bytes, ctx: dict[str, Any]) -> list[dict[str, Any]]:
    """Parse a Smart Meter Texas 15-minute interval CSV into usage row dicts.

    Header: ESIID,USAGE_DATE,REVISION_DATE,USAGE_START_TIME,USAGE_END_TIME,
    USAGE_KWH,ESTIMATED_ACTUAL,CONSUMPTION_SURPLUSGENERATION.

    The ESIID column (which carries a leading apostrophe) is ignored entirely:
    the series is keyed on the configured account id from ctx, never on values
    parsed from the document. ts is the interval start (USAGE_DATE MM/DD/YYYY
    + USAGE_START_TIME HH:MM, Central local converted to UTC). Blank lines are
    skipped; unknown metric labels raise ValueError (loud beats silent).
    """
    account_id = ctx["account_id"]
    reader = csv.DictReader(io.StringIO(content.decode("utf-8-sig")))
    rows: list[dict[str, Any]] = []
    for record in reader:
        if not any((value or "").strip() for value in record.values()):
            continue  # blank line

        # SMT emits empty USAGE_KWH (and empty ESTIMATED_ACTUAL) for the
        # nonexistent local intervals of a DST spring-forward hour — there is
        # no reading to store, so skip the row.
        if not record["USAGE_KWH"].strip():
            continue

        naive = datetime.strptime(
            f"{record['USAGE_DATE']} {record['USAGE_START_TIME']}",
            "%m/%d/%Y %H:%M",
        )
        ts = naive.replace(tzinfo=CENTRAL, fold=0).astimezone(timezone.utc)

        label = record["CONSUMPTION_SURPLUSGENERATION"].strip()
        metric = _METRIC_BY_LABEL.get(label)
        if metric is None:
            raise ValueError(
                f"unknown CONSUMPTION_SURPLUSGENERATION value {label!r}"
            )

        flag = record["ESTIMATED_ACTUAL"].strip()
        if flag not in _ESTIMATED_BY_FLAG:
            raise ValueError(f"unknown ESTIMATED_ACTUAL value {flag!r}")

        rows.append(
            {
                "account_id": account_id,
                "ts": ts,
                "granularity": "15min",
                "metric": metric,
                "value": Decimal(record["USAGE_KWH"]),
                "unit": "kwh",
                "rate": None,
                "cost": None,
                "estimated": _ESTIMATED_BY_FLAG[flag],
            }
        )
    return rows
