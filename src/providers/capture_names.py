"""Parse transaction-capture filenames, for any provider.

The capture store (transaction_downloader/store.py) writes every capture as

    {provider}_csv_export_{account}_{YYYYMMDD}_{YYYYMMDD}_captured{YYYYMMDD}.csv

and that name is the only place the *requested* window and the capture date
live once the bytes are in raw_documents. Both the per-provider parsers and
transaction_store's restatement reconciliation need to read it back, so the
inverse lives here once rather than per provider.
"""

# %%
# Imports #

import datetime
import re
from typing import Any

# %%
# Constants #

# The `_csv_export_` literal is the unambiguous delimiter between the provider
# slug (which may itself contain underscores) and the account.
_CAPTURE_META_RE = re.compile(
    r"^(?P<provider>[a-z0-9_]+?)_csv_export_(?P<account>[A-Za-z0-9\-]+)"
    r"_(?P<start>\d{8})_(?P<end>\d{8})_captured(?P<captured>\d{8})(?:\(\d+\))?\.csv$",
    re.IGNORECASE,
)


# %%
# Functions #


def capture_meta_from_name(name: str, provider: str | None = None) -> dict[str, Any] | None:
    """Parse provider, account, requested window, and capture date from a capture name.

    Returns {'provider', 'account', 'start', 'end', 'captured'} with real date
    objects, or None when the name is not a capture — or belongs to a different
    provider than the one asked for. The capture date is the authority order
    between overlapping downloads of the same window: a later download reflects
    the portal's restatements and supersedes an earlier one.
    """
    match = _CAPTURE_META_RE.match(name or "")
    if match is None:
        return None
    if provider is not None and match.group("provider").lower() != provider:
        return None
    try:
        return {
            "provider": match.group("provider").lower(),
            "account": match.group("account"),
            "start": datetime.datetime.strptime(match.group("start"), "%Y%m%d").date(),
            "end": datetime.datetime.strptime(match.group("end"), "%Y%m%d").date(),
            "captured": datetime.datetime.strptime(match.group("captured"), "%Y%m%d").date(),
        }
    except ValueError:
        return None


# %%
