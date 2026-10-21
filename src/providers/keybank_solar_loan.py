# %%
"""KeyBank Solar Lending monthly statement parser.

One statement PDF (the `AFCLoanStatement.pdf` attachment of the monthly
statement mail) -> one account_balances row (account_kind 'loan': the
principal owed on the statement date, opening at the previous statement's
close) and one loan_transactions row per transaction in the period.

pypdf extracts the statement as one value per line. The transaction table runs
from `Beginning Balance` to `Ending Balance`; each transaction is one or two
dates, the signed amount, a description, the signed split values and the
principal balance after it. The split columns are not labelled per value, so
the principal share is taken from the balance change and the rest is interest.

The account id is the loan id the statement prints.
"""

# %%
# Imports #

import io
import re
from collections import Counter
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any

from pypdf import PdfReader


# %%
# Constants #

PARSER_VERSION = "keybank-solar-loan/1.0.0"

ACCOUNT_KIND = "loan"

_DATE_RE = re.compile(r"^(\d{2}/\d{2}/\d{4})(?:\s+\d{2}/\d{2}/\d{4})?$")
_NUMBER_RE = re.compile(r"^-?[\d,]+\.\d{2}$")


# %%
# Helpers #


def _tokens(content: bytes) -> list[str]:
    text = PdfReader(io.BytesIO(content)).pages[0].extract_text() or ""
    return [line.strip() for line in text.splitlines() if line.strip()]


def _us_date(text: str) -> date:
    return datetime.strptime(text, "%m/%d/%Y").date()


def _number(text: str) -> Decimal:
    return Decimal(text.replace(",", ""))


def _field(tokens: list[str], label: str) -> str:
    try:
        return tokens[tokens.index(label) + 1]
    except (ValueError, IndexError):
        raise ValueError(f"keybank statement: no {label!r} field") from None


def _check_account(account_id: str, ctx: dict[str, Any]) -> None:
    expected = ctx.get("account_id")
    if expected and str(expected) != account_id:
        raise ValueError(f"keybank statement: capture is for loan {account_id}, providers.local.yaml says {expected}")


def _transactions(body: list[str], opening: Decimal) -> list[dict[str, Any]]:
    """Split the tokens between the two balance lines into transactions."""
    rows: list[dict[str, Any]] = []
    balance = opening
    i = 0
    while i < len(body):
        date_match = _DATE_RE.match(body[i])
        if date_match is None:
            raise ValueError(f"keybank statement: expected a transaction date, got {body[i]!r}")
        trans_date = _us_date(date_match.group(1))
        i += 1
        while i < len(body) and _DATE_RE.match(body[i]):  # the effective date column
            i += 1
        amount = _number(body[i])
        description = body[i + 1]
        i += 2
        values = []
        while i < len(body) and _NUMBER_RE.match(body[i]):
            values.append(_number(body[i]))
            i += 1
        if not values:
            raise ValueError(f"keybank statement: no balance after {description!r}")
        *split, after = values
        if sum(split, Decimal("0")) != amount:
            raise ValueError(f"keybank statement: {description!r} splits {split} do not sum to {amount}")
        principal = after - balance
        rows.append(
            {
                "trans_date": trans_date,
                "description": description,
                "amount": amount,
                "principal": principal,
                "interest": amount - principal,
                "principal_balance": after,
            }
        )
        balance = after
    return rows


# %%
# Parsers #


def parse_statement_pdf(content: bytes, ctx: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    """One monthly statement -> the principal owed and the period's transactions."""
    tokens = _tokens(content)
    account_id = _field(tokens, "Account Number:")
    _check_account(account_id, ctx)
    statement_date = _us_date(_field(tokens, "Statement Date:"))
    outstanding = _number(_field(tokens, "Outstanding Principal:"))

    begin = tokens.index("Beginning Balance")
    end = tokens.index("Ending Balance")
    opened_on = _us_date(tokens[begin - 1])
    opening = _number(tokens[begin + 1])
    closing = _number(tokens[end + 1])
    if _us_date(tokens[end - 1]) != statement_date:
        raise ValueError("keybank statement: Ending Balance is not dated the statement date")
    transactions = _transactions(tokens[begin + 2 : end - 1], opening)

    last = transactions[-1]["principal_balance"] if transactions else opening
    if last != closing or closing != outstanding:
        raise ValueError(
            f"keybank statement: balances disagree (last transaction {last}, ending {closing}, "
            f"outstanding principal {outstanding})"
        )

    seen: Counter[tuple[Any, ...]] = Counter()
    for row in transactions:
        key = (row["trans_date"], row["description"], row["amount"])
        row.update(provider=ctx["provider"], account_id=account_id, occurrence=seen[key])
        seen[key] += 1

    return {
        "account_balances": [
            {
                "provider": ctx["provider"],
                "account_id": account_id,
                "account_kind": ACCOUNT_KIND,
                "as_of_date": statement_date,
                "balance": closing,
                # The opening balance is dated the previous statement date, so
                # this period starts the day after it.
                "period_start": opened_on + timedelta(days=1),
                "beginning_balance": opening,
            }
        ],
        "loan_transactions": transactions,
    }
