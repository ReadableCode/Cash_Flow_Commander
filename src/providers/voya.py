# %%
"""Voya Financial retirement account parsers.

Three captures, three shapes:

  - the quarterly statement PDF -> one account_balances row with the period's
    flows (Your Account Summary on page 1, fee totals further in);
  - the dashboard accounts JSON -> one account_balances row per plan, balance
    only, as of the date the portal states;
  - the transaction history CSV -> account_activity rows, the whole history.

The account id is the plan number, which every capture prints.
"""

# %%
# Imports #

import csv
import io
import json
import re
from collections import Counter
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from pypdf import PdfReader


# %%
# Constants #

PARSER_VERSION = "voya/1.0.0"

ACCOUNT_KIND = "retirement"

_MONEY = r"(-?)\$([\d,]+\.\d{2})"

_BEGINNING_RE = re.compile(rf"^Account Balance on (\d{{2}}/\d{{2}}/\d{{4}}) {_MONEY}$", re.M)
_YOUR_CONTRIBUTIONS_RE = re.compile(rf"^Your Contributions {_MONEY}$", re.M)
_EMPLOYER_CONTRIBUTIONS_RE = re.compile(rf"^Employer Contributions {_MONEY}$", re.M)
_WITHDRAWALS_RE = re.compile(rf"^Withdrawals {_MONEY}$", re.M)
_EARNINGS_RE = re.compile(rf"^Investment Earnings {_MONEY}$", re.M)
_VESTED_RE = re.compile(rf"^Vested Balance {_MONEY}$", re.M)
# `Total Asset-Based Fee -$23.92`, one line per fee type in the fee detail.
_FEE_TOTAL_RE = re.compile(rf"^Total [^\n$]*Fee {_MONEY}\s*$", re.M)
_PLAN_NUMBER_RE = re.compile(r"Plan Number Plan Type\s*\n\s*(\d+)")

# `"$163.46"`, `"($10.63)"`, `"(0.2015)"`: parentheses mean negative.
_CSV_NUMBER_RE = re.compile(r"^(\()?\$?([\d,]+\.\d+)\)?$")


# %%
# Helpers #


def _money(match: re.Match[str], first_group: int = 1) -> Decimal:
    sign, digits = match.group(first_group), match.group(first_group + 1)
    value = Decimal(digits.replace(",", ""))
    return -value if sign else value


def _required(pattern: re.Pattern[str], text: str, label: str) -> re.Match[str]:
    match = pattern.search(text)
    if match is None:
        raise ValueError(f"voya statement: no {label} line")
    return match


def _us_date(text: str) -> date:
    return datetime.strptime(text, "%m/%d/%Y").date()


def _csv_number(text: str) -> Decimal | None:
    text = text.strip()
    if not text:
        return None
    match = _CSV_NUMBER_RE.match(text)
    if match is None:
        raise ValueError(f"voya activity: unreadable number {text!r}")
    value = Decimal(match.group(2).replace(",", ""))
    return -value if match.group(1) else value


def _check_account(account_id: str, ctx: dict[str, Any]) -> None:
    expected = ctx.get("account_id")
    if expected and str(expected) != account_id:
        raise ValueError(f"voya: capture is for plan {account_id}, providers.local.yaml says {expected}")


# %%
# Parsers #


def parse_statement_pdf(content: bytes, ctx: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    """One quarterly statement -> its closing balance and the period's flows.

    The summary must add up: beginning + contributions + withdrawals +
    earnings == ending. A statement that does not is an error, not a row.
    """
    text = "\n".join(page.extract_text() or "" for page in PdfReader(io.BytesIO(content)).pages)
    balances = _BEGINNING_RE.findall(text)
    if len(balances) < 2:
        raise ValueError("voya statement: no beginning and ending Account Balance lines")
    (start_text, start_sign, start_digits), (end_text, end_sign, end_digits) = balances[0], balances[1]
    beginning = Decimal(start_digits.replace(",", "")) * (-1 if start_sign else 1)
    ending = Decimal(end_digits.replace(",", "")) * (-1 if end_sign else 1)

    yours = _money(_required(_YOUR_CONTRIBUTIONS_RE, text, "Your Contributions"))
    employer_match = _EMPLOYER_CONTRIBUTIONS_RE.search(text)
    employer = _money(employer_match) if employer_match else Decimal("0.00")
    withdrawals = _money(_required(_WITHDRAWALS_RE, text, "Withdrawals"))
    earnings = _money(_required(_EARNINGS_RE, text, "Investment Earnings"))
    vested_match = _VESTED_RE.search(text)
    fee_totals = [_money(match) for match in _FEE_TOTAL_RE.finditer(text)]

    contributions = yours + employer
    if beginning + contributions + withdrawals + earnings != ending:
        raise ValueError(
            f"voya statement: summary does not add up ({beginning} + {contributions} + "
            f"{withdrawals} + {earnings} != {ending})"
        )

    account_id = _required(_PLAN_NUMBER_RE, text, "Plan Number").group(1)
    _check_account(account_id, ctx)
    return {
        "account_balances": [
            {
                "provider": ctx["provider"],
                "account_id": account_id,
                "account_kind": ACCOUNT_KIND,
                "as_of_date": _us_date(end_text),
                "balance": ending,
                "vested_balance": _money(vested_match) if vested_match else None,
                "period_start": _us_date(start_text),
                "beginning_balance": beginning,
                "contributions": contributions,
                "employer_contributions": employer,
                "withdrawals": withdrawals,
                "earnings": earnings,
                "fees": sum(fee_totals, Decimal("0.00")) if fee_totals else None,
            }
        ]
    }


def parse_accounts_json(content: bytes, ctx: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    """The dashboard's account list -> one balance per plan, as of the stated date."""
    payload = json.loads(content)
    rows = []
    for category in payload.get("categorizedAccounts") or []:
        for account in category.get("accounts") or []:
            if account.get("isRetailAccount"):
                continue
            account_id = str(account["planId"])
            _check_account(account_id, ctx)
            rows.append(
                {
                    "provider": ctx["provider"],
                    "account_id": account_id,
                    "account_kind": ACCOUNT_KIND,
                    "as_of_date": _us_date(account["accountBalanceAsOf"]),
                    "balance": Decimal(str(account["accountBalance"])),
                }
            )
    if not rows:
        raise ValueError("voya accounts: no retirement accounts in the capture")
    return {"account_balances": rows}


def parse_activity_csv(content: bytes, ctx: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
    """The transaction history export -> one account_activity row per line.

    The file opens with a blank line and a `<sponsor name>  <plan number>`
    line, then the header. A window with no activity has no header, only
    "You have no transaction history." and yields no rows.
    """
    text = content.decode("iso-8859-1")
    lines = text.splitlines()
    header_at = next((i for i, line in enumerate(lines) if line.startswith("Activity Date,")), None)
    if header_at is None:
        if "no transaction history" in text:
            return {"account_activity": []}
        raise ValueError("voya activity: no header line")
    plan_line = next((line for line in lines[:header_at] if line.strip()), "")
    plan_match = re.search(r"(\d+)\s*$", plan_line)
    if plan_match is None:
        raise ValueError("voya activity: no plan number above the header")
    account_id = plan_match.group(1)
    _check_account(account_id, ctx)

    rows: list[dict[str, Any]] = []
    seen: Counter[tuple[Any, ...]] = Counter()
    for record in csv.reader(lines[header_at + 1 :]):
        if not record or not record[0].strip():
            continue
        activity_date, activity, fund, money_source, units, unit_price, amount = record[:7]
        row = {
            "provider": ctx["provider"],
            "account_id": account_id,
            "activity_date": _us_date(activity_date),
            "activity": activity.strip(),
            "fund": fund.strip(),
            "money_source": money_source.strip(),
            "units": _csv_number(units),
            "unit_price": _csv_number(unit_price),
            "amount": _csv_number(amount),
        }
        if row["amount"] is None:
            raise ValueError(f"voya activity: no amount on {record!r}")
        key = (row["activity_date"], row["activity"], row["fund"], row["money_source"], row["amount"])
        row["occurrence"] = seen[key]
        seen[key] += 1
        rows.append(row)
    return {"account_activity": rows}
