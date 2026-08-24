# Expected Transactions

The bills, paychecks, and transfers we KNOW are coming, and which real
transactions turned out to be them. This replaces the manual step of typing
`Amount_Paid` / `Date_Paid` into the Our_Cash sheet.

## The three tables

```
expected_series       the obligation      ('Mortgage', monthly on the 6th, -2294)
expected_occurrences  one instance of it  (the Sept 6 mortgage payment)
expected_matches      occurrence -> real transaction(s), chosen by a person
```

These are **human-authored source of truth**, like `raw_documents` and unlike
the `transactions` projection. The never-hand-edit rule does not apply here;
the never-automatically-edit rule does. Nothing automated may modify or delete
a row a person wrote.

## Rules that keep history true

- **Series are append-only.** Switching electric companies, a price change, a
  new card — close the old series (`active_until`) and add a new one,
  optionally linked via `replaces_series_id`. Old matches keep pointing at the
  old series forever. Overlapping series are fine. The TUI has no edit action
  on purpose.
- **Paid status is derived, never stored.** An occurrence is paid when its
  active matches resolve against live `transactions` rows AND their net moves
  the expected direction. A restated transaction (`sync_capture` prunes and
  re-inserts) makes the match stop resolving → status `broken`. A payment plus
  its reversal nets to zero → status `net_zero`. Both come back to the pairing
  queue by themselves.
- **Matching is suggest-then-confirm.** `expected_suggest` ranks candidates;
  only a person pressing a key in the TUI creates a match. A transaction that
  merely looks like your mortgage (a friend's mortgage at the same company)
  can never attach itself — pair it to its own series instead.
- **One transaction belongs to one occurrence — unless every claim states its
  share.** One occurrence may hold many transactions (two gym charges, split
  tax payments, both legs of a card payoff). The reverse — one bill paying
  several expected items, like the mortgage payment that also covers the PMI
  line — is a *split*: each match carries a `matched_amount`, and a whole-
  transaction claim can never coexist with a split, so nothing double-counts.
  A double payment covering next month too: match it here, skip next month's
  occurrence with a note.
- **Transfers are captured, not double-counted.** A card-payoff series has
  `is_transfer` and a `transfer_account_id`; its occurrence matches both legs
  (-X from the bank, +X on the card). Only the cash leg counts toward the paid
  net; the card purchases themselves remain the real expenses.

## Daily use

```
uv run python src/expected_tui.py        # one app: pairing, t = transactions, e = series
uv run python src/expected_checks.py     # broken / past-due / net-zero / drift report
uv run python src/expected_discover.py   # recurring transactions no series accounts for
```

In the pairing screen's match view: space claims a whole transaction, x claims
a stated share of one (splits). Closing a series deletes its untouched
generated future occurrences; matched, skipped, or hand-added ones survive.

Occurrences are generated to a 2-year horizon by `generate_occurrences`
(idempotent; runs after the import and whenever a series is added in the TUI).

## Modules

```
src/expected_schedule.py      schedule -> due dates (pure date math)
src/expected_store.py         all reads/writes + the derived-status query
src/expected_suggest.py       candidate ranking (pure; never writes)
src/expected_tui.py           Textual UI: pairing + transactions + series screens
src/expected_checks.py        quiet-failure report, exit 1 on findings
src/expected_discover.py      recurring-but-unbudgeted transaction finder
src/expected_import_sheet.py  one-time import from the sheet backups
```

Personal mappings (sheet account labels -> `transactions.account_id`, transfer
counterparties) live in the `our_cash` section of `providers.local.yaml`,
never in this repo.

## The one-time import (done 2026-08-24)

Backups of the ingested tabs live in `data/sheet_backups/2026-08-24/`
(untracked). The import created series from Income_Expense (plus closed series
for names found only in report history), imported past report rows verbatim as
occurrences, and recovered matches best-effort: exact amount within ±5 days of
the sheet's `Date_Paid`, unambiguous single hit only, preferring the sheet's
autopay account. Payments predating Chase's 24-month transaction retention
were skipped with the sheet's facts preserved in `skip_note`. Everything else
was left unpaid for the pairing screen — see `import_report.csv` in the backup
dir for the row-by-row outcome.
