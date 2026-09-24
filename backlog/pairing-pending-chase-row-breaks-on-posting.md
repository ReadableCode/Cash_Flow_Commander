# pairing: a match on a still-pending Chase bank row breaks when the row posts

    found:  2026-09-24
    status: open
    verify: grep -c "pending" src/pairing_web.py   # 0 = the board still accepts a pair on a row with no balance

A Chase bank CSV includes same-day ACH activity before it has posted. Those
rows carry the NACHA detail as their description (`ORIG CO NAME:... CO ENTRY
DESCR:...`) and an EMPTY `Balance` column. When the row posts, Chase restates
the description to its final short form and fills the balance. The next
capture of an overlapping window replaces the stored row (`sync_capture`
drops rows the newer capture no longer carries), and any match made on the
pending row no longer resolves: `expected_checks` reports the occurrence as
`broken`, and the board shows the bill unpaid again.

Matches store the transaction's natural key (`account_id, post_date,
description, amount, occurrence`), so the restated description is a different
transaction as far as the match is concerned. `src/transaction_store.py:35`
already documents that "a pending row's description and even its amount can
change once it posts"; nothing stops a person pairing one before that happens.

## Evidence

It has happened twice, both times on the day the pending row was captured:

    id   txn_post_date  txn_description                                                                            matched_at            voided_at
    999  2026-08-24     ORIG CO NAME:<payee> CO ENTRY DESCR:PAYMENT SEC:PPD ORIG ID:<id>                    2026-08-24 17:44:07   2026-09-14 14:59:53
    1024 2026-09-14     ORIG CO NAME:<processor> CO ENTRY DESCR:PURCHASE SEC:WEB IND ID:<merchant> ORIG ID:<id>  2026-09-14 16:54:44   2026-09-24 16:53:02

The same row in the two raw captures (`raw_documents` 2014, captured
2026-09-14, and 2257, captured 2026-09-24). Pending: no balance. Posted:
balance present, description rewritten.

    DEBIT,09/14/2026,"ORIG CO NAME:<processor>      CO ENTRY DESCR:PURCHASE   SEC:WEB IND ID:<merchant> ORIG ID:<id>",-X.XX,ACH_DEBIT, ,,
    DEBIT,09/14/2026,"<processor>      PURCHASE   <merchant> WEB ID: <id>",-X.XX,ACH_DEBIT,<balance>,,

`expected_checks --days-back 120` on 2026-09-24, before the re-pair:

    broken   <series>   2026-09-12   a matched transaction no longer exists

Exposure right now (0 after the 2026-09-24 re-pair; re-run before acting):

    uv run python -c "import sys; sys.path.insert(0, 'src'); import bootstrap, db; from sqlalchemy import text; print(db.get_engine().connect().execute(text(\"select count(*) from expected_matches where voided_at is null and txn_description like 'ORIG CO NAME%'\")).scalar())"

## fix

Every posted Chase bank row carries a balance, so "bank row with no balance"
is the pending test. Refuse the pair at the write, in both front ends:

1. `src/pairing_web.py:348` `apply_match`: after building `txn_dict`, select
   the row from `db.transactions` by the natural key; if
   `account_kind == "bank"` and `balance is None`, raise
   `ValueError("this row is still pending; Chase restates its description when it posts. Pair it after the next capture.")`.
   The server already answers a `ValueError` as a 400 with the message, and
   the page shows it in the card (`test_server_answers_store_errors_as_400`).
2. The TUI's pair action in `src/cfc_tui.py` (space / x in the match view)
   goes through the same check; put the check in one function next to
   `apply_match` and call it from both, so there is one rule.
3. Optional, same change: dim such rows on the board's right column so the
   reason is visible before the click.
4. Test in `tests/test_pairing_web.py`: `add_transaction` a bank row with
   `balance=None`, `call(srv, "/api/match", ...)` → 400 and no row in
   `expected_matches`.

## blast radius

Pairing only. The guard must be limited to `account_kind == "bank"`: card
exports never carry a balance, and an
unconditional check would block every card pair. Nothing else reads balance
null-ness except `expected_forecast.get_anchor`, which already filters on
`balance IS NOT NULL`.

## not doing yet

Out of scope of the 2026-09-24 pairing session, which only re-paired the two
broken lines. One decision first: hide pending rows from the board entirely
(parser or query level) or show them unpairable (write level, as above).
Showing them keeps "the payment is on its way" visible, so the write-level
guard is the recommendation.
