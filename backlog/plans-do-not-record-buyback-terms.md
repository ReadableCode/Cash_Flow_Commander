# solar: nothing records whether a plan pays buyback, so a missing buyback line cannot be checked

    found:  2026-09-11
    status: open
    verify: grep -n -i -E "buyback|plans" src/db.py   # nothing = no plan terms are stored

The dashboards read solar buyback from the `solar_buyback` line-item category,
which only a provider's own buyback wording can produce. That part was fixed on
2026-09-28, and a plan that pays no buyback now shows a buyback of zero and is
valued on avoided cost alone. What is still missing is the other direction: the
schema has no record of a plan's terms, so nothing can tell a bill that
correctly has no buyback line from one whose buyback line failed to parse.

## Evidence

`verify:` returned nothing on 2026-09-28.

Line items by category on 2026-09-28, after the reparse:

    base 21, delivery_fixed 57, delivery_variable 58, energy 68, fee 1,
    other 1, solar_buyback 57, tax 177

All 57 `solar_buyback` rows are Rhythm's. The Gexa bill (service
2026-09-04..2026-09-24) has none, which is right for a plan with no buyback,
and is also what a parse that dropped the line would look like. The meter
recorded export in that period either way.

Rhythm's plan terms sit unparsed in its orders captures (see
`rhythm-orders-json-no-parser.md`). Gexa's are in its Electricity Facts Label,
which is not captured at all.

## fix

1. Decide the `plans` sink from `rhythm-orders-json-no-parser.md`: one row per
   account and term, with the energy rate, the base charge, and a buyback rate
   that is NULL when the plan pays none.
2. Rhythm terms come from the orders JSON (`solar_buyback_kwh_rate`, trailing
   13 months) and, for older bills, the PDF line
   `Solar Buyback Credit N kWh x R c/kWh`.
3. Capture the Gexa Electricity Facts Label in `/bills-gexa` and record its
   plan with no buyback.
4. Add `bills_buyback_mismatch` to `CHECKS` in `src/checks.py`: fail when a
   bill has a `solar_buyback` line but its plan has no buyback rate, or its
   plan has a rate, the meter shows export over the service period, and the
   bill has no `solar_buyback` line. Tests alongside the existing checks tests.

## blast radius

A new table and a new check; no existing row changes. The check needs a plan
term for every bill it covers, or it fails the whole history: backfill terms
first, or scope the rule to bills on or after the first recorded term.

## not doing yet

The `plans` sink is still undecided, and it is the larger half of this. The
risk it guards is small today: the only plan with buyback has ended, and a
credit on the current plan lands in `credit`, which no panel reads as buyback.
