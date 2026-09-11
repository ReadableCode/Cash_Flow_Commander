# solar: buyback is inferred from a generic credit line, so any credit on a no-buyback plan reads as buyback

    found:  2026-09-11
    status: open
    verify: grep -c "li.category = 'credit'" deploy/grafana/solar_net_metering.json; grep -n '"solar buyback credit", "credit"' src/providers/rhythm.py; grep -n -i -E "buyback|plans" src/db.py

Rhythm service appears to have ended 2026-09-03 (closing invoice INV05217386,
service 2026-08-28..2026-09-03). The next retail plan pays **no** solar buyback,
and that provider is not onboarded yet (no login as of 2026-09-11). Nothing in
the schema records whether a plan pays buyback. The dashboards infer it from bill
lines, and the category they read is the generic `credit`, which is the natural
category for a new provider's parser to use for a sign-up bonus, goodwill credit
or autopay discount. Any of those would be charted as solar buyback and added to
"Total solar value".

## Evidence

`verify:` on 2026-09-11 returned `3`, then
`172:    ("solar buyback credit", "credit"),`, then nothing from `src/db.py`.

The three value panels on `cfc-solar-net-metering` ("What the panels were worth,
per billing period", "Solar value in selected range", "Where the value comes
from"; `solar_net_metering.json` lines 162, 226, 301) all compute

    COALESCE(-SUM(li.amount) FILTER (WHERE li.category = 'credit'), 0) AS buyback_credit

and plot it as "Buyback credit (exported)" / "Buyback credit".

Today every `credit` row is a solar buyback line, so the current numbers are
right (57 rows):

    description                                                 section     rows  first       last        total
    Credit -- Jan 30, 2022 -- Solar Buyback Credit 362 kWh ...  adjustment     1  2022-01-30  2022-01-30   -26.02
    Credit -- Feb 27, 2022 -- Solar Buyback Credit 427 kWh ...  adjustment     1  2022-02-27  2022-02-27   -30.70
    Credit -- Mar 30, 2022 -- Solar Buyback Credit 648 kWh ...  adjustment     1  2022-03-30  2022-03-30   -46.51
    Solar Buyback Credit                                        current       33  2022-04-28  2024-12-30  -785.49
    Solar Buyback Credit - Applied Towards Energy               adjustment     4  2025-01-30  2025-04-29   -76.76
    Solar Buyback Credit - Applied Towards Energy               energy        17  2025-05-29  2026-09-09  -293.50

All line-item categories: `base 21, credit 57, delivery 113, energy 67, other 1, tax 171`.

A related label: "Monthly energy: billed vs hidden vs exported" (line 590) names
SMT surplus generation `AS "Exported (buyback)"`. SMT keeps recording export
under any retailer, so after the switch that label describes kWh that earn nothing.

Hourly `cost` values are not a risk: they come verbatim from Rhythm's usage API
(`generation_earned`), SMT rows store `cost` as NULL, and no SQL carries Rhythm's
5.24 c/kWh buyback rate forward.

## fix

1. Dedicated category. In `src/providers/rhythm.py` `_CATEGORY_RULES` change
   `("solar buyback credit", "credit")` to `("solar buyback credit", "solar_buyback")`
   (the 2022 `Credit -- ... Solar Buyback Credit` lines match the same rule).
   Bump the rhythm bills `PARSER_VERSION`, then

       uv run python src/parse_raw.py --provider rhythm --status all

   and confirm the category counts show `solar_buyback 57` and no `credit`.
2. Dashboards. In the three value panels change `li.category = 'credit'` to
   `li.category = 'solar_buyback'`, and relabel `"Exported (buyback)"` to
   `"Exported"`. Round-trip:

       uv run python deploy/grafana_sync.py import deploy/grafana/solar_net_metering.json
       uv run python deploy/grafana_sync.py verify cfc-solar-net-metering

3. Plan buyback terms. Decide the `plans` sink from
   `rhythm-orders-json-no-parser.md`, with a buyback rate that is NULL when the
   plan pays none. Rhythm terms come from the orders JSON
   (`solar_buyback_kwh_rate`, trailing 13 months) and, for older bills, the PDF
   line `Solar Buyback Credit N kWh x R c/kWh`. Record the new provider's plan
   with no buyback when onboarding it through `/bills-add-company`.
4. Consistency check. Add `bills_buyback_mismatch` to `CHECKS` in
   `src/checks.py`: fail when a bill has a `solar_buyback` line but its plan
   term has no buyback rate, or its plan has a rate, SMT shows export over the
   service period, and the bill has no `solar_buyback` line. Tests alongside the
   existing rhythm parser and checks tests.

## blast radius

The reparse rewrites the category on 57 existing rows (natural-key upsert, no
row counts change). "Monthly cost by category (stacked)" on `cfc-energy` groups
by category, so its `credit` series is renamed `solar_buyback`, a label change
only. Between the reparse and the dashboard import the buyback panels read 0, so
do both in one sitting. The new check needs a plan term for every historical
bill, or it fails the whole history; backfill terms first, or scope the rule to
bills on or after the first recorded term.

## not doing yet

Deferred on 2026-09-11 to the onboarding of the new provider, where it will be
picked up. The `plans` sink is still undecided, and the new provider's credit
wording is unknown until its login exists. Do it together with
`avoided-cost-keys-on-rhythm-delivery-wording.md`, which reparses the same
bills, and `solar-value-drops-unbilled-periods.md`.
