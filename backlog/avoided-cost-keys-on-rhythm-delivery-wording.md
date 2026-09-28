# solar: avoided cost finds per-kWh delivery by Rhythm's line wording

    found:  2026-09-11
    status: open
    verify: grep -c "li.description ILIKE '%per kWh%' OR li.description ILIKE '%- Energy%'" deploy/grafana/solar_net_metering.json

The three solar value panels on `cfc-solar-net-metering` ("What the panels were
worth, per billing period", "Solar value in selected range", "Where the value
comes from") price self-consumed solar at a marginal rate built from the energy
rate plus the per-kWh part of delivery. They find that per-kWh part by matching
the line description, which is Rhythm's wording. Rhythm service appears to have
ended 2026-09-03, and the next retail provider (not onboarded yet, 2026-09-11)
will word its bill differently.

## Evidence

`verify:` returned `3` on 2026-09-11. Each panel's `bill_rates` CTE has:

    COALESCE(SUM(li.amount) FILTER (
      WHERE li.category = 'delivery'
        AND (li.description ILIKE '%per kWh%' OR li.description ILIKE '%- Energy%')
    ), 0) AS delivery_variable

and then

    (COALESCE(energy_rate_cents, 0) + 100.0 * delivery_variable / total_kwh)
      * (1 + taxes / NULLIF(total_current_charges - taxes, 0)) / 100.0 AS marginal_rate

Rhythm's two delivery lines on INV05217386 (`bill_line_items`, category
`delivery` for both):

    Oncor - Delivery charge per kWh     $26.73   (443.313 kWh -> 6.03 c/kWh)
    Oncor - Delivery charge per month    $1.35

On that bill the energy rate is 17.471 c/kWh, so the per-kWh delivery is about a
quarter of the pre-tax marginal rate. A bill whose per-kWh delivery line matches
neither pattern gets `delivery_variable = 0`, and its avoided cost is understated
by that quarter with no error anywhere. `src/checks.py` does not look at delivery,
so the run still finishes green. (A missing energy rate *is* caught, by
`bills_without_energy_rate`.)

## fix

Classify variable versus fixed delivery at parse time, in each provider's parser,
instead of by description in SQL:

1. Split the category: per-kWh delivery lines become `delivery_variable`, flat
   ones `delivery_fixed`. In `src/providers/rhythm.py` `_CATEGORY_RULES`, put
   `("oncor - delivery charge per kwh", "delivery_variable")` and the per-month
   rule ahead of the generic `("oncor - delivery", ...)` / `("tdu delivery", ...)`
   rules, and check the 2022 flat layout's `- Energy` lines against real bills.
2. Bump the rhythm bills `PARSER_VERSION`, then
   `uv run python src/parse_raw.py --provider rhythm --status all`.
3. In the three panels replace the description filter with
   `li.category = 'delivery_variable'`, then round-trip:

       uv run python deploy/grafana_sync.py import deploy/grafana/solar_net_metering.json
       uv run python deploy/grafana_sync.py verify cfc-solar-net-metering

4. Add a `checks.py` rule: a bill with delivery charges but no
   `delivery_variable` line fails the run. Tests alongside the existing
   rhythm parser and checks tests.

## blast radius

Reparse rewrites the `category` of existing Rhythm delivery rows (same rows,
natural-key upsert, no counts change). "Monthly cost by category (stacked)" on
`cfc-energy` groups by category, so its single delivery stack becomes two until
the panel maps them back together. Between the reparse and the dashboard import
the value panels read `delivery_variable = 0`, so do both in one sitting.

## not doing yet

The new provider's bill layout is unknown until its login exists. Design the
categories against a real second bill format, as part of onboarding it with
`/bills-add-company`, rather than guessing now. Do it together with
`solar-buyback-inferred-from-generic-credit.md`, which reparses the same bills.

## update 2026-09-28: the second bill format is known, and it has no per-kWh line

Gexa was onboarded on 2026-09-28 (`src/providers/gexa.py`). `verify:` still
returns `3`. Its first bill prints delivery as one lump sum, with no kWh and no
rate on the line:

    *TDU Delivery Charges                          $83.22
    Out of Cycle Meter Reading Regular Hours        $0.20

The parser files the first as `delivery` and the second as `fee`. Neither
description matches `%per kWh%` or `%- Energy%`, so once this bill is landed
the three panels compute `delivery_variable = 0` for it, exactly the silent
understatement described above.

This changes step 1 of the fix. Splitting the category by line wording cannot
work for a bill that never states the split: the variable part has to be
derived, from the delivery utility's published per-kWh and per-month charges
for the service period, or as the lump sum less the fixed monthly charge.
Decide which before writing the parser change. No Gexa bill had been landed
when this was written, so the panels have not yet shown the wrong number.
