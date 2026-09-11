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
