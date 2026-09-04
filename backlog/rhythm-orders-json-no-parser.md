# parse: rhythm api_orders_json has no parser; 28 docs permanently no_parser

    found:  2026-09-04
    status: open
    verify: uv run python src/parse_raw.py --provider rhythm | grep no_parser

`src/providers/__init__.py::_REGISTRY` has no `("rhythm", "api_orders_json", ...)`
entry, so those documents can never be parsed and are reported on every run. The
`/bills-rhythm` verification checklist requires **zero** `no_parser`, so that box
can never be ticked as things stand.

## Evidence

`parse_raw.py --provider rhythm`, 2026-09-04, after a clean acquisition run:

    rhythm/api_invoice_json: parsed ok 2, errored 0, no_parser 0,  upserted 56 bills
    rhythm/api_orders_json:  parsed ok 0, errored 0, no_parser 21, upserted 0 rows
    rhythm/api_usage_json:   parsed ok 1, errored 0, no_parser 0,  upserted 1440 usage rows
    rhythm/bill_pdf:         parsed ok 1, errored 0, no_parser 0,  upserted 1 bills_patched, 8 line_items
    rhythm/csv_export:       parsed ok 0, errored 0, no_parser 5,  upserted 0 rows
    rhythm/other:            parsed ok 0, errored 0, no_parser 2,  upserted 0 rows
    rhythm/smt_export:       parsed ok 1, errored 0, no_parser 0,  upserted 12288 usage rows
    Totals: parsed ok 5, errored 0, no_parser 28

Two separate problems sum to 28.

### 21 x api_orders_json — a real parser, blocked on a schema decision

The payload is a plan snapshot. `rhythm_api_orders_INV05157699.json` in full:

    [{"id": 3010034, "start_date": "2026-07-28", "end_date": "2026-08-28",
      "status": "EXPIRED", "offersnapshot_id": 2135458,
      "title": "Solar Buyback Flex", "average_rate_at_2000_kwh": "0.245411",
      "base_charge_amount": "14.95000", "solar_eligible": true,
      "solar_generation_capped": false, "solar_buyback_kwh_rate": "0.05240",
      "is_time_of_use": false, "is_variable_rate": true}]

There is no `plans` table in `src/db.py`, and these fields do not map onto
`bills` — which already carries `plan_name` and `contract_rate_cents_kwh`, both
parsed from the bill PDF (`INV05157699` -> `Solar Buyback Flex`, 17.4710 c/kWh).
`average_rate_at_2000_kwh` (0.245411), `solar_buyback_kwh_rate` (0.05240) and the
plan term/status have nowhere to go. So this is a sink/schema question first and
a parser second.

Not urgent: nothing is lost. The JSON is captured verbatim in `raw_documents`,
and `checks.py --provider rhythm` passes — every billing period is valuable, so
no dashboard panel is understating.

### 7 junk docs — should be purged, not parsed

Five legacy `csv_export` docs (`bill_line_items.csv`, `monthly_bills.csv`,
`rhythm_api_invoices.csv`, `rhythm_api_orders.csv`, `weekly_usage.csv`) and two
`other` docs (`README.md`, `Rythm contract packet 2021-12.pdf`). The CSVs are
superseded projections from before the raw-first pipeline; the other two were
never data.

This is exactly the case `src/purge_raw.py` was written for — its own docstring:
a permanent false alarm that trains you to ignore the one that matters.

## fix

The junk, first — it is independent of the schema question and removes 7 of 28:

    uv run python src/purge_raw.py --provider rhythm --doc-type csv_export   # review dry run first
    uv run python src/purge_raw.py --provider rhythm --doc-type other

then re-run each with `--yes`. Note `--doc-type csv_export` also matches
`hourly_usage.csv` and `payments.csv`, which DO have parsers and are parsed `ok`
— check the dry-run list before adding `--yes`, and if it is not exactly the five
legacy files, purge by a narrower filter instead.

For the orders JSON, once a sink is chosen, add to `_REGISTRY`:

    ("rhythm", "api_orders_json", _any_name, rhythm.parse_api_orders_json, rhythm.PLAN_PARSER_VERSION),

with a `plans` table keyed on `(account_id, start_date)` and tests alongside the
existing rhythm parser tests.

## blast radius

The purge: none downstream. `purge_raw.py` refuses to delete any document that
something was parsed from, and these 7 have never parsed. It does mean the files
stay on disk in `raw_dir` and will be re-ingested on the next run unless they are
also moved out of the way — worth doing in the same pass.

The orders parser: additive. A new table and a new registry row; no existing
parser, row, or dashboard changes. `bills.plan_name` and
`bills.contract_rate_cents_kwh` keep coming from the PDF, so nothing that reads
them moves.

## not doing yet

The sink is undecided, and inventing a `plans` schema in the middle of a monthly
acquisition run is the wrong moment to make that call. Decide first whether plan
snapshots are their own dimension table (one row per plan term, joinable to bills
by date range) or just extra columns patched onto `bills`. The date-ranged term
and `status` transitions argue for a table.

The purge is blocked on permissions, not on a decision: the session's command
classifier refuses `src/purge_raw.py`, so Jason needs to run it.
