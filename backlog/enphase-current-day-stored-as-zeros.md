# usage: a same-day Enphase capture stores the rest of today as zero production

    found:  2026-09-30
    status: open
    verify: grep -n "if not production" src/providers/enphase_enlighten.py

Enlighten's `daily_energy` response carries all 96 fifteen-minute slots for
the current day, including the ones that have not happened yet, as zeros. The
parser skips a day only when its `production` array is empty, so a capture
made during the day writes zero readings for every interval still to come, and
the daily rollup from `lifetime_energy` carries the running total as if it were
the day's.

The next capture restates them by upsert, so the error lasts until the next
run. Until then today's solar production reads low on both energy dashboards,
and a run made in the morning makes the afternoon look like a dead system.

## Evidence

After the capture of 2026-09-30, taken about 09:20 local:

    today's 15-minute production rows held: 96; positive: 8
    rows for times that have not happened yet: 58 (all zero)     at 09:28 local

`verify:` prints the one guard, line 126, which tests the whole day's array and
nothing per interval.

`.claude/commands/bills-enphase_enlighten.md` section 0.1 used to say today's
array is empty until the panels start producing. On 2026-09-30 it was zero
padded instead; the command now says so.

## fix

The parser cannot tell a real zero from a slot that has not happened without
knowing when the capture was taken.

1. Pass the document's `fetched_at` to parsers in `ctx` (`parse_raw.py`
   builds `ctx` from the `raw_documents` row, which has it).
2. In `parse_daily_energy_json`, drop every interval that starts at or after
   `fetched_at`. In `parse_lifetime_energy_json`, drop the rollup for the
   capture's own local day.
3. Bump `enphase_enlighten.PARSER_VERSION`. Tests in
   `tests/test_enphase_enlighten.py`: a capture taken mid-day yields rows only
   up to the capture time and no rollup for that day; a capture of a past day
   is unchanged.
4. No cleanup is needed: the next run's overlap rewrites the rows.

## blast radius

`ctx` gains a key every parser may ignore. Enphase rows for the capture's own
day stop being written until a later capture holds them, so the newest partial
day disappears from the panels instead of showing as zeros.

## not doing yet

Found during the first `/cfc-update` run, which does not change parsers. The
rows are corrected by the next run, so nothing is lost.
