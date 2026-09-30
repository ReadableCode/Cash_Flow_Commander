# captures: a file left in the browser's download location is never noticed

    found:  2026-09-29
    status: open
    verify: ls "$(uv run python -c 'from src.user_paths import onedrive_documents; print(onedrive_documents())')" | grep -iE "^(rhythm|gexa|enphase_enlighten|chase|citi|elan)[_ ]" || echo "download location is clean"

A capture that lands in the browser's download location and is not moved stays
there. No step of any provider command and no line of the verification
checklist looks at the download location after the run, so the leftover is
found only by someone reading the folder.

## Evidence

One leftover, found 2026-09-29 in the download location, which on this machine
is the root of the synced documents tree. It was removed the same day, after
`cmp` confirmed it against the filed copy:

    gexa_api_invoice-history_60mo_20260928.json   345 bytes   created 2026-09-28 14:57:49

It was byte-identical to the capture that was filed three minutes later:

    raw/gexa_api_invoice-history_20260928.json     345 bytes   created 2026-09-28 15:00:07
    sha256 a092e9c6bc36425081c4207fb3f28e32cb1d063af5a9bf2b2f2a6bc2065d821f   (both)

and to `raw_documents` row 2377 (`provider='gexa'`, `doc_type='api_invoice_json'`,
`parse_status='ok'`), which holds the same bytes in its `content` column. Both
files carry Chrome's download attributes for the portal origin, so both came
from the browser. So the leftover was a third copy and nothing read it.

Its name does not match the pattern in `.claude/commands/bills-gexa.md`
section 2 (`gexa_api_invoice-history_{YYYYMMDD}.json`). The `_60mo_` form
appears nowhere in this repo. It was written during the onboarding session of
2026-09-28, the same session that found Chrome dropping every scripted download
after the first until the portal origin was allowed. Which step of that session
wrote the first file is not recorded.

Two things let it stay:

- `docs/LANDING.md` section 10 checks that every artifact has a file, that
  `raw_documents` grew, and that a re-run dedups. It never checks that the
  download location is empty of this provider's captures.
- The bill providers have no `download_dir` key. Only the banking providers do
  (`template_providers.yaml`). For a bill provider the download location is a
  sentence in `notes`, so no script can look there.

## fix

1. Add `download_dir` to every bill provider in `template_providers.yaml`, with
   the same meaning it has for the banking providers.
2. Add to `docs/LANDING.md` section 10:

       - [ ] The download location holds no file whose name starts with this
             provider's slug.

3. Add the same line to the close-out section of each
   `.claude/commands/bills-*.md` and `transactions-*.md`, and to both
   `*-add-company.md` templates.

## blast radius

The three steps change documentation and one config template. No code path
reads the new key until a command is told to.

## not doing yet

The one known leftover is gone, so nothing is wrong today. The steps touch
every provider command at once and want one pass, not a change made in the
middle of an acquisition run.
