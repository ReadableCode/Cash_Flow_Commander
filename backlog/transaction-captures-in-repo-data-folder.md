# transactions: verbatim captures are filed in the repo's data folder, on one machine

    found:  2026-09-29
    status: open
    verify: grep -nE "^\s+raw_dir:" providers.local.yaml | grep -c "data/"

The verify line prints how many providers still file captures under `data/`.
It printed `3` on 2026-09-29: chase, citi and elan.

The bill providers file every verbatim capture in the synced documents tree.
The transaction providers file theirs in `data/<provider>/incoming`, which is
gitignored and exists on one machine. The bytes are not at risk, because
`raw_documents` holds them. The files are: they are the only copy outside the
database, the only form a person can open, and the copy that a second machine
or a rebuilt checkout does not have.

## Evidence

`raw_dir` by provider, from `providers.local.yaml`, 2026-09-29:

    rhythm              ${ONEDRIVE_DOCS}/FinancialLegal/Utilities/Rythm/raw
    gexa                ${ONEDRIVE_DOCS}/FinancialLegal/Utilities/Gexa/raw
    enphase_enlighten   ${ONEDRIVE_DOCS}/FinancialLegal/Utilities/Enphase/raw
    chase               data/chase/incoming
    citi                data/citi/incoming
    elan                data/elan/incoming

What is in those three folders, 2026-09-29:

    provider   csv_export   window markers   manifest
    chase      48           10               .chase_captures.jsonl
    citi        5            0               .citi_captures.jsonl
    elan        3            2               .elan_captures.jsonl
    total      56           12               3

All 68 capture files were hashed and looked up in `raw_documents`. Every one
has a row with the same sha256, and no row for these providers lacks a file:

    files on disk 68, distinct sha256 68, found in raw_documents 68, rows with no file 0

So a lost `data/` folder loses no transaction. `transaction_downloader/plan.py`
also reads coverage from the database first and the disk second. What it
would lose is the verbatim file archive and the three manifests.

`.gitignore` excludes `data/*`, and `README.md` says of that folder: "nothing
in this repo and nothing on any server has a copy of it".

The code already allows the move. `raw_dir` accepts `${ONEDRIVE_DOCS}/...`
(`src/user_paths.py::expand_config_path`), and
`transaction_downloader/capture.py::_load_raw_dir` resolves it. What stands in
the way is the documented intent: the docstring of `_resolve_repo_relative`
says "Staging lives inside the repo (data/, gitignored) ... nothing outside
the repo is ever written", and `plan.py` says "staging is cleared once
ingested".

## fix

1. In `template_providers.yaml`, write the banking providers' `raw_dir` and
   `data_dir` examples as `${ONEDRIVE_DOCS}/...`, matching the bill providers.
2. Rewrite the two docstrings above, the `raw_dir` comments in
   `template_providers.yaml`, `transaction_downloader/README.md` and the three
   `.claude/commands/transactions-*.md` so they describe an archive, not a
   staging folder.
3. In the user's `providers.local.yaml`, point each provider at its folder
   under `${ONEDRIVE_DOCS}/FinancialLegal/Banks and Credit/`. `Chase`,
   `Citi AAdvantage` and `Elan Financial` exist there already, so the folder is
   `<existing folder>/raw`.
4. Move the held files, one provider at a time, manifest included:

       mkdir -p "<new raw_dir>"
       mv -n data/chase/incoming/* data/chase/incoming/.chase_captures.jsonl "<new raw_dir>/"

5. Prove nothing changed. Both must report every file as already held:

       uv run python src/ingest_raw.py --provider chase --dry-run "<new raw_dir>"
       uv run python transaction_downloader/plan.py --provider chase --from-disk

   The dry run must show 0 to ingest. The plan from disk must name the same
   windows as the plan from the database.

## blast radius

The move changes no row: ingest dedups on sha256, and step 5 is a dry run.
Statement CSVs carry account activity, so they enter a synced cloud folder
they were not in before. The bill PDFs are already there, so this widens what
the folder holds and not who can reach it.

The window markers are small text files with no content of their own. They
move with the captures, because `plan.py --from-disk` reads them.

## not doing yet

Step 3 edits the user's config and step 4 moves the user's files, so both
wait for the user. Step 2 rewrites the stated design of the downloader, which
should be one deliberate change with the config change, not two.
