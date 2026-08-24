---
description: Onboard a new transaction source (bank or card) — interview, assess what transaction_downloader must generalize, scaffold its /transactions-<slug> command from the Chase reference, stub its local config, and optionally run a portal discovery session
argument-hint: [company name]
---

# /transactions-add-company — scaffold a new transaction source

You are onboarding a NEW transaction source: a bank or card issuer whose
deliverable is transaction exports. The output is a new per-provider command at
`.claude/commands/transactions-<slug>.md`, a local config stub, and an honest
list of the code work the new source needs. The reference implementation is
`.claude/commands/transactions-chase.md` — read it first, together with
`transaction_downloader/README.md` and `docs/LANDING.md`.

This is NOT the bills pipeline. Bill and usage providers (invoices, PDFs, meter
intervals) are onboarded with `/bills-add-company`. Transactions differ in one
load-bearing way: **coverage is tracked by requested window, not by data
cadence**. Transactions have no cadence — a card can legitimately go ten days
with no activity — so a month you asked for and got nothing back is *covered*,
and a month you never asked for is *not*, however much data surrounds it. Every
design decision below flows from that.

This repo is PUBLIC. No personal values anywhere in the generated command file:
no account numbers or last-4s, no personal paths, no emails, no credentials, no
balances or merchant names. Personal values live only in `providers.local.yaml`
(gitignored).

Follow these steps in order.

## 0. Orient

**This command is deployed globally, so the session may not start in the repo.** Every path below is relative to the Cash Flow Commander clone — conventionally `~/GitHub/Cash_Flow_Commander`. Change into it before anything else; if it lives somewhere else on this host, find it and use that. Do not hardcode a machine-specific path here, and do not scaffold into an unrelated directory's `.claude/commands/`.

## 1. Interview

If `$ARGUMENTS` contains a company name, use it as the starting point;
otherwise ask. Use AskUserQuestion where available. Collect:

1. **Company name** → derive the **slug**: lowercase snake_case. Confirm the
   slug with the user.
2. **Account kinds** under one login (checking, savings, cards). One login
   covering several accounts means there is no single `account_number` for the
   provider — the parser must derive the account per file, like `chase` in
   `parse_raw.DERIVES_OWN_ACCOUNT_ID`.
3. **Export formats** the portal offers (CSV, OFX/QFX, PDF-only). CSV is what
   the pipeline eats natively; PDF-only means parser work of a different order —
   say so up front.
4. **Claimed retention** on exports. Treat it as unverified until probed live
   in the discovery session — labels lie (Chase's "All transactions" on a bank
   account meant 24 months).
5. **Known export caps** — rows per report, and whether hitting the cap fails
   loudly or truncates silently.

If `.claude/commands/transactions-<slug>.md` already exists, STOP and tell the
user — this command onboards new sources, it does not overwrite existing ones.

## 2. Code reality check — say what the pipeline needs for this source

`transaction_downloader/` (`plan.py`, `capture.py`, `store.py`) is
multi-provider: `store.PROVIDERS` carries each source's layouts, row cap,
retention, and capture-filename hints, and `plan.py`/`capture.py` take
`--provider`. Onboarding is therefore additive, but it is still real code
with tests, not scaffolding. Tell the user plainly what this source needs:

- **A `store.PROVIDERS` entry** — retention floor, row cap, filename hints,
  always-refetch overlap. Capture reconciliation is provider-scoped through
  the shared capture-name parser (`src/providers/capture_names.py`), so one
  provider's capture can never prune another's rows even when two cards share
  a last-4 — keep new capture names inside that scheme.
- **A parser** — `src/providers/<slug>.py`, registered in
  `src/providers/__init__.py::_REGISTRY`, with tests. Rules learned the hard
  way, to *check* against live data rather than assume:
  - **Verify money signs against real exported rows.** Sources split or sign
    amount columns in surprising ways — Citi's Credit column prints payments
    as NEGATIVE numbers, so a naive projection flipped every payment to
    money-out and the suite still passed, because the test fixture modeled
    the assumed shape instead of copying a live row. Build fixtures from
    verbatim live rows, and after the first real parse confirm a card's
    payments land positive (an account with zero rows in one direction is
    the tell).
  - If exports carry **no transaction id**, the natural key needs an
    `occurrence` counter for genuinely identical same-day rows — and that is
    only safe if **export order is stable across re-downloads**. Verify it
    live (two overlapping downloads; compare the overlap region) before
    trusting it.
  - If the source **restates posted rows** (a tip settles, a description
    changes), a changed amount is a new natural key and upsert alone leaves
    the old row behind as a phantom. Plan for window-scoped pruning of stale
    rows plus an always-refetch overlap tail, like the Chase parser.
- **Preserve everything**: every source field verbatim into
  `transactions.extra`, keyed by original header, with the shape fixed by
  parser code — no run decides what is worth keeping.
- **A truncation guard**: if the export has a row cap and truncates silently,
  refuse capped files at capture time (and again at parse time, as defence in
  depth) rather than recording a truncated month as complete.

## 3. Scaffold the provider command

There is deliberately no fill-in-the-blanks template — the onboarded commands
*are* the template. `transactions-chase.md` has the fullest section skeleton;
`transactions-citi.md` shows the same skeleton applied to a second source
(SPA portal, Debit/Credit columns, no row cap) and is the better model for
what changes between sources. Copy the section skeleton and the
provider-independent rules into `.claude/commands/transactions-<slug>.md`:

- **0 Orient** — deployed globally, so locate the repo first; load config from
  `providers.local.yaml`; STOP if the entry is absent.
- **0.1 Plan** — planner invocation and argument handling (`full`, `YYYY-MM`).
  One download per planner window; never merge windows.
- **1 Browser session** — the user's real Chrome, one signed-in tab, credential
  rules.
- **2 Popups and interstitials** — dismiss-never-accept, the
  security-modal stop rule, re-navigate instead of clicking through.
- **3 The download form** — leave as TODO until the discovery session; when
  filled, stamp it "as last observed YYYY-MM-DD".
- **4 File each download verbatim** — `--start`/`--end` are the window you
  *requested*, not the dates inside the file; record empty windows explicitly
  so the planner stops re-asking.
- **5 Land into raw_documents**, **6 Normalize**, **7 Report and verify**
  (checklist), **8 Keeping this command current**, and the pre-commit hygiene
  checklist at the bottom.

Leave every portal-specific section as a clearly marked TODO block
(`<!-- TODO: fill after discovery session -->`) — never invent endpoints, form
mechanics, or retention numbers.

## 4. Local config stub

1. If `providers.local.yaml` does not exist at the repo root, create it by
   copying `template_providers.yaml`.
2. Append a `<slug>` stub following the shape of the `chase` block:
   `external_ids.accounts` (last-4 → label), `raw_dir`, `archive_dir`,
   `backfill_start`, `notes`. Tell the user to set `backfill_start` explicitly
   when the goal is "all of it" — anchoring history to the oldest existing
   capture has silently suppressed a whole backfill before (one stray capture
   made a 24-month plan look like a 2-month refresh).
3. NEVER commit `providers.local.yaml`. It is gitignored and stays local — it
   is where all personal values belong.

## 5. Discovery session (offer, don't force)

Offer to run a live discovery session now; if the user declines, skip to
step 6 — the TODO blocks mark what remains.

If accepted, with the user:

1. **Use the user's real Chrome profile.** The password manager lives there;
   embedded browser panels and fresh automation profiles don't autofill. Two
   pathways that work: **Claude in Chrome** when its tools are in the session,
   or **AppleScript on macOS** — `osascript -e 'tell app "Google Chrome" to
   open location ...'`, then `execute <tab> javascript "..."` (needs the
   one-time **View → Developer → Allow JavaScript from Apple Events** toggle
   plus Automation consent). Find the tab by URL match on every call, never by
   index.
2. The user signs in themselves — never type, store, or echo credentials, and
   never read them out of the password manager. Hand control over for MFA and
   wait; accept "remember this device" if offered. Keep exactly **one**
   signed-in tab — two tabs sharing a session can log both out when one idles
   into timeout.
3. **Popups: dismiss, never accept.** Never enroll, never change an account
   setting, never accept an offer. If a modal looks like security or account
   settings rather than marketing, stop and ask the user. No visible
   dismissal → re-navigate to the download URL instead of clicking through.
4. Find the export/download form and work out how to drive it. **Expect web
   components**: `document.querySelectorAll('select')` returning nothing does
   not mean there is no form — options may live in element attributes or in
   shadow roots, sometimes nested two deep, and sibling components on one page
   can differ. Drive controls the way a user does (open the dropdown, click
   the option; set inputs with the native value setter, then dispatch
   `input`/`change` with `{bubbles:true, composed:true}`), and **verify every
   step by reading state back**.
5. **Probe the limits live** — these become planner constants, so get them
   exact:
   - Retention: test the boundary with real dates. It may be a rolling *daily*
     floor rather than a month boundary, and may apply to each endpoint of a
     range independently — widening the range does not evade it. Beware forms
     that don't validate until every field is filled.
   - The row cap, and whether exceeding it errors or truncates silently.
   - Which date a range filter applies to (posting vs transaction date).
   - What an empty window produces — possibly no file at all, which is a
     normal outcome the command must record, not a failed download.
   - Whether download permission needs a one-time browser prompt (e.g. Chrome's
     "wants to download multiple files" Allow, without which every download
     after the first silently vanishes).
6. Downloads land wherever the browser profile says, not necessarily
   `~/Downloads` — record the real location in the `notes` field of
   `providers.local.yaml`. Filename patterns can differ per product; detect a
   completed download by watching for a file newer than a marker timestamp
   taken just before triggering it, never by predicting the name.
7. Write the findings into §3 of the new command, replacing the TODO blocks,
   stamped **"as last observed YYYY-MM-DD"**. Describe forms and endpoints
   generically — no account numbers, internal account ids, tokens, or other
   personal identifiers in the command file.

## 6. Finish

1. Run the pre-commit hygiene checklist (bottom of `transactions-chase.md`)
   against the new command file: no account numbers or last-4s, no personal
   paths, no emails, no credentials, no balances or merchant names — personal
   values only via `providers.local.yaml`. Fix violations before proceeding.
2. Restate the §2 list of remaining code work — the new command is not
   runnable until the planner, capture, and parser support this source.
3. Tell the user `.claude/commands/transactions-<slug>.md` is ready for review
   and commit. Do NOT commit anything yourself — the user reviews and commits.
   Finished per-provider transaction commands are deployed globally like
   `/transactions-chase`; remind the user to add a deployment entry for the
   new command (the user handles deployment).
