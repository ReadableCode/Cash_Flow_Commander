---
description: Update everything in Cash Flow Commander in one run. Open every portal, sign in once, download from all of them at the same time with one agent each, then land, check and report
argument-hint: [optional: provider slugs to limit the run, e.g. "elan citi"; "chain" to run one portal at a time]
---

# /cfc-update: one run for every provider

Runs every provider command in one sitting: the bank and card exports
(`/transactions-*`), the electricity bills (`/bills-*`), the meter readings and
the solar production. The user signs in to each portal once, at the start. The
downloads then run at the same time, one agent per portal, and everything is
landed, checked and reported once at the end.

This command holds only what is different about running the providers
together. How each portal works stays in that provider's own command, and the
agents read it there.

As last observed 2026-09-30, the first run: seven portals at once, no stall,
no download taken by the wrong agent. See "What the runs have shown".

## 0. Orient

**This command is deployed globally, so the session may not start in the repo.**
Every path below is relative to the Cash Flow Commander clone, conventionally
`~/GitHub/Cash_Flow_Commander`. Change into it first; if it lives elsewhere on
this host, find it and use that.

- Read `docs/LANDING.md` and the project `CLAUDE.md`.
- The providers of a run are every `.claude/commands/transactions-<slug>.md`
  and `.claude/commands/bills-<slug>.md` (not the two `*-add-company.md`
  commands) whose `<slug>` has an entry in `providers.local.yaml`. A command
  with no entry is reported and left out. If "$ARGUMENTS" names slugs, the run
  is those providers only.
- **Every provider runs, every time.** Do not decide from the database that a
  portal has nothing new and skip it. A provider whose service has ended still
  runs, because its account may still be settling (a closing bill, a late
  credit, a restated invoice); when its run lands nothing new, say so in the
  report as `no new updates`. It leaves the run only when the user says the
  account is settled.
- macOS with the user's real Chrome, driven over AppleScript, as
  `transactions-chase.md` section 1 describes. The personal profile's
  directory name is in the notes of `providers.local.yaml`.

## 1. Open every portal, each in its own window

For each provider, read section 1 of its command for the login URL and the
portal's sign-in quirks. Then, one portal at a time:

1. **Enumerate the tabs on that portal's domain and close all of them**, by URL
   match in reverse index order, and verify by count that none is left.
2. Open the portal in a **new window** of the personal profile:
   `open -na "Google Chrome" --args --profile-directory=<personal dir> --new-window <url>`.
   Never `open location`.
3. Verify by count that exactly one tab is on that domain.

Then arrange the windows so that none is fully covered by another (set each
window's `bounds` to a cell of a grid on one screen). Chrome slows scripts and
timers in a tab it considers hidden, and a covered or minimised window counts
as hidden. The agents never activate, move or resize a window after this.

A tiled window is narrow (960 px wide with seven on one screen). A portal can
switch to its compact layout at that width and hide the controls its command
names, and a button can sit below the fold. Each command records what its
portal does; an agent that meets a new case writes it there.

## 2. Sign in, once

For each window, check whether the session is already live, the way that
provider's command describes (its section 1). Then give the user one list: the
portals that are signed in, and the ones waiting on them.

- **The user signs in themselves.** Never type, store or echo credentials, and
  never click a Log In button without asking; each command's own rule on that
  stands.
- When the user says they are done, verify every portal again. Launch the
  agents for the portals that are signed in. A portal the user could not or
  did not sign in to is left out of this run and named in the report; do not
  hold the others for it.
- Do not leave signed-in portals waiting: bank sessions time out in minutes.
  If sign-in is taking long, launch the agents for the portals that are ready
  and add the rest as they are confirmed.

## 3. Download from every portal at once

Launch one agent per signed-in provider, **all in one message** so they run at
the same time. If "$ARGUMENTS" says `chain`, or this session cannot launch
agents, run the providers one after another in this session under the same
rules; start with the portals whose sessions time out soonest (the banks and
cards).

Give each agent this brief, filled in for its provider:

> You are running the download half of `/<command name>` for Cash Flow
> Commander, as one of several agents working different portals at the same
> time. Change into the clone at `<repo path>`. Read
> `.claude/commands/<command file>` in full and follow it from its start
> through its filing section, with these changes:
>
> 1. **The tab is already open and signed in**, in its own window. Find it by
>    URL match on every call. Do not open another tab or window for this
>    portal. Do not touch any other tab or window, and do not activate, move or
>    resize any window: other agents are working in them.
> 2. **If the portal is not signed in, or the session dies, stop and report.**
>    Do not wait for a sign-in and never type credentials. Re-navigate once
>    first where the command says a transient error is normal.
> 3. **Claim every download with `src/downloads.py`.** Run
>    `uv run python src/downloads.py mark` just before the click and
>    `uv run python src/downloads.py claim --provider <slug> --since <marker>`
>    after it, and file the path it prints. Other agents are downloading into
>    the same folder; never take a file any other way, and never touch a file
>    `claim` did not print.
> 4. **Stop after filing.** File each capture the way the command's filing
>    section says (for a transaction provider, `capture.py --provider <slug>
>    file` and `record-empty`). Do NOT run `ingest_raw.py`, `parse_raw.py`,
>    `land.sh`, `checks.py` or `deploy/grafana_sync.py`: everything is landed
>    once, in order, after every agent has finished.
> 5. If the portal differed from what the command says, update that command
>    file as its "Keeping this command current" section asks. Edit no other
>    file, and do not commit.
> 6. Leave the portal tab open and signed in.
>
> Report back: what the plan or coverage step asked for; each file filed
> (name only) and each empty window recorded; anything asked for that you did
> not get, and why; whether `uv run python src/downloads.py leftovers
> --provider <slug>` exits 0; anything in the portal that differed from the
> command.

What the agents run before filing only reads the database (`plan.py`,
`coverage.py`) and writes files in that provider's own folders, so they do not
collide. Landing is held back because every `parse_raw.py` run rebuilds the
cash forecast table.

## 4. Land, one provider at a time

When every agent has reported, land each provider that filed anything by
running exactly what the **Land** and **Normalize** sections of its command
say. One at a time, in this order: the transaction providers, then the meter
readings and the solar production, then the electricity bills (their checks
read the interval data).

- A provider whose agent stopped early is still landed: whatever it filed is
  real.
- A `provider_conflict` from ingest is a failed landing. Stop on it and report
  it; do not relabel anything without the user.
- A parse error is fixed in the parser, never by editing rows (each command's
  Normalize section).

## 5. Check, once

```sh
uv run python src/checks.py
uv run python src/expected_checks.py --days-back 60
uv run python deploy/grafana_sync.py verify cfc-transactions
uv run python deploy/grafana_sync.py verify cfc-cash-forecast
uv run python deploy/grafana_sync.py verify cfc-energy
uv run python deploy/grafana_sync.py verify cfc-solar-net-metering
```

`expected_checks.py` reports more than this run caused. What to read from it
here is `broken`: a match whose transaction no longer exists, because a newer
capture restated the row. Name each one in the report; the user re-pairs it on
the pairing board.

Then, for every provider of the run,
`uv run python src/downloads.py leftovers --provider <slug>` must exit 0, and
each provider command's own verification checklist applies to what that
provider landed.

## 6. Report

One report for the whole run, one line per provider first:

- `<slug>`: what was new (files filed, rows landed), or `no new updates`, or
  why it did not run (not signed in, session lost, portal changed).

Then: anything a plan or coverage step asked for that was not obtained; the
result of the checks and the four dashboard verifications; any `broken` match
to re-pair; any leftover in the download folder; anything an agent put in a
`_to_delete/` folder; any command file an agent changed, with what changed.
**Do not commit**: the user reviews and commits.

## What the runs have shown

2026-09-30, the first run, all seven providers:

- Every portal was still signed in from earlier sessions, so the sign-in round
  asked for nothing. Do not count on that.
- No agent stalled and every tab reported itself visible, with the windows
  tiled four across and two down and never brought to the front.
- Every download was claimed by the agent that asked for it, with seven agents
  downloading into one folder. No leftover, no file taken by another agent.
- The agents took between 1.5 and 4 minutes each and about 20 minutes added
  together, so the download half took about 4 minutes of wall-clock time.
- Citi switched to its compact layout in the narrow window, and the Smart
  Meter Texas export button was below the fold. Both are now in those commands.
- Chrome's automatic-downloads allowance is per portal origin. None blocked on
  this run; a portal that has never downloaded twice in one run will block its
  second download and ask, and each command says how to spot it.

To try a change to this command on less than everything, name the portals:
`/cfc-update elan citi`.

## Keeping this command current

When a run shows that something above is wrong or missing, fix it here and date
it. What belongs here is only what is about running providers together: the
windows, the sign-in round, the agent brief, the landing order. A portal's own
quirk belongs in that portal's command. Tell the user what you changed.
**Do not commit**: they review and commit.
