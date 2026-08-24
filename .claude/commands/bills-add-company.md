---
description: Onboard a new bill provider — interview, scaffold its /bills-<slug> command from the template, stub its local config, and optionally run a portal discovery session
argument-hint: [company name]
---

# /bills-add-company — scaffold a new provider

You are onboarding a NEW bill provider into this repo. The output is a new per-provider command at `.claude/commands/bills-<slug>.md` plus a local config stub. Every generated command must follow the contract in `docs/LANDING.md` — read it first.

**Scope: bill and usage providers only** — invoices, bill PDFs, meter/production intervals. A bank or card whose deliverable is transaction exports is a different pipeline (coverage is tracked by requested window, not reading cadence) — use `/transactions-add-company` for those.

This repo is PUBLIC. No personal values anywhere in the generated command file: no account numbers, no meter/ESI-style identifiers, no personal paths, no emails, no credentials. Personal values live only in `providers.local.yaml` (gitignored).

Follow these steps in order.

## 0. Orient

**This command is deployed globally, so the session may not start in the repo.** Every path below is relative to the Cash Flow Commander clone — conventionally `~/GitHub/Cash_Flow_Commander`. Change into it before anything else; if it lives somewhere else on this host, find it and use that. Do not hardcode a machine-specific path here, and do not scaffold into an unrelated directory's `.claude/commands/`.

## 1. Interview

If `$ARGUMENTS` contains a company name, use it as the starting point; otherwise ask. Use AskUserQuestion where available. Collect:

1. **Company name** → derive the **slug**: lowercase snake_case (e.g. "City Gas & Power" → `city_gas_power`). Confirm the slug with the user.
2. **Service type**: electricity / gas / water / internet / cellular / other.
3. **How bills arrive**: portal with API / portal PDF-only / email-only / paper scans. (Multiple can apply.)
4. **Known retention limits** on portal data (e.g. "only 24 months of bills online") — this drives backfill urgency. Treat what the portal or the user *believes* as unverified until probed live in the discovery session — labels lie (a Chase export labelled "All transactions" meant 24 months).
5. **Export caps** — rows or size per download, and whether hitting the cap fails loudly or truncates silently. Silent truncation is the dangerous kind; it needs a guard, not a habit.

If `.claude/commands/bills-<slug>.md` already exists, STOP and tell the user — this command onboards new providers, it does not overwrite existing ones.

## 2. Scaffold the provider command

1. Copy `templates/provider-command.md` to `.claude/commands/bills-<slug>.md`.
2. Fill every section the interview answered: provider name, slug, service type, artifact sources, retention notes, artifact naming per `docs/LANDING.md` section 4.
3. Leave every section you cannot fill (API endpoints, auth mechanics, parsing details) as a clearly marked TODO block, e.g. `<!-- TODO: fill after discovery session -->` — never invent endpoints or guess at portal behavior.

## 3. Local config stub

1. If `providers.local.yaml` does not exist at the repo root, create it by copying `template_providers.yaml`.
2. Append a stub entry for `<slug>`, following the shape in `template_providers.yaml`, with placeholder values for the user to fill in (account number, directories, etc.).
3. NEVER commit `providers.local.yaml`. Remind the user it is gitignored and stays local — it is where all personal values belong.

## 4. Discovery session (offer, don't force)

Offer to run a live discovery session now; if the user declines, skip to step 5 — the TODO blocks mark what remains.

If accepted, with the user:

1. **Use the user's real Chrome profile.** The password manager lives there; embedded browser panels and fresh automation profiles don't autofill, which forces manual credential entry the user will reject. Two pathways that work: **Claude in Chrome** when its tools are in the session, or **AppleScript on macOS** — `osascript -e 'tell app "Google Chrome" to open location ...'`, then `execute <tab> javascript "..."` (needs the one-time **View → Developer → Allow JavaScript from Apple Events** toggle plus Automation consent). Find the tab by URL match on every call, never by index — tabs move.
2. The user signs in themselves — never type, store, or echo credentials, and never read them out of the password manager. Hand control over for MFA and wait. Keep exactly **one** signed-in tab: two tabs sharing a session can log both out when one idles into timeout.
3. **Popups: dismiss, never accept.** Never enroll in anything, never change an account setting, never accept an offer. If a modal looks like security or account settings rather than marketing, stop and ask the user. If a modal has no visible dismissal, re-navigate to the target URL rather than clicking through it.
4. Watch DevTools / network traffic while navigating billing and usage pages. Identify:
   - invoice-list, usage, and document-download endpoints
   - auth mechanics (session cookie vs bearer token, where it's obtained)
   - pagination and date-range parameters
5. **Expect web components.** `document.querySelectorAll('select')` returning nothing does not mean there is no form — options may live in element attributes or in shadow roots, sometimes nested two deep, and sibling components on one page can differ. Drive controls the way a user does (open the dropdown, click the option; set input values with the native setter and dispatch `input`/`change` with `{bubbles:true, composed:true}`), and **verify every step by reading state back** rather than assuming the click landed.
6. **Probe claimed limits live.** Test the retention boundary with real dates — it may be a rolling daily floor rather than a month boundary, and may apply to each endpoint of a range independently. Find any export cap and check whether exceeding it fails loudly or truncates silently. Note which date a range filter actually applies to, and what an empty result produces — some portals serve no file at all, which must not read as a failed download.
7. Downloads land wherever the browser profile says, not necessarily `~/Downloads` — record the real location in the `notes` field of `providers.local.yaml`. Detect a completed download by watching for a file newer than a marker timestamp taken just before triggering it — never by predicting the filename, and never by grabbing the newest file without the timestamp check.
8. Write the findings into the new command's API artifact catalog section, replacing the corresponding TODO blocks, and **stamp each observed section "as last observed YYYY-MM-DD"** so future runs know how stale the notes are. Describe endpoints generically — no account numbers, tokens, or other personal identifiers in the command file.

## 5. Finish

1. Run the pre-commit hygiene checklist (bottom of `templates/provider-command.md`) against the new command file: no account numbers, no meter/ESI-style identifiers, no personal paths, no emails, no credentials — personal values only via `providers.local.yaml`. Fix any violations before proceeding.
2. Tell the user `.claude/commands/bills-<slug>.md` is ready for review and commit. Do NOT commit anything yourself — the user reviews and commits.
3. Finished per-provider commands are deployed globally like `/bills-rhythm`; remind the user to add a deployment entry for the new command (the user handles deployment).
