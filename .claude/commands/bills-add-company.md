---
description: Onboard a new bill provider — interview, scaffold its /bills-<slug> command from the template, stub its local config, and optionally run a portal discovery session
argument-hint: [company name]
---

# /bills-add-company — scaffold a new provider

You are onboarding a NEW bill provider into this repo. The output is a new per-provider command at `.claude/commands/bills-<slug>.md` plus a local config stub. Every generated command must follow the contract in `docs/LANDING.md` — read it first.

This repo is PUBLIC. No personal values anywhere in the generated command file: no account numbers, no meter/ESI-style identifiers, no personal paths, no emails, no credentials. Personal values live only in `providers.local.yaml` (gitignored).

Follow these steps in order.

## 1. Interview

If `$ARGUMENTS` contains a company name, use it as the starting point; otherwise ask. Use AskUserQuestion where available. Collect:

1. **Company name** → derive the **slug**: lowercase snake_case (e.g. "City Gas & Power" → `city_gas_power`). Confirm the slug with the user.
2. **Service type**: electricity / gas / water / internet / cellular / other.
3. **How bills arrive**: portal with API / portal PDF-only / email-only / paper scans. (Multiple can apply.)
4. **Known retention limits** on portal data (e.g. "only 24 months of bills online") — this drives backfill urgency.

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

1. Log into the new provider's portal in the browser (the user drives credentials; never store them).
2. Watch DevTools / network traffic while navigating billing and usage pages. Identify:
   - invoice-list, usage, and document-download endpoints
   - auth mechanics (session cookie vs bearer token, where it's obtained)
   - pagination and date-range parameters
3. Write the findings into the new command's API artifact catalog section, replacing the corresponding TODO blocks. Describe endpoints generically — no account numbers, tokens, or other personal identifiers in the command file.

## 5. Finish

1. Run the pre-commit hygiene checklist (bottom of `templates/provider-command.md`) against the new command file: no account numbers, no meter/ESI-style identifiers, no personal paths, no emails, no credentials — personal values only via `providers.local.yaml`. Fix any violations before proceeding.
2. Tell the user `.claude/commands/bills-<slug>.md` is ready for review and commit. Do NOT commit anything yourself — the user reviews and commits.
