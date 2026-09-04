#!/usr/bin/env bash
#
# Land transaction captures for one provider: file any downloads named on the
# command line, then run the repo's ingest and parse steps.
#
# The directory arguments to ingest_raw are long, live in providers.local.yaml,
# and contain spaces — retyping them is the step that goes wrong. This reads
# them from the config so a run is one command. Prefer it over hand-typing
# ingest_raw: a mistyped run once filed five rhythm documents under elan.
#
# Usage:
#   bash transaction_downloader/land.sh [--provider SLUG] ...
#       --provider defaults to chase, so every pre-existing invocation below
#       behaves exactly as it always has.
#
#   bash transaction_downloader/land.sh
#       ingest + parse whatever is already in raw_dir
#
#   bash transaction_downloader/land.sh ACCT:START:END:FILE [ACCT:START:END:FILE ...]
#       file those downloads first, then ingest + parse.
#       FILE may be a bare filename; it resolves against download_dir.
#
#   bash transaction_downloader/land.sh --legacy FILE [FILE ...]
#       import archived exports with an inferred window, then ingest + parse.
#
#   bash transaction_downloader/land.sh [--provider SLUG] --dry-run
#       resolve and print the provider and its directories, then stop. Nothing
#       is filed, ingested or parsed. Use it to confirm what a run would touch.
#
# Examples:
#   bash transaction_downloader/land.sh 1234:2024-08-01:2024-09-30:Chase1234_Activity_20260822.CSV
#   bash transaction_downloader/land.sh --provider elan 5678:2026-07-01:2026-09-04:export.csv
#   bash transaction_downloader/land.sh --provider citi

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(dirname "$HERE")"
cd "$REPO"

if ! command -v uv >/dev/null 2>&1; then
  echo "uv not found on PATH. Install it, or run the python commands directly." >&2
  exit 1
fi

# --provider must be read before the config lookup below. Default is chase, so
# callers that predate this flag are unaffected.
PROVIDER="chase"
if [ "${1:-}" = "--provider" ]; then
  if [ -z "${2:-}" ]; then
    echo "--provider needs a slug (e.g. --provider elan)" >&2
    exit 2
  fi
  PROVIDER="$2"; shift 2
else
  case "${1:-}" in
    --provider=*) PROVIDER="${1#--provider=}"; shift ;;
  esac
fi

DRY_RUN=0
if [ "${1:-}" = "--dry-run" ]; then
  DRY_RUN=1; shift
fi

# Directories come from providers.local.yaml so they are never retyped.
#
# Read ONE PATH PER LINE with `IFS= read -r`. These paths contain spaces
# ("Banks and Credit"), so a single `read -r A B C` splits them on the default
# IFS and silently hands ingest_raw three fragments. Line-based reads also work
# on the bash 3.2 that ships with macOS, where `mapfile` does not exist.
_paths_script='
import os, sys, yaml
provider = sys.argv[1]
repo = os.getcwd()
sys.path.insert(0, repo)
from transaction_downloader import store
if provider not in store.PROVIDERS:
    sys.exit(
        "unknown transaction provider %r; this script handles %s. Bill providers "
        "land through their own /bills-<slug> command."
        % (provider, ", ".join(sorted(store.PROVIDERS)))
    )
path = os.path.join(repo, "providers.local.yaml")
if not os.path.isfile(path):
    sys.exit("providers.local.yaml not found (or its symlink target is missing)")
entry = (yaml.safe_load(open(path)) or {}).get(provider)
if not isinstance(entry, dict):
    sys.exit("no %s entry in providers.local.yaml" % provider)
missing = [k for k in ("raw_dir", "data_dir") if not entry.get(k)]
if missing:
    sys.exit("%s entry is missing: %s" % (provider, ", ".join(missing)))
sys.path.insert(0, os.path.join(repo, "src"))
import user_paths
def resolve(v):
    # user_paths also expands ${ONEDRIVE_DOCS}, so a config path stays correct
    # on every machine the sync folder lands on.
    return user_paths.expand_config_path(v, repo)
# archive_dir is optional: the card providers have no bill PDFs to file, and
# pointing it at a correspondence folder makes ingest re-walk unrelated
# documents every run.
print(resolve(entry["archive_dir"]) if entry.get("archive_dir") else "")
print(resolve(entry["raw_dir"]))
print(resolve(entry["data_dir"]))
'

ARCHIVE_DIR=""; RAW_DIR=""; DATA_DIR=""
{
  IFS= read -r ARCHIVE_DIR
  IFS= read -r RAW_DIR
  IFS= read -r DATA_DIR
} < <(uv run python -c "$_paths_script" "$PROVIDER")

if [ -z "$RAW_DIR" ] || [ -z "$DATA_DIR" ]; then
  echo "could not read raw_dir/data_dir from providers.local.yaml" >&2
  exit 1
fi
mkdir -p "$RAW_DIR" "$DATA_DIR"

echo "  provider: $PROVIDER"
echo "  raw_dir:  $RAW_DIR"
echo "  data_dir: $DATA_DIR"
if [ -n "$ARCHIVE_DIR" ]; then
  echo "  archive_dir: $ARCHIVE_DIR"
else
  echo "  archive_dir: (unset - nothing outside the repo is walked)"
fi
echo

if [ "$DRY_RUN" -eq 1 ]; then
  echo "dry run — nothing filed, ingested or parsed."
  exit 0
fi

MODE="capture"
if [ "${1:-}" = "--legacy" ]; then
  MODE="legacy"
  shift
fi

if [ "$#" -gt 0 ]; then
  echo "=== 1. filing downloads ==="
  if [ "$MODE" = "legacy" ]; then
    uv run python transaction_downloader/capture.py --provider "$PROVIDER" import-legacy "$@"
  else
    for spec in "$@"; do
      # ACCT:START:END:FILE — FILE may itself contain colons, so split only 3 times.
      acct="${spec%%:*}"; rest="${spec#*:}"
      start="${rest%%:*}"; rest="${rest#*:}"
      end="${rest%%:*}"; file="${rest#*:}"
      if [ -z "$acct" ] || [ -z "$start" ] || [ -z "$end" ] || [ -z "$file" ]; then
        echo "  ! bad spec: $spec  (expected ACCT:START:END:FILE)" >&2
        exit 2
      fi
      uv run python transaction_downloader/capture.py --provider "$PROVIDER" file \
        --account "$acct" --start "$start" --end "$end" "$file"
    done
  fi
  echo
fi

echo "=== 2. ingest (raw_documents) ==="
# Only directories that actually hold captures are walked. data_dir is the
# provider's work directory -- it holds derived output, not captures, and every
# configured one is empty today, so leaving it out changes nothing that lands.
if [ -n "$ARCHIVE_DIR" ]; then
  uv run python src/ingest_raw.py --provider "$PROVIDER" "$ARCHIVE_DIR" "$RAW_DIR"
else
  uv run python src/ingest_raw.py --provider "$PROVIDER" "$RAW_DIR"
fi
echo
echo "=== 3. parse (transactions) ==="
uv run python src/parse_raw.py --provider "$PROVIDER"
echo
echo "=== 4. what is still missing ==="
uv run python transaction_downloader/plan.py --provider "$PROVIDER" || true
