"""Export and import Grafana dashboards between this repo and a Grafana instance.

Dashboards are committed repo artifacts (deploy/grafana/*.json), not
Grafana-only state. This script moves them in both directions and is the only
supported way to do so — hand-rolled curl gets the datasource wiring wrong.

    uv run python deploy/grafana_sync.py export <uid> deploy/grafana/<name>.json
    uv run python deploy/grafana_sync.py import deploy/grafana/<name>.json
    uv run python deploy/grafana_sync.py verify <uid>
    uv run python deploy/grafana_sync.py list

The committed file is instance-independent: its datasource is the placeholder
${DS_CASH_FLOW_COMMANDER}, declared in __inputs. Import resolves that
placeholder to the real datasource uid on this instance.

THE TRAP: Grafana's /api/dashboards/import resolves __inputs placeholders only
when the posted dashboard STILL CONTAINS its __inputs block. Strip it (an easy
mistake — it looks like export-only metadata) and the import silently succeeds
while leaving every panel pointing at the literal string
"${DS_CASH_FLOW_COMMANDER}". The dashboard then renders "No data" on every
panel with no useful error. `verify` exists to catch exactly that.

Credentials come from the private .env: GRAFANA_URL, GRAFANA_ADMIN_USER,
GRAFANA_ADMIN_PASSWORD. Nothing secret is printed.
"""

# %%
# Imports #

import argparse
import base64
import json
import os
import re
import sys
import urllib.error
import urllib.request
from typing import Any

from dotenv import dotenv_values

# %%
# Constants #

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENV_PATH = os.path.join(_REPO_ROOT, ".env")

PG_PLUGIN = "grafana-postgresql-datasource"
DS_INPUT_NAME = "DS_CASH_FLOW_COMMANDER"
DS_PLACEHOLDER = "${" + DS_INPUT_NAME + "}"

DS_INPUTS = [
    {
        "name": DS_INPUT_NAME,
        "label": "Cash Flow Commander Postgres",
        "description": "Postgres datasource for the apps database, connecting as grafana_ro.",
        "type": "datasource",
        "pluginId": PG_PLUGIN,
        "pluginName": "PostgreSQL",
    }
]
DS_REQUIRES = [
    {"type": "grafana", "id": "grafana", "name": "Grafana", "version": "13.0.0"},
    {"type": "datasource", "id": PG_PLUGIN, "name": "PostgreSQL", "version": "1.0.0"},
]


# %%
# HTTP #


def _config() -> dict[str, str]:
    """Read Grafana connection settings from the private .env."""
    env = dotenv_values(ENV_PATH)
    missing = [
        key
        for key in ("GRAFANA_URL", "GRAFANA_ADMIN_USER", "GRAFANA_ADMIN_PASSWORD")
        if not (env.get(key) or "").strip()
    ]
    if missing:
        raise SystemExit(
            f"missing in {ENV_PATH}: {', '.join(missing)} (see template.env)"
        )
    return {
        "url": str(env["GRAFANA_URL"]).rstrip("/"),
        "user": str(env["GRAFANA_ADMIN_USER"]),
        "password": str(env["GRAFANA_ADMIN_PASSWORD"]),
    }


def _request(path: str, payload: dict[str, Any] | None = None) -> Any:
    """Call the Grafana API; GET when payload is None, else POST."""
    cfg = _config()
    token = base64.b64encode(f"{cfg['user']}:{cfg['password']}".encode()).decode()
    headers = {"Authorization": "Basic " + token, "Content-Type": "application/json"}
    data = json.dumps(payload).encode() if payload is not None else None
    request = urllib.request.Request(cfg["url"] + path, data=data, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.load(response)
    except urllib.error.HTTPError as error:
        detail = error.read()[:400].decode(errors="replace")
        raise SystemExit(f"Grafana {error.code} on {path}: {detail}")


def _postgres_datasource_uid() -> str:
    """Find the Cash Flow Commander postgres datasource uid on this instance."""
    candidates = [ds for ds in _request("/api/datasources") if ds["type"] == PG_PLUGIN]
    if not candidates:
        raise SystemExit(f"no {PG_PLUGIN} datasource configured in Grafana")
    for ds in candidates:
        if "cfc" in ds["name"].lower() or "cash" in ds["name"].lower():
            return str(ds["uid"])
    if len(candidates) == 1:
        return str(candidates[0]["uid"])
    names = ", ".join(f"{ds['name']}({ds['uid']})" for ds in candidates)
    raise SystemExit(
        f"several postgres datasources and none named for Cash Flow Commander: {names}"
    )


# %%
# Commands #


def cmd_list(_args: argparse.Namespace) -> int:
    """List dashboards on the instance."""
    for item in _request("/api/search?type=dash-db"):
        folder = item.get("folderTitle", "General")
        print(f"  uid={item.get('uid'):<24} folder={folder:<16} {item.get('title')}")
    return 0


def cmd_export(args: argparse.Namespace) -> int:
    """Fetch a dashboard and write it as an instance-independent file."""
    dashboard = _request(f"/api/dashboards/uid/{args.uid}")["dashboard"]
    text = json.dumps(dashboard, indent=2)

    concrete = sorted(
        set(
            re.findall(
                r'"type":\s*"' + PG_PLUGIN + r'",\s*"uid":\s*"([^"$][^"]*)"', text
            )
        )
    )
    for uid in concrete:
        text = text.replace(f'"{uid}"', f'"{DS_PLACEHOLDER}"')

    model = json.loads(text)
    model.pop("id", None)
    model["__inputs"] = DS_INPUTS
    model["__requires"] = DS_REQUIRES
    ordered = {key: model[key] for key in ("__inputs", "__requires")}
    ordered.update({k: v for k, v in model.items() if k not in ordered})

    with open(args.path, "w", encoding="utf-8") as handle:
        json.dump(ordered, handle, indent=2)
        handle.write("\n")

    print(f"exported {args.uid} ({ordered.get('title')!r}) -> {args.path}")
    print(f"  datasource uids templated: {concrete or 'none found'}")
    print(f"  panels: {len(ordered.get('panels', []))}")
    return 0


def cmd_import(args: argparse.Namespace) -> int:
    """Push a committed dashboard file to the instance, resolving its datasource."""
    with open(args.path, "r", encoding="utf-8") as handle:
        model = json.load(handle)

    if "__inputs" not in model:
        raise SystemExit(
            f"{args.path} has no __inputs block — Grafana cannot resolve "
            f"{DS_PLACEHOLDER} without it, and every panel would import broken. "
            "Re-export with this script."
        )

    ds_uid = args.datasource or _postgres_datasource_uid()

    # __inputs MUST stay on the posted dashboard: it is what Grafana matches
    # the `inputs` values against. Removing it is the silent-breakage trap.
    payload = {
        "dashboard": model,
        "overwrite": True,
        "inputs": [
            {
                "name": DS_INPUT_NAME,
                "type": "datasource",
                "pluginId": PG_PLUGIN,
                "value": ds_uid,
            }
        ],
    }
    result = _request("/api/dashboards/import", payload)
    uid = result.get("uid") or model.get("uid")
    print(f"imported {args.path} -> uid={uid} (datasource {ds_uid})")

    unresolved = _unresolved_panels(uid)
    if unresolved:
        print("  IMPORT LEFT PLACEHOLDERS UNRESOLVED on: " + ", ".join(unresolved))
        return 1
    print("  all panel datasources resolved")
    return 0


def _unresolved_panels(uid: str) -> list[str]:
    """Return titles of panels whose datasource uid is still a placeholder."""
    live = _request(f"/api/dashboards/uid/{uid}")["dashboard"]
    bad = []
    for panel in live.get("panels", []):
        refs = [panel.get("datasource")] + [
            target.get("datasource") for target in panel.get("targets", [])
        ]
        for ref in refs:
            if isinstance(ref, dict) and str(ref.get("uid", "")).startswith("${"):
                bad.append(panel.get("title", "<untitled>"))
                break
    return bad


def cmd_verify(args: argparse.Namespace) -> int:
    """Run every panel query of a live dashboard exactly as the UI would.

    Uses each panel's OWN datasource reference rather than substituting a
    working uid — substituting is what hides an unresolved placeholder and
    makes a broken dashboard look healthy.
    """
    live = _request(f"/api/dashboards/uid/{args.uid}")["dashboard"]

    unresolved = _unresolved_panels(args.uid)
    if unresolved:
        print("UNRESOLVED datasource placeholders on: " + ", ".join(unresolved))
        print("Re-run the import command; the dashboard is not usable as-is.")
        return 1

    ok = failed = 0
    for panel in live.get("panels", []):
        panel_ds = panel.get("datasource")
        for target in panel.get("targets", []):
            if not target.get("rawSql"):
                continue
            ref_id = target.get("refId", "A")
            query = {**target, "intervalMs": 86_400_000, "maxDataPoints": 1000}
            query.setdefault("datasource", panel_ds)
            body = {"from": args.frm, "to": args.to, "queries": [query]}
            result = _request("/api/ds/query", body)["results"].get(ref_id, {})
            title = str(panel.get("title", ""))[:52]
            if result.get("error"):
                print(f"FAIL {title:<52} {result['error'][:110]}")
                failed += 1
                continue
            frames = result.get("frames") or []
            values = frames[0]["data"]["values"] if frames else []
            points = len(values[0]) if values else 0
            flag = "" if points else "   (no rows)"
            print(f"OK   {title:<52} {points:>5} point(s){flag}")
            ok += 1
    print(f"--- {ok} ok, {failed} failed ---")
    return 1 if failed else 0


# %%
# CLI #


def build_arg_parser() -> argparse.ArgumentParser:
    """Build the grafana_sync CLI parser."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("list", help="list dashboards on the instance").set_defaults(func=cmd_list)

    export = sub.add_parser("export", help="instance -> repo file")
    export.add_argument("uid")
    export.add_argument("path")
    export.set_defaults(func=cmd_export)

    imp = sub.add_parser("import", help="repo file -> instance")
    imp.add_argument("path")
    imp.add_argument("--datasource", default=None, help="datasource uid; auto-detected by default")
    imp.set_defaults(func=cmd_import)

    verify = sub.add_parser("verify", help="run every panel query as the UI would")
    verify.add_argument("uid")
    verify.add_argument("--frm", default="now-2y", help="range start (default: now-2y)")
    verify.add_argument("--to", default="now", help="range end (default: now)")
    verify.set_defaults(func=cmd_verify)

    return parser


def main(argv: list[str] | None = None) -> int:
    """Entry point; returns a process exit code."""
    args = build_arg_parser().parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
