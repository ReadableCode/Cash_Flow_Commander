#!/usr/bin/env bash
# Step functions for Cash_Flow_Commander commands (darwin + linux). Sourced by
# cmdr, never executed directly.

cashflow_tui() {
    # From src/, like the cashflow shell function (run_python_script cds to
    # the script's directory): the app opens files by relative path.
    (cd "$CMDR_REPO_DIR/src" && uv run python cfc_tui.py)
}
