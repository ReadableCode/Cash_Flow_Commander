# Step functions for Cash_Flow_Commander commands (windows). Dot-sourced by cmdr.

function cashflow_tui {
    Set-Location (Join-Path $env:CMDR_REPO_DIR 'src')
    uv run python cfc_tui.py
    exit $LASTEXITCODE
}
