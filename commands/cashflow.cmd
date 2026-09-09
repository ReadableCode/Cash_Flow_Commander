# The Cash_Flow_Commander TUI as a cmdr command: terminal step, so cmdr's
# TUI hands the screen over. No check: the TUI has no headless render.
description: cash flow commander - the finance tui
order: 260
platforms: darwin linux windows
steps:
  cashflow_tui requires=uv terminal
