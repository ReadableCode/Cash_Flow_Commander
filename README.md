# Cash_Flow_Commander

- The goal of this project is to allow anyone to manage their finances with a python application that can be interfaced with using an api, TUI, CLI, or Web Application.
- The initial project will not have a split frontend and backend since the initial use case will be with a TUI
- The project as configured will use an existing postgres database, but the goal is to allow for multiple database backends in the future including google sheets, sqlite, and excel files.

## Setting Up

## Running with uv

- Install uv:

  Linux:

    ```bash
    curl -LsSf https://astral.sh/uv/install.sh | sh
    ```

  Windows:

    In powershell as admin:

    ```powershell
    powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
    ```

- Run the following command to install the dependencies from the project:

  ```bash
  uv sync
  ```

- To Activate or Source the environment and not have to prepend each command with `uv run`:

  On Linux:

  ```bash
  source ./.venv/bin/activate
  ```

  On Windows (Powershell):

  ```powershell
  .\.venv\Scripts\Activate.ps1
  ```

- To Deactivate:

  ```bash
  deactivate
  ```

- To add project dependencies:

  ```bash
  uv add <package name>
  ```

- To see a tree of dependencies:

  ```bash
  uv tree
  ```
