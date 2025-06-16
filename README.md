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

## Design Decisions

### 1️⃣ Layered Components

#### a. Data Layer

**DataSource**

- Knows how to read and parse data (Google Sheets, local files, etc.)
- Produces native Python objects, not Pandas.

#### b. Domain Models

**Transaction**

- id
- date
- amount
- account
- recurrence (Monthly, BiWeekly, Once, etc.)
- maturity_date

**Account**

- name
- category
- sub_category
- current_balance

**Ledger**

- Holds all transactions and accounts.
- Knows how to apply new transactions.
- Exposes methods for state updates.

**Projection**

- Simulates future balances.
- Takes starting balance, transactions, date range.
- Stateless: pure function that returns daily balances.

#### c. Recurrence System

**Recurrence (interface/protocol)**

- `occurs_on(date: datetime.date) -> bool`

**Concrete Recurrence Implementations:**

- MonthlyRecurrence
- YearlyRecurrence
- BiWeeklyRecurrence
- EveryXDaysRecurrence
- OnceRecurrence

#### d. Projection Engine

- Stateless.
- Projects N days into future.
- Allows branches: pass modified ledger copy into engine.
- Efficient: does not rebuild entire state on minor changes.

#### e. Branch Manager

- Manages snapshots.
- Allows user to:
  - Add transaction.
  - Rewind state.
  - Fork state.
  - Compare scenarios.

---

### 2️⃣ High-Level Flow

- `DataSource` loads external data → list of `Transaction` + `Account`.
- `Ledger` initialized with accounts + transactions.
- User modifies `Ledger` (add/delete transactions).
- `ProjectionEngine` generates daily balances forward.
- `BranchManager` allows for rapid branching, undo, scenario testing.

---

### 3️⃣ Principles

- No Pandas in core.
- State lives in memory as native Python objects.
- Changes affect only relevant parts of the projection.
- IO boundaries are cleanly isolated.
- Immutable data for projection, mutable state for ledger.
- Deterministic output for the same inputs.
- Full support for CLI, API, and UI clients later.

---

### 4️⃣ Performance Goals

- Modifying one transaction should not rebuild entire dataset.
- Only recalculate affected dates forward.
- Fast enough for realtime updates for interactive UI.

---

### 5️⃣ Growth Ready

- Easy to plug in:
  - Persistence layer (SQLite, JSON, S3)
  - REST API (FastAPI)
  - CLI (Typer)
  - UI (Streamlit, React frontend)
