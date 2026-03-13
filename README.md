# CMC Framework
### *The SQL-First Framework*

**CMC (Carlos Martel Cornejo)** is a framework designed for developers who demand absolute control over their infrastructure. No ORMs, no unnecessary abstractions; just **Python 3.13**, **pure PostgreSQL**, and **raw performance**.

---

## Philosophy

* **SQL-First (No-ORM):** We eliminate hidden layers. You write real SQL, optimize your own JOINs, and manage your indexes. The database is the heart of the application, not an implementation detail.
* **Automated Scaffolding:** The `cmc` CLI forges the base structure, allowing you to move from a concept to a functional API in minutes.
* **Atomicity by Default:** All data operations (migrations and registrations) are transactional. If something fails, the system reverts to its last safe state.

---

## Project Structure

A CMC project is divided into three key sectors:

1.  **sv[Project] (The Backend):** The backend core. A WSGI server in Python 3.13 designed to handle handler logic and migrations.
2.  **cl[Project] (The Cockpit):** The control interface. Automatically generated with **Vite + React** (Web) or **Tauri** (Desktop). Flexible, it can be deleted without risks.
3.  **commons/ (The Core):** The shared brain. Contains global configuration (`config.toml`) and the database connection engine.

---

## CLI Capabilities

The `cmc` command is the central tool of the forge:

### Initialization
* `cmc init --new [name]`: Forges a project from scratch, creates the databases (`dev` and `prod`), and configures the virtual environment.

### Database Management
* `cmc db create [name]`: Generates a pair of migration files (UP/DOWN) with a timestamp.
* `cmc db migrate --dev|--prod`: Sequentially synchronizes pending migrations.
* `cmc db rollback [steps]`: Safely reverts the database schema.

### API Registration
* `cmc api register entity [table]`: Scans a PostgreSQL table and automatically generates a **CMC Contract** and a full **CRUD Handler**.
    * *Note: Requires the CMC Protocol (mandatory `id` column).*

---

## Quick Start

1.  **Install and Initiate:**
    ```bash
    ./install.sh
    cmc init --new my_project
    ```

2.  **Define the DB:**
    Create a table in a migration and apply it:
    ```bash
    cmc db migrate --dev
    ```

3.  **Forge the API:**
    ```bash
    cmc api register entity users
    ```

4.  **Start the Engines (for now):**
    ```bash
    gunicorn --log-level debug svPrueba.app:application
    ```

---

> [!TIP]
> **Security:** Use `cmc remove` to uninstall a project. The engine will handle cleaning up not only the files but also the databases and roles in PostgreSQL to ensure no residue is left behind.