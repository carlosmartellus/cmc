# CMC Framework
### *The SQL-First Framework*

**CMC** is a framework designed for developers who demand absolute control over their infrastructure. No ORMs, no unnecessary abstractions; just **Python 3.13**, **pure PostgreSQL**, and **raw performance**.

---

## Philosophy
* **Database as protagonist:** We refuse to treat PostgreSQL as a dumb storage layer. It is the engine of your data integrity, performance, and truth. CMC wraps around the database, not the other way around.
* **SQL-First (No-ORM):** We eliminate hidden layers. You write real SQL, optimize your own JOINs, and manage your indexes. The database is the heart of the application, not an implementation detail.
* **Automated Scaffolding:** The CMC CLI forges the base structure, allowing you to move from a concept to a functional API in minutes.
* **The Lab (Isolated):** Never test in production. CMC clones your live database development schema into an ephemeral, isolated environment (_cmc_lab) to run destructive stress tests and benchmark RAM/CPU impact.
* **Atomicity by Default:** All data operations (migrations and registrations) are transactional. If something fails, the system reverts to its last safe state.
* **Real Resources Result:** Performance isn't theoretical. CMC doesn't just guess query costs; it executes them. By measuring actual CPU load, RAM deltas, and millisecond execution times directly from the OS and PostgreSQL, you get empirical proof of your optimizations, not just execution plans.
---

## Project Structure

A CMC project is divided into three key sectors:

1.  **sv[Project] (The Backend):** The backend core. A WSGI server in Python 3.13 designed to handle handler logic and migrations.
2.  **cl[Project] (The Cockpit):** The control interface. Automatically generated with **Vite + React** (Web) or **Tauri** (Desktop). Flexible, it can be deleted without risks.
3.  **commons/ (The Core):** The shared layer containing the routes.json API Contract, ensuring the Frontend and Backend always speak the exact same language.

---

## Quick Start

The `cmc` command is the central tool of the forge:

### Installation
CMC comes with a self-contained installer that handles system dependencies and global binary mapping.

```bash
git clone https://github.com/carlosmartellus/cmc.git
cd cmc
bash install.sh
source ~/.bashrc
```
### Initialization
```bash
cmc init --new my_project
cd my_project
```

### Build Database
```bash
cmc db create users
cmc db migrate --dev
```

### Wire API & Sync
```bash
cmc api register entity users
cmc api sync
```
### Start Engines
```bash
cmc up
```

> [!TIP]
> **Security:** Use `cmc remove` to uninstall a project. The engine will handle cleaning up not only the files but also the databases and roles in PostgreSQL to ensure no residue is left behind.