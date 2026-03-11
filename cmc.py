#!/usr/bin/env python3
from __future__ import annotations
import os
import argparse
import getpass
import subprocess
import shutil
import tomllib
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, List, Any, Set, Final

import psycopg2
from psycopg2.extensions import connection as PgConnection

# 1. CORE SERVICES (LOGGING & DB ENGINE)

class Logger:
    def info(self, msg: str) -> None: print(f"\033[94m[INFO]\033[0m {msg}")
    def log(self, msg: str) -> None: print(f"\033[92m[SUCCESS]\033[0m {msg}")
    def error(self, msg: str) -> None: print(f"\033[91m[ERROR]\033[0m {msg}")
    def debug(self, msg: str) -> None: print(f"\033[90m[DEBUG]\033[0m {msg}")
    def warn(self, msg: str) -> None: print(f"\033[91m[WARNING]\033[0m {msg}")

class DBEngine:
    def __init__(self, bunker: Bunker):
        self.b = bunker

    def get_connection(self, cfg: Optional[Dict] = None) -> Optional[PgConnection]:
        target = cfg or self.b.context
        if not target: return None
        try:
            return psycopg2.connect(
                user=target['user'],
                password=target['password'],
                host=target['host'],
                port=target['port'],
                dbname=target['dbname']
            )
        except Exception as e:
            self.b.log.error(f"PostgreSQL Connection Failed: {e}")
            return None

    def forge_infrastructure(self, admin_user: str, admin_pass: str, target_db: str, project_user: str, project_pass: str):
        """Administrative creation of roles and databases."""
        try:
            admin_conn = psycopg2.connect(user=admin_user, password=admin_pass, host="localhost", port=5432, dbname="postgres")
            admin_conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            with admin_conn.cursor() as cur:
                cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (project_user,))
                if not cur.fetchone():
                    self.b.log.info(f"Forging new role: {project_user}")
                    cur.execute(f"CREATE ROLE {project_user} WITH LOGIN PASSWORD %s", (project_pass,))
                    cur.execute(f"ALTER ROLE {project_user} CREATEDB CREATEROLE")
                
                cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (target_db,))
                if not cur.fetchone():
                    self.b.log.info(f"Forging database: {target_db}")
                    cur.execute(f'CREATE DATABASE "{target_db}" OWNER {project_user}')
            admin_conn.close()
        except Exception as e:
            self.b.log.error(f"Infrastructure forge failed: {e}")

    def drop_specific_assets(self, admin_user: str, admin_pass: str, dbs_to_drop: List[str], drop_role: bool, project_user: str):
        """Surgical teardown of specific databases and roles."""
        try:
            admin_conn = psycopg2.connect(user=admin_user, password=admin_pass, host="localhost", port=5432, dbname="postgres")
            admin_conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            with admin_conn.cursor() as cur:
                for db in dbs_to_drop:
                    cur.execute(f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{db}' AND pid <> pg_backend_pid();")
                    self.b.log.info(f"Terminating sessions and dropping: {db}...")
                    cur.execute(f'DROP DATABASE IF EXISTS "{db}"')
                
                if drop_role:
                    self.b.log.info(f"Removing role: {project_user}")
                    cur.execute(f'DROP ROLE IF EXISTS "{project_user}"')
            admin_conn.close()
        except Exception as e:
            self.b.log.error(f"Infrastructure teardown failed: {e}")

# 2. VERSIONING SERVICE (MIGRATIONS)

class MigrationEngine:
    """Service dedicated to the evolution and rollback of the database DNA."""
    def __init__(self, bunker: Bunker):
        self.b = bunker

    def create(self, name: str):
        m_dir = self.b.get_migrations_path()
        if not m_dir: return
        
        ts = datetime.now().strftime("%Y%m%d%H%M%S")
        safe_name = name.lower().replace(' ', '_')
        header = f"-- Migration: {name}\n-- Created on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        
        for ext in ["", ".down"]:
            with open(m_dir / f"{ts}_{safe_name}{ext}.sql", "w") as f:
                f.write(f"{header}BEGIN;\n\nCOMMIT;")
        self.b.log.log(f"Dual migration created: {ts}_{safe_name}")

    def migrate(self, target_env: str):
        m_dir = self.b.get_migrations_path()
        cfg = self.b.load_env_config(target_env)
        conn = self.b.db.get_connection(cfg)
        if not conn: return

        self.b.log.info(f"Synchronizing Bunker DNA with: [{target_env.upper()}]")
        try:
            with conn.cursor() as cur:
                cur.execute("CREATE TABLE IF NOT EXISTS cmc_migrations (id SERIAL PRIMARY KEY, name TEXT UNIQUE, applied_at TIMESTAMP DEFAULT NOW());")
                cur.execute("SELECT name FROM cmc_migrations")
                applied = {r[0] for r in cur.fetchall()}
            
            files = sorted([f for f in os.listdir(m_dir) if f.endswith(".sql") and ".down." not in f])
            pending = [f for f in files if f not in applied]
            
            if not pending:
                self.b.log.info("Database is already up to date.")
                return

            for f in pending:
                with open(m_dir / f, 'r') as sql_file:
                    content = sql_file.read()
                    with conn.cursor() as cur:
                        cur.execute(content)
                        cur.execute("INSERT INTO cmc_migrations (name) VALUES (%s)", (f,))
                conn.commit()
                self.b.log.log(f"Applied: {f}")
        except Exception as e:
            conn.rollback()
            self.b.log.error(f"Migration sequence aborted: {e}")
        finally: conn.close()

    def rollback(self, target_env: str, steps: int):
        m_dir = self.b.get_migrations_path()
        cfg = self.b.load_env_config(target_env)
        conn = self.b.db.get_connection(cfg)
        if not conn: return

        self.b.log.info(f"Initiating rollback ({steps} steps) on: [{target_env.upper()}]")
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT name FROM cmc_migrations ORDER BY id DESC LIMIT %s", (steps,))
                to_rollback = [r[0] for r in cur.fetchall()]

            if not to_rollback:
                self.b.log.info("Nothing to rollback.")
                return

            for f in to_rollback:
                down_file = f.replace(".sql", ".down.sql")
                if not (m_dir / down_file).exists():
                    self.b.log.error(f"Missing undo script: {down_file}")
                    continue

                with open(m_dir / down_file, 'r') as sql_file:
                    content = sql_file.read()
                    with conn.cursor() as cur:
                        cur.execute(content)
                        cur.execute("DELETE FROM cmc_migrations WHERE name = %s", (f,))
                conn.commit()
                self.b.log.log(f"Successful rollback: {f}")
        except Exception as e:
            conn.rollback()
            self.b.log.error(f"Rollback failed: {e}")
        finally: conn.close()


# 3. CENTRAL ORCHESTRATOR (THE BUNKER)

class Bunker:
    """The Engine's Core. Manages context, state, and coordinates all services."""
    def __init__(self):
        self.log = Logger()
        self.root = Path(os.getcwd())
        self.config_path = self.root / "commons" / "config.toml"
        
        self.context: Optional[Dict[str, Any]] = None
        self.metadata: Dict[str, Any] = {}
        
        self.db = DBEngine(self)
        self.migrations = MigrationEngine(self)
        
        self._detect_context()

    def _detect_context(self):
        """Surgically extracts the current project context if available."""
        if not self.config_path.exists(): return
        try:
            with open(self.config_path, "rb") as f:
                data = tomllib.load(f)
            
            self.metadata = data.get("project", {})
            env = data.get("current_env", "dev")
            
            self.context = {
                "user": data.get("user"),
                "password": data.get("password"),
                "port": data.get("port", 5432),
                "host": data.get(env, {}).get("host"),
                "dbname": data.get(env, {}).get("name"),
                "env": env
            }
        except Exception as e:
            self.log.error(f"Failed to load Bunker context: {e}")

    def load_env_config(self, env: str) -> Optional[Dict]:
        """Loads specific environment settings without switching global state."""
        try:
            with open(self.config_path, "rb") as f:
                data = tomllib.load(f)
            return {
                "user": data.get("user"),
                "password": data.get("password"),
                "port": data.get("port", 5432),
                "host": data.get(env, {}).get("host"),
                "dbname": data.get(env, {}).get("name")
            }
        except Exception: return None

    def get_migrations_path(self) -> Optional[Path]:
        for item in self.root.iterdir():
            if item.is_dir() and item.name.startswith("sv"):
                p = item / "migrations"
                if p.exists(): return p
        self.log.error("Migrations directory not found in the Bunker.")
        return None

    def init_new(self, name: str):
        cap_name = name.capitalize()
        p_root = self.root / name
        if p_root.exists():
            self.log.error(f"Directory '{name}' already exists.")
            return

        self.log.info(f"Forging new Bunker: {name}")
        p_user = name.lower().replace("-", "_")
        p_pass = getpass.getpass(f"Password for project user '{p_user}': ").strip()
        adm_u = input("Admin user [postgres]: ").strip() or "postgres"
        adm_p = getpass.getpass(f"Password for admin '{adm_u}': ").strip()

        try:
            sv_dir, cm_dir = p_root / f"sv{cap_name}", p_root / "commons"
            for d in [sv_dir / "handlers", sv_dir / "migrations", cm_dir]:
                d.mkdir(parents=True, exist_ok=True)
                (d / ".gitkeep").touch()

            with open(cm_dir / "config.toml", "w") as f:
                f.write(f"current_env = 'dev'\nuser = '{p_user}'\nport = 5432\npassword = '{p_pass}'\n\n"
                        f"[project]\nname = '{cap_name}'\nsv_version = '0.1.0'\ncl_version = '0.1.0'\n\n"
                        f"[dev]\nhost = 'localhost'\nname = '{p_user}_db_dev'\n\n"
                        f"[prod]\nhost = 'localhost'\nname = '{p_user}_db_prod'\n")
            
            with open(sv_dir / "app.py", "w") as f:
                f.write("def application(env, start_response):\n    status = '200 OK'\n"
                        "    headers = [('Content-type', 'application/json')]\n"
                        "    start_response(status, headers)\n"
                        "    return [b'{\"status\": \"ready\", \"engine\": \"CMC\"}']\n")

            for env in ["dev", "prod"]:
                self.db.forge_infrastructure(adm_u, adm_p, f"{p_user}_db_{env}", p_user, p_pass)

            print("\n" + "─" * 40)
            self.log.info("Interface Choice")
            print("1) Web (Vite + React + TS)\n2) App (Tauri + React + TS)")
            choice = input("\nSelection [1]: ").strip() or "1"
            fe_cmd = ["npm", "create", "vite@latest" if choice == "1" else "tauri-app@latest", f"cl{cap_name}", "--", "--template", "react-ts"]
            subprocess.run(fe_cmd, cwd=p_root, check=True)

            self.log.log(f"Bunker '{name}' successfully initialized!")
        except Exception as e:
            self.log.error(f"Creation failed: {e}")

    def switch_env(self, target: str):
        if not self.config_path.exists(): return
        try:
            lines = self.config_path.read_text().splitlines()
            new_lines = [f"current_env = '{target}'" if l.startswith("current_env") else l for l in lines]
            self.config_path.write_text("\n".join(new_lines) + "\n")
            self.log.log(f"Global Bunker state swapped to: [{target.upper()}]")
        except Exception as e: self.log.error(f"Switch failed: {e}")

    def remove_self(self):
        """Surgical removal with specific y/n confirmation for each asset."""
        if not self.context:
            self.log.error("No Bunker context found.")
            return

        p_name = self.metadata.get("name")
        self.log.warn(f"NUCLEAR SEQUENCE DETECTED: {p_name}")
        confirm = input(f"Type '{p_name}' to confirm the destruction process: ")
        if confirm != p_name: return

        adm_u = input("Admin user [postgres]: ").strip() or "postgres"
        adm_p = getpass.getpass(f"Password for admin '{adm_u}': ").strip()
        
        dbs_to_drop = []
        project_user = self.context['user']
        
        for env in ["dev", "prod"]:
            db_name = f"{project_user}_db_{env}"
            if input(f"Delete database '{db_name}'? (y/n): ").lower() == 'y':
                dbs_to_drop.append(db_name)
        
        drop_role = input(f"Remove role '{project_user}'? (y/n): ").lower() == 'y'

        if dbs_to_drop or drop_role:
            self.db.drop_specific_assets(adm_u, adm_p, dbs_to_drop, drop_role, project_user)
        
        if input("Vaporize local Bunker files? (y/n): ").lower() == 'y':
            try:
                shutil.rmtree(self.root)
                print("\033[92m[SUCCESS]\033[0m Local files vaporized.")
            except Exception as e: self.log.error(f"File wipe failed: {e}")

# 4. ENTRY POINT (CLI PARSER)

def main():
    bunker = Bunker()
    parser = argparse.ArgumentParser(description="CMC CLI - The Bunker Engine v2")
    subparsers = parser.add_subparsers(dest="command")

    # init
    p_init = subparsers.add_parser("init", help="Forge a new project")
    p_init.add_argument("--new", metavar="NAME", required=True)

    # env
    p_env = subparsers.add_parser("env", help="Switch between dev/prod")
    eg = p_env.add_mutually_exclusive_group(required=True)
    eg.add_argument("--dev", action="store_true")
    eg.add_argument("--prod", action="store_true")

    # db
    p_db = subparsers.add_parser("db", help="Genetic management (Migrations)")
    db_sub = p_db.add_subparsers(dest="db_command", required=True)
    
    # db create
    db_sub.add_parser("create").add_argument("name", help="Migration name")
    
    # db migrate
    p_migrate = db_sub.add_parser("migrate")
    mg = p_migrate.add_mutually_exclusive_group()
    mg.add_argument("--dev", action="store_true")
    mg.add_argument("--prod", action="store_true")

    # db rollback
    p_rollback = db_sub.add_parser("rollback")
    p_rollback.add_argument("steps", type=int, nargs="?", default=1)
    rg = p_rollback.add_mutually_exclusive_group()
    rg.add_argument("--dev", action="store_true")
    rg.add_argument("--prod", action="store_true")

    # remove & up
    subparsers.add_parser("remove", help="Nuclear wipe")
    subparsers.add_parser("up", help="Start the engines")

    args = parser.parse_args()

    # Orchestration
    if args.command == "init":
        bunker.init_new(args.new)
    
    elif args.command == "env":
        bunker.switch_env("prod" if args.prod else "dev")

    elif args.command == "db":
        if args.db_command == "create":
            bunker.migrations.create(args.name)
        elif args.db_command == "migrate":
            target = "prod" if args.prod else "dev"
            bunker.migrations.migrate(target)
        elif args.db_command == "rollback":
            target = "prod" if args.prod else "dev"
            bunker.migrations.rollback(target, args.steps)

    elif args.command == "remove":
        bunker.remove_self()

    elif args.command == "up":
        if bunker.context:
            bunker.log.info(f"Engines online. Env: [{bunker.context['env'].upper()}]")
            bunker.log.log("System Ready.")
        else:
            bunker.log.error("No context loaded.")

    else: parser.print_help()

if __name__ == "__main__":
    main()