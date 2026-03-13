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


class Logger:
    def info(self, msg: str) -> None: print(f"\033[94m[INFO]\033[0m {msg}")
    def log(self, msg: str) -> None: print(f"\033[92m[SUCCESS]\033[0m {msg}")
    def error(self, msg: str) -> None: print(f"\033[91m[ERROR]\033[0m {msg}")
    def debug(self, msg: str) -> None: print(f"\033[90m[DEBUG]\033[0m {msg}")
    def warn(self, msg: str) -> None: print(f"\033[93m[WARNING]\033[0m {msg}")


class DBEngine:
    def __init__(self, CMC: CMC):
        self.b = CMC

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
        
    def table_exists(self, table_name: str, cfg: Optional[Dict] = None) -> bool:
        """Surgical check for table existence in the current context."""
        conn = self.get_connection(cfg)
        if not conn: return False
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'public' 
                        AND table_name = %s
                    );
                """, (table_name,))
                return cur.fetchone()[0]
        except Exception as e:
            self.b.log.error(f"Failed to inspect table '{table_name}': {e}")
            return False
        finally:
            conn.close()

    def get_table_columns(self, table_name: str) -> List[str]:
        conn = self.get_connection()
        if not conn: return []
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT column_name FROM information_schema.columns 
                    WHERE table_name = %s AND table_schema = 'public'
                    ORDER BY ordinal_position;
                """, (table_name,))
                return [row[0] for row in cur.fetchall() if row[0] != 'id']
        finally: conn.close()

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
    
    def get_returning_column(self, table_name: str) -> Optional[str]:
        conn = self.get_connection()
        if not conn: return None
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT kcu.column_name
                    FROM information_schema.table_constraints tco
                    JOIN information_schema.key_column_usage kcu 
                      ON kcu.constraint_name = tco.constraint_name
                    WHERE tco.constraint_type = 'PRIMARY KEY' AND tco.table_name = %s
                    LIMIT 1;
                """, (table_name,))
                res = cur.fetchone()
                if res: return res[0]

                cur.execute("""
                    SELECT a.attname
                    FROM pg_index i
                    JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                    WHERE i.indrelid = %s::regclass AND NOT i.indisprimary
                    LIMIT 1;
                """, (table_name,))
                res = cur.fetchone()
                if res: return res[0]
                
                return None
        except Exception: return None
        finally: conn.close()


class MigrationEngine:
    """Service dedicated to the evolution and rollback of the database."""
    def __init__(self, CMC: CMC):
        self.b = CMC

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

        self.b.log.info(f"Synchronizing CMC with: [{target_env.upper()}]")
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


class CMC:
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
            self.log.error(f"Failed to load CMC context: {e}")

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
        self.log.error("Migrations directory not found in the CMC.")
        return None

    def init_new(self, name: str):
        cap_name = name.capitalize()
        p_root = self.root / name
        if p_root.exists():
            self.log.error(f"Directory '{name}' already exists.")
            return

        self.log.info(f"Forging new CMC: {name}")
        p_user = name.lower().replace("-", "_")
        p_pass = getpass.getpass(f"Password for project user '{p_user}': ").strip()
        adm_u = input("Admin user [postgres]: ").strip() or "postgres"
        adm_p = getpass.getpass(f"Password for admin '{adm_u}': ").strip()

        try:
            sv_dir, cm_dir = p_root / f"sv{cap_name}", p_root / "commons"
            for d in [sv_dir / "handlers", sv_dir / "migrations", cm_dir]:
                d.mkdir(parents=True, exist_ok=True)
                fname = "__init__.py" if d.name == "handlers" else ".gitkeep"
                (d / fname).touch()

            with open(cm_dir / "config.toml", "w") as f:
                f.write(f"current_env = 'dev'\nuser = '{p_user}'\nport = 5432\npassword = '{p_pass}'\n\n"
                        f"[project]\nname = '{cap_name}'\nsv_version = '0.1.0'\ncl_version = '0.1.0'\n\n"
                        f"[dev]\nhost = 'localhost'\nname = '{p_user}_db_dev'\n\n"
                        f"[prod]\nhost = 'localhost'\nname = '{p_user}_db_prod'\n")
                
            with open(p_root / "requirements.txt", "w") as f:
                f.write("psycopg2-binary\ngunicorn\n")

            gitignore_content = self._get_template(".gitignore")
            if gitignore_content:
                with open(p_root / ".gitignore", "w") as f:
                    f.write(gitignore_content)
            
            for env in ["dev", "prod"]:
                self.db.forge_infrastructure(adm_u, adm_p, f"{p_user}_db_{env}", p_user, p_pass)

            app_content = self._get_template("app.py")
            if app_content:
                with open(sv_dir / "app.py", "w") as f:
                    f.write(app_content)
                self.log.log("Server engine (app.py) forged from template.")

            print("\n" + "─" * 40)
            self.log.info("Interface Choice")
            print("1) Web (Vite + React + TS)\n2) App (Tauri + React + TS)")
            choice = input("\nSelection [1]: ").strip() or "1"
            fe_cmd = ["npm", "create", "vite@latest" if choice == "1" else "tauri-app@latest", f"cl{cap_name}", "--", "--template", "react-ts"]
            subprocess.run(fe_cmd, cwd=p_root, check=True)

            self.log.log(f"CMC '{name}' successfully initialized!")
        except Exception as e:
            self.log.error(f"Creation failed: {e}")

    def switch_env(self, target: str):
        if not self.config_path.exists(): return
        try:
            lines = self.config_path.read_text().splitlines()
            new_lines = [f"current_env = '{target}'" if l.startswith("current_env") else l for l in lines]
            self.config_path.write_text("\n".join(new_lines) + "\n")
            self.log.log(f"Global CMC state swapped to: [{target.upper()}]")
        except Exception as e: self.log.error(f"Switch failed: {e}")

    def remove_self(self):
        """Surgical removal with specific y/n confirmation for each asset."""
        if not self.context:
            self.log.error("No CMC context found.")
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
        
        if input("Vaporize local CMC files? (y/n): ").lower() == 'y':
            try:
                shutil.rmtree(self.root)
                print("\033[92m[SUCCESS]\033[0m Local files vaporized.")
            except Exception as e: self.log.error(f"File wipe failed: {e}")

    def _get_template(self, name: str) -> str:
        """Searchs and reads a template from CMC's installation"""
        cmc_dir = Path(__file__).parent.resolve()
        template_path = cmc_dir / "templates" / name
        
        if not template_path.exists():
            self.log.error(f"Template '{name}' not found in {template_path}")
            return ""
        
        return template_path.read_text()

    def register_entity(self, entity_name: str):
        """Forges a CRUD based on the strict 'id' convention."""
        if not self.context:
            self.log.error("No context detected.")
            return

        if not self.db.table_exists(entity_name):
            self.log.error(f"Table '{entity_name}' does not exist.")
            return

        all_cols = self.db.get_table_columns(entity_name)
        
        has_id = self._check_column_exists(entity_name, "id")
        if not has_id:
            self.log.error(f"Table '{entity_name}' violates CMC Protocol: Missing 'id' column.")
            return

        insert_cols = [c for c in all_cols if c != 'id']
        all_cols_with_id = insert_cols + ['id']
        
        create_input, update_input, get_output = "", "", ""
        for col in insert_cols:
            create_input += f'            "{col}": {{"type": "any", "nullable": False}},\n'
            update_input += f'            "{col}": {{"type": "any", "nullable": True}},\n'
            get_output   += f'            "{col}": "any",\n'
        get_output += '            "id": "any"'

        template = self._get_template("handler.py")
        
        content = (template
            .replace("{{entity}}", entity_name.lower())
            .replace("{{table}}", entity_name)
            .replace("{{col_names}}", ", ".join(insert_cols))
            .replace("{{all_col_names}}", ", ".join(all_cols_with_id))
            .replace("{{placeholders}}", ", ".join(["%s"] * len(insert_cols)))
            .replace("{{update_set}}", ", ".join([f"{c} = %s" for c in insert_cols]))
            .replace("{{columns_list}}", str(insert_cols))
            .replace("{{all_columns_list}}", str(all_cols_with_id))
            .replace("{{contract_create_input}}", create_input.rstrip())
            .replace("{{contract_update_input}}", update_input.rstrip())
            .replace("{{contract_get_output}}", get_output.rstrip())
        )
        
        self._write_handler(entity_name, content)
        self.log.log(f"Entity '{entity_name}' registered successfully under CMC Protocol.")

    def _check_column_exists(self, table: str, column: str) -> bool:
        """Helper that validates ID"""
        conn = self.db.get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_name = %s AND column_name = %s
                """, (table, column))
                return cur.fetchone() is not None
        finally: conn.close()

    def _write_handler(self, name: str, content: str):
        """Writes handler and adds no __init__.py"""
        sv_dir = self.get_migrations_path().parent
        handler_dir = sv_dir / "handlers"
        handler_path = handler_dir / f"{name.lower()}.py"
        init_path = handler_dir / "__init__.py"

        if handler_path.exists():
            self.log.warn(f"Handler '{name.lower()}.py' already exists. Skipping.")
            return
            
        handler_path.write_text(content)

        import_line = f"from .{name.lower()} import create_{name.lower()}_routes\n"
        
        current_init = ""
        if init_path.exists():
            current_init = init_path.read_text()
        
        if import_line not in current_init:
            with open(init_path, "a") as f:
                f.write(import_line)
            self.log.info(f"Registered in __init__.py: {name.lower()}")


def main():
    check_for_updates()

    cmc = CMC() 
    
    parser = argparse.ArgumentParser(description="CMC CLI - The Framework Engine v2")
    subparsers = parser.add_subparsers(dest="command")

    # [init]
    p_init = subparsers.add_parser("init", help="Forge a new project")
    p_init.add_argument("--new", metavar="NAME", required=True)

    # [env]
    p_env = subparsers.add_parser("env", help="Switch between dev/prod")
    eg = p_env.add_mutually_exclusive_group(required=True)
    eg.add_argument("--dev", action="store_true")
    eg.add_argument("--prod", action="store_true")

    # [db]
    p_db = subparsers.add_parser("db", help="Genetic management (Migrations)")
    db_sub = p_db.add_subparsers(dest="db_command", required=True)
    db_sub.add_parser("create").add_argument("name", help="Migration name")
    
    p_migrate = db_sub.add_parser("migrate")
    mg = p_migrate.add_mutually_exclusive_group()
    mg.add_argument("--dev", action="store_true")
    mg.add_argument("--prod", action="store_true")

    p_rollback = db_sub.add_parser("rollback")
    p_rollback.add_argument("steps", type=int, nargs="?", default=1)
    rg = p_rollback.add_mutually_exclusive_group()
    rg.add_argument("--dev", action="store_true")
    rg.add_argument("--prod", action="store_true")

    # [api]
    p_api = subparsers.add_parser("api", help="API management")
    api_sub = p_api.add_subparsers(dest="api_command", required=True)
    p_reg = api_sub.add_parser("register", help="Forge new API components")
    reg_sub = p_reg.add_subparsers(dest="reg_type", required=True)
    
    p_ent = reg_sub.add_parser("entity", help="Forge full CRUD from a table")
    p_ent.add_argument("name", help="Table name")
    
    p_rte = reg_sub.add_parser("route", help="Forge a simple custom route")
    p_rte.add_argument("name", help="Route name")

    # [system]
    subparsers.add_parser("remove", help="Nuclear wipe")
    subparsers.add_parser("up", help="Start the engines")

    args = parser.parse_args()

    if args.command == "init":
        cmc.init_new(args.new)
    
    elif args.command == "env":
        cmc.switch_env("prod" if args.prod else "dev")

    elif args.command == "db":
        if args.db_command == "create":
            cmc.migrations.create(args.name)
        elif args.db_command == "migrate":
            target = "prod" if args.prod else "dev"
            cmc.migrations.migrate(target)
        elif args.db_command == "rollback":
            target = "prod" if args.prod else "dev"
            cmc.migrations.rollback(target, args.steps)

    elif args.command == "api":
        if args.api_command == "register":
            if args.reg_type == "entity":
                cmc.register_entity(args.name)

    elif args.command == "remove":
        cmc.remove_self()

    elif args.command == "up":
        if cmc.context:
            cmc.log.info(f"Engines online. Env: [{cmc.context['env'].upper()}]")
            cmc.log.log("System Ready.")
        else:
            cmc.log.error("No context loaded.")

    else:
        parser.print_help()

def check_for_updates():
    user_name = getpass.getuser()
    skip_file = Path(f"/tmp/cmc_update_skip_{user_name}")

    if skip_file.exists():
        return

    try:
        cmc_dir = Path(__file__).parent.resolve()
        
        subprocess.run(["git", "-C", str(cmc_dir), "fetch"], 
                       capture_output=True, check=True, timeout=5)
        
        local_commit = subprocess.check_output(["git", "-C", str(cmc_dir), "rev-parse", "HEAD"]).decode().strip()
        remote_commit = subprocess.check_output(["git", "-C", str(cmc_dir), "rev-parse", "@{u}"]).decode().strip()

        if local_commit != remote_commit:
            print("\n[UPDATE] A new version of the CMC Engine is available.")
            ans = input("Do you want to update now? (y/n): ").lower()
            
            if ans == 'y':
                print("Updating CMC...")
                subprocess.run(["sudo", "git", "-C", str(cmc_dir), "pull"], check=True)
                subprocess.run(["sudo", f"{cmc_dir}/.venv/bin/pip", "install", "-r", f"{cmc_dir}/requirements.txt"], check=True)
                print("[SUCCESS] CMC updated. Please re-run your command.\n")
                exit(0)
            else:
                skip_file.touch()
                print("Update skipped for this session.\n")
                
    except Exception:
        pass

if __name__ == "__main__":
    main()