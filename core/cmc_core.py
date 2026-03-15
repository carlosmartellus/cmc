import os
import tomllib
import getpass
import subprocess
import shutil
from pathlib import Path
from typing import Optional, Dict, Any

from flask import Flask, jsonify

from core.utils import Logger
from core.engines.db import DBEngine
from core.engines.migration import MigrationEngine

class CMC:
    def __init__(self):
        self.log = Logger()
        self.root = Path(os.getcwd())
        self.config_path = self.root / "commons" / "config.toml"
        
        self.app = Flask(__name__)
        self.api_contract = {}
        
        self.context: Optional[Dict[str, Any]] = None
        self.metadata: Dict[str, Any] = {}
        self.db = DBEngine(self)
        self.migrations = MigrationEngine(self)
        self._detect_context()
        

    def _detect_context(self):
        if not self.config_path.exists(): return
        try:
            with open(self.config_path, "rb") as f:
                data = tomllib.load(f)
            self.metadata = data.get("project", {})
            env = data.get("current_env", "dev")
            self.context = {
                "user": data.get("user"), "password": data.get("password"),
                "port": data.get("port", 5432), "host": data.get(env, {}).get("host"),
                "dbname": data.get(env, {}).get("name"), "env": env
            }
        except Exception as e: self.log.error(f"Context error: {e}")

    def _get_template(self, name: str) -> str:
        cmc_dir = Path(__file__).parent.resolve()
        template_path = cmc_dir / "templates" / name
        return template_path.read_text() if template_path.exists() else ""
    
    def _setup_routes(self):
        import sys, importlib, pkgutil

        @self.app.route('/_contract')
        def get_contract():
            return jsonify(self.api_contract)

        try:
            mig_path = self.get_migrations_path()
            if not mig_path: return
            
            sv_dir = mig_path.parent
            h_dir = sv_dir / "handlers"
            
            if not (sv_dir / "__init__.py").exists():
                (sv_dir / "__init__.py").touch()

            if str(self.root) not in sys.path:
                sys.path.append(str(self.root))

            pkg_name = f"{sv_dir.name}.handlers"
            
            for _, modname, _ in pkgutil.iter_modules([str(h_dir)]):
                try:
                    importlib.import_module(f"{pkg_name}.{modname}")
                    print(f"\033[92m[LOADED]\033[0m {modname}")
                except Exception as e:
                    print(f"\033[91m[ERROR]\033[0m Failed to load {modname}: {e}")

        except Exception as e:
            print(f"Critical Loader Error: {e}")
    
    def route(self, path: str, method: str = "GET", schema: dict = None):
        from flask import request
        def decorator(f):
            endpoint = f"{method} {path}"
            self.api_contract[endpoint] = {
                "path": path, "method": method, "schema": schema or {}
            }

            def wrapper(*args, **kwargs):
                if method in ["POST", "PUT"] and schema:
                    data = request.get_json()
                    if not data:
                        return jsonify({"error": "Request body must be JSON"}), 400
                    
                    for field, rules in schema.items():
                        is_nullable = rules.get("nullable", False)
                        if field not in data and not is_nullable:
                            return jsonify({"error": f"Missing required field: {field}"}), 400
                
                return f(*args, **kwargs)

            self.app.add_url_rule(path, endpoint=endpoint, view_func=wrapper, methods=[method])
            return f
        return decorator

    def load_env_config(self, env: str):
        try:
            with open(self.config_path, "rb") as f:
                data = tomllib.load(f)
            return {"user": data.get("user"), "password": data.get("password"),
                    "port": data.get("port", 5432), "host": data.get(env, {}).get("host"),
                    "dbname": data.get(env, {}).get("name")}
        except: return None

    def get_migrations_path(self):
        for item in self.root.iterdir():
            if item.is_dir() and item.name.startswith("sv"):
                p = item / "migrations"
                if p.exists(): return p
        return None

    def init_new(self, name: str):
        cap_name = name.capitalize()
        p_root = self.root / name
        if p_root.exists(): return self.log.error(f"Directory exists: {name}")

        self.log.info(f"Forging CMC: {name}")
        p_user = name.lower().replace("-", "_")
        p_pass = getpass.getpass(f"DB Pass for {p_user}: ").strip()
        adm_u = input("Admin user [postgres]: ").strip() or "postgres"
        adm_p = getpass.getpass(f"Admin pass: ").strip()

        try:
            sv_dir, cm_dir = p_root / f"sv{cap_name}", p_root / "commons"
            for d in [sv_dir, sv_dir / "handlers", sv_dir / "migrations", cm_dir]:
                d.mkdir(parents=True, exist_ok=True)
                (d / "__init__.py").touch()

            vscode_dir = p_root / ".vscode"
            vscode_dir.mkdir(exist_ok=True)
            with open(vscode_dir / "settings.json", "w") as f:
                f.write('{\n  "python.analysis.extraPaths": ["/usr/local/lib/cmc"],\n'
                        '  "python.autoComplete.extraPaths": ["/usr/local/lib/cmc"]\n}')

            with open(cm_dir / "config.toml", "w") as f:
                f.write(
                    f"current_env = 'dev'\n"
                    f"user = '{p_user}'\n"
                    f"password = '{p_pass}'\n\n"
                    
                    f"[project]\n"
                    f"name = '{cap_name}'\n\n"
                    
                    f"[server]\n"
                    f"host = '0.0.0.0'\n"
                    f"port = 8000\n"
                    f"workers = 4\n"
                    f"threads = 2\n"
                    f"timeout = 30\n"
                    f"log_level = 'info'\n\n"
                    
                    f"[dev]\n"
                    f"host = 'localhost'\n"
                    f"db_port = 5432\n"
                    f"name = '{p_user}_db_dev'\n\n"
                    
                    f"[prod]\n"
                    f"host = 'localhost'\n"
                    f"db_port = 5432\n"
                    f"name = '{p_user}_db_prod'\n"
                )
            
            for env in ["dev", "prod"]:
                self.db.forge_infrastructure(adm_u, adm_p, f"{p_user}_db_{env}", p_user, p_pass)
            
            subprocess.run(["npm", "create", "vite@latest", f"cl{cap_name}", "--", "--template", "react-ts"], cwd=p_root, check=True)
            self.log.log(f"CMC '{name}' ready!")
        except Exception as e: self.log.error(f"Init failed: {e}")

    def register_entity(self, entity_name: str):
        if not self.context: return self.log.error("No context.")
        if not self.db.table_exists(entity_name): return self.log.error(f"Table '{entity_name}' not found.")

        all_cols_raw = self.db.get_table_columns(entity_name)
        fks = self.db.get_foreign_keys(entity_name)
        
        managed_cols = [c for c in all_cols_raw if c not in fks]
        output_cols = managed_cols + ['id']

        fk_hint = ""
        if fks:
            self.log.warn(f"FKs detected in '{entity_name}': {fks}. Manual Joins suggested.")
            fk_hint = "# [CMC NOTE] The following FKs were excluded from auto-gen to prevent complex Joins:\n"
            fk_hint += "".join([f"# - {fk} (Needs manual implementation in GET_SQL)\n" for fk in fks])

        c_in = "".join([f'            "{c}": {{"type": "any", "nullable": False}},\n' for c in managed_cols])
        u_in = "".join([f'            "{c}": {{"type": "any", "nullable": True}},\n' for c in managed_cols])
        g_out = "".join([f'            "{c}": "any",\n' for c in managed_cols]) + '            "id": "any"'

        content = (self._get_template("handler.py")
            .replace("{{entity}}", entity_name.lower())
            .replace("{{table}}", entity_name)
            .replace("{{col_names}}", ", ".join(managed_cols))
            .replace("{{all_col_names}}", ", ".join(output_cols))
            .replace("{{placeholders}}", ", ".join(["%s"] * len(managed_cols)))
            .replace("{{update_set}}", ", ".join([f"{c} = %s" for c in managed_cols]))
            .replace("{{columns_list}}", str(managed_cols))
            .replace("{{all_columns_list}}", str(output_cols))
            .replace("{{contract_create_input}}", c_in.rstrip())
            .replace("{{contract_update_input}}", u_in.rstrip())
            .replace("{{contract_get_output}}", g_out.rstrip())
            .replace("{{fk_hints}}", fk_hint)
        )
        
        self._write_handler(entity_name, content)
        self.log.log(f"Entity '{entity_name}' registered. Check handler for manual Join hints.")

    def _write_handler(self, name, content):
        sv_dir = self.get_migrations_path().parent
        h_dir = sv_dir / "handlers"
        
        handler_file = h_dir / f"{name.lower()}.py"
        handler_file.write_text(content)
        
        init_file = h_dir / "__init__.py"
        if not init_file.exists():
            init_file.touch()
            
        self.log.log(f"Handler for {name} forged at {handler_file.name}")

    def remove_self(self):
        if not self.context:
            self.log.error("No CMC context found.")
            return

        p_name = self.metadata.get("name")
        self.log.warn(f"Initializing remove procedure for {p_name}")
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

    def switch_env(self, target):
        lines = self.config_path.read_text().splitlines()
        new = [f"current_env = '{target}'" if l.startswith("current_env") else l for l in lines]
        self.config_path.write_text("\n".join(new) + "\n")
        self.log.log(f"Swapped to: [{target.upper()}]")

cmc = CMC()

if cmc.context:
    cmc._setup_routes()

application = cmc.app