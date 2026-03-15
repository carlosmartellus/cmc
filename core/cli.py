import argparse
import subprocess
import tomllib
import os
from pathlib import Path
from core.cmc_core import cmc

def main():
    parser = argparse.ArgumentParser(description="CMC CLI v2")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # init
    p_init = subparsers.add_parser("init")
    p_init.add_argument("--new", required=True)
    p_init.set_defaults(func=lambda args: cmc.init_new(args.new))

    # env
    p_env = subparsers.add_parser("env")
    eg = p_env.add_mutually_exclusive_group(required=True)
    eg.add_argument("--dev", action="store_true")
    eg.add_argument("--prod", action="store_true")
    p_env.set_defaults(func=lambda args: cmc.switch_env("prod" if args.prod else "dev"))

    # db
    p_db = subparsers.add_parser("db")
    db_sub = p_db.add_subparsers(dest="db_command", required=True)
    
    # db create
    p_db_create = db_sub.add_parser("create")
    p_db_create.add_argument("name")
    p_db_create.set_defaults(func=lambda args: cmc.migrations.create(args.name))
    
    # db migrate
    p_db_mig = db_sub.add_parser("migrate")
    p_db_mig.add_argument("--dev", action="store_true")
    p_db_mig.add_argument("--prod", action="store_true")
    p_db_mig.set_defaults(func=lambda args: cmc.migrations.migrate("prod" if args.prod else "dev"))
    
    # db rollback
    p_db_rol = db_sub.add_parser("rollback")
    p_db_rol.add_argument("steps", type=int, nargs="?", default=1)
    
    eg_rol = p_db_rol.add_mutually_exclusive_group()
    eg_rol.add_argument("--dev", action="store_true")
    eg_rol.add_argument("--prod", action="store_true")
    
    p_db_rol.set_defaults(func=lambda args: cmc.migrations.rollback(
        "prod" if args.prod else "dev", 
        args.steps
    ))

    # api
    p_api = subparsers.add_parser("api")
    api_sub = p_api.add_subparsers(dest="api_command", required=True)
    
    reg_sub_parser = api_sub.add_parser("register")
    reg_types = reg_sub_parser.add_subparsers(dest="reg_type", required=True)
    
    # api register
    p_ent = reg_types.add_parser("entity")
    p_ent.add_argument("name")
    p_ent.set_defaults(func=lambda args: cmc.register_entity(args.name))

    # remove
    p_rem = subparsers.add_parser("remove")
    p_rem.set_defaults(func=lambda args: cmc.remove_self())

    p_up = subparsers.add_parser("up")
    def handle_up(args):
        import sys
        import core
        
        config_path = Path("commons/config.toml")
        if not config_path.exists():
            print(f"\033[91m[ERROR]\033[0m No se encontró {config_path.absolute()}. ¿Estás en la raíz del proyecto?")
            return

        with open(config_path, "rb") as f:
            config = tomllib.load(f)
        
        srv = config.get('server', {})
        host = srv.get('host', '0.0.0.0')
        port = srv.get('port', 8000)
        workers = srv.get('workers', 4)
        threads = srv.get('threads', 2)
        timeout = srv.get('timeout', 30)
        loglevel = srv.get('log_level', 'info')

        framework_dir = str(Path(core.__file__).resolve().parent.parent)
        
        project_dir = os.getcwd()

        venv_bin = os.path.dirname(sys.executable)
        gunicorn_bin = os.path.join(venv_bin, "gunicorn")

        cmc.log.info(f"Launching CMC Engine on {host}:{port} ({workers} workers)...")

        current_env = os.environ.copy()
        paths = [project_dir, framework_dir]
        current_env["PYTHONPATH"] = ":".join(paths) + ":" + current_env.get("PYTHONPATH", "")

        subprocess.run([
            gunicorn_bin,
            "-w", str(workers),
            "--threads", str(threads),
            "--capture-output",
            "--log-level", "debug",
            "-b", f"{host}:{port}",
            "--chdir", project_dir,
            "core.cmc_core:application"
        ], env=current_env)
    p_up.set_defaults(func=handle_up)

    # execution
    args = parser.parse_args()
    
    args.func(args)

if __name__ == "__main__":
    main()