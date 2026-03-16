import argparse
import subprocess
import tomllib
import os
from pathlib import Path
import argcomplete
from core.cmc_core import cmc

def migration_name_completer(prefix, parsed_args, **kwargs):
    m_dir = cmc.get_migrations_path()
    if not m_dir or not m_dir.exists():
        return []
    
    files = [f for f in os.listdir(m_dir) if f.endswith('.sql')]
    
    valid_names = []
    for f in files:
        if '_' in f:
            clean_name = f.split('_', 1)[1].replace('.sql', '')
            valid_names.append(clean_name)
            
    return [name for name in valid_names if name.startswith(prefix)]

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

    # db rescue
    p_db_rescue = db_sub.add_parser("rescue")
    p_db_rescue.add_argument("name", help="Fix name")
    p_db_rescue.set_defaults(func=lambda args: cmc.migrations.rescue(args.name))

    # db restore
    p_db_restore = db_sub.add_parser("restore")
    p_db_restore.add_argument("backup_name", nargs="?", default=None, help="Backup file name (optional)")
    
    eg_res = p_db_restore.add_mutually_exclusive_group()
    eg_res.add_argument("--dev", action="store_true")
    eg_res.add_argument("--prod", action="store_true")
    
    p_db_restore.set_defaults(func=lambda args: cmc.migrations.restore(
        "prod" if args.prod else "dev", 
        args.backup_name
    ))

    # db status
    p_db_status = db_sub.add_parser("status")
    eg_stat = p_db_status.add_mutually_exclusive_group()
    eg_stat.add_argument("--dev", action="store_true")
    eg_stat.add_argument("--prod", action="store_true")
    p_db_status.set_defaults(func=lambda args: cmc.migrations.status("prod" if args.prod else "dev"))

    # db gen
    p_db_gen = db_sub.add_parser("gen", help="Generators for DB structures")
    gen_sub = p_db_gen.add_subparsers(dest="gen_command", required=True)
    
    p_gen_analysis = gen_sub.add_parser("analysis", help="Forge an analysis contract for a table or view")
    p_gen_analysis.add_argument("name", help="Structure name")
    p_gen_analysis.set_defaults(func=lambda args: cmc.gen_analysis_config(args.name))

    # db analysis
    p_db_analysis = db_sub.add_parser("analysis", help="Run performance analysis on an entity")
    p_db_analysis.add_argument("name", nargs="?", default=None, help="Table or View to analyze (default: all in analysis.json)")
    p_db_analysis.add_argument("--reset", action="store_true", help="Hard reset: re-clone schema from source")
    p_db_analysis.add_argument("--remove", action="store_true", help="Vaporize the lab database")
    
    p_db_analysis.set_defaults(func=lambda args: cmc.analysis.execute(
        args.name, 
        reset=args.reset, 
        remove=args.remove
    ))

    # api
    p_api = subparsers.add_parser("api")
    api_sub = p_api.add_subparsers(dest="api_command", required=True)
    
    # api sync
    p_sync = api_sub.add_parser("sync")
    def handle_sync(args):
        cmc._setup_routes()
        cmc.sync_api_metadata()
    p_sync.set_defaults(func=handle_sync)
    
    reg_sub_parser = api_sub.add_parser("register")
    reg_types = reg_sub_parser.add_subparsers(dest="reg_type", required=True)
    
    # api register entity
    p_ent = reg_types.add_parser("entity")
    p_ent.add_argument("name")
    p_ent.set_defaults(func=lambda args: cmc.register_entity(args.name))

    # remove
    p_rem = subparsers.add_parser("remove")
    p_rem.set_defaults(func=lambda args: cmc.remove_self())

    # up
    p_up = subparsers.add_parser("up")
    def handle_up(args):
        import sys
        
        if not cmc.config_path or not cmc.config_path.exists():
            cmc.log.error("config.toml not found. Are you in the project root?")
            return

        with open(cmc.config_path, "rb") as f:
            config = tomllib.load(f)
        
        srv = config.get('server', {})
        host, port = srv.get('host', '0.0.0.0'), srv.get('port', 8000)
        workers, threads = srv.get('workers', 4), srv.get('threads', 2)

        venv_bin = os.path.dirname(sys.executable)
        gunicorn_bin = os.path.join(venv_bin, "gunicorn")

        cmc.log.info(f"Launching CMC Engine on {host}:{port}...")

        env = os.environ.copy()
        env["PYTHONPATH"] = f".:{env.get('PYTHONPATH', '')}"

        subprocess.run([
            gunicorn_bin,
            "-w",
            str(workers),
            "--threads",
            str(threads),
            "-b", f"{host}:{port}",
            "--chdir",
            ".",
            "core.cmc_core:application"
        ], env=env)
    p_up.set_defaults(func=handle_up)

    argcomplete.autocomplete(parser)
    
    args = parser.parse_args()
    if hasattr(args, "func"):
        args.func(args)
    
if __name__ == "__main__":
    main()