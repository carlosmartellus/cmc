import os
import json
import tomllib
import psycopg2
import re
from pathlib import Path
from typing import Callable, Dict, Any, List
import importlib
import pkgutil


class Logger:
    def info(self, msg: str) -> None: print(f"\033[94m[INFO]\033[0m {msg}")
    def log(self, msg: str) -> None: print(f"\033[92m[SUCCESS]\033[0m {msg}")
    def error(self, msg: str) -> None: print(f"\033[91m[ERROR]\033[0m {msg}")
    def debug(self, msg: str) -> None: print(f"\033[90m[DEBUG]\033[0m {msg}")
    def warn(self, msg: str) -> None: print(f"\033[93m[WARNING]\033[0m {msg}")


class CMCApp:
    def __init__(self, force_env: str = None):
        self.logger = Logger()
        self.config = self._load_config()
        
        self.env_name = force_env or os.environ.get('CMC_ENV') or self.config.get('current_env', 'dev')
        
        self.routes: List[Dict[str, Any]] = [] 

        self.env_cfg = self.config.get(self.env_name, {})
    
        if not self.env_cfg:
            self.logger.error(f"No configuration found for environment: {self.env_name}")
            return
        
        self.host = self.env_cfg.get('host', 'localhost')
        self.port = self.config.get('port', 5432)
        self.user = self.config.get('user')
        self.password = self.config.get('password')
        self.dbname = self.env_cfg.get('name')

        self.db = self._connect_db()

    def _load_config(self) -> Dict[str, Any]:
        path = Path("commons/config.toml")
        if not path.exists():
            msg = "commons/config.toml not found. CMC cannot start."
            raise FileNotFoundError(f"\033[91m[CRITICAL]\033[0m {msg}")
        with open(path, "rb") as f:
            return tomllib.load(f)

    def _connect_db(self):
        try:
            conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                dbname=self.dbname
            )
            conn.autocommit = True
            self.logger.log(f"CMC ACTIVE | Mode: {self.env_name.upper()} | DB: {self.dbname}")
            return conn
        except Exception as e:
            self.logger.error(f"DB Connection failed: {e}")
            return None

    def add_route(self, method: str, path: str, handler: Callable):
        pattern = re.sub(r'<[^>]+>', r'([^/]+)', path)
        self.routes.append({
            'method': method.upper(),
            'path_original': path,
            'pattern': re.compile(f'^{pattern}$'),
            'handler': handler
        })

    def __call__(self, environ: Dict[str, Any], start_response: Callable):
        environ['cmc.db'] = self.db
        path = environ.get('PATH_INFO', '/')
        method = environ.get('REQUEST_METHOD', 'GET').upper()

        if method == 'OPTIONS':
            start_response('200 OK', [
                ('Access-Control-Allow-Origin', '*'),
                ('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS'),
                ('Access-Control-Allow-Headers', 'Content-Type')
            ])
            return [b'']

        handler = None
        args = []
        
        for route in self.routes:
            if route['method'] == method:
                match = route['pattern'].match(path)
                if match:
                    handler = route['handler']
                    args = match.groups()
                    break

        if handler:
            try:
                status, response_body = handler(environ, *args) if args else handler(environ)
            except Exception as e:
                status = '500 INTERNAL SERVER ERROR'
                response_body = {'error': 'Internal Server Error', 'details': str(e)}
        else:
            status = '404 NOT FOUND'
            response_body = {'error': f'Route {method} {path} not registered in CMC'}

        response_data = json.dumps(response_body).encode('utf-8')
        headers = [
            ('Content-Type', 'application/json'),
            ('Content-Length', str(len(response_data))),
            ('Access-Control-Allow-Origin', '*'),
            ('X-Engine', 'CMC-Framework-3.13')
        ]
        start_response(status, headers)
        return [response_data]
    
def forge_cmc_routes(app: CMCApp):
    """Scans and injects handlers into the app"""
    base_path = Path(__file__).parent.resolve()
    handlers_path = base_path / "handlers"
    
    if not handlers_path.exists():
        app.logger.warn("Handlers directory not found. No routes were forged.")
        return
    
    project_sv_folder = base_path.name 

    for _, name, _ in pkgutil.iter_modules([str(handlers_path)]):
        module_path = f"{project_sv_folder}.handlers.{name}"
        try:
            module = importlib.import_module(module_path)
            func_name = f"create_{name}_routes"
            
            if hasattr(module, func_name):
                register_fn = getattr(module, func_name)
                register_fn(app)
                app.logger.log(f"Routes forged for entity: {name}")
        except Exception as e:
            app.logger.error(f"Could not forge routes for {name}: {e}")

application = CMCApp()
forge_cmc_routes(application)