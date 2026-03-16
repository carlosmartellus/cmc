import psycopg2
import uuid
import random
import string
import getpass
import json
import psutil
import statistics
import time
from pathlib import Path
from typing import Optional, Dict, List, Any
from datetime import datetime, timedelta

from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.panel import Panel
from rich.theme import Theme

class AnalysisEngine:
    def __init__(self, core):
        self.cmc = core
        self.lab_db_name = None
        self.processed_tables = set()
        self.visiting_stack = []
        
        self.console = Console(record=True, theme=Theme({
            "success": "bold green",
            "error": "bold red",
            "warning": "bold yellow",
            "info": "bold cyan",
            "accent": "bold magenta"
        }))

        self._generators = {
            'integer': lambda: random.randint(1, 2147483647),
            'bigint': lambda: random.randint(1, 9223372036854775807),
            'character varying': lambda: ''.join(random.choices(string.ascii_letters, k=random.randint(5, 15))),
            'text': lambda: ' '.join([''.join(random.choices(string.ascii_letters, k=random.randint(3, 8))) for _ in range(10)]),
            'boolean': lambda: random.choice([True, False]),
            'timestamp without time zone': lambda: datetime.now() - timedelta(days=random.randint(0, 365), seconds=random.randint(0, 86400)),
            'date': lambda: (datetime.now() - timedelta(days=random.randint(0, 365))).date(),
            'uuid': lambda: str(uuid.uuid4()),
            'numeric': lambda: round(random.uniform(1.0, 1000.0), 2),
            'double precision': lambda: random.uniform(1.0, 1000000.0)
        }

    def _get_random_value(self, pg_type: str):
        base_type = pg_type.split('(')[0].strip().lower()
        return self._generators.get(base_type, lambda: None)()

    def _get_stats(self, data: List[float]):
        if not data: return 0.0, 0.0, 0.0
        if len(data) < 2: return data[0], 0.0, 0.0
        return statistics.mean(data), statistics.stdev(data), statistics.variance(data)

    def _get_stats_dict(self, data: List[float]) -> dict:
        mean, stdev, var = self._get_stats(data)
        return {
            "mean": round(mean, 4),
            "stdev": round(stdev, 4),
            "variance": round(var, 4)
        }

    def _format_bytes(self, size):
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size < 1024: return f"{size:.2f} {unit}"
            size /= 1024
        return f"{size:.2f} TB"

    def _get_db_migration_version(self, conn) -> str:
        with conn.cursor() as cur:
            try:
                cur.execute("SELECT name FROM cmc_migrations ORDER BY applied_at DESC LIMIT 1")
                res = cur.fetchone()
                return res[0] if res else "initial"
            except:
                return "initial"

    def _get_existing_value_from_db(self, table_name, column_name, conn):
        with conn.cursor() as cur:
            try:
                cur.execute(f'SELECT "{column_name}" FROM "{table_name}" TABLESAMPLE SYSTEM (10) LIMIT 1')
                res = cur.fetchone()
                if not res:
                    cur.execute(f'SELECT "{column_name}" FROM "{table_name}" LIMIT 1')
                    res = cur.fetchone()
                return res[0] if res else None
            except: return None

    def _get_any_id_from_db(self, table_name, conn):
        return self._get_existing_value_from_db(table_name, "id", conn)

    def _populate_table(self, entity_name, config_all, conn):
        if entity_name in self.visiting_stack:
            self.visiting_stack.append(entity_name)
            path = " -> ".join(self.visiting_stack)
            self.console.print(Panel(f"[error]Cycle Detected:[/error] {path}", title="Critical Flaw", expand=False))
            raise Exception("Cycle detected")

        if entity_name in self.processed_tables: return
        conf = config_all.get(entity_name)
        if not conf: return

        self.visiting_stack.append(entity_name)
        try:
            relationships = self.cmc.db.get_table_relationships(entity_name)
            for col, parent_table in relationships.items():
                if parent_table in config_all:
                    self._populate_table(parent_table, config_all, conn)

            target_rows = conf['defaults']['rows_to_generate']
            cols_with_types = self.cmc.db.get_table_columns(entity_name)
            
            with Progress(SpinnerColumn(), TextColumn("[accent]{task.description}[/accent]"), BarColumn(bar_width=40), TaskProgressColumn(), console=self.console) as progress:
                task = progress.add_task(f"Bombarding {entity_name}...", total=target_rows)
                rows_done = 0
                while rows_done < target_rows:
                    payload = {}
                    for col, dtype in cols_with_types.items():
                        if col in relationships:
                            payload[col] = self._get_any_id_from_db(relationships[col], conn)
                        else:
                            payload[col] = self._get_random_value(dtype)
                    
                    try:
                        with conn.cursor() as cur:
                            cols = [f'"{c}"' for c in payload.keys()]
                            cur.execute(f'INSERT INTO "{entity_name}" ({", ".join(cols)}) VALUES ({", ".join(["%s"]*len(cols))})', list(payload.values()))
                        rows_done += 1
                        progress.update(task, advance=1)
                    except Exception as e:
                        conn.rollback()
                        self.console.print(f"[error]Error at {entity_name}:[/error] {e}")
                        break
                    if rows_done % 1000 == 0: conn.commit()
            conn.commit()
            self.processed_tables.add(entity_name)
        finally: self.visiting_stack.pop()

    def _generate_range_values(self, entity_name, col_name, pg_type, conn):
        base_val = self._get_existing_value_from_db(entity_name, col_name, conn)
        if not base_val: base_val = self._get_random_value(pg_type)
        if isinstance(base_val, (datetime, datetime.date)):
            return base_val, base_val + timedelta(days=random.randint(7, 30))
        if isinstance(base_val, (int, float)):
            return base_val, base_val + 500
        return base_val, base_val

    def _run_benchmarks(self, entity_name, conf, conn, migration) -> dict:
        defaults = conf['defaults']
        iterations = defaults.get('iterations', 1)
        range_cols = conf.get('ranges', [])
        cols_with_types = self.cmc.db.get_table_columns(entity_name)
        selected_cols = [c for c in cols_with_types.keys() if c not in conf.get('excludes', [])]

        with conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM "{entity_name}"')
            total_rows_in_db = cur.fetchone()[0]

        baseline_ram = psutil.virtual_memory().used
        psutil.cpu_percent(interval=None)
        samples = []

        self.console.print(f"\n[info]Analyzing workload: {entity_name} ({iterations} iterations)[/info]")
        
        for i in range(iterations):
            params, clauses = [], []
            for col in selected_cols:
                if col in range_cols:
                    v1, v2 = self._generate_range_values(entity_name, col, cols_with_types[col], conn)
                    clauses.append(f'"{col}" BETWEEN %s AND %s')
                    params.extend([v1, v2])
                else:
                    clauses.append(f'"{col}" = %s')
                    params.append(self._get_existing_value_from_db(entity_name, col, conn))
            
            with conn.cursor() as cur:
                cur.execute(f"EXPLAIN (ANALYZE, FORMAT JSON) SELECT * FROM \"{entity_name}\" WHERE {' AND '.join(clauses)}", params)
                cpu, ram = psutil.cpu_percent(interval=None), max(0, psutil.virtual_memory().used - baseline_ram)
                raw_plan = cur.fetchone()[0]
                plan = raw_plan[0] if isinstance(raw_plan, list) else json.loads(raw_plan)[0]
                samples.append({
                    "n": i + 1, "time": plan.get('Execution Time', 0),
                    "rows": plan.get('Plan', {}).get('Actual Rows', 0),
                    "cpu": cpu, "ram": ram
                })

        delta_str = ""
        t_mean, t_std, t_var = self._get_stats([s['time'] for s in samples])

        raw_table = Table(title=f"Raw Samples: {entity_name}", title_style="accent")
        raw_table.add_column("#", justify="center"); raw_table.add_column("Rows", justify="right", style="success")
        raw_table.add_column("Time (ms)", justify="right"); raw_table.add_column("Eff (ms/r)", justify="right", style="magenta")
        raw_table.add_column("CPU %", justify="right"); raw_table.add_column("RAM Delta", justify="right")

        for s in samples:
            eff = s["time"] / s["rows"] if s["rows"] > 0 else s["time"]
            raw_table.add_row(str(s["n"]), str(s["rows"]), f"{s['time']:.3f}", f"{eff:.4f}" if s["rows"] > 0 else "N/A", f"{s['cpu']:.1f}%", self._format_bytes(s["ram"]))
        self.console.print(raw_table)

        c_mean, c_std, c_var = self._get_stats([s['cpu'] for s in samples])
        r_mean, r_std, r_var = self._get_stats([s['ram'] for s in samples])
        
        metrics = {
            "iterations": iterations,
            "total_table_rows": total_rows_in_db,
            "execution_time_ms": self._get_stats_dict([s['time'] for s in samples]),
            "cpu_load_percent": self._get_stats_dict([s['cpu'] for s in samples]),
            "ram_impact_bytes": self._get_stats_dict([s['ram'] for s in samples])
        }

        stats_table = Table(title=f"Stability: {entity_name}{delta_str}", title_style="accent", box=None)
        stats_table.add_column("Metric", style="info"); stats_table.add_column("Mean", justify="right")
        stats_table.add_column("Stdev", justify="right", style="warning"); stats_table.add_column("Variance", justify="right", style="magenta")
        stats_table.add_row("Execution", f"{t_mean:.3f} ms", f"{t_std:.3f}", f"{t_var:.3f}")
        stats_table.add_row("CPU Load", f"{c_mean:.1f}%", f"{c_std:.2f}", f"{c_var:.2f}")
        stats_table.add_row("RAM Delta", self._format_bytes(r_mean), self._format_bytes(r_std), self._format_bytes(r_var))
        self.console.print(stats_table)

        if t_std > (t_mean * 0.5): self.console.print(Panel("High Instability Detected", style="warning"))
        return metrics

    def _save_report(self, run_results: dict, migration: str):
        mig_path = self.cmc.get_migrations_path()
        if not mig_path: return
        log_dir = mig_path.parent / "logs" / "analysis"
        log_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_path = log_dir / f"run_{timestamp}.json"
        
        with open(report_path, "w") as f:
            json.dump({
                "timestamp": datetime.now().isoformat(),
                "migration": migration,
                "project": self.cmc.metadata.get("name", "Unknown"),
                "results": run_results
            }, f, indent=4)
        
        self.console.print(f"\n[success]Audit JSON forged at:[/success] {report_path.relative_to(self.cmc.root)}")

    def _check_db_exists(self, db_name: str) -> bool:
        try:
            adm_ctx = self.cmc.context.copy(); adm_ctx['dbname'] = 'postgres'
            conn = self.cmc.db.get_connection(adm_ctx)
            with conn.cursor() as cur:
                cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
                exists = cur.fetchone() is not None
            conn.close()
            return exists
        except: return False

    def _prepare_lab(self):
        source_db, project_user = self.cmc.context['dbname'], self.cmc.context['user']
        adm_u = input("Admin user [postgres]: ").strip() or "postgres"
        adm_p = getpass.getpass("Admin password: ").strip()
        try:
            conn = psycopg2.connect(user=adm_u, password=adm_p, host=self.cmc.context['host'], port=self.cmc.context['port'], dbname="postgres")
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            with conn.cursor() as cur:
                cur.execute(f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{self.lab_db_name}'")
                cur.execute(f'DROP DATABASE IF EXISTS "{self.lab_db_name}"')
                cur.execute(f'CREATE DATABASE "{self.lab_db_name}" WITH TEMPLATE "{source_db}" OWNER {project_user}')
            conn.close()
            self._truncate_lab_tables()
            return True
        except: return False

    def _truncate_lab_tables(self):
        lab_ctx = self.cmc.context.copy(); lab_ctx['dbname'] = self.lab_db_name
        conn = self.cmc.db.get_connection(lab_ctx)
        if not conn: return
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE'")
                tables = [row[0] for row in cur.fetchall() if row[0] not in ['pg_stat_statements', 'cmc_migrations']]
                if tables: cur.execute(f"TRUNCATE TABLE {', '.join([f'\"{t}\"' for t in tables])} CASCADE")
            conn.commit()
            conn.close()
        except: pass

    def _drop_lab(self):
        adm_u = input("Admin user [postgres]: ").strip() or "postgres"
        adm_p = getpass.getpass("Admin password: ").strip()
        try:
            conn = psycopg2.connect(user=adm_u, password=adm_p, host=self.cmc.context['host'], port=self.cmc.context['port'], dbname="postgres")
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            with conn.cursor() as cur: 
                cur.execute(f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{self.lab_db_name}'")
                cur.execute(f'DROP DATABASE IF EXISTS "{self.lab_db_name}"')
            conn.close()
        except: pass

    def _load_analysis_config(self) -> dict:
        mig_path = self.cmc.get_migrations_path()
        if not mig_path: return {}
        config_file = mig_path.parent / "config" / "analysis.json"
        if not config_file.exists(): return {}
        with open(config_file, "r") as f: return json.load(f)

    def execute(self, target_name=None, reset=False, remove=False):
        if not self.cmc.context: return
        self.lab_db_name = f"{self.cmc.context['user']}_cmc_lab"
        
        if remove: 
            self._drop_lab()
            return self.console.print("[success]Lab vaporized[/success]")
        
        if reset or not self._check_db_exists(self.lab_db_name):
            if not self._prepare_lab(): return
        else:
            self._truncate_lab_tables()

        config_all = self._load_analysis_config()
        lab_ctx = self.cmc.context.copy(); lab_ctx['dbname'] = self.lab_db_name
        conn = self.cmc.db.get_connection(lab_ctx)
        
        source_conn = self.cmc.db.get_connection(self.cmc.context)
        migration_ver = self._get_db_migration_version(source_conn) if source_conn else "unknown"
        if source_conn: source_conn.close()

        run_results = {}
        try:
            self.processed_tables.clear()
            targets = [target_name] if target_name else list(config_all.keys())
            
            for entity in targets:
                if not self.cmc.db.table_exists(entity, lab_ctx):
                    self.console.print(f"[warning]Skipping '{entity}': Not found in database schema.[/warning]")
                    continue

                self.visiting_stack = []
                self._populate_table(entity, config_all, conn)
                run_results[entity] = self._run_benchmarks(entity, config_all[entity], conn, migration_ver)
            
            self._save_report(run_results, migration_ver)
        finally: 
            if conn: conn.close()