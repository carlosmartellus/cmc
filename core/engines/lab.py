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
from datetime import datetime, timedelta, date
from itertools import permutations

from rich.console import Console
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.panel import Panel
from rich.theme import Theme


class CMCLab:
    def __init__(self, core):
        self.cmc = core
        self.lab_db_name = None
        self.processed_tables = set()
        self.visiting_stack = []

        if self.cmc.context:
            self.lab_db_name = f"{self.cmc.context['user']}_cmc_lab"
        else:
            self.lab_db_name = None
        
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
        if not base_val: 
            base_val = self._get_random_value(pg_type)
        
        if isinstance(base_val, (datetime, date)):
            return base_val, base_val + timedelta(days=random.randint(7, 30))
        
        if isinstance(base_val, (int, float)):
            return base_val, base_val + 500
            
        return base_val, base_val

    def _run_benchmarks(self, entity_name, conf, conn, migration, workload_plan=None, target_cols_arg=None) -> dict:
        defaults = conf['defaults']
        iterations = defaults.get('iterations', 3)
        
        n_filters = max(defaults.get('filter_columns', 3), len(target_cols_arg or []))
        
        cols_with_types = self.cmc.db.get_table_columns(entity_name)
        range_cols = conf.get('ranges', [])
        excludes = conf.get('excludes', [])
        includes = conf.get('includes', [])

        if workload_plan is None:
            workload_plan = []
            
            mandatory = [c for c in (target_cols_arg or []) if c in cols_with_types]
            
            preferred = [c for c in includes if c in cols_with_types and c not in mandatory]
            
            others = [c for c in cols_with_types.keys() 
                     if c not in mandatory and c not in preferred and c not in excludes and c != 'id']

            for _ in range(iterations):
                current_selection = list(mandatory)
                
                needed = n_filters - len(current_selection)
                if needed > 0 and preferred:
                    sampled_p2 = random.sample(preferred, min(needed, len(preferred)))
                    current_selection.extend(sampled_p2)
                
                needed = n_filters - len(current_selection)
                if needed > 0 and others:
                    sampled_p3 = random.sample(others, min(needed, len(others)))
                    current_selection.extend(sampled_p3)

                params, clauses = [], []
                for col in current_selection:
                    if col in range_cols:
                        v1, v2 = self._generate_range_values(entity_name, col, cols_with_types[col], conn)
                        clauses.append(f'"{col}" BETWEEN %s AND %s')
                        params.extend([v1, v2])
                    else:
                        val = self._get_existing_value_from_db(entity_name, col, conn)
                        clauses.append(f'"{col}" = %s')
                        params.append(val)
                
                workload_plan.append({
                    "cols": current_selection,
                    "where": f" WHERE {' AND '.join(clauses)}" if clauses else "",
                    "params": params
                })

        baseline_ram = psutil.virtual_memory().used
        psutil.cpu_percent(interval=None)
        samples = []

        self.console.print(f"\n[info]Analyzing workload: {entity_name} ({iterations} iterations)[/info]")
        
        for i in range(iterations):
            step = workload_plan[i]
            
            with conn.cursor() as cur:
                query = f"EXPLAIN (ANALYZE, FORMAT JSON) SELECT * FROM \"{entity_name}\"{step['where']}"
                cur.execute(query, step['params'])
                
                cpu = psutil.cpu_percent(interval=None)
                ram = max(0, psutil.virtual_memory().used - baseline_ram)
                
                raw_plan = cur.fetchone()[0]
                plan = raw_plan[0] if isinstance(raw_plan, list) else json.loads(raw_plan)[0]
                
                samples.append({
                    "n": i + 1, 
                    "time": plan.get('Execution Time', 0),
                    "rows": plan.get('Plan', {}).get('Actual Rows', 0),
                    "cpu": cpu, 
                    "ram": ram,
                    "target_cols": step['cols'] 
                })

        raw_table = Table(title=f"Raw Samples: {entity_name}", title_style="accent")
        raw_table.add_column("#", justify="center")
        raw_table.add_column("Index on", style="magenta")
        raw_table.add_column("Filter Scenario (Columns)", style="cyan")
        raw_table.add_column("Rows", justify="right", style="success")
        raw_table.add_column("Time (ms)", justify="right")
        raw_table.add_column("Eff (ms/r)", justify="right")
        raw_table.add_column("CPU %", justify="right")

        for s in samples:
            eff = s["time"] / s["rows"] if s["rows"] > 0 else s["time"]
            cols_str = ", ".join(s["target_cols"]) if s["target_cols"] else "FULL SCAN"
            
            raw_table.add_row(
                str(s["n"]), 
                str(migration),
                cols_str,
                str(s["rows"]), 
                f"{s['time']:.3f}", 
                f"{eff:.4f}" if s["rows"] > 0 else "N/A", 
                f"{s['cpu']:.1f}%"
            )
        self.console.print(raw_table)
        
        return {
            "execution_time_ms": self._get_stats_dict([s['time'] for s in samples]),
            "cpu_load_percent": self._get_stats_dict([s['cpu'] for s in samples]),
            "ram_impact_bytes": self._get_stats_dict([s['ram'] for s in samples]),
            "workload_plan": workload_plan
        }

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

    def execute(self, target_name=None, reset=False, remove=False, test=False):
        if not self.cmc.context: return
        self.lab_db_name = f"{self.cmc.context['user']}_cmc_lab"
        
        if remove: 
            self._drop_lab()
            return self.console.print("[success]Lab vaporized.[/success]")
        
        is_new_lab = False
        if reset or not self._check_db_exists(self.lab_db_name):
            self.console.print("[info]Forging fresh lab environment...[/info]")
            if not self._prepare_lab(): return
            is_new_lab = True

        config_all = self._load_analysis_config()
        lab_ctx = self.cmc.context.copy(); lab_ctx['dbname'] = self.lab_db_name
        conn = self.cmc.db.get_connection(lab_ctx)

        try:
            targets = [target_name] if target_name else list(config_all.keys())

            if not test:
                if not is_new_lab and not reset:
                    self.console.print("[info]Cleaning existing data for a fresh start...[/info]")
                    self._truncate_lab_tables()
                
                self.processed_tables.clear()
                for entity in targets:
                    self.visiting_stack = []
                    self._populate_table(entity, config_all, conn)
                self.console.print("[success]Lab fresh and repopulated.[/success]")

            if test:
                run_results = {}
                source_conn = self.cmc.db.get_connection(self.cmc.context)
                migration_ver = self._get_db_migration_version(source_conn) if source_conn else "unknown"
                if source_conn: source_conn.close()

                for entity in targets:
                    if not self.cmc.db.table_exists(entity, lab_ctx):
                        self.console.print(f"[warning]Skipping '{entity}': Not found.[/warning]")
                        continue
                    run_results[entity] = self._run_benchmarks(entity, config_all[entity], conn, migration_ver)
                
                self._save_report(run_results, migration_ver)
                
        finally: 
            if conn: conn.close()

    def _measure_write_ops(self, entity_name, conn, percentage=0.15):
        with conn.cursor() as cur:
            cur.execute(f"SELECT count(*) FROM \"{entity_name}\"")
            total_rows = cur.fetchone()[0]
        
        target_rows = max(10, int(total_rows * percentage))
        cols_with_types = self.cmc.db.get_table_columns(entity_name)
        
        cols = [f'"{c}"' for c in cols_with_types.keys() if c != 'id']
        num_cols = len(cols)
        
        self.console.print(f"[dim]  ↳ Stressing with {target_rows} bulk rows (15%)...[/dim]")

        payloads = []
        for _ in range(target_rows):
            row = [self._get_random_value(dtype) for col, dtype in cols_with_types.items() if col != 'id']
            payloads.append(tuple(row))

        avg_insert_time = 0.0
        with conn.cursor() as cur:
            single_row_placeholders = "(" + ",".join(["%s"] * num_cols) + ")"
            all_placeholders = ",".join([single_row_placeholders] * target_rows)
            
            query = f"EXPLAIN (ANALYZE, FORMAT JSON) INSERT INTO \"{entity_name}\" ({', '.join(cols)}) VALUES {all_placeholders}"
            
            flat_params = [item for sublist in payloads for item in sublist]
            
            try:
                cur.execute(query, flat_params)
                plan = cur.fetchone()[0][0]
                avg_insert_time = plan.get('Execution Time', 0) / target_rows
            except Exception as e:
                self.console.print(f"[warning]Insert Stress failed: {e}[/warning]")
                avg_insert_time = 0.0

        avg_delete_time = 0.0
        with conn.cursor() as cur:
            cur.execute(f"SELECT id FROM \"{entity_name}\" LIMIT %s", (target_rows,))
            ids_to_delete = [r[0] for r in cur.fetchall()]

        if ids_to_delete:
            with conn.cursor() as cur:
                query = f"EXPLAIN (ANALYZE, FORMAT JSON) DELETE FROM \"{entity_name}\" WHERE id IN ({','.join(['%s']*len(ids_to_delete))})"
                cur.execute(query, ids_to_delete)
                plan = cur.fetchone()[0][0]
                avg_delete_time = plan.get('Execution Time', 0) / target_rows
        
        conn.rollback()

        return avg_insert_time, avg_delete_time

    def run_index_experiment(self, entity_name: str, columns: List[str] = None):
        if not self._check_db_exists(self.lab_db_name):
            self.console.print("[error]Lab not found. Run 'cmc lab' first.[/error]")
            return

        lab_ctx = self.cmc.context.copy(); lab_ctx['dbname'] = self.lab_db_name
        conn = self.cmc.db.get_connection(lab_ctx)
        config_all = self._load_analysis_config()
        conf = config_all.get(entity_name)
        
        if columns:
            experiments = [list(p) for p in permutations(columns)]
            self.console.print(f"[info]Generated {len(experiments)} permutations for a total of {len(columns)} columns.[/info]")
        else:
            experiments = [[col] for col in self.cmc.db.get_table_columns(entity_name).keys() if col != 'id']

        self.console.print(Panel(f"[accent]Comparative Index Audit: {entity_name}[/accent]\n[dim]Fixed workload consistency + Storage analysis[/dim]", expand=False))

        self.console.print("[info]Defining Master Workload Plan and Capturing Baseline...[/info]")
        res_baseline = self._run_benchmarks(entity_name, conf, conn, "baseline", target_cols_arg=columns)
        master_plan = res_baseline['workload_plan']
        
        b_read_avg = res_baseline['execution_time_ms']['mean']
        b_ins, b_del = self._measure_write_ops(entity_name, conn)

        table = Table(title=f"Lab Results: {entity_name}", title_style="accent", show_lines=True)
        table.add_column("Index Configuration", style="info", width=30)
        table.add_column("Operation", justify="center")
        table.add_column("Baseline (avg)", justify="right")
        table.add_column("With Index (avg)", justify="right")
        table.add_column("Delta % (Time ± σ)", justify="right")
        table.add_column("Storage", justify="right", style="cyan")

        for cols in experiments:
            idx_name = f"cmc_lab_idx_{'_'.join(cols)}"
            cols_str = ", ".join([f'"{c}"' for c in cols])
            
            try:
                with conn.cursor() as cur:
                    cur.execute(f'CREATE INDEX "{idx_name}" ON "{entity_name}" ({cols_str})')
                    cur.execute("SELECT pg_relation_size(%s)", (idx_name,))
                    idx_size_pretty = self._format_bytes(cur.fetchone()[0])
                    conn.commit()

                res_test = self._run_benchmarks(entity_name, conf, conn, idx_name, workload_plan=master_plan)
                t_stats = res_test['execution_time_ms']
                t_ins, t_del = self._measure_write_ops(entity_name, conn)

                def get_delta(old, new): return ((new - old) / old) * 100
                def color_delta(val): return "red" if val > 0 else "green"

                read_d = get_delta(b_read_avg, t_stats['mean'])
                ins_d = get_delta(b_ins, t_ins)
                del_d = get_delta(b_del, t_del)

                table.add_row(
                    f"IDX on ({cols_str})", 
                    "READ", 
                    f"{b_read_avg:.2f} ms", 
                    f"{t_stats['mean']:.2f} ms", 
                    f"[{color_delta(read_d)}]{read_d:+.2f}%[/] [dim]±{t_stats['stdev']:.1f}[/]",
                    idx_size_pretty
                )
                
                table.add_row(
                    "", "INSERT (Tax)", 
                    f"{b_ins:.3f} ms", f"{t_ins:.3f} ms", 
                    f"[{color_delta(ins_d)}]{ins_d:+.2f}%[/]",
                    ""
                )

                table.add_row(
                    "", "DELETE (Tax)", 
                    f"{b_del:.3f} ms", f"{t_del:.3f} ms", 
                    f"[{color_delta(del_d)}]{del_d:+.2f}%[/]",
                    ""
                )
                
                table.add_section()

            finally:
                with conn.cursor() as cur:
                    cur.execute(f'DROP INDEX IF EXISTS "{idx_name}"')
                    conn.commit()

        self.console.print(table)
        conn.close()