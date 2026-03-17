import os
import re
from datetime import datetime
import subprocess
from pathlib import Path

class MigrationEngine:
    def __init__(self, core):
        self.cmc = core

    def _generate_down_logic(self, sql_content: str) -> str:
        pattern = r"CREATE\s+(TABLE|INDEX|VIEW)\s+(?:IF\s+NOT\s+EXISTS\s+)?([a-zA-Z0-9_\" \.]+)"
        matches = re.findall(pattern, sql_content, re.IGNORECASE)
        
        drops = []
        for type_, name in matches:
            clean_name = name.split('(')[0].strip()
            
            if type_.upper() == "INDEX":
                clean_name = clean_name.split()[0]
                
            drops.append(f"DROP {type_} IF EXISTS {clean_name} CASCADE;")
        
        return "\n".join(drops[::-1])

    def create(self, name: str):
        m_dir = self.cmc.get_migrations_path()
        if not m_dir: return
        ts = datetime.now().strftime("%Y%m%d%H%M%S")
        safe_name = name.lower().replace(' ', '_')
        
        filename = m_dir / f"{ts}_{safe_name}.sql"
        with open(filename, "w") as f:
            f.write(f"-- Migration: {name}\n-- CMC auto-generates rollback logic\n\nBEGIN;\n\nCOMMIT;")
            
        self.cmc.log.log(f"Migration created: {ts}_{safe_name}")

    def migrate(self, target_env: str):
        m_dir = self.cmc.get_migrations_path()
        cfg = self.cmc.load_env_config(target_env)
        conn = self.cmc.db.get_connection(cfg)
        if not conn: return
        
        self.cmc.log.info(f"Synchronizing CMC with: [{target_env.upper()}]")
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS cmc_migrations (
                        id SERIAL PRIMARY KEY, 
                        name TEXT UNIQUE, 
                        down_sql TEXT, 
                        applied_at TIMESTAMP DEFAULT NOW()
                    );
                """)
                cur.execute("SELECT name FROM cmc_migrations")
                applied = {r[0] for r in cur.fetchall()}

            files = sorted([f for f in os.listdir(m_dir) if f.endswith(".sql")])
            pending = [f for f in files if f not in applied]
            
            if not pending:
                self.cmc.log.info("Database is already up to date.")
                return

            for f in pending:
                with open(m_dir / f, 'r') as sql_file:
                    content = sql_file.read()
                    
                    destructive_pattern = r"\b(DROP\s+TABLE|DROP\s+COLUMN|ALTER\s+COLUMN\s+.*?TYPE)\b"
                    if re.search(destructive_pattern, content, re.IGNORECASE):
                        self.cmc.log.warn(f"Destructive action detected in migration: {f}")
                        self.cmc.log.info("A rollback will restore structure, but NOT deleted data.")
                        
                        ans = input("Do you want to secure a database backup before applying? (y/n): ").strip().lower()
                        if ans == 'y':
                            db_dir = m_dir.parent / "db"
                            db_dir.mkdir(parents=True, exist_ok=True)
                            
                            ts = datetime.now().strftime("%Y%m%d%H%M%S")
                            bkp_file = db_dir / f"{ts}-backup.sql"
                            
                            self.cmc.log.info(f"Dumping database to db/{bkp_file.name}...")
                            
                            env_vars = os.environ.copy()
                            env_vars["PGPASSWORD"] = str(cfg.get("password", ""))
                            
                            dump_cmd = [
                                "pg_dump",
                                "-h", str(cfg.get("host", "localhost")),
                                "-p", str(cfg.get("port", 5432)),
                                "-U", str(cfg.get("user", "")),
                                "-d", str(cfg.get("dbname", "")),
                                "-F", "p",
                                "-f", str(bkp_file)
                            ]
                            
                            try:
                                subprocess.run(dump_cmd, env=env_vars, check=True, capture_output=True)
                                self.cmc.log.log(f"Backup secured successfully.")
                            except subprocess.CalledProcessError as e:
                                self.cmc.log.error(f"Backup failed: {e.stderr.decode('utf-8')}")
                                self.cmc.log.error("Migration aborted to protect data integrity.")
                                return

                    down_logic = self._generate_down_logic(content)
                    
                    with conn.cursor() as cur:
                        cur.execute(content)
                        cur.execute(
                            "INSERT INTO cmc_migrations (name, down_sql) VALUES (%s, %s)", 
                            (f, down_logic)
                        )
                conn.commit()
                self.cmc.log.log(f"Applied: {f}")
        except Exception as e:
            conn.rollback()
            self.cmc.log.error(f"Migration aborted: {e}")
        finally: conn.close()

    def rollback(self, target_env: str, steps: int):
        cfg = self.cmc.load_env_config(target_env)
        conn = self.cmc.db.get_connection(cfg)
        if not conn: return
        
        self.cmc.log.info(f"Rolling back [{target_env.upper()}] - Steps: {steps}")
        
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT name, down_sql FROM cmc_migrations ORDER BY id DESC LIMIT %s", (steps,))
                to_rollback = cur.fetchall()

            if not to_rollback:
                self.cmc.log.info("Nothing to rollback.")
                return

            for name, down_sql in to_rollback:
                if down_sql:
                    with conn.cursor() as cur:
                        cur.execute(down_sql)
                        cur.execute("DELETE FROM cmc_migrations WHERE name = %s", (name,))
                    conn.commit()
                    self.cmc.log.log(f"Rolled back: {name}")
                else:
                    self.cmc.log.warning(f"No rollback logic stored for: {name}")

        except Exception as e: 
            if conn: conn.rollback()
            self.cmc.log.error(f"Rollback failed: {e}")
        finally: 
            if conn: conn.close()

    def rescue(self, name: str):
        m_dir = self.cmc.get_migrations_path()
        if not m_dir: return
        
        ts = datetime.now().strftime("%Y%m%d%H%M%S")
        safe_name = name.lower().replace(' ', '_')
        
        if not safe_name.startswith("rescue_"):
            safe_name = f"rescue_{safe_name}"
            
        filename = m_dir / f"{ts}_{safe_name}.sql"
        
        template_content = self.cmc._get_template("rescue.sql")
        
        if not template_content:
            self.cmc.log.error("Missing template: 'rescue.sql' not found in templates directory.")
            return

        final_content = template_content.replace("{{name}}", name)
        
        with open(filename, "w") as f:
            f.write(final_content)
            
        self.cmc.log.log(f"Rescue template forged: {filename.name}")

    def restore(self, target_env: str, backup_name: str = None):
        m_dir = self.cmc.get_migrations_path()
        if not m_dir:
            self.cmc.log.error("Could not locate server directory.")
            return
            
        db_dir = m_dir.parent / "db"
        
        db_dir.mkdir(parents=True, exist_ok=True)

        backups = sorted([f for f in os.listdir(db_dir) if f.endswith(".sql")], reverse=True)
        if not backups:
            self.cmc.log.error(f"No SQL backups found in the '{db_dir.parent.name}/db/' directory.")
            return

        target_file = None
        
        if backup_name:
            if backup_name in backups:
                target_file = backup_name
            else:
                self.cmc.log.error(f"Backup '{backup_name}' not found in {db_dir.parent.name}/db/ directory.")
                return
        else:
            self.cmc.log.info(f"Available backups for [{target_env.upper()}]:")
            for i, b in enumerate(backups):
                print(f"  [{i+1}] {b}")
            
            try:
                choice = int(input("\nSelect a backup number to restore (or 0 to cancel): ").strip())
                if choice == 0:
                    self.cmc.log.info("Restore cancelled.")
                    return
                if 1 <= choice <= len(backups):
                    target_file = backups[choice-1]
                else:
                    self.cmc.log.error("Invalid choice.")
                    return
            except ValueError:
                self.cmc.log.error("Invalid input.")
                return

        backup_path = db_dir / target_file

        self.cmc.log.warn(f"You are about to restore '{target_file}' into [{target_env.upper()}].")
        self.cmc.log.warn("This will DESTROY all current data and replace it with the backup.")
        
        confirm = input(f"Type '{target_env.upper()}' to confirm full database overwrite: ").strip()
        if confirm != target_env.upper():
            self.cmc.log.info("Restore aborted by user.")
            return

        cfg = self.cmc.load_env_config(target_env)
        
        self.cmc.log.info("Wiping current schema...")
        conn = self.cmc.db.get_connection(cfg)
        if not conn: return
        try:
            with conn.cursor() as cur:
                cur.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public; GRANT ALL ON SCHEMA public TO public;")
            conn.commit()
        except Exception as e:
            conn.rollback()
            self.cmc.log.error(f"Failed to wipe schema: {e}")
            return
        finally:
            conn.close()

        self.cmc.log.info(f"Restoring from {target_file}...")
        env_vars = os.environ.copy()
        env_vars["PGPASSWORD"] = str(cfg.get("password", ""))
        
        restore_cmd = [
            "psql",
            "-h", str(cfg.get("host", "localhost")),
            "-p", str(cfg.get("port", 5432)),
            "-U", str(cfg.get("user", "")),
            "-d", str(cfg.get("dbname", "")),
            "-f", str(backup_path),
            "-q"
        ]
        
        try:
            subprocess.run(restore_cmd, env=env_vars, check=True, capture_output=True)
            self.cmc.log.success(f"Database successfully restored to the state of {target_file}")
            self.cmc.log.info("Migration history (cmc_migrations) has been automatically synced.")
        except subprocess.CalledProcessError as e:
            self.cmc.log.error(f"Restore failed: {e.stderr.decode('utf-8')}")
            self.cmc.log.error("WARNING: Your database might be empty. Manual intervention required.")

    def status(self, target_env: str):
        from rich.console import Console
        from rich.table import Table

        m_dir = self.cmc.get_migrations_path()
        cfg = self.cmc.load_env_config(target_env)
        conn = self.cmc.db.get_connection(cfg)
        
        console = Console()
        
        if not conn:
            console.print(f"[bold red]Error:[/bold red] Could not connect to [{target_env.upper()}].")
            return

        try:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS cmc_migrations (
                        id SERIAL PRIMARY KEY, 
                        name TEXT UNIQUE, 
                        down_sql TEXT, 
                        applied_at TIMESTAMP DEFAULT NOW()
                    );
                """)
                cur.execute("SELECT name, applied_at FROM cmc_migrations ORDER BY id ASC")
                applied_data = {row[0]: row[1] for row in cur.fetchall()}

            local_files = []
            if m_dir and m_dir.exists():
                local_files = sorted([f for f in os.listdir(m_dir) if f.endswith(".sql")])

            table = Table(
                title=f"CMC DB STATUS | Target: [bold cyan]{target_env.upper()}[/bold cyan]", 
                show_header=True, 
                header_style="bold magenta"
            )
            table.add_column("STATUS", justify="center", style="bold")
            table.add_column("MIGRATION FILE", style="white")
            table.add_column("APPLIED AT", justify="right", style="dim")

            all_files = sorted(list(set(local_files).union(set(applied_data.keys()))))
            
            for f in all_files:
                if f in applied_data and f in local_files:
                    date_str = applied_data[f].strftime("%Y-%m-%d %H:%M:%S")
                    table.add_row("[green]APPLIED[/green]", f, date_str)
                elif f in local_files and f not in applied_data:
                    table.add_row("[yellow]PENDING[/yellow]", f, "-")
                elif f in applied_data and f not in local_files:
                    date_str = applied_data[f].strftime("%Y-%m-%d %H:%M:%S")
                    table.add_row("[red]MISSING LOCAL[/red]", f, date_str)

            console.print(table)
            
            pending_count = len([f for f in local_files if f not in applied_data])
            console.print(f"\nSummary: [green]{len(applied_data)} Applied[/green] | [yellow]{pending_count} Pending[/yellow]\n")

        except Exception as e:
            console.print(f"[bold red]Status check failed:[/bold red] {e}")
        finally:
            conn.close()