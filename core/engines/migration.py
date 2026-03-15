import os
import re
from datetime import datetime
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