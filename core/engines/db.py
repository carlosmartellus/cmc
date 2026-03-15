from typing import Optional, List, Dict
import psycopg2
from psycopg2.extensions import connection as PgConnection
from contextlib import contextmanager


class DBEngine:
    def __init__(self, core):
        self.cmc = core

    def get_connection(self, cfg: Optional[Dict] = None) -> Optional[PgConnection]:
        target = cfg or self.cmc.context
        if not target: return None
        try:
            return psycopg2.connect(
                user=target['user'], password=target['password'],
                host=target['host'], port=target['port'], dbname=target['dbname']
            )
        except Exception as e:
            self.cmc.log.error(f"PostgreSQL Connection Failed: {e}")
            return None
        
    def table_exists(self, table_name: str, cfg: Optional[Dict] = None) -> bool:
        conn = self.get_connection(cfg)
        if not conn: return False
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'public' AND table_name = %s);", (table_name,))
                return cur.fetchone()[0]
        except Exception as e:
            self.cmc.log.error(f"Failed to inspect table '{table_name}': {e}")
            return False
        finally: conn.close()

    def get_table_columns(self, table_name: str) -> List[str]:
        conn = self.get_connection()
        if not conn: return []
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = %s AND table_schema = 'public' ORDER BY ordinal_position;", (table_name,))
                return [row[0] for row in cur.fetchall() if row[0] != 'id']
        finally: conn.close()

    def forge_infrastructure(self, admin_u, admin_p, target_db, project_u, project_p):
        try:
            admin_conn = psycopg2.connect(user=admin_u, password=admin_p, host="localhost", port=5432, dbname="postgres")
            admin_conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            with admin_conn.cursor() as cur:
                cur.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (project_u,))
                if not cur.fetchone():
                    self.cmc.log.info(f"Forging new role: {project_u}")
                    cur.execute(f"CREATE ROLE {project_u} WITH LOGIN PASSWORD %s", (project_p,))
                    cur.execute(f"ALTER ROLE {project_u} CREATEDB CREATEROLE")
                cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (target_db,))
                if not cur.fetchone():
                    self.cmc.log.info(f"Forging database: {target_db}")
                    cur.execute(f'CREATE DATABASE "{target_db}" OWNER {project_u}')
            admin_conn.close()
        except Exception as e: self.cmc.log.error(f"Infrastructure forge failed: {e}")

    def drop_specific_assets(self, admin_u, admin_p, dbs, drop_role, project_u):
        try:
            admin_conn = psycopg2.connect(user=admin_u, password=admin_p, host="localhost", port=5432, dbname="postgres")
            admin_conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
            with admin_conn.cursor() as cur:
                for db in dbs:
                    cur.execute(f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = '{db}' AND pid <> pg_backend_pid();")
                    self.cmc.log.info(f"Dropping: {db}...")
                    cur.execute(f'DROP DATABASE IF EXISTS "{db}"')
                if drop_role:
                    self.cmc.log.info(f"Removing role: {project_u}")
                    cur.execute(f'DROP ROLE IF EXISTS "{project_u}"')
            admin_conn.close()
        except Exception as e: self.cmc.log.error(f"Teardown failed: {e}")

    @contextmanager
    def cursor(self):
        conn = self.get_connection()
        if not conn: 
            raise Exception("Database connection failed")
        cur = conn.cursor()
        try:
            yield cur
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            cur.close()
            conn.close()

    def get_foreign_keys(self, table_name: str) -> List[str]:
        conn = self.get_connection()
        if not conn: return []
        try:
            with conn.cursor() as cur:
                query = """
                SELECT kcu.column_name
                FROM information_schema.table_constraints AS tc
                JOIN information_schema.key_column_usage AS kcu
                  ON tc.constraint_name = kcu.constraint_name
                WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_name = %s;
                """
                cur.execute(query, (table_name,))
                return [row[0] for row in cur.fetchall()]
        finally: 
            conn.close()