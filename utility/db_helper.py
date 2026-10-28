import os
import time
import psycopg2
from psycopg2.extras import DictCursor, RealDictCursor


class DBConnection:
    """Wrapper around psycopg2 connection providing SQLite-like convenience methods."""

    def __init__(self, raw_conn):
        self._conn = raw_conn

    def cursor(self):
        return self._conn.cursor()

    def execute(self, query, params=None):
        cur = self._conn.cursor()
        # Convert SQLite-style '?' placeholders to PostgreSQL '%s'
        formatted_query = query.replace('?', '%s')
        cur.execute(formatted_query, params)
        return cur

    def executemany(self, query, params_list):
        cur = self._conn.cursor()
        formatted_query = query.replace('?', '%s')
        cur.executemany(formatted_query, params_list)
        return cur

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        self._conn.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self._conn.rollback()
        else:
            self._conn.commit()
        self._conn.close()


def get_db_connection(schema=None, retries=10, delay=3):
    """
    Returns a database connection configured for PostgreSQL.
    Configured via DATABASE_URL or individual DB_* environment variables.
    Automatically sets search_path to the specified schema and retries on startup.
    """
    db_url = os.getenv('DATABASE_URL')
    host = os.getenv('DB_HOST', 'postgres')
    port = int(os.getenv('DB_PORT', 5432))
    user = os.getenv('DB_USER', 'swiftuser')
    password = os.getenv('DB_PASSWORD', 'swiftpassword')
    dbname = os.getenv('DB_NAME', 'swifttrack')
    schema = schema or os.getenv('DB_SCHEMA', 'public')

    conn_kwargs = {
        'cursor_factory': DictCursor
    }

    options = f"-c search_path={schema},public"

    for attempt in range(1, retries + 1):
        try:
            if db_url:
                conn = psycopg2.connect(db_url, options=options, **conn_kwargs)
            else:
                conn = psycopg2.connect(
                    host=host,
                    port=port,
                    user=user,
                    password=password,
                    dbname=dbname,
                    options=options,
                    **conn_kwargs
                )
            # Ensure schema exists
            with conn.cursor() as cur:
                cur.execute(f"CREATE SCHEMA IF NOT EXISTS {schema};")
            conn.commit()
            return DBConnection(conn)
        except Exception as e:
            print(f"[*] Waiting for PostgreSQL (Attempt {attempt}/{retries}): {e}", flush=True)
            if attempt == retries:
                raise
            time.sleep(delay)
