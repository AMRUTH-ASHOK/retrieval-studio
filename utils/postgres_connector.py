"""
Lakehouse Postgres connector for transactional application state
"""
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor
import uuid
import time
import os
from typing import Optional, List, Dict, Any
from contextlib import contextmanager


class LakehousePostgresConnector:
    """
    Connection manager for Lakehouse Postgres with token rotation

    Uses OAuth tokens from Databricks SDK for authentication.
    Implements connection pooling for web application workloads.
    """

    def __init__(
        self,
        instance_name: str = None,
        database: str = "databricks_postgres",
        min_connections: int = 2,
        max_connections: int = 20
    ):
        """
        Initialize Postgres connector

        Args:
            instance_name: Lakebase Postgres instance name
            database: Database name (default: databricks_postgres)
            min_connections: Minimum pool connections
            max_connections: Maximum pool connections
        """
        self.instance_name = instance_name or os.environ.get(
            "LAKEBASE_INSTANCE_NAME",
            "retrieval-studio-db"
        )
        self.database = database
        self.min_connections = min_connections
        self.max_connections = max_connections

        # Initialize Databricks client
        cfg = Config()
        self.w = WorkspaceClient(config=cfg)

        # Get instance details
        try:
            print(f"[DEBUG] Looking for Lakebase instance: {self.instance_name}")
            self.instance = self.w.database.get_database_instance(name=self.instance_name)
            self.host = self.instance.read_write_dns
            self.port = 5432
            print(f"[DEBUG] Found instance - Host: {self.host}, State: {self.instance.state}")
        except Exception as e:
            print(f"[DEBUG] ❌ Failed to get instance '{self.instance_name}': {e}")
            raise RuntimeError(
                f"Failed to get Lakebase instance '{self.instance_name}': {e}. "
                "Make sure the instance exists and you have access."
            ) from e

        # Get username from current user (service principal for Databricks Apps)
        # The Postgres role is auto-created when database resource is declared in app.yaml
        try:
            current_user = self.w.current_user.me()
            self.username = current_user.user_name
            print(f"[DEBUG] Using username: {self.username}")
        except Exception as e:
            print(f"[DEBUG] Failed to get current user: {e}")
            # Fallback to generic username
            self.username = "databricks_user"

        # Token management
        self._token = None
        self._token_last_refresh = 0
        self._token_ttl = 900  # 15 minutes

        # Initialize connection pool
        self._pool = None
        self._initialize_pool()

    def _get_fresh_token(self) -> str:
        """Generate a fresh OAuth token for Postgres authentication"""
        cred = self.w.database.generate_database_credential(
            request_id=str(uuid.uuid4()),
            instance_names=[self.instance_name]
        )
        return cred.token

    def _ensure_valid_token(self):
        """Ensure we have a valid token, refresh if needed"""
        current_time = time.time()
        if (self._token is None or
            current_time - self._token_last_refresh > self._token_ttl):
            self._token = self._get_fresh_token()
            self._token_last_refresh = current_time

            # Recreate pool with new token
            if self._pool:
                self._pool.closeall()
            self._initialize_pool()

    def _initialize_pool(self):
        """Initialize connection pool with current token"""
        self._ensure_valid_token()

        try:
            print(f"[DEBUG] Connecting to Lakebase instance '{self.instance_name}'")
            print(f"[DEBUG] Host: {self.host}")
            print(f"[DEBUG] Port: {self.port}")
            print(f"[DEBUG] Database: {self.database}")
            print(f"[DEBUG] User: {self.username}")

            self._pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=self.min_connections,
                maxconn=self.max_connections,
                user=self.username,
                password=self._token,
                host=self.host,
                port=self.port,
                database=self.database,
                sslmode='require',
                connect_timeout=10
            )
            print(f"[DEBUG] ✅ Connection pool created successfully")
        except Exception as e:
            print(f"[DEBUG] ❌ Connection pool failed: {type(e).__name__}: {e}")
            raise RuntimeError(
                f"Failed to create connection pool to {self.host} (instance: {self.instance_name}): {e}"
            ) from e

    @contextmanager
    def get_connection(self):
        """
        Context manager for getting a connection from the pool

        Usage:
            with connector.get_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT ...")
        """
        self._ensure_valid_token()
        conn = None
        try:
            conn = self._pool.getconn()
            yield conn
            conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                self._pool.putconn(conn)

    def execute(
        self,
        query: str,
        params: tuple = None,
        fetch: str = "all"
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Execute a SELECT query and return results as list of dicts

        Args:
            query: SQL query string
            params: Query parameters tuple
            fetch: 'all', 'one', or 'none'

        Returns:
            List of dicts for fetch='all', single dict for fetch='one', None for fetch='none'
        """
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, params)

                if fetch == "all":
                    rows = cur.fetchall()
                    return [dict(row) for row in rows]
                elif fetch == "one":
                    row = cur.fetchone()
                    return dict(row) if row else None
                else:
                    return None

    def execute_many(
        self,
        query: str,
        params_list: List[tuple]
    ) -> int:
        """
        Execute query multiple times with different parameters

        Args:
            query: SQL query string (INSERT, UPDATE, DELETE)
            params_list: List of parameter tuples

        Returns:
            Total number of rows affected
        """
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(query, params_list)
                return cur.rowcount

    def execute_update(
        self,
        query: str,
        params: tuple = None
    ) -> int:
        """
        Execute an INSERT, UPDATE, or DELETE query

        Args:
            query: SQL query string
            params: Query parameters tuple

        Returns:
            Number of rows affected
        """
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, params)
                return cur.rowcount

    def test_connection(self) -> bool:
        """Test the database connection"""
        try:
            result = self.execute("SELECT version()", fetch="one")
            print(f"✅ Connected to Postgres: {result['version']}")
            return True
        except Exception as e:
            print(f"❌ Connection test failed: {e}")
            return False

    def close(self):
        """Close all connections in the pool"""
        if self._pool:
            self._pool.closeall()
            self._pool = None


# Singleton instance for the application
_connector_instance: Optional[LakehousePostgresConnector] = None


def get_postgres_connector() -> LakehousePostgresConnector:
    """
    Get or create the singleton Postgres connector

    This ensures we reuse the same connection pool across requests.
    """
    global _connector_instance

    if _connector_instance is None:
        _connector_instance = LakehousePostgresConnector()

    return _connector_instance


def close_postgres_connector():
    """Close the singleton connector (for cleanup)"""
    global _connector_instance

    if _connector_instance:
        _connector_instance.close()
        _connector_instance = None
