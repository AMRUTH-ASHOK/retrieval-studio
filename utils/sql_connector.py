"""
SQL Connector for Databricks Apps
Uses Databricks SQL connector with proper App and User authorization
Based on: https://docs.databricks.com/aws/en/dev-tools/databricks-apps/auth
"""
from databricks import sql
from databricks.sdk.core import Config
from typing import List, Dict, Any, Optional
import os


class DatabricksSQLConnector:
    """
    SQL connector for Databricks Apps (replaces SparkSession for App runtime)
    
    Supports both App Authorization (service principal) and User Authorization (user token)
    """
    
    def __init__(self, server_hostname: str = None, http_path: str = None, 
                 access_token: str = None, use_user_auth: bool = False,
                 catalog: str = None, schema: str = None):
        """
        Initialize SQL connector
        
        Args:
            server_hostname: Databricks workspace URL (optional, auto-detected from Config)
            http_path: SQL warehouse HTTP path (required)
            access_token: User access token for user authorization (optional)
            use_user_auth: If True, use user authorization; if False, use app authorization
            catalog: Default catalog name
            schema: Default schema name
        """
        self.catalog = catalog
        self.schema = schema
        self.use_user_auth = use_user_auth
        
        # Initialize Config for app authorization (automatically detects DATABRICKS_CLIENT_ID/SECRET)
        self.config = Config()
        
        # Get server_hostname from Config if not provided
        if not server_hostname:
            self.server_hostname = self.config.host.replace("https://", "").replace("http://", "")
        else:
            self.server_hostname = server_hostname.replace("https://", "").replace("http://", "")
        
        # Get http_path from environment or parameter
        self.http_path = http_path or os.environ.get("DATABRICKS_HTTP_PATH")
        
        if not self.http_path:
            raise ValueError(
                "DATABRICKS_HTTP_PATH is required. "
                "Please set it in environment variables. "
                "Format: /sql/1.0/warehouses/xxx or /sql/1.0/endpoints/xxx"
            )
        
        # Handle authentication
        if use_user_auth:
            # User authorization: use provided access token
            if not access_token:
                raise ValueError("access_token is required when use_user_auth=True")
            self.access_token = access_token
            self.credentials_provider = None
        else:
            # App authorization: use Config's authenticate method as credentials_provider
            # According to Databricks docs: credentials_provider=lambda: cfg.authenticate
            # Reference: https://docs.databricks.com/aws/en/dev-tools/databricks-apps/auth
            self.access_token = None
            self.credentials_provider = lambda: self.config.authenticate
        
        self.connection = None
    
    def _get_connection(self):
        """Get or create SQL connection"""
        if self.connection is None:
            try:
                if self.use_user_auth or self.access_token:
                    # User authorization or app authorization with direct token
                    self.connection = sql.connect(
                        server_hostname=self.server_hostname,
                        http_path=self.http_path,
                        access_token=self.access_token
                    )
                else:
                    # App authorization: OAuth with service principal using credentials_provider
                    # According to Databricks docs: credentials_provider=lambda: cfg.authenticate
                    # Reference: https://docs.databricks.com/aws/en/dev-tools/databricks-apps/auth
                    self.connection = sql.connect(
                        server_hostname=self.server_hostname,
                        http_path=self.http_path,
                        credentials_provider=lambda: self.config.authenticate
                    )
            except Exception as e:
                # Provide more detailed error information
                error_details = [
                    f"Failed to connect to SQL warehouse/endpoint: {e}",
                    f"Server: {self.server_hostname}",
                    f"HTTP Path: {self.http_path}",
                    f"Auth Mode: {'User' if self.use_user_auth else 'App (Service Principal)'}",
                    "",
                    "Please verify:",
                    "1. SQL warehouse/endpoint is running and accessible",
                    "2. HTTP path is correct (check in Databricks SQL → Warehouses → Connection details)",
                    "3. Authentication credentials are valid (DATABRICKS_CLIENT_ID and DATABRICKS_CLIENT_SECRET)",
                    "4. Service principal has access to the SQL warehouse/endpoint",
                    "5. App Resources are configured in Databricks Apps settings"
                ]
                raise ConnectionError("\n".join(error_details)) from e
        return self.connection
    
    def execute(self, query: str, parameters: List[Any] = None) -> List[Dict]:
        """
        Execute SQL query and return results as list of dictionaries
        
        Args:
            query: SQL query string (uses ? placeholders for parameters)
            parameters: Optional query parameters list (positional)
        
        Returns:
            List of dictionaries (rows)
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            if parameters:
                # Databricks SQL connector uses ? placeholders with positional parameters
                cursor.execute(query, parameters)
            else:
                cursor.execute(query)
            
            # Get column names
            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            
            # Fetch all results
            rows = cursor.fetchall()
            
            # Convert to list of dictionaries
            results = []
            for row in rows:
                results.append(dict(zip(columns, row)))
            
            return results
        finally:
            cursor.close()
    
    def execute_parameterized(self, query: str, **kwargs) -> List[Dict]:
        """
        Execute parameterized query with keyword arguments
        
        Convenience method for parameterized queries
        
        Args:
            query: SQL query string with :param_name placeholders
            **kwargs: Parameter values
        
        Returns:
            List of dictionaries (rows)
        """
        return self.execute(query, kwargs)
    
    def execute_update(self, query: str, parameters: List[Any] = None) -> int:
        """
        Execute UPDATE/INSERT/DELETE query
        
        Args:
            query: SQL query string (uses ? placeholders for parameters)
            parameters: Optional query parameters list (positional)
        
        Returns:
            Number of affected rows
        """
        conn = self._get_connection()
        cursor = conn.cursor()
        
        try:
            if parameters:
                # Databricks SQL connector uses ? placeholders with positional parameters
                cursor.execute(query, parameters)
            else:
                cursor.execute(query)
            
            conn.commit()
            return cursor.rowcount
        finally:
            cursor.close()
    
    def sql(self, query: str) -> List[Dict]:
        """
        Execute SQL query (SparkSession.sql() replacement)
        
        Args:
            query: SQL query string
        
        Returns:
            List of dictionaries (rows)
        """
        return self.execute(query)
    
    def create_dataframe(self, data: List[Dict], schema: List[str] = None) -> 'DataFrame':
        """
        Create DataFrame-like object from data
        
        Note: This is a simplified implementation for compatibility
        """
        return DataFrame(data, schema)
    
    def close(self):
        """Close connection"""
        if self.connection:
            self.connection.close()
            self.connection = None
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class DataFrame:
    """Simplified DataFrame-like class for compatibility"""
    
    def __init__(self, data: List[Dict], schema: List[str] = None):
        self.data = data
        self.schema = schema or (list(data[0].keys()) if data else [])
    
    def toPandas(self):
        """Convert to pandas DataFrame"""
        import pandas as pd
        return pd.DataFrame(self.data)
    
    def collect(self):
        """Collect rows (returns data as-is)"""
        return self.data
    
    def write(self):
        """Return writer for compatibility"""
        return DataFrameWriter(self.data, self.schema)


class DataFrameWriter:
    """Simplified DataFrame writer for compatibility"""
    
    def __init__(self, data: List[Dict], schema: List[str]):
        self.data = data
        self.schema = schema
    
    def format(self, format_type: str):
        """Set format"""
        return self
    
    def mode(self, mode: str):
        """Set write mode"""
        return self
    
    def saveAsTable(self, table_name: str):
        """Save to table (requires SQL connector)"""
        raise NotImplementedError(
            "saveAsTable not implemented. Use SQL connector execute_update() with INSERT statements instead."
        )


def get_sql_connector(catalog: str = None, schema: str = None, 
                     use_user_auth: bool = False, user_token: str = None) -> DatabricksSQLConnector:
    """
    Get SQL connector instance (replaces SparkSession.builder.getOrCreate())
    
    Args:
        catalog: Default catalog name
        schema: Default schema name
        use_user_auth: If True, use user authorization; if False, use app authorization
        user_token: User access token (required if use_user_auth=True)
    
    Returns:
        DatabricksSQLConnector instance
    """
    return DatabricksSQLConnector(
        catalog=catalog, 
        schema=schema,
        use_user_auth=use_user_auth,
        access_token=user_token
    )
