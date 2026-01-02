"""
State management using Databricks SQL connector (replaces SparkSession for App runtime)
"""
from utils.sql_connector import DatabricksSQLConnector
from utils.query_builder import escape_identifier, sanitize_string
import json
import uuid
from typing import Optional


def initialize_tables(sql_connector: DatabricksSQLConnector, catalog: str, schema: str):
    """Create all Delta tables if they don't exist"""
    
    try:
        # Create schema - identifiers are safe after validation
        catalog_escaped = escape_identifier(catalog)
        schema_escaped = escape_identifier(schema)
        sql_connector.execute_update(f"""
            CREATE SCHEMA IF NOT EXISTS {catalog_escaped}.{schema_escaped}
        """)
    except Exception as e:
        # If schema creation fails, it might already exist or there's a permission issue
        # Continue anyway - tables might still be creatable
        print(f"Warning: Could not create schema {catalog}.{schema}: {e}")
        pass
    
    # rl_runs table - identifiers are safe after validation
    catalog_escaped = escape_identifier(catalog)
    schema_escaped = escape_identifier(schema)
    sql_connector.execute_update(f"""
        CREATE TABLE IF NOT EXISTS {catalog_escaped}.{schema_escaped}.rl_runs (
            run_id STRING,
            project_id STRING,
            project_name STRING,
            experiment_id STRING,
            state STRING,
            build_job_run_id BIGINT,
            eval_job_run_id BIGINT,
            config STRING,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            created_by STRING,
            error_message STRING
        ) USING DELTA
        PARTITIONED BY (project_id)
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true'
        )
    """)
    
    # rl_eval_results table
    sql_connector.execute_update(f"""
        CREATE TABLE IF NOT EXISTS {catalog_escaped}.{schema_escaped}.rl_eval_results (
            eval_result_id STRING,
            run_id STRING,
            strategy STRING,
            mlflow_run_id STRING,
            query_id STRING,
            query_text STRING,
            retrieved_chunks STRING,
            metrics STRING,
            retrieval_latency_ms DOUBLE,
            created_at TIMESTAMP
        ) USING DELTA
        PARTITIONED BY (run_id, strategy)
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true'
        )
    """)
    
    # rl_human_feedback table
    sql_connector.execute_update(f"""
        CREATE TABLE IF NOT EXISTS {catalog_escaped}.{schema_escaped}.rl_human_feedback (
            feedback_id STRING,
            run_id STRING,
            strategy STRING,
            query_id STRING,
            chunk_id STRING,
            chunk_text STRING,
            rating INT,
            notes STRING,
            created_at TIMESTAMP,
            created_by STRING
        ) USING DELTA
        PARTITIONED BY (run_id)
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true'
        )
    """)
    
    # rl_eval_queries table
    sql_connector.execute_update(f"""
        CREATE TABLE IF NOT EXISTS {catalog_escaped}.{schema_escaped}.rl_eval_queries (
            query_id STRING,
            run_id STRING,
            query_text STRING,
            expected_chunks STRING,
            created_at TIMESTAMP
        ) USING DELTA
        PARTITIONED BY (run_id)
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true'
        )
    """)
    
    # rl_projects table
    # Use execute_update for DDL to ensure commit
    sql_connector.execute_update(f"""
        CREATE TABLE IF NOT EXISTS {catalog_escaped}.{schema_escaped}.rl_projects (
            project_id STRING,
            project_name STRING,
            description STRING,
            catalog STRING,
            schema STRING,
            vs_endpoint_name STRING,
            embedding_model_endpoint STRING,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            created_by STRING
        ) USING DELTA
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true'
        )
    """)
    
    # Note: rl_chunks_{strategy} tables are created dynamically during build
    # They follow the schema defined in the build notebook

def create_run(sql_connector: DatabricksSQLConnector, catalog: str, schema: str, project_id: str, 
               project_name: str, config: dict, created_by: str = None) -> str:
    """Create a new run record and return run_id"""
    run_id = str(uuid.uuid4())
    
    # Get current user if not provided
    if created_by is None:
        try:
            from databricks.sdk import WorkspaceClient
            from databricks.sdk.core import Config
            # Use Config() for app authorization
            cfg = Config()
            w = WorkspaceClient(config=cfg)
            created_by = w.current_user.me().user_name
        except Exception:
            created_by = "unknown"
    
    # Escape identifiers and sanitize string values
    catalog_escaped = escape_identifier(catalog)
    schema_escaped = escape_identifier(schema)
    project_id_safe = sanitize_string(project_id)
    project_name_safe = sanitize_string(project_name, max_length=500)
    config_safe = sanitize_string(json.dumps(config), max_length=10000)
    created_by_safe = sanitize_string(created_by, max_length=200)
    run_id_safe = sanitize_string(run_id)
    
    # Insert into Delta table using parameterized query
    # Databricks SQL connector uses ? placeholders for positional parameters
    query = f"""
        INSERT INTO {catalog_escaped}.{schema_escaped}.rl_runs (
            run_id, project_id, project_name, experiment_id, state,
            build_job_run_id, eval_job_run_id, config,
            created_at, updated_at, created_by, error_message
        ) VALUES (?, ?, ?, '', 'PENDING', NULL, NULL, ?, current_timestamp(), current_timestamp(), ?, NULL)
    """
    
    sql_connector.execute_update(query, [
        run_id_safe,
        project_id_safe,
        project_name_safe,
        config_safe,
        created_by_safe
    ])
    
    return run_id

def update_run_state(sql_connector: DatabricksSQLConnector, catalog: str, schema: str, 
                    run_id: str, state: str, **kwargs):
    """Update run state and optional fields"""
    updates = {"state": state}
    updates.update(kwargs)
    
    # Escape identifiers
    catalog_escaped = escape_identifier(catalog)
    schema_escaped = escape_identifier(schema)
    run_id_safe = sanitize_string(run_id)
    
    # Build UPDATE SQL with ? placeholders for positional parameters
    set_clauses = []
    param_list = []
    
    for key, value in updates.items():
        # Validate key name to prevent injection
        if not key.replace("_", "").isalnum():
            raise ValueError(f"Invalid key name: {key}")
        
        escaped_key = escape_identifier(key)
        
        if value is None:
            set_clauses.append(f"{escaped_key} = NULL")
        else:
            set_clauses.append(f"{escaped_key} = ?")
            if isinstance(value, (int, float)):
                param_list.append(value)
            else:
                param_list.append(sanitize_string(str(value)))
    
    set_clauses.append("updated_at = current_timestamp()")
    
    # Build query with ? placeholders
    query = f"""
        UPDATE {catalog_escaped}.{schema_escaped}.rl_runs
        SET {', '.join(set_clauses)}
        WHERE run_id = ?
    """
    
    # Add run_id as last parameter
    param_list.append(run_id_safe)
    
    sql_connector.execute_update(query, param_list)

def get_run_status(sql_connector: DatabricksSQLConnector, catalog: str, schema: str, run_id: str) -> Optional[dict]:
    """Get current run status"""
    # Escape identifiers and sanitize run_id
    catalog_escaped = escape_identifier(catalog)
    schema_escaped = escape_identifier(schema)
    run_id_safe = sanitize_string(run_id)
    
    query = f"""
        SELECT *
        FROM {catalog_escaped}.{schema_escaped}.rl_runs
        WHERE run_id = ?
    """
    
    results = sql_connector.execute(query, [run_id_safe])
    
    if not results:
        return None
    
    row = results[0]
    return {
        "run_id": row.get("run_id"),
        "project_id": row.get("project_id"),
        "project_name": row.get("project_name"),
        "experiment_id": row.get("experiment_id"),
        "state": row.get("state"),
        "build_job_run_id": row.get("build_job_run_id"),
        "eval_job_run_id": row.get("eval_job_run_id"),
        "config": json.loads(row.get("config", "{}")) if row.get("config") else {},
        "created_at": row.get("created_at"),
        "updated_at": row.get("updated_at"),
        "created_by": row.get("created_by"),
        "error_message": row.get("error_message")
    }

def get_runs_by_project(sql_connector: DatabricksSQLConnector, catalog: str, schema: str, 
                        project_id: str) -> list:
    """Get all runs for a project"""
    # Escape identifiers and sanitize project_id
    catalog_escaped = escape_identifier(catalog)
    schema_escaped = escape_identifier(schema)
    project_id_safe = sanitize_string(project_id)
    
    query = f"""
        SELECT *
        FROM {catalog_escaped}.{schema_escaped}.rl_runs
        WHERE project_id = ?
        ORDER BY created_at DESC
    """
    
    results = sql_connector.execute(query, [project_id_safe])
    
    return results


def create_project(sql_connector: DatabricksSQLConnector, catalog: str, schema: str,
                   project_id: str, project_name: str, description: str = None,
                   vs_endpoint_name: str = None, embedding_model_endpoint: str = None,
                   created_by: str = None) -> dict:
    """Create a new project and return project dict"""
    # Get current user if not provided
    if created_by is None:
        try:
            from databricks.sdk import WorkspaceClient
            from databricks.sdk.core import Config
            cfg = Config()
            w = WorkspaceClient(config=cfg)
            created_by = w.current_user.me().user_name
        except Exception:
            created_by = "unknown"
    
    # Escape identifiers and sanitize values
    catalog_escaped = escape_identifier(catalog)
    schema_escaped = escape_identifier(schema)
    project_id_safe = sanitize_string(project_id)
    project_name_safe = sanitize_string(project_name, max_length=500)
    description_safe = sanitize_string(description or "", max_length=2000)
    vs_endpoint_safe = sanitize_string(vs_endpoint_name or "", max_length=200)
    embedding_endpoint_safe = sanitize_string(embedding_model_endpoint or "", max_length=200)
    created_by_safe = sanitize_string(created_by, max_length=200)
    
    # Databricks SQL connector uses ? placeholders for positional parameters
    query = f"""
        INSERT INTO {catalog_escaped}.{schema_escaped}.rl_projects (
            project_id, project_name, description, catalog, schema,
            vs_endpoint_name, embedding_model_endpoint,
            created_at, updated_at, created_by
        ) VALUES (?, ?, ?, ?, ?, ?, ?, current_timestamp(), current_timestamp(), ?)
    """
    
    sql_connector.execute_update(query, [
        project_id_safe,
        project_name_safe,
        description_safe,
        catalog,
        schema,
        vs_endpoint_safe,
        embedding_endpoint_safe,
        created_by_safe
    ])
    
    # Return the created project with timestamps
    from datetime import datetime
    now = datetime.now()
    return {
        "project_id": project_id,
        "project_name": project_name,
        "description": description,
        "catalog": catalog,
        "schema": schema,
        "vs_endpoint_name": vs_endpoint_name,
        "embedding_model_endpoint": embedding_model_endpoint,
        "created_at": now,
        "updated_at": now,
        "created_by": created_by
    }


def get_project(sql_connector: DatabricksSQLConnector, catalog: str, schema: str,
                project_id: str) -> Optional[dict]:
    """Get project by ID"""
    catalog_escaped = escape_identifier(catalog)
    schema_escaped = escape_identifier(schema)
    project_id_safe = sanitize_string(project_id)
    
    query = f"""
        SELECT *
        FROM {catalog_escaped}.{schema_escaped}.rl_projects
        WHERE project_id = ?
    """
    
    results = sql_connector.execute(query, [project_id_safe])
    
    if not results:
        return None
    
    row = results[0]
    return {
        "project_id": row.get("project_id"),
        "project_name": row.get("project_name"),
        "description": row.get("description", ""),
        "catalog": row.get("catalog"),
        "schema": row.get("schema"),
        "vs_endpoint_name": row.get("vs_endpoint_name"),
        "embedding_model_endpoint": row.get("embedding_model_endpoint"),
        "created_at": row.get("created_at"),
        "updated_at": row.get("updated_at"),
        "created_by": row.get("created_by"),
    }


def get_all_projects(sql_connector: DatabricksSQLConnector, catalog: str, schema: str) -> list:
    """Get all projects"""
    catalog_escaped = escape_identifier(catalog)
    schema_escaped = escape_identifier(schema)
    
    query = f"""
        SELECT *
        FROM {catalog_escaped}.{schema_escaped}.rl_projects
        ORDER BY created_at DESC
    """
    
    results = sql_connector.execute(query)
    
    # Handle case where results might be None or empty
    if not results:
        return []
    
    return [
        {
            "project_id": row.get("project_id"),
            "project_name": row.get("project_name"),
            "description": row.get("description", ""),
            "catalog": row.get("catalog"),
            "schema": row.get("schema"),
            "vs_endpoint_name": row.get("vs_endpoint_name"),
            "embedding_model_endpoint": row.get("embedding_model_endpoint"),
            "created_at": row.get("created_at"),
            "updated_at": row.get("updated_at"),
            "created_by": row.get("created_by"),
        }
        for row in results
    ]


def get_run(sql_connector: DatabricksSQLConnector, catalog: str, schema: str, run_id: str) -> Optional[dict]:
    """Get run by ID - alias for get_run_status"""
    return get_run_status(sql_connector, catalog, schema, run_id)


def get_project_runs(sql_connector: DatabricksSQLConnector, catalog: str, schema: str, project_id: str) -> list:
    """Get all runs for a project - alias for get_runs_by_project"""
    return get_runs_by_project(sql_connector, catalog, schema, project_id)
