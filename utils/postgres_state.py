"""
State management using Lakehouse Postgres for transactional app data
Replaces Delta-based state management for better OLTP performance
"""
from utils.postgres_connector import get_postgres_connector
from typing import Optional, Dict, List, Any
import uuid
from datetime import datetime
import json


def initialize_tables():
    """
    Initialize Postgres schema

    Note: Run postgres_schema.sql manually first in Lakebase SQL Editor
    This function just validates the tables exist.
    """
    connector = get_postgres_connector()

    # Test connection and verify tables exist
    try:
        tables = connector.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_name IN ('projects', 'builds', 'evaluations', 'job_runs')
        """)

        table_names = [t['table_name'] for t in tables]
        expected = {'projects', 'builds', 'evaluations', 'job_runs'}
        missing = expected - set(table_names)

        if missing:
            print(f"⚠️  Warning: Missing tables: {missing}")
            print("   Run postgres_schema.sql in Lakebase SQL Editor to create them")
            return False

        print(f"✅ All required tables exist: {', '.join(table_names)}")
        return True

    except Exception as e:
        print(f"❌ Failed to verify tables: {e}")
        return False


# ============================================================================
# PROJECT OPERATIONS
# ============================================================================

def create_project(
    project_id: str,
    project_name: str,
    description: Optional[str] = None,
    catalog: Optional[str] = None,
    schema: Optional[str] = None,
    vs_endpoint_name: Optional[str] = None,
    embedding_model_endpoint: Optional[str] = None,
    created_by: Optional[str] = None
) -> Dict[str, Any]:
    """Create a new project"""
    connector = get_postgres_connector()

    # Get current user if not provided
    if created_by is None:
        try:
            created_by = connector.w.current_user.me().user_name
        except Exception:
            created_by = "unknown"

    query = """
        INSERT INTO projects (
            project_id, project_name, description, catalog, db_schema,
            vs_endpoint_name, embedding_model_endpoint, created_by
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING *
    """

    result = connector.execute(
        query,
        (project_id, project_name, description, catalog, schema,
         vs_endpoint_name, embedding_model_endpoint, created_by),
        fetch="one"
    )

    return result


def get_project(project_id: str) -> Optional[Dict[str, Any]]:
    """Get project by ID"""
    connector = get_postgres_connector()

    query = "SELECT * FROM projects WHERE project_id = %s"
    return connector.execute(query, (project_id,), fetch="one")


def get_all_projects() -> List[Dict[str, Any]]:
    """Get all projects"""
    connector = get_postgres_connector()

    query = "SELECT * FROM projects ORDER BY created_at DESC"
    return connector.execute(query)


def update_project(
    project_id: str,
    **updates
) -> Optional[Dict[str, Any]]:
    """Update project fields"""
    connector = get_postgres_connector()

    if not updates:
        return get_project(project_id)

    # Build SET clause
    set_parts = []
    params = []
    for key, value in updates.items():
        set_parts.append(f"{key} = %s")
        params.append(value)

    params.append(project_id)

    query = f"""
        UPDATE projects
        SET {', '.join(set_parts)}
        WHERE project_id = %s
        RETURNING *
    """

    return connector.execute(query, tuple(params), fetch="one")


# ============================================================================
# BUILD OPERATIONS
# ============================================================================

def create_build(
    run_id: str,
    project_id: str,
    project_name: str,
    config: Dict[str, Any],
    created_by: Optional[str] = None
) -> Dict[str, Any]:
    """Create a new build record"""
    connector = get_postgres_connector()

    if created_by is None:
        try:
            created_by = connector.w.current_user.me().user_name
        except Exception:
            created_by = "unknown"

    query = """
        INSERT INTO builds (
            run_id, project_id, project_name, state, config, created_by
        ) VALUES (%s, %s, %s, 'PENDING', %s, %s)
        RETURNING *
    """

    return connector.execute(
        query,
        (run_id, project_id, project_name, json.dumps(config), created_by),
        fetch="one"
    )


def get_build(run_id: str) -> Optional[Dict[str, Any]]:
    """Get build by run_id"""
    connector = get_postgres_connector()

    query = "SELECT * FROM builds WHERE run_id = %s"
    result = connector.execute(query, (run_id,), fetch="one")

    # Parse config JSON
    if result and result.get('config'):
        result['config'] = json.loads(result['config']) if isinstance(result['config'], str) else result['config']

    return result


def get_builds_by_project(project_id: str) -> List[Dict[str, Any]]:
    """Get all builds for a project"""
    connector = get_postgres_connector()

    query = """
        SELECT * FROM builds
        WHERE project_id = %s
        ORDER BY created_at DESC
    """

    results = connector.execute(query, (project_id,))

    # Parse config JSON for each result
    for result in results:
        if result.get('config'):
            result['config'] = json.loads(result['config']) if isinstance(result['config'], str) else result['config']

    return results


def update_build_state(
    run_id: str,
    state: str,
    **kwargs
) -> Optional[Dict[str, Any]]:
    """Update build state and optional fields"""
    connector = get_postgres_connector()

    updates = {'state': state}
    updates.update(kwargs)

    # Build SET clause
    set_parts = []
    params = []
    for key, value in updates.items():
        set_parts.append(f"{key} = %s")
        params.append(value)

    params.append(run_id)

    query = f"""
        UPDATE builds
        SET {', '.join(set_parts)}
        WHERE run_id = %s
        RETURNING *
    """

    result = connector.execute(query, tuple(params), fetch="one")

    # Parse config JSON
    if result and result.get('config'):
        result['config'] = json.loads(result['config']) if isinstance(result['config'], str) else result['config']

    return result


# ============================================================================
# EVALUATION OPERATIONS
# ============================================================================

def create_evaluation(
    eval_id: str,
    run_id: str,
    project_id: str,
    queries_table: Optional[str] = None,
    corpus_table: Optional[str] = None,
    dataset_type: str = "delta_table",
    top_k: int = 10,
    auto_generate_queries: bool = False,
    num_queries: Optional[int] = None,
    query_style: Optional[str] = None,
    compare_query_types: bool = False,
    judge_model_endpoint: Optional[str] = None,
    created_by: Optional[str] = None
) -> Dict[str, Any]:
    """Create a new evaluation record"""
    connector = get_postgres_connector()

    if created_by is None:
        try:
            created_by = connector.w.current_user.me().user_name
        except Exception:
            created_by = "unknown"

    query = """
        INSERT INTO evaluations (
            eval_id, run_id, project_id, state, queries_table, corpus_table,
            dataset_type, top_k, auto_generate_queries, num_queries, query_style,
            compare_query_types, judge_model_endpoint, created_by
        ) VALUES (
            %s, %s, %s, 'PENDING', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        RETURNING *
    """

    return connector.execute(
        query,
        (eval_id, run_id, project_id, queries_table, corpus_table,
         dataset_type, top_k, auto_generate_queries, num_queries, query_style,
         compare_query_types, judge_model_endpoint, created_by),
        fetch="one"
    )


def get_evaluation(eval_id: str) -> Optional[Dict[str, Any]]:
    """Get evaluation by eval_id"""
    connector = get_postgres_connector()

    query = "SELECT * FROM evaluations WHERE eval_id = %s"
    return connector.execute(query, (eval_id,), fetch="one")


def get_evaluations_by_build(run_id: str) -> List[Dict[str, Any]]:
    """Get all evaluations for a build"""
    connector = get_postgres_connector()

    query = """
        SELECT * FROM evaluations
        WHERE run_id = %s
        ORDER BY created_at DESC
    """

    return connector.execute(query, (run_id,))


def update_evaluation_state(
    eval_id: str,
    state: str,
    **kwargs
) -> Optional[Dict[str, Any]]:
    """Update evaluation state and optional fields"""
    connector = get_postgres_connector()

    updates = {'state': state}
    updates.update(kwargs)

    # Build SET clause
    set_parts = []
    params = []
    for key, value in updates.items():
        set_parts.append(f"{key} = %s")
        params.append(value)

    params.append(eval_id)

    query = f"""
        UPDATE evaluations
        SET {', '.join(set_parts)}
        WHERE eval_id = %s
        RETURNING *
    """

    return connector.execute(query, tuple(params), fetch="one")


# ============================================================================
# JOB TRACKING OPERATIONS
# ============================================================================

def upsert_job_run(
    job_run_id: int,
    run_id: str,
    job_type: str,
    state: str,
    result_state: Optional[str] = None,
    job_url: Optional[str] = None,
    start_time: Optional[int] = None,
    end_time: Optional[int] = None,
    setup_duration: Optional[int] = None,
    execution_duration: Optional[int] = None,
    cleanup_duration: Optional[int] = None
) -> Dict[str, Any]:
    """Insert or update job run tracking"""
    connector = get_postgres_connector()

    query = """
        INSERT INTO job_runs (
            job_run_id, run_id, job_type, state, result_state, job_url,
            start_time, end_time, setup_duration, execution_duration, cleanup_duration
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (job_run_id) DO UPDATE SET
            state = EXCLUDED.state,
            result_state = EXCLUDED.result_state,
            job_url = EXCLUDED.job_url,
            start_time = EXCLUDED.start_time,
            end_time = EXCLUDED.end_time,
            setup_duration = EXCLUDED.setup_duration,
            execution_duration = EXCLUDED.execution_duration,
            cleanup_duration = EXCLUDED.cleanup_duration,
            last_checked_at = CURRENT_TIMESTAMP
        RETURNING *
    """

    return connector.execute(
        query,
        (job_run_id, run_id, job_type, state, result_state, job_url,
         start_time, end_time, setup_duration, execution_duration, cleanup_duration),
        fetch="one"
    )


def get_job_run(job_run_id: int) -> Optional[Dict[str, Any]]:
    """Get job run by job_run_id"""
    connector = get_postgres_connector()

    query = "SELECT * FROM job_runs WHERE job_run_id = %s"
    return connector.execute(query, (job_run_id,), fetch="one")


# ============================================================================
# BACKWARD COMPATIBILITY ALIASES
# ============================================================================

# Aliases for backward compatibility with old state.py API
create_run = create_build
get_run = get_build
get_runs_by_project = get_builds_by_project
update_run_state = update_build_state
get_run_status = get_build
get_project_runs = get_builds_by_project
