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
    Initialize Postgres schema — auto-creates any missing tables on startup.
    Core tables (projects, builds, evaluations, job_runs) must already exist
    (created via postgres_schema.sql in Lakebase SQL Editor).
    Migration tables (index_selections, studies, study_builds, study_evaluations)
    are created automatically with CREATE TABLE IF NOT EXISTS.
    """
    connector = get_postgres_connector()

    # Verify core tables exist
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
            print(f"⚠️  Warning: Missing core tables: {missing}")
            print("   Run postgres_schema.sql in Lakebase SQL Editor to create them")
            return False

        print(f"✅ All required tables exist: {', '.join(table_names)}")

    except Exception as e:
        print(f"❌ Failed to verify tables: {e}")
        return False

    # Auto-run migration 002: index_selections and studies tables
    # Use fetch="none" for DDL statements (no rows returned)
    try:
        connector.execute("""
            CREATE TABLE IF NOT EXISTS index_selections (
                id VARCHAR(50) PRIMARY KEY,
                project_id VARCHAR(50) REFERENCES projects(project_id) ON DELETE CASCADE,
                build_run_id VARCHAR(50),
                source_name VARCHAR(255),
                strategy_name VARCHAR(255),
                index_name VARCHAR(500),
                chunks_table VARCHAR(500),
                vs_endpoint VARCHAR(255),
                status VARCHAR(50) DEFAULT 'active',
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        """, fetch="none")
        connector.execute("""
            CREATE INDEX IF NOT EXISTS idx_index_selections_project
            ON index_selections(project_id)
        """, fetch="none")
        connector.execute("""
            CREATE INDEX IF NOT EXISTS idx_index_selections_status
            ON index_selections(status)
        """, fetch="none")
        connector.execute("""
            CREATE TABLE IF NOT EXISTS studies (
                study_id VARCHAR(50) PRIMARY KEY,
                project_id VARCHAR(50) REFERENCES projects(project_id) ON DELETE CASCADE,
                study_name VARCHAR(255) NOT NULL,
                description TEXT,
                status VARCHAR(50) DEFAULT 'active',
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        """, fetch="none")
        connector.execute("""
            CREATE INDEX IF NOT EXISTS idx_studies_project ON studies(project_id)
        """, fetch="none")
        connector.execute("""
            CREATE TABLE IF NOT EXISTS study_builds (
                study_id VARCHAR(50) REFERENCES studies(study_id) ON DELETE CASCADE,
                build_run_id VARCHAR(50) REFERENCES builds(run_id) ON DELETE CASCADE,
                PRIMARY KEY (study_id, build_run_id)
            )
        """, fetch="none")
        connector.execute("""
            CREATE TABLE IF NOT EXISTS study_evaluations (
                study_id VARCHAR(50) REFERENCES studies(study_id) ON DELETE CASCADE,
                eval_id VARCHAR(50) REFERENCES evaluations(eval_id) ON DELETE CASCADE,
                PRIMARY KEY (study_id, eval_id)
            )
        """, fetch="none")
        print("✅ Migration 002 applied: index_selections, studies, study_builds, study_evaluations")
    except Exception as e:
        print(f"⚠️  Migration 002 warning (non-fatal): {e}")

    return True


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


def delete_project(project_id: str) -> bool:
    """
    Delete a project and all associated builds and evaluations

    Returns True if project was deleted, False if not found
    """
    connector = get_postgres_connector()

    # Check if project exists
    project = get_project(project_id)
    if not project:
        return False

    # Delete in reverse order of foreign key dependencies
    # 1. Delete evaluations for all builds in this project
    connector.execute(
        """
        DELETE FROM evaluations
        WHERE project_id = %s
        """,
        (project_id,),
        fetch="none"
    )

    # 2. Delete builds for this project
    connector.execute(
        """
        DELETE FROM builds
        WHERE project_id = %s
        """,
        (project_id,),
        fetch="none"
    )

    # 3. Delete the project itself
    connector.execute(
        """
        DELETE FROM projects
        WHERE project_id = %s
        """,
        (project_id,),
        fetch="none"
    )

    return True


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


def delete_build(run_id: str) -> bool:
    """Delete a build and its associated evaluations. Returns True if deleted."""
    connector = get_postgres_connector()

    build = get_build(run_id)
    if not build:
        return False

    connector.execute(
        "DELETE FROM evaluations WHERE run_id = %s",
        (run_id,), fetch="none"
    )
    connector.execute(
        "DELETE FROM builds WHERE run_id = %s",
        (run_id,), fetch="none"
    )
    return True


def delete_evaluation(eval_id: str) -> bool:
    """Delete a single evaluation. Returns True if deleted."""
    connector = get_postgres_connector()

    result = connector.execute(
        "DELETE FROM evaluations WHERE eval_id = %s RETURNING eval_id",
        (eval_id,), fetch="one"
    )
    return result is not None


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
# MLflow Integration Helpers
# ============================================================================

def get_experiment_id_for_project(project_id: str) -> Optional[str]:
    """
    Get the project's experiment_id

    Args:
        project_id: The project UUID

    Returns:
        experiment_id string if found, None otherwise
    """
    connector = get_postgres_connector()

    result = connector.execute(
        """
        SELECT experiment_id
        FROM projects
        WHERE project_id = %s
          AND experiment_id IS NOT NULL
        """,
        (project_id,),
        fetch="one"
    )

    return result['experiment_id'] if result else None


# ============================================================================
# INDEX SELECTION OPERATIONS
# ============================================================================

def create_index_selection(
    project_id: str,
    build_run_id: str,
    source_name: str,
    strategy_name: str,
    index_name: str,
    chunks_table: str,
    vs_endpoint: str,
    status: str = "active"
) -> Dict[str, Any]:
    """Create an index selection record"""
    connector = get_postgres_connector()

    sel_id = str(uuid.uuid4())
    query = """
        INSERT INTO index_selections (
            id, project_id, build_run_id, source_name, strategy_name,
            index_name, chunks_table, vs_endpoint, status
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO NOTHING
        RETURNING *
    """
    result = connector.execute(
        query,
        (sel_id, project_id, build_run_id, source_name, strategy_name,
         index_name, chunks_table, vs_endpoint, status),
        fetch="one"
    )
    return result or {}


def get_index_selections(project_id: str, status: Optional[str] = None) -> List[Dict[str, Any]]:
    """Get all index selections for a project"""
    connector = get_postgres_connector()

    if status:
        query = "SELECT * FROM index_selections WHERE project_id = %s AND status = %s ORDER BY created_at DESC"
        return connector.execute(query, (project_id, status))
    else:
        query = "SELECT * FROM index_selections WHERE project_id = %s ORDER BY created_at DESC"
        return connector.execute(query, (project_id,))


def update_index_selection_status(selection_id: str, status: str) -> Optional[Dict[str, Any]]:
    """Update an index selection status"""
    connector = get_postgres_connector()

    query = """
        UPDATE index_selections SET status = %s
        WHERE id = %s
        RETURNING *
    """
    return connector.execute(query, (status, selection_id), fetch="one")


def bulk_update_index_status(project_id: str, updates: List[Dict[str, str]]) -> int:
    """Bulk update index statuses. updates = [{"id": ..., "status": ...}]"""
    connector = get_postgres_connector()
    count = 0
    for upd in updates:
        upd_id = upd.get("id")
        upd_status = upd.get("status")
        if not upd_id or not upd_status:
            continue
        result = connector.execute(
            "UPDATE index_selections SET status = %s WHERE id = %s AND project_id = %s RETURNING id",
            (upd_status, upd_id, project_id),
            fetch="one"
        )
        if result:
            count += 1
    return count


# ============================================================================
# STUDY OPERATIONS
# ============================================================================

def create_study(
    project_id: str,
    study_name: str,
    description: Optional[str] = None
) -> Dict[str, Any]:
    """Create a new study within a project"""
    connector = get_postgres_connector()
    study_id = str(uuid.uuid4())
    query = """
        INSERT INTO studies (study_id, project_id, study_name, description)
        VALUES (%s, %s, %s, %s)
        RETURNING *
    """
    return connector.execute(query, (study_id, project_id, study_name, description), fetch="one")


def get_studies(project_id: str) -> List[Dict[str, Any]]:
    """Get all studies for a project"""
    connector = get_postgres_connector()
    return connector.execute(
        "SELECT * FROM studies WHERE project_id = %s ORDER BY created_at DESC",
        (project_id,)
    )


def get_study(study_id: str) -> Optional[Dict[str, Any]]:
    """Get a study by ID"""
    connector = get_postgres_connector()
    return connector.execute("SELECT * FROM studies WHERE study_id = %s", (study_id,), fetch="one")


def delete_study(study_id: str) -> bool:
    """Delete a study"""
    connector = get_postgres_connector()
    result = connector.execute("DELETE FROM studies WHERE study_id = %s RETURNING study_id", (study_id,), fetch="one")
    return result is not None


def add_build_to_study(study_id: str, build_run_id: str):
    """Associate a build with a study"""
    connector = get_postgres_connector()
    connector.execute(
        "INSERT INTO study_builds (study_id, build_run_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
        (study_id, build_run_id),
        fetch="none"
    )


def add_evaluation_to_study(study_id: str, eval_id: str):
    """Associate an evaluation with a study"""
    connector = get_postgres_connector()
    connector.execute(
        "INSERT INTO study_evaluations (study_id, eval_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
        (study_id, eval_id),
        fetch="none"
    )


def get_study_builds(study_id: str) -> List[Dict[str, Any]]:
    """Get all builds for a study"""
    connector = get_postgres_connector()
    builds = connector.execute(
        """SELECT b.* FROM builds b
           JOIN study_builds sb ON b.run_id = sb.build_run_id
           WHERE sb.study_id = %s ORDER BY b.created_at DESC""",
        (study_id,)
    )
    for build in builds:
        if isinstance(build.get("config"), str):
            try:
                build["config"] = json.loads(build["config"])
            except (json.JSONDecodeError, TypeError):
                pass
    return builds


def get_study_evaluations(study_id: str) -> List[Dict[str, Any]]:
    """Get all evaluations for a study"""
    connector = get_postgres_connector()
    return connector.execute(
        """SELECT e.* FROM evaluations e
           JOIN study_evaluations se ON e.eval_id = se.eval_id
           WHERE se.study_id = %s ORDER BY e.created_at DESC""",
        (study_id,)
    )


# ============================================================================
# BACKWARD COMPATIBILITY ALIASES
# ============================================================================

create_run = create_build
get_run = get_build
get_runs_by_project = get_builds_by_project
update_run_state = update_build_state
get_run_status = get_build
get_project_runs = get_builds_by_project
