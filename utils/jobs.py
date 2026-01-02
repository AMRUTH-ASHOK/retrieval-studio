"""
Job submission utilities for Retrieval Studio

All jobs use Databricks Serverless Compute.
No cluster specification needed - fully managed by Databricks.
"""
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import Task, NotebookTask
import json
from utils.config import get_config
from utils.errors import JobSubmissionError, handle_error
from utils.query_builder import escape_identifier, sanitize_string


def submit_build_job(
    w: WorkspaceClient,
    notebook_path: str,
    run_id: str,
    config: dict,
    timeout_minutes: int = 60,
    compute_size: str = None
) -> int:
    """
    Submit build job with serverless compute and return job_run_id
    
    Uses Databricks serverless compute - no cluster configuration needed.
    Databricks automatically provisions and manages all compute resources.
    
    Args:
        w: WorkspaceClient instance
        notebook_path: Path to build notebook
        run_id: Run identifier
        config: Build configuration dictionary
        timeout_minutes: Job timeout in minutes (default 60)
        compute_size: Not used for serverless (parameter kept for compatibility)
    
    Returns:
        Job run ID (int)
    """
    try:
        app_config = get_config()
        
        # Prepare job parameters as dictionary
        base_parameters = {
            "run_id": run_id,
            "config": json.dumps(config),
            "catalog": config.get("catalog", app_config.get_catalog()),
            "schema": config.get("schema", app_config.get_schema()),
        }
        
        # Calculate timeout with buffer (30 minutes buffer)
        timeout_seconds = (timeout_minutes + 30) * 60
        
        # For serverless compute, we don't specify new_cluster or existing_cluster_id
        # Databricks automatically uses serverless compute when no cluster is specified
        task = Task(
            task_key="build",
            notebook_task=NotebookTask(
                notebook_path=notebook_path,
                base_parameters=base_parameters
            ),
            timeout_seconds=timeout_seconds
        )
        
        # Submit job run with serverless compute
        # When no cluster is specified, Databricks uses serverless compute automatically
        run = w.jobs.submit(
            run_name=f"RetrievalStudio-Build-{run_id[:8]}",
            tasks=[task]
        )
        
        return run.run_id
    
    except Exception as e:
        raise JobSubmissionError(
            handle_error(e, "Failed to submit build job", show_traceback=False)
        ) from e


def submit_eval_job(
    w: WorkspaceClient,
    notebook_path: str,
    run_id: str,
    queries_table: str,
    catalog: str,
    schema: str,
    timeout_minutes: int = 30,
    compute_size: str = None
) -> int:
    """
    Submit eval job with serverless compute and return job_run_id
    
    Uses Databricks serverless compute - no cluster configuration needed.
    Databricks automatically provisions and manages all compute resources.
    
    Args:
        w: WorkspaceClient instance
        notebook_path: Path to eval notebook
        run_id: Run identifier
        queries_table: Full table name (catalog.schema.table) containing evaluation queries
        catalog: Catalog name
        schema: Schema name
        timeout_minutes: Job timeout in minutes
        compute_size: Not used for serverless (parameter kept for compatibility)
    
    Returns:
        Job run ID (int)
    """
    try:
        app_config = get_config()
        
        # Prepare job parameters
        base_parameters = {
            "run_id": run_id,
            "queries_table": queries_table,
            "catalog": catalog,
            "schema": schema,
        }
        
        timeout_seconds = (timeout_minutes + 15) * 60
        
        # For serverless compute, we don't specify new_cluster or existing_cluster_id
        # Databricks automatically uses serverless compute when no cluster is specified
        task = Task(
            task_key="eval",
            notebook_task=NotebookTask(
                notebook_path=notebook_path,
                base_parameters=base_parameters
            ),
            timeout_seconds=timeout_seconds
        )
        
        # Submit job run with serverless compute
        # When no cluster is specified, Databricks uses serverless compute automatically
        run = w.jobs.submit(
            run_name=f"RetrievalStudio-Eval-{run_id[:8]}",
            tasks=[task]
        )
        
        return run.run_id
    
    except Exception as e:
        raise JobSubmissionError(
            handle_error(e, "Failed to submit eval job", show_traceback=False)
        ) from e


def cancel_job_run(w: WorkspaceClient, job_run_id: int):
    """Cancel a job run"""
    try:
        w.jobs.cancel_run(run_id=job_run_id)
    except Exception as e:
        raise JobSubmissionError(
            handle_error(e, f"Failed to cancel job run {job_run_id}", show_traceback=False)
        ) from e


def get_job_run_status(w: WorkspaceClient, job_run_id: int) -> dict:
    """Get job run status"""
    try:
        run = w.jobs.get_run(run_id=job_run_id)
        
        return {
            "run_id": run.run_id,
            "state": run.state.life_cycle_state.value if run.state else None,
            "result_state": run.state.result_state.value if run.state and run.state.result_state else None,
            "start_time": run.start_time,
            "end_time": run.end_time,
            "setup_duration": run.setup_duration,
            "execution_duration": run.execution_duration,
            "cleanup_duration": run.cleanup_duration,
        }
    except Exception as e:
        raise JobSubmissionError(
            handle_error(e, f"Failed to get job run status for {job_run_id}", show_traceback=False)
        ) from e
