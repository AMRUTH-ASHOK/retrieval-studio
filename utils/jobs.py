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
    build_run_id: str,
    project_name: str,
    catalog: str,
    schema: str,
    queries_table: str = None,
    corpus_table: str = None,
    dataset_type: str = "delta_table",
    top_k: int = 10,
    auto_generate_queries: bool = False,
    num_queries: int = 50,
    query_style: str = "keyword",
    compare_query_types: bool = False,
    judge_model_endpoint: str = None,
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
        build_run_id: Build run identifier
        queries_table: Full table name (catalog.schema.table) containing evaluation queries
        project_name: Project name
        catalog: Catalog name
        schema: Schema name
        top_k: Number of top results to retrieve (default 10)
        timeout_minutes: Job timeout in minutes
        compute_size: Not used for serverless (parameter kept for compatibility)
    
    Returns:
        Job run ID (int)
    """
    try:
        app_config = get_config()
        
        # Prepare job parameters
        base_parameters = {
            "build_run_id": build_run_id,
            "project_name": project_name,
            "dataset_type": dataset_type,
            "top_k": str(top_k),
            "catalog": catalog,
            "schema": schema,
            "auto_generate_queries": str(auto_generate_queries).lower(),
            "num_queries": str(num_queries),
            "query_style": query_style,
            "compare_query_types": str(compare_query_types).lower(),
        }
        
        # Add optional parameters
        if queries_table:
            base_parameters["queries_table"] = queries_table
        if corpus_table:
            base_parameters["corpus_table"] = corpus_table
        if judge_model_endpoint:
            base_parameters["judge_model_endpoint"] = judge_model_endpoint
        
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
            run_name=f"RetrievalStudio-Eval-{build_run_id[:8]}",
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
        
        # Get task run ID from the first task if available
        task_run_id = None
        if hasattr(run, 'tasks') and run.tasks and len(run.tasks) > 0:
            task = run.tasks[0]
            if hasattr(task, 'run_id'):
                task_run_id = task.run_id
        
        # Get run as user/service principal
        run_as = None
        if hasattr(run, 'run_as'):
            if hasattr(run.run_as, 'service_principal_name'):
                run_as = run.run_as.service_principal_name
            elif hasattr(run.run_as, 'user_name'):
                run_as = run.run_as.user_name
        
        return {
            "run_id": run.run_id,
            "job_id": run.job_id if hasattr(run, 'job_id') and run.job_id else None,
            "task_run_id": task_run_id,
            "run_as": run_as,
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


def get_job_url(w: WorkspaceClient, job_run_id: int) -> str:
    """Construct Databricks job run URL"""
    try:
        run = w.jobs.get_run(run_id=job_run_id)
        job_id = run.job_id if hasattr(run, 'job_id') and run.job_id else None

        if not job_id:
            # For one-time runs submitted via jobs.submit(), the job_id might be None
            # In this case, we can't construct a proper URL
            return None

        # Get workspace URL from config
        from databricks.sdk.core import Config
        cfg = Config()
        host = cfg.host

        # Try to get workspace ID - it's often in the host URL or can be extracted
        # Format: https://<host>/jobs/<job_id>/runs/<run_id>?o=<workspace_id>
        workspace_id = None

        # Try to get workspace ID from workspace client
        try:
            # The workspace ID might be available through the current user or workspace info
            # For now, we'll construct URL without it if not available
            # The URL will still work without the ?o parameter
            pass
        except:
            pass

        # Construct URL - workspace_id is optional
        if workspace_id:
            return f"{host}/jobs/{job_id}/runs/{job_run_id}?o={workspace_id}"
        else:
            return f"{host}/jobs/{job_id}/runs/{job_run_id}"
    except Exception as e:
        raise JobSubmissionError(
            handle_error(e, f"Failed to get job URL for {job_run_id}", show_traceback=False)
        ) from e


def get_job_run_output(w: WorkspaceClient, job_run_id: int) -> dict:
    """
    Get notebook output from a completed job run

    Returns the notebook's return value (dbutils.notebook.exit(value))
    This contains build results like chunks_table, index_name, etc. for each strategy

    Args:
        w: WorkspaceClient instance
        job_run_id: Job run ID

    Returns:
        Dictionary with notebook output including status and results
    """
    try:
        # Get run output from Databricks
        output = w.jobs.get_run_output(run_id=job_run_id)

        # Get run status
        run = w.jobs.get_run(run_id=job_run_id)
        status = run.state.result_state.value if run.state and run.state.result_state else "UNKNOWN"

        # Parse notebook output
        # The notebook_output.result contains the JSON string from dbutils.notebook.exit()
        result_data = None
        if hasattr(output, 'notebook_output') and output.notebook_output:
            if hasattr(output.notebook_output, 'result'):
                result_str = output.notebook_output.result
                if result_str:
                    try:
                        result_data = json.loads(result_str)
                    except json.JSONDecodeError:
                        # If not valid JSON, return as-is
                        result_data = result_str

        return {
            "status": status,
            "results": result_data
        }

    except Exception as e:
        raise JobSubmissionError(
            handle_error(e, f"Failed to get job run output for {job_run_id}", show_traceback=False)
        ) from e
