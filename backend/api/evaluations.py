"""
Evaluation jobs API endpoints
"""
from fastapi import APIRouter, Depends, HTTPException
from typing import List
import uuid

from backend.models.schemas import EvaluationCreate, EvaluationResponse
from backend.auth import get_workspace_client, get_sql_connector
from backend.config import settings
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from utils.jobs import submit_eval_job, get_job_run_status, get_job_url
from utils.postgres_state import get_build as get_run

router = APIRouter()


@router.post("/", response_model=EvaluationResponse)
async def create_evaluation(
    eval_request: EvaluationCreate,
    w=Depends(get_workspace_client),
    sql_connector=Depends(get_sql_connector)
):
    """Submit an evaluation job to Databricks"""
    try:
        # Get build run to extract project_name
        build_run = get_run(eval_request.run_id)
        if not build_run:
            raise HTTPException(status_code=404, detail="Build run not found")
        
        project_name = build_run.get("project_name", "default")
        top_k = eval_request.top_k or 10
        dataset_type = eval_request.dataset_type or "delta_table"
        auto_generate = eval_request.auto_generate_queries or False
        build_parent_run_id = build_run.get("build_parent_run_id")
        use_golden = eval_request.use_golden_dataset or False
        generate_golden = eval_request.generate_golden_dataset or False
        
        # Validate required parameters based on mode
        if generate_golden and use_golden:
            raise HTTPException(status_code=400, detail="generate_golden_dataset and use_golden_dataset cannot both be true")
        if use_golden:
            if not eval_request.golden_dataset_table:
                raise HTTPException(status_code=400, detail="golden_dataset_table is required when use_golden_dataset is true")
        elif auto_generate:
            if generate_golden and not eval_request.golden_dataset_table:
                raise HTTPException(status_code=400, detail="golden_dataset_table is required when auto_generate_queries is true")
            if not eval_request.corpus_table:
                # Construct corpus_table name using the same logic as build notebook
                # No need to query Databricks API or PostgreSQL - just pure computation
                try:
                    from backend.utils.build_results import construct_build_results, extract_corpus_table

                    # Get build config to determine which strategies were enabled
                    build_config = build_run.get("config", {})
                    project_name = build_run.get("project_name")

                    if not project_name:
                        raise HTTPException(status_code=400, detail="Project name not found in build record.")

                    # Extract enabled strategies from config
                    strategies = []
                    if build_config.get("baseline_enabled"):
                        strategies.append("baseline")
                    if build_config.get("semantic_enabled"):
                        strategies.append("semantic")
                    if build_config.get("structured_enabled"):
                        strategies.append("structured")

                    if not strategies:
                        # Default to baseline if no strategies specified
                        strategies = ["baseline"]

                    # Construct results using same logic as notebook (deterministic)
                    build_results = construct_build_results(
                        project_name=project_name,
                        strategies=strategies,
                        catalog=settings.CATALOG,
                        schema=settings.SCHEMA
                    )

                    # Extract corpus_table (prefer baseline, then semantic, then structured)
                    corpus_table = extract_corpus_table(build_results)

                    # Set the auto-constructed corpus_table
                    eval_request.corpus_table = corpus_table

                except Exception as e:
                    if isinstance(e, HTTPException):
                        raise
                    raise HTTPException(status_code=400, detail=f"Failed to construct corpus table: {str(e)}")
        else:
            if not eval_request.queries_table:
                raise HTTPException(status_code=400, detail="queries_table is required when auto_generate_queries is false")
        
        # Create evaluation record ID before submission so it can be logged in MLflow
        eval_id = str(uuid.uuid4())

        # Use the unified eval_notebook (now includes all features)
        notebook_path = settings.EVAL_NOTEBOOK_PATH

        # Submit the evaluation job
        job_run_id = submit_eval_job(
            w=w,
            notebook_path=notebook_path,
            build_run_id=eval_request.run_id,
            eval_id=eval_id,
            build_parent_run_id=build_parent_run_id,
            queries_table=eval_request.queries_table,
            corpus_table=eval_request.corpus_table,
            project_name=project_name,
            catalog=settings.CATALOG,
            schema=settings.SCHEMA,
            dataset_type=dataset_type,
            top_k=top_k,
            auto_generate_queries=auto_generate,
            num_queries=eval_request.num_queries or 50,
            query_style=eval_request.query_style or "keyword",
            compare_query_types=eval_request.compare_query_types or False,
            judge_model_endpoint=eval_request.judge_model_endpoint,
            generate_golden_dataset=generate_golden,
            use_golden_dataset=use_golden,
            golden_dataset_table=eval_request.golden_dataset_table,
            golden_dataset_id=eval_request.golden_dataset_id,
            golden_strategy=eval_request.golden_strategy,
            golden_query_type=eval_request.golden_query_type,
            golden_top_k=eval_request.golden_top_k
        )
        
        # Job submitted successfully - now get job details (non-critical)
        job_url = None
        try:
            job_url = get_job_url(w, job_run_id)
        except Exception as e:
            print(f"Warning: Failed to get job URL: {e}")

        # Create evaluation record in Postgres
        from utils.postgres_state import create_evaluation, update_evaluation_state
        import datetime
        evaluation = create_evaluation(
            eval_id=eval_id,
            run_id=eval_request.run_id,
            project_id=build_run.get("project_id"),
            queries_table=eval_request.queries_table,
            corpus_table=eval_request.corpus_table,
            dataset_type=dataset_type,
            top_k=top_k,
            auto_generate_queries=auto_generate,
            num_queries=eval_request.num_queries,
            query_style=eval_request.query_style,
            compare_query_types=eval_request.compare_query_types,
            judge_model_endpoint=eval_request.judge_model_endpoint
        )

        # Update evaluation with job details
        evaluation = update_evaluation_state(
            eval_id=eval_id,
            state="RUNNING",
            job_run_id=job_run_id,
            job_url=job_url
        )

        # Return evaluation details
        return {
            "eval_id": eval_id,
            "run_id": eval_id,  # Frontend expects run_id for status polling
            "state": "RUNNING",
            "job_id": str(job_run_id),
            "job_url": job_url,
            "created_at": evaluation.get("created_at") if evaluation.get("created_at") else datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "updated_at": evaluation.get("updated_at") if evaluation.get("updated_at") else datetime.datetime.now(datetime.timezone.utc).isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{eval_id}/status")
async def get_evaluation_status(eval_id: str, w=Depends(get_workspace_client), sql_connector=Depends(get_sql_connector)):
    """Get evaluation job status with job URL"""
    try:
        # Get evaluation from Postgres (eval_id is actually the evaluation ID)
        from utils.postgres_state import get_evaluation
        evaluation = get_evaluation(eval_id)
        if not evaluation:
            raise HTTPException(status_code=404, detail="Evaluation not found")

        job_run_id = evaluation.get("job_run_id")
        if not job_run_id:
            return {
                "run_id": eval_id,
                "state": evaluation.get("state", "UNKNOWN"),
                "job_url": None,
                "status": None,
                "start_time": None,
            }

        # Get job status from Databricks
        job_status = get_job_run_status(w, job_run_id)
        job_url = get_job_url(w, job_run_id)

        # Update state in database if it changed
        current_state = evaluation.get("state")
        new_state = job_status.get("result_state") or job_status.get("state")

        # Map Databricks states to our states
        state_mapping = {
            "SUCCESS": "SUCCESS",
            "FAILED": "FAILED",
            "CANCELED": "FAILED",
            "TIMEDOUT": "FAILED",
            "RUNNING": "RUNNING",
            "PENDING": "PENDING",
        }
        mapped_state = state_mapping.get(new_state, current_state)

        if mapped_state != current_state:
            from utils.postgres_state import update_evaluation_state
            update_evaluation_state(
                eval_id=eval_id,
                state=mapped_state
            )

        return {
            "run_id": eval_id,
            "state": mapped_state,
            "job_url": job_url,
            "status": job_status,
            "start_time": job_status.get("start_time"),
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{run_id}/results")
async def get_evaluation_results(run_id: str, sql_connector=Depends(get_sql_connector)):
    """Get evaluation results for a run"""
    try:
        from utils.query_builder import escape_identifier, sanitize_string

        # Query evaluation results using parameterized query
        # Check against build_run_id (parent), eval_id, eval_run_id (eval parent/child), or build_child_run_id (strategy)
        run_id_safe = sanitize_string(run_id)

        # First check if the table exists
        table_name = f"{escape_identifier(settings.CATALOG)}.raw.rs_eval_results"
        try:
            # Try to select from the table to see if it exists
            check_query = f"SELECT 1 FROM {table_name} LIMIT 1"
            sql_connector.execute(check_query)
        except Exception as table_check_error:
            # Table likely doesn't exist
            error_str = str(table_check_error).lower()
            if "table_or_view_not_found" in error_str or "table not found" in error_str or "does not exist" in error_str:
                # Return empty results instead of 500 error
                print(f"Table {table_name} does not exist yet. Returning empty results.")
                return []
            # Re-raise if it's a different error
            raise

        query = f"""
            SELECT * FROM {table_name}
            WHERE build_run_id = ?
               OR eval_id = ?
               OR eval_run_id = ?
               OR build_child_run_id = ?
            ORDER BY created_at DESC
        """

        # Pass run_id 4 times for the 4 placeholders
        results = sql_connector.execute(query, [run_id_safe, run_id_safe, run_id_safe, run_id_safe])
        return results if results else []

    except Exception as e:
        # Log the full error for debugging
        print(f"Error in get_evaluation_results: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Failed to retrieve evaluation results: {str(e)}")


@router.get("/build/{build_run_id}", response_model=List[EvaluationResponse])
async def get_evaluations_by_build(build_run_id: str, w=Depends(get_workspace_client), sql_connector=Depends(get_sql_connector)):
    """Get all evaluations for a specific build run"""
    try:
        from utils.postgres_state import get_evaluations_by_build

        evaluations = get_evaluations_by_build(build_run_id)

        # Transform to EvaluationResponse format
        transformed_evals = []
        for eval_data in evaluations:
            job_run_id = eval_data.get("job_run_id")
            job_url = None

            # Generate job URL if job_run_id exists
            if job_run_id:
                try:
                    job_url = get_job_url(w, job_run_id)
                except Exception:
                    pass  # If URL generation fails, continue without it

            transformed_evals.append({
                "eval_id": eval_data.get("eval_id"),
                "run_id": eval_data.get("eval_id"),  # Frontend expects run_id
                "state": eval_data.get("state", "UNKNOWN"),
                "job_id": str(job_run_id) if job_run_id else None,
                "job_url": job_url,
                "created_at": eval_data.get("created_at"),
                "updated_at": eval_data.get("updated_at"),
            })

        return transformed_evals
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
