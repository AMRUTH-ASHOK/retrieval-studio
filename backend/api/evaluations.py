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
        
        # Validate required parameters based on mode
        if auto_generate:
            # ALWAYS extract corpus_table directly from build results
            from utils.jobs import get_job_run_output
            try:
                # Get build job output to extract chunks_table
                build_job_run_id = build_run.get("job_run_id")
                if not build_job_run_id:
                    raise HTTPException(status_code=400, detail="Build job_run_id not found. Cannot auto-extract corpus table.")

                build_output = get_job_run_output(w, build_job_run_id)
                if not build_output or "results" not in build_output:
                    raise HTTPException(status_code=400, detail="Build results not found.")

                # Extract the nested results from notebook output
                # build_output["results"] contains the notebook JSON output
                # which itself has a "results" field with strategy outputs
                notebook_results = build_output.get("results", {})
                strategy_results = notebook_results.get("results", {}) if isinstance(notebook_results, dict) else {}

                if not strategy_results:
                    raise HTTPException(status_code=400, detail="Build strategy results not found.")

                # Find first available chunks_table (prefer baseline, then semantic, then structured)
                strategies = ['baseline', 'semantic', 'structured']
                corpus_table = None

                for strategy in strategies:
                    if strategy in strategy_results and isinstance(strategy_results[strategy], dict):
                        corpus_table = strategy_results[strategy].get("chunks_table")
                        if corpus_table:
                            break

                # If no preferred strategy found, use first available
                if not corpus_table:
                    for strategy, result in strategy_results.items():
                        if isinstance(result, dict) and result.get("chunks_table"):
                            corpus_table = result["chunks_table"]
                            break

                if not corpus_table:
                    raise HTTPException(status_code=400, detail="No chunks_table found in build results.")

                # Set the auto-extracted corpus_table
                eval_request.corpus_table = corpus_table

            except Exception as e:
                if isinstance(e, HTTPException):
                    raise
                raise HTTPException(status_code=400, detail=f"Failed to auto-extract corpus table from build results: {str(e)}")
        else:
            if not eval_request.queries_table:
                raise HTTPException(status_code=400, detail="queries_table is required when auto_generate_queries is false")
        
        # Use the unified eval_notebook (now includes all features)
        notebook_path = settings.EVAL_NOTEBOOK_PATH

        # Submit the evaluation job
        job_run_id = submit_eval_job(
            w=w,
            notebook_path=notebook_path,
            build_run_id=eval_request.run_id,
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
            judge_model_endpoint=eval_request.judge_model_endpoint
        )
        
        # Get job run details to return timestamps
        job_run_status = get_job_run_status(w, job_run_id)
        job_url = get_job_url(w, job_run_id)

        # Create evaluation record in Postgres
        from utils.postgres_state import create_evaluation, update_evaluation_state
        eval_id = str(uuid.uuid4())

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
            "created_at": evaluation.get("created_at"),
            "updated_at": evaluation.get("updated_at")
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
        run_id_safe = sanitize_string(run_id)

        query = f"""
            SELECT * FROM {escape_identifier(settings.CATALOG)}.raw.rs_eval_results
            WHERE build_run_id = ?
            ORDER BY created_at DESC
        """

        results = sql_connector.execute(query, [run_id_safe])
        return results

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


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
