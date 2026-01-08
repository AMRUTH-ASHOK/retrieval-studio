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
from utils.state import get_run

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
        build_run = get_run(sql_connector, settings.CATALOG, settings.SCHEMA, eval_request.run_id)
        if not build_run:
            raise HTTPException(status_code=404, detail="Build run not found")
        
        project_name = build_run.get("project_name", "default")
        top_k = eval_request.top_k or 10
        dataset_type = eval_request.dataset_type or "delta_table"
        
        # Submit the evaluation job
        job_run_id = submit_eval_job(
            w=w,
            notebook_path=settings.EVAL_NOTEBOOK_PATH,
            build_run_id=eval_request.run_id,
            queries_table=eval_request.queries_table,
            project_name=project_name,
            catalog=settings.CATALOG,
            schema=settings.SCHEMA,
            dataset_type=dataset_type,
            top_k=top_k
        )
        
        # Get job run details to return timestamps
        job_run_status = get_job_run_status(w, job_run_id)
        job_url = get_job_url(w, job_run_id)
        
        # Update build run with eval_job_run_id
        from utils.state import update_run_state
        update_run_state(
            sql_connector=sql_connector,
            catalog=settings.CATALOG,
            schema=settings.SCHEMA,
            run_id=eval_request.run_id,
            eval_job_run_id=job_run_id
        )
        
        # Return evaluation details
        return {
            "eval_id": str(uuid.uuid4()),
            "run_id": eval_request.run_id,
            "state": "RUNNING",
            "job_id": str(job_run_id),
            "job_url": job_url,
            "created_at": job_run_status.get("start_time"),
            "updated_at": job_run_status.get("start_time")
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{run_id}/status")
async def get_evaluation_status(run_id: str, w=Depends(get_workspace_client), sql_connector=Depends(get_sql_connector)):
    """Get evaluation job status with job URL"""
    try:
        run = get_run(sql_connector, settings.CATALOG, settings.SCHEMA, run_id)
        if not run:
            raise HTTPException(status_code=404, detail="Evaluation run not found")
        
        job_run_id = run.get("eval_job_run_id")
        if not job_run_id:
            return {
                "run_id": run_id,
                "state": run.get("state", "UNKNOWN"),
                "job_url": None,
                "status": None,
                "start_time": None,
            }
        
        # Get job status from Databricks
        job_status = get_job_run_status(w, job_run_id)
        job_url = get_job_url(w, job_run_id)
        
        # Update state in database if it changed
        current_state = run.get("state")
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
            from utils.state import update_run_state
            update_run_state(
                sql_connector=sql_connector,
                catalog=settings.CATALOG,
                schema=settings.SCHEMA,
                run_id=run_id,
                state=mapped_state
            )
        
        return {
            "run_id": run_id,
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
