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
from utils.jobs import submit_eval_job

router = APIRouter()


@router.post("/", response_model=EvaluationResponse)
async def create_evaluation(
    eval_request: EvaluationCreate,
    w=Depends(get_workspace_client),
    sql_connector=Depends(get_sql_connector)
):
    """Submit an evaluation job to Databricks"""
    try:
        # Submit the evaluation job
        job_run_id = submit_eval_job(
            w=w,
            catalog=settings.CATALOG,
            schema=settings.SCHEMA,
            run_id=eval_request.run_id,
            queries_table=eval_request.queries_table,
            notebook_path=settings.EVAL_NOTEBOOK_PATH
        )
        
        # Get job run details to return timestamps
        from utils.jobs import get_job_run_status
        job_run_status = get_job_run_status(w, job_run_id)
        
        # Return evaluation details
        return {
            "eval_id": str(uuid.uuid4()),
            "run_id": eval_request.run_id,
            "state": "RUNNING",
            "job_id": str(job_run_id),
            "created_at": job_run_status.get("start_time"),
            "updated_at": job_run_status.get("start_time")
        }
        
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
            SELECT * FROM {escape_identifier(settings.CATALOG)}.{escape_identifier(settings.SCHEMA)}.rl_eval_results
            WHERE run_id = ?
            ORDER BY created_at DESC
        """
        
        results = sql_connector.execute(query, [run_id_safe])
        return results
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
