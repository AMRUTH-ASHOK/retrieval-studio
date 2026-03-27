"""
Resource cleanup API endpoints
"""
from fastapi import APIRouter, Depends, HTTPException
from typing import List, Dict, Any
from pydantic import BaseModel

from backend.auth import get_workspace_client, get_sql_connector
from backend.config import settings
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from utils.postgres_state import get_index_selections, bulk_update_index_status
from utils.jobs import submit_cleanup_job, get_job_run_status, get_job_url

router = APIRouter()


class IndexStatusUpdate(BaseModel):
    id: str
    status: str


class BulkStatusUpdate(BaseModel):
    updates: List[IndexStatusUpdate]


@router.get("/projects/{project_id}/indexes")
async def get_project_indexes(project_id: str, status: str = None, sql_connector=Depends(get_sql_connector)):
    """Get all tracked indexes for a project"""
    try:
        indexes = get_index_selections(project_id, status=status)
        return indexes
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/projects/{project_id}/indexes/status")
async def update_indexes_status(project_id: str, body: BulkStatusUpdate, sql_connector=Depends(get_sql_connector)):
    """Bulk update index statuses (keep/discard)"""
    try:
        updates = [{"id": u.id, "status": u.status} for u in body.updates]
        count = bulk_update_index_status(project_id, updates)
        return {"updated": count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/projects/{project_id}/cleanup/preview")
async def preview_cleanup(project_id: str, sql_connector=Depends(get_sql_connector)):
    """Preview what will be deleted"""
    try:
        discarded = get_index_selections(project_id, status="discard")
        return {
            "indexes_to_delete": [
                {
                    "selection_id": idx.get("id"),
                    "index_name": idx.get("index_name"),
                    "chunks_table": idx.get("chunks_table"),
                    "source_name": idx.get("source_name"),
                    "strategy_name": idx.get("strategy_name"),
                    "vs_endpoint": idx.get("vs_endpoint"),
                }
                for idx in discarded
            ],
            "count": len(discarded)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/job/{job_run_id}/status")
async def get_cleanup_job_status(job_run_id: int, w=Depends(get_workspace_client)):
    """Get cleanup job status by Databricks job_run_id"""
    try:
        status = get_job_run_status(w, job_run_id)
        mapped = status.get("result_state") or status.get("state")
        state_map = {"SUCCESS": "SUCCESS", "FAILED": "FAILED", "CANCELED": "FAILED",
                     "TIMEDOUT": "FAILED", "RUNNING": "RUNNING", "PENDING": "PENDING", "TERMINATED": "SUCCESS"}
        return {"state": state_map.get(mapped, mapped), "status": status}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/projects/{project_id}/cleanup")
async def run_cleanup(project_id: str, w=Depends(get_workspace_client), sql_connector=Depends(get_sql_connector)):
    """Submit cleanup notebook job to delete discarded resources"""
    try:
        discarded = get_index_selections(project_id, status="discard")
        if not discarded:
            return {"message": "No resources marked for cleanup", "count": 0}

        cleanup_config = {
            "project_id": project_id,
            "indexes": [
                {
                    "selection_id": idx.get("id"),
                    "index_name": idx.get("index_name"),
                    "chunks_table": idx.get("chunks_table"),
                    "vs_endpoint": idx.get("vs_endpoint"),
                }
                for idx in discarded
            ]
        }

        notebook_path = settings.BUILD_NOTEBOOK_PATH.rsplit('/', 1)[0] + '/cleanup_notebook'

        job_run_id = submit_cleanup_job(
            w=w,
            notebook_path=notebook_path,
            cleanup_config=cleanup_config,
            catalog=settings.CATALOG,
            schema=settings.SCHEMA
        )

        job_url = None
        try:
            job_url = get_job_url(w, job_run_id)
        except Exception:
            pass

        return {
            "message": f"Cleanup job submitted for {len(discarded)} resources",
            "job_run_id": job_run_id,
            "job_url": job_url,
            "count": len(discarded)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
