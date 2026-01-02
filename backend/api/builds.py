"""
Build jobs API endpoints
"""
from fastapi import APIRouter, Depends, HTTPException
from typing import List
import uuid
import json

from backend.models.schemas import BuildJobCreate, BuildJobResponse
from backend.auth import get_workspace_client, get_sql_connector
from backend.config import settings
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from utils.jobs import submit_build_job
from utils.state import get_run, get_project_runs

router = APIRouter()


@router.post("/", response_model=BuildJobResponse)
async def create_build_job(
    build_request: BuildJobCreate,
    w=Depends(get_workspace_client),
    sql_connector=Depends(get_sql_connector)
):
    """Submit a build job to Databricks"""
    try:
        from utils.state import create_run, update_run_state, get_project
        
        # Get project details
        project = get_project(sql_connector, settings.CATALOG, settings.SCHEMA, build_request.project_id)
        if not project:
            raise HTTPException(status_code=404, detail="Project not found")
        
        # Prepare config with catalog and schema
        config = build_request.config.model_dump()
        config["catalog"] = settings.CATALOG
        config["schema"] = settings.SCHEMA
        
        # Create run record first
        run_id = create_run(
            sql_connector=sql_connector,
            catalog=settings.CATALOG,
            schema=settings.SCHEMA,
            project_id=build_request.project_id,
            project_name=project["project_name"],
            config=config
        )
        
        # Submit the build job
        job_run_id = submit_build_job(
            w=w,
            notebook_path=settings.BUILD_NOTEBOOK_PATH,
            run_id=run_id,
            config=config
        )
        
        # Update run with job_run_id
        update_run_state(
            sql_connector=sql_connector,
            catalog=settings.CATALOG,
            schema=settings.SCHEMA,
            run_id=run_id,
            state="RUNNING",
            build_job_run_id=job_run_id
        )
        
        # Get the created run
        run = get_run(sql_connector, settings.CATALOG, settings.SCHEMA, run_id)
        return run
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{run_id}", response_model=BuildJobResponse)
async def get_build_job(run_id: str, sql_connector=Depends(get_sql_connector)):
    """Get build job by run ID"""
    try:
        run = get_run(sql_connector, settings.CATALOG, settings.SCHEMA, run_id)
        if not run:
            raise HTTPException(status_code=404, detail="Build job not found")
        return run
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/project/{project_id}", response_model=List[BuildJobResponse])
async def get_project_builds(project_id: str, sql_connector=Depends(get_sql_connector)):
    """Get all build jobs for a project"""
    try:
        runs = get_project_runs(sql_connector, settings.CATALOG, settings.SCHEMA, project_id)
        return runs
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
