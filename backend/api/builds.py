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
from utils.jobs import submit_build_job, get_job_run_status, get_job_url
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
        config["project_name"] = project["project_name"]
        
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
        
        # Get job URL
        job_url = get_job_url(w, job_run_id)
        
        # Get the created run and transform to BuildJobResponse format
        run = get_run(sql_connector, settings.CATALOG, settings.SCHEMA, run_id)
        if not run:
            raise HTTPException(status_code=500, detail="Failed to retrieve created run")
        
        # Transform to BuildJobResponse format
        return {
            "run_id": run.get("run_id"),
            "project_id": run.get("project_id"),
            "state": run.get("state", "RUNNING"),
            "job_id": str(run.get("build_job_run_id")) if run.get("build_job_run_id") else None,
            "job_url": job_url,
            "config": run.get("config", {}),
            "created_at": run.get("created_at"),
            "updated_at": run.get("updated_at"),
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{run_id}", response_model=BuildJobResponse)
async def get_build_job(run_id: str, sql_connector=Depends(get_sql_connector), w=Depends(get_workspace_client)):
    """Get build job by run ID"""
    try:
        run = get_run(sql_connector, settings.CATALOG, settings.SCHEMA, run_id)
        if not run:
            raise HTTPException(status_code=404, detail="Build job not found")
        
        # Generate job URL if build_job_run_id exists
        job_url = None
        build_job_run_id = run.get("build_job_run_id")
        if build_job_run_id:
            try:
                job_url = get_job_url(w, build_job_run_id)
            except Exception:
                pass  # If URL generation fails, continue without it
        
        # Transform to BuildJobResponse format
        return {
            "run_id": run.get("run_id"),
            "project_id": run.get("project_id"),
            "state": run.get("state", "UNKNOWN"),
            "job_id": str(build_job_run_id) if build_job_run_id else None,
            "job_url": job_url,
            "config": run.get("config", {}),
            "created_at": run.get("created_at"),
            "updated_at": run.get("updated_at"),
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/project/{project_id}", response_model=List[BuildJobResponse])
async def get_project_builds(project_id: str, sql_connector=Depends(get_sql_connector), w=Depends(get_workspace_client)):
    """Get all build jobs for a project"""
    try:
        runs = get_project_runs(sql_connector, settings.CATALOG, settings.SCHEMA, project_id)
        
        # Transform raw SQL results to BuildJobResponse format
        transformed_runs = []
        for row in runs:
            build_job_run_id = row.get("build_job_run_id")
            job_url = None
            
            # Generate job URL if build_job_run_id exists (only for recent/running builds to avoid performance issues)
            # Skip URL generation for old completed builds
            state = row.get("state", "UNKNOWN")
            if build_job_run_id and state in ["RUNNING", "PENDING", "SUCCESS"]:
                try:
                    job_url = get_job_url(w, build_job_run_id)
                except Exception:
                    # If URL generation fails, continue without it
                    pass
            
            # Parse config JSON string to dict
            config_str = row.get("config", "{}")
            try:
                if isinstance(config_str, str):
                    config_dict = json.loads(config_str) if config_str else {}
                else:
                    config_dict = config_str or {}
            except (json.JSONDecodeError, TypeError):
                config_dict = {}
            
            transformed_runs.append({
                "run_id": row.get("run_id"),
                "project_id": row.get("project_id"),
                "state": state,
                "job_id": str(build_job_run_id) if build_job_run_id else None,
                "job_url": job_url,
                "config": config_dict,
                "created_at": row.get("created_at"),
                "updated_at": row.get("updated_at"),
            })
        
        return transformed_runs
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{run_id}/status")
async def get_build_job_status(run_id: str, w=Depends(get_workspace_client), sql_connector=Depends(get_sql_connector)):
    """Get build job status with job URL"""
    try:
        run = get_run(sql_connector, settings.CATALOG, settings.SCHEMA, run_id)
        if not run:
            raise HTTPException(status_code=404, detail="Build job not found")
        
        job_run_id = run.get("build_job_run_id")
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
