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
from utils.jobs import submit_build_job, get_job_run_status, get_job_url, get_job_run_output
from utils.postgres_state import get_build as get_run, get_builds_by_project as get_project_runs

router = APIRouter()


@router.post("", response_model=BuildJobResponse, include_in_schema=False)
@router.post("/", response_model=BuildJobResponse)
async def create_build_job(
    build_request: BuildJobCreate,
    w=Depends(get_workspace_client),
    sql_connector=Depends(get_sql_connector)
):
    """Submit a build job to Databricks"""
    try:
        print(f"[DEBUG] === BUILD JOB SUBMISSION START ===")
        print(f"[DEBUG] Project ID: {build_request.project_id}")

        from utils.postgres_state import create_build as create_run, update_build_state as update_run_state, get_project

        # Get project details
        print(f"[DEBUG] Step 1: Getting project details...")
        project = get_project(build_request.project_id)
        if not project:
            print(f"[DEBUG] ❌ Project not found: {build_request.project_id}")
            raise HTTPException(status_code=404, detail="Project not found")
        print(f"[DEBUG] ✅ Project found: {project.get('project_name')}")
        
        # Prepare config with catalog and schema
        print(f"[DEBUG] Step 2: Preparing config...")
        config = build_request.config.model_dump()
        config["catalog"] = settings.CATALOG
        config["schema"] = settings.SCHEMA
        config["project_name"] = project["project_name"]
        config["project_id"] = build_request.project_id
        print(f"[DEBUG] Config prepared: {list(config.keys())}")

        # Create run record first - generate UUID
        print(f"[DEBUG] Step 3: Creating build record in Postgres...")
        run_id = str(uuid.uuid4())
        print(f"[DEBUG] Generated run_id: {run_id}")

        try:
            created_build = create_run(
                run_id=run_id,
                project_id=build_request.project_id,
                project_name=project["project_name"],
                config=config
            )
            print(f"[DEBUG] ✅ Build record created in Postgres")
        except Exception as e:
            print(f"[DEBUG] ❌ Failed to create build record: {type(e).__name__}: {e}")
            import traceback
            traceback.print_exc()
            raise
        
        # Submit the build job
        print(f"[DEBUG] Step 4: Submitting build job to Databricks...")
        try:
            job_run_id = submit_build_job(
                w=w,
                notebook_path=settings.BUILD_NOTEBOOK_PATH,
                run_id=run_id,
                config=config
            )
            print(f"[DEBUG] ✅ Job submitted. Job run ID: {job_run_id}")
        except Exception as e:
            print(f"[DEBUG] ❌ Failed to submit job: {type(e).__name__}: {e}")
            import traceback
            traceback.print_exc()
            raise

        # Update run with job_run_id
        print(f"[DEBUG] Step 5: Updating build with job_run_id...")
        try:
            update_run_state(
                run_id=run_id,
                state="RUNNING",
                job_run_id=job_run_id
            )
            print(f"[DEBUG] ✅ Build updated with job_run_id")
        except Exception as e:
            print(f"[DEBUG] ❌ Failed to update build state: {type(e).__name__}: {e}")
            import traceback
            traceback.print_exc()
            raise

        # Get job URL
        print(f"[DEBUG] Step 6: Getting job URL...")
        job_url = get_job_url(w, job_run_id)
        print(f"[DEBUG] Job URL: {job_url}")
        
        # Get the created run and transform to BuildJobResponse format
        print(f"[DEBUG] Step 7: Retrieving build record...")
        run = get_run(run_id)
        if not run:
            print(f"[DEBUG] ❌ Failed to retrieve build record")
            raise HTTPException(status_code=500, detail="Failed to retrieve created run")

        print(f"[DEBUG] ✅ Build record retrieved")

        # Transform to BuildJobResponse format
        response = {
            "run_id": run.get("run_id"),
            "project_id": run.get("project_id"),
            "state": run.get("state", "RUNNING"),
            "job_id": str(run.get("job_run_id")) if run.get("job_run_id") else None,
            "job_url": job_url,
            "config": run.get("config", {}),
            "created_at": run.get("created_at"),
            "updated_at": run.get("updated_at"),
        }

        print(f"[DEBUG] === BUILD JOB SUBMISSION SUCCESS ===")
        return response

    except HTTPException:
        print(f"[DEBUG] === BUILD JOB SUBMISSION FAILED (HTTPException) ===")
        raise
    except Exception as e:
        print(f"[DEBUG] === BUILD JOB SUBMISSION FAILED ===")
        print(f"[DEBUG] Error type: {type(e).__name__}")
        print(f"[DEBUG] Error message: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{run_id}", response_model=BuildJobResponse)
async def get_build_job(run_id: str, sql_connector=Depends(get_sql_connector), w=Depends(get_workspace_client)):
    """Get build job by run ID"""
    try:
        run = get_run(run_id)
        if not run:
            raise HTTPException(status_code=404, detail="Build job not found")
        
        # Generate job URL if job_run_id exists
        job_url = None
        job_run_id = run.get("job_run_id")
        if job_run_id:
            try:
                job_url = get_job_url(w, job_run_id)
            except Exception:
                pass  # If URL generation fails, continue without it
        
        # Transform to BuildJobResponse format
        return {
            "run_id": run.get("run_id"),
            "project_id": run.get("project_id"),
            "state": run.get("state", "UNKNOWN"),
            "job_id": str(job_run_id) if job_run_id else None,
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
        runs = get_project_runs(project_id)
        
        # Transform raw SQL results to BuildJobResponse format
        transformed_runs = []
        for row in runs:
            job_run_id = row.get("job_run_id")
            job_url = None
            
            # Generate job URL if job_run_id exists (only for recent/running builds to avoid performance issues)
            # Skip URL generation for old completed builds
            state = row.get("state", "UNKNOWN")
            if job_run_id and state in ["RUNNING", "PENDING", "SUCCESS"]:
                try:
                    job_url = get_job_url(w, job_run_id)
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
                "job_id": str(job_run_id) if job_run_id else None,
                "job_url": job_url,
                "config": config_dict,
                "created_at": row.get("created_at"),
                "updated_at": row.get("updated_at"),
            })
        
        return transformed_runs
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{run_id}/results")
async def get_build_results(run_id: str, w=Depends(get_workspace_client), sql_connector=Depends(get_sql_connector)):
    """Get build job results from PostgreSQL database"""
    try:
        run = get_run(run_id)
        if not run:
            raise HTTPException(status_code=404, detail="Build job not found")

        # Get results from PostgreSQL (stored when build completes)
        build_results = run.get("results")

        if build_results:
            return {
                "run_id": run_id,
                "results": build_results,
                "status": run.get("state", "UNKNOWN")
            }
        else:
            return {
                "run_id": run_id,
                "results": None,
                "message": "Build results not available yet. Results are stored when build completes successfully."
            }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{run_id}")
async def delete_build_job(run_id: str, sql_connector=Depends(get_sql_connector)):
    """Delete a build and all its associated evaluations"""
    try:
        from utils.postgres_state import delete_build
        success = delete_build(run_id)
        if not success:
            raise HTTPException(status_code=404, detail="Build not found")
        return {"success": True, "message": f"Build {run_id} and its evaluations deleted"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{run_id}/status")
async def get_build_job_status(run_id: str, w=Depends(get_workspace_client), sql_connector=Depends(get_sql_connector)):
    """Get build job status with job URL

    IMPORTANT: This endpoint checks ONLY the BUILD job status (job_run_id).
    Evaluation job status is tracked separately via /api/evaluations/{eval_id}/status
    """
    try:
        run = get_run(run_id)
        if not run:
            raise HTTPException(status_code=404, detail="Build job not found")

        # ONLY check the BUILD job (job_run_id), NOT the evaluation job (eval_job_run_id)
        job_run_id = run.get("job_run_id")
        if not job_run_id:
            return {
                "run_id": run_id,
                "state": run.get("state", "UNKNOWN"),
                "job_url": None,
                "status": None,
                "start_time": None,
            }

        # Get BUILD job status from Databricks (not evaluation job)
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

        # When Databricks says SUCCESS, check the notebook's exit value
        # for the actual build status (PARTIAL_SUCCESS, FAILED, etc.)
        notebook_output = None
        error_message = None
        if mapped_state == "SUCCESS":
            try:
                output = get_job_run_output(w, job_run_id)
                notebook_output = output.get("results")
                if isinstance(notebook_output, dict):
                    nb_status = notebook_output.get("status", "SUCCESS")
                    if nb_status == "FAILED":
                        mapped_state = "FAILED"
                        failed_items = {
                            k: v.get("error", "unknown error")
                            for k, v in notebook_output.get("results", {}).items()
                            if isinstance(v, dict) and v.get("status") == "FAILED"
                        }
                        error_message = f"All sources/strategies failed: {failed_items}" if failed_items else "Build failed"
                    elif nb_status == "PARTIAL_SUCCESS":
                        failed_items = {
                            k: v.get("error", "unknown error")
                            for k, v in notebook_output.get("results", {}).items()
                            if isinstance(v, dict) and v.get("status") == "FAILED"
                        }
                        if failed_items:
                            total = len(notebook_output.get("results", {}))
                            if len(failed_items) == total:
                                mapped_state = "FAILED"
                                error_message = f"All {total} source-strategy combos failed: {failed_items}"
                            else:
                                mapped_state = "PARTIAL_SUCCESS"
                                error_message = f"{len(failed_items)}/{total} failed: {failed_items}"
                        else:
                            mapped_state = "PARTIAL_SUCCESS"
            except Exception as e:
                print(f"[WARNING] Failed to inspect notebook output: {e}")
        
        if mapped_state != current_state:
            from utils.postgres_state import update_build_state as update_run_state
            update_run_state(
                run_id=run_id,
                state=mapped_state,
                error_message=error_message
            )
        
        return {
            "run_id": run_id,
            "state": mapped_state,
            "job_url": job_url,
            "status": job_status,
            "start_time": job_status.get("start_time"),
            "error_message": error_message,
            "notebook_output": notebook_output,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
