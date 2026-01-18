"""
Projects API endpoints
"""
from fastapi import APIRouter, Depends, HTTPException
from typing import List
import uuid

from backend.models.schemas import ProjectCreate, ProjectResponse
from backend.auth import get_sql_connector, get_user_sql_connector
from backend.config import settings
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from utils.postgres_state import get_all_projects, get_project, create_project, delete_project

router = APIRouter()


@router.get("/", response_model=List[ProjectResponse])
async def list_projects(sql_connector=Depends(get_user_sql_connector)):
    """Get all projects"""
    try:
        projects = get_all_projects()
        return projects
    except Exception as e:
        import traceback
        error_detail = f"Failed to list projects: {str(e)}\n{traceback.format_exc()}"
        raise HTTPException(status_code=500, detail=error_detail)


@router.get("/{project_id}", response_model=ProjectResponse)
async def get_project_by_id(project_id: str, sql_connector=Depends(get_user_sql_connector)):
    """Get project by ID"""
    try:
        project = get_project(project_id)
        if not project:
            raise HTTPException(status_code=404, detail="Project not found")
        return project
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/", response_model=ProjectResponse)
async def create_new_project(
    project: ProjectCreate,
    sql_connector=Depends(get_sql_connector)
):
    """Create a new project"""
    try:
        # Ensure tables are initialized first
        from utils.postgres_state import initialize_tables
        try:
            initialize_tables()
        except Exception as init_error:
            # Log but continue - tables might already exist
            import logging
            logging.warning(f"Table initialization warning (may already exist): {init_error}")
        
        project_id = str(uuid.uuid4())
        created_project = create_project(
            project_id=project_id,
            project_name=project.project_name,
            description=project.description,
            catalog=settings.CATALOG,
            schema=settings.SCHEMA
        )
        return created_project
    except Exception as e:
        import traceback
        error_detail = f"Failed to create project: {str(e)}\n{traceback.format_exc()}"
        raise HTTPException(status_code=500, detail=error_detail)


@router.delete("/{project_id}")
async def delete_project_by_id(
    project_id: str,
    sql_connector=Depends(get_sql_connector)
):
    """Delete a project and all associated builds and evaluations"""
    try:
        success = delete_project(project_id)
        if not success:
            raise HTTPException(status_code=404, detail="Project not found")

        return {
            "success": True,
            "message": f"Project {project_id} deleted successfully",
            "project_id": project_id
        }
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        error_detail = f"Failed to delete project: {str(e)}\n{traceback.format_exc()}"
        raise HTTPException(status_code=500, detail=error_detail)


@router.get("/{project_id}/mlflow")
async def get_mlflow_experiment_url(project_id: str, sql_connector=Depends(get_user_sql_connector)):
    """Get MLflow experiment URL for a project"""
    try:
        from retrieval_core.configs import config as core_config
        from backend.auth import get_workspace_client
        from databricks.sdk import WorkspaceClient
        from mlflow.tracking import MlflowClient

        # Get project to get the project name
        project = get_project(project_id)
        if not project:
            raise HTTPException(status_code=404, detail="Project not found")

        project_name = project.get("project_name", "default")

        # Get MLflow experiment name using the same pattern as notebooks
        experiment_name = core_config.get_experiment_name(project_name)

        # Get workspace client to get the workspace URL
        w = WorkspaceClient()
        workspace_url = w.config.host

        # Get organization/workspace ID for MLflow URL
        org_id = None

        # Method 1: Try from workspace config
        org_id = getattr(w.config, 'workspace_id', None)

        # Method 2: Extract from current user (fallback)
        if not org_id:
            try:
                current_user = w.current_user.me()
                # Try to get workspace ID from user context
                if hasattr(current_user, 'active_workspace_id'):
                    org_id = current_user.active_workspace_id
            except Exception:
                pass

        # Get MLflow client and experiment
        client = MlflowClient()
        experiment = client.get_experiment_by_name(experiment_name)

        if not experiment:
            # Experiment doesn't exist yet - return search URL as fallback
            import urllib.parse
            encoded_name = urllib.parse.quote(experiment_name)
            mlflow_url = f"{workspace_url}/#mlflow/experiments?searchFilter=name%3D%22{encoded_name}%22"
        else:
            # Construct direct MLflow experiment URL with /runs path and org_id
            # Format: https://<host>/ml/experiments/<experiment_id>/runs?o=<org_id>
            experiment_id = experiment.experiment_id
            if org_id:
                mlflow_url = f"{workspace_url}/ml/experiments/{experiment_id}/runs?o={org_id}"
            else:
                # Fallback without org_id parameter (still functional but less ideal)
                mlflow_url = f"{workspace_url}/ml/experiments/{experiment_id}/runs"

        return {
            "experiment_name": experiment_name,
            "mlflow_url": mlflow_url,
            "workspace_url": workspace_url
        }
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        error_detail = f"Failed to get MLflow experiment URL: {str(e)}\n{traceback.format_exc()}"
        raise HTTPException(status_code=500, detail=error_detail)


@router.get("/{project_id}/mlflow/runs")
async def get_mlflow_runs(project_id: str, sql_connector=Depends(get_user_sql_connector)):
    """Get MLflow runs for a project's experiment"""
    try:
        from retrieval_core.configs import config as core_config
        import mlflow
        from mlflow.tracking import MlflowClient

        # Get project to get the project name
        project = get_project(project_id)
        if not project:
            raise HTTPException(status_code=404, detail="Project not found")

        project_name = project.get("project_name", "default")

        # Get MLflow experiment name
        experiment_name = core_config.get_experiment_name(project_name)

        # Initialize MLflow client
        client = MlflowClient()

        # Get experiment
        try:
            experiment = client.get_experiment_by_name(experiment_name)
            if not experiment:
                return {
                    "experiment_name": experiment_name,
                    "runs": []
                }
        except Exception as e:
            print(f"Experiment not found: {e}")
            return {
                "experiment_name": experiment_name,
                "runs": []
            }

        # Search for all runs in this experiment
        runs = mlflow.search_runs(
            experiment_ids=[experiment.experiment_id],
            order_by=["start_time DESC"],
            max_results=100
        )

        # Convert DataFrame to list of dicts
        runs_list = []
        if not runs.empty:
            for _, run in runs.iterrows():
                run_data = {
                    "run_id": run.get("run_id"),
                    "run_name": run.get("tags.mlflow.runName", "Unnamed Run"),
                    "status": run.get("status"),
                    "start_time": int(run.get("start_time", 0)),
                    "end_time": int(run.get("end_time", 0)) if run.get("end_time") else None,
                    "role": run.get("tags.rs_role", "unknown"),
                    "metrics": {},
                    "params": {},
                    "tags": {}
                }

                # Extract metrics (columns starting with "metrics.")
                for col in runs.columns:
                    if col.startswith("metrics."):
                        metric_name = col.replace("metrics.", "")
                        value = run.get(col)
                        if value is not None and not (isinstance(value, float) and value != value):  # Check for NaN
                            run_data["metrics"][metric_name] = float(value)

                # Extract params (columns starting with "params.")
                for col in runs.columns:
                    if col.startswith("params."):
                        param_name = col.replace("params.", "")
                        value = run.get(col)
                        if value is not None:
                            run_data["params"][param_name] = str(value)

                # Extract tags (columns starting with "tags.")
                for col in runs.columns:
                    if col.startswith("tags."):
                        tag_name = col.replace("tags.", "")
                        value = run.get(col)
                        if value is not None:
                            run_data["tags"][tag_name] = str(value)

                runs_list.append(run_data)

        return {
            "experiment_name": experiment_name,
            "experiment_id": experiment.experiment_id,
            "runs": runs_list
        }

    except HTTPException:
        raise
    except Exception as e:
        import traceback
        error_detail = f"Failed to get MLflow runs: {str(e)}\n{traceback.format_exc()}"
        raise HTTPException(status_code=500, detail=error_detail)
