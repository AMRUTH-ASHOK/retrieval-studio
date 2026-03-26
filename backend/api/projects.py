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
from utils.postgres_state import get_all_projects, get_project, create_project, delete_project, update_project

router = APIRouter()


@router.get("", response_model=List[ProjectResponse], include_in_schema=False)
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


@router.post("", response_model=ProjectResponse, include_in_schema=False)
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
        from databricks.sdk import WorkspaceClient
        from mlflow.tracking import MlflowClient

        # Get project
        project = get_project(project_id)
        if not project:
            raise HTTPException(status_code=404, detail="Project not found")

        project_name = project.get("project_name", "default")

        # Set MLflow tracking URI
        import mlflow
        mlflow.set_tracking_uri("databricks")

        # Get experiment_id from projects table
        from utils.postgres_state import get_experiment_id_for_project

        experiment_id = get_experiment_id_for_project(project_id)

        client = MlflowClient()
        experiment = None
        experiment_name = None

        if experiment_id:
            print(f"[INFO] Using stored experiment_id: {experiment_id}")
            try:
                experiment = client.get_experiment(experiment_id)
                experiment_name = experiment.name
            except Exception as e:
                print(f"[WARNING] Failed to get experiment by ID: {e}")
                experiment = None

        if not experiment:
            # Fallback to name-based lookup
            experiment_name = core_config.get_experiment_name(project_name)
            experiment = client.get_experiment_by_name(experiment_name)

        # Get workspace URL and org ID early for error handling
        w = WorkspaceClient()
        workspace_url = w.config.host
        org_id = getattr(w.config, 'workspace_id', None)

        # Method 2: Extract from current user (fallback)
        if not org_id:
            try:
                current_user = w.current_user.me()
                if hasattr(current_user, 'active_workspace_id'):
                    org_id = current_user.active_workspace_id
            except Exception:
                pass

        if not experiment:
            # Experiment doesn't exist yet - create it
            import mlflow
            try:
                experiment_id = mlflow.create_experiment(experiment_name)
                print(f"[INFO] Created MLflow experiment: {experiment_name} with ID: {experiment_id}")
                experiment = client.get_experiment(experiment_id)
            except Exception as create_error:
                # Experiment might have been created by another process - try to get it again
                print(f"[WARNING] Failed to create experiment, attempting to retrieve: {create_error}")
                experiment = client.get_experiment_by_name(experiment_name)
                if experiment:
                    experiment_id = experiment.experiment_id
                else:
                    # Last resort - return search URL
                    import urllib.parse
                    encoded_name = urllib.parse.quote(experiment_name)
                    mlflow_url = f"{workspace_url}/#mlflow/experiments?searchFilter=name%3D%22{encoded_name}%22"
                    return {
                        "experiment_name": experiment_name,
                        "mlflow_url": mlflow_url,
                        "workspace_url": workspace_url
                    }
        else:
            experiment_id = experiment.experiment_id

        if experiment_id:
            try:
                update_project(project_id, experiment_id=experiment_id)
            except Exception as e:
                print(f"[WARNING] Failed to update project experiment_id: {e}")

        # Construct direct MLflow experiment URL with /runs path and org_id
        # Format: https://<host>/ml/experiments/<experiment_id>/runs?o=<org_id>
        if org_id:
            mlflow_url = f"{workspace_url}/ml/experiments/{experiment_id}/runs?o={org_id}"
        else:
            # Fallback without org_id parameter (still functional but less ideal)
            mlflow_url = f"{workspace_url}/ml/experiments/{experiment_id}/runs"

        return {
            "experiment_name": experiment_name,
            "experiment_id": experiment_id,  # NEW: Include in response for debugging
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

        print(f"[DEBUG] ===== MLflow Runs Lookup =====")
        print(f"[DEBUG] Project ID: {project_id}")
        print(f"[DEBUG] Project Name: {project_name}")

        # Set MLflow tracking URI
        mlflow.set_tracking_uri("databricks")

        # Initialize MLflow client
        client = MlflowClient()

        # Get experiment_id from projects table
        from utils.postgres_state import get_experiment_id_for_project

        experiment_id = get_experiment_id_for_project(project_id)

        experiment = None

        # Initialize experiment_name variable
        experiment_name = None
        
        if experiment_id:
            print(f"[DEBUG] Found stored experiment_id: {experiment_id}")
            try:
                experiment = client.get_experiment(experiment_id)
                experiment_name = experiment.name
                print(f"[DEBUG] ✓ Retrieved experiment by ID: {experiment_name}")
            except Exception as e:
                print(f"[WARNING] Failed to get experiment by stored ID {experiment_id}: {e}")
                experiment = None
        else:
            print(f"[DEBUG] No stored experiment_id found in projects table")

        # Fallback: Name-based lookup (for backward compatibility)
        if not experiment:
            experiment_name = core_config.get_experiment_name(project_name)
            print(f"[DEBUG] Falling back to name-based lookup: {experiment_name}")

            try:
                experiment = client.get_experiment_by_name(experiment_name)
                if experiment:
                    experiment_name = experiment.name  # Ensure it's set
                    print(f"[DEBUG] ✓ Found experiment by name: {experiment.experiment_id}")
            except Exception as e:
                print(f"[ERROR] Failed to get experiment by name: {e}")

        # If still not found, attempt to create the experiment
        if not experiment:
            print(f"[ERROR] Experiment not found by either ID or name")
            try:
                experiment_id = mlflow.create_experiment(experiment_name)
                experiment = client.get_experiment(experiment_id)
            except Exception as create_error:
                print(f"[ERROR] Failed to create experiment: {create_error}")
                print(f"[DEBUG] Listing all available experiments:")

                try:
                    all_experiments = client.search_experiments()
                    for exp in all_experiments[:20]:  # First 20
                        print(f"  - Name: {exp.name}, ID: {exp.experiment_id}, Lifecycle: {exp.lifecycle_stage}")
                except Exception as list_error:
                    print(f"[ERROR] Failed to list experiments: {list_error}")

                return {
                    "experiment_name": experiment_name or "unknown",
                    "runs": [],
                    "debug_info": "Experiment not found. Check logs for available experiments."
                }

        # Ensure experiment_name is set from experiment object
        if not experiment_name:
            experiment_name = experiment.name

        try:
            update_project(project_id, experiment_id=experiment.experiment_id)
        except Exception as e:
            print(f"[WARNING] Failed to update project experiment_id: {e}")
            
        print(f"[DEBUG] Using experiment: {experiment_name} (ID: {experiment.experiment_id})")

        # Search for all runs in this experiment
        runs = mlflow.search_runs(
            experiment_ids=[experiment.experiment_id],
            order_by=["start_time DESC"],
            max_results=100
        )

        print(f"[DEBUG] MLflow search returned {len(runs)} runs")

        if runs.empty:
            print(f"[WARNING] No runs found in experiment {experiment.experiment_id}")
            print(f"[DEBUG] This could mean:")
            print(f"  1. No evaluation jobs have been run yet")
            print(f"  2. Runs exist but are in a different experiment")
            print(f"  3. Runs exist but experiment_id doesn't match")
            return {
                "experiment_name": experiment_name,
                "experiment_id": experiment.experiment_id,
                "runs": [],
                "debug_info": "Experiment exists but has no runs. Ensure evaluation jobs completed successfully."
            }

        print(f"[DEBUG] Processing {len(runs)} runs from DataFrame...")
        print(f"[DEBUG] Available columns: {list(runs.columns)[:10]}...")  # First 10 columns

        # Count runs by role
        if 'tags.rs_role' in runs.columns:
            role_counts = runs['tags.rs_role'].value_counts().to_dict()
            print(f"[DEBUG] Runs by role: {role_counts}")

        # Convert DataFrame to list of dicts
        runs_list = []
        if not runs.empty:
            for _, run in runs.iterrows():
                # Safely convert timestamps to int (handle pandas Timestamp, None, etc.)
                start_time_val = run.get("start_time", 0)
                if start_time_val is not None:
                    try:
                        start_time = int(start_time_val) if not isinstance(start_time_val, (int, float)) else int(start_time_val)
                    except (ValueError, TypeError):
                        start_time = 0
                else:
                    start_time = 0
                
                end_time_val = run.get("end_time")
                end_time = None
                if end_time_val is not None:
                    try:
                        end_time = int(end_time_val) if not isinstance(end_time_val, (int, float)) else int(end_time_val)
                    except (ValueError, TypeError):
                        end_time = None
                
                run_data = {
                    "run_id": run.get("run_id"),
                    "run_name": run.get("tags.mlflow.runName", "Unnamed Run"),
                    "status": run.get("status"),
                    "start_time": start_time,
                    "end_time": end_time,
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

        print(f"[DEBUG] Successfully processed {len(runs_list)} runs")
        print(f"[DEBUG] Run roles: {[r.get('role') for r in runs_list[:5]]}")  # First 5 roles

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
