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
from utils.state import get_all_projects, get_project, create_project

router = APIRouter()


@router.get("/", response_model=List[ProjectResponse])
async def list_projects(sql_connector=Depends(get_user_sql_connector)):
    """Get all projects"""
    try:
        projects = get_all_projects(sql_connector, settings.CATALOG, settings.SCHEMA)
        return projects
    except Exception as e:
        import traceback
        error_detail = f"Failed to list projects: {str(e)}\n{traceback.format_exc()}"
        raise HTTPException(status_code=500, detail=error_detail)


@router.get("/{project_id}", response_model=ProjectResponse)
async def get_project_by_id(project_id: str, sql_connector=Depends(get_user_sql_connector)):
    """Get project by ID"""
    try:
        project = get_project(sql_connector, settings.CATALOG, settings.SCHEMA, project_id)
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
        from utils.state import initialize_tables
        try:
            initialize_tables(sql_connector, settings.CATALOG, settings.SCHEMA)
        except Exception as init_error:
            # Log but continue - tables might already exist
            import logging
            logging.warning(f"Table initialization warning (may already exist): {init_error}")
        
        project_id = str(uuid.uuid4())
        created_project = create_project(
            sql_connector,
            settings.CATALOG,
            settings.SCHEMA,
            project_id,
            project.project_name,
            project.description
        )
        return created_project
    except Exception as e:
        import traceback
        error_detail = f"Failed to create project: {str(e)}\n{traceback.format_exc()}"
        raise HTTPException(status_code=500, detail=error_detail)
