"""
Study management API endpoints
"""
from fastapi import APIRouter, Depends, HTTPException
from typing import List, Optional
from pydantic import BaseModel

from backend.auth import get_sql_connector
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
from utils.postgres_state import (
    create_study, get_studies, get_study, delete_study,
    add_build_to_study, add_evaluation_to_study,
    get_study_builds, get_study_evaluations
)

router = APIRouter()


class StudyCreate(BaseModel):
    project_id: str
    study_name: str
    description: Optional[str] = None


class StudyAssociate(BaseModel):
    build_run_ids: List[str] = []
    eval_ids: List[str] = []


@router.get("/project/{project_id}")
async def list_studies(project_id: str, sql_connector=Depends(get_sql_connector)):
    """Get all studies for a project"""
    try:
        return get_studies(project_id)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/")
async def create_new_study(body: StudyCreate, sql_connector=Depends(get_sql_connector)):
    """Create a new study"""
    try:
        result = create_study(
            project_id=body.project_id,
            study_name=body.study_name,
            description=body.description
        )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{study_id}")
async def get_study_detail(study_id: str, sql_connector=Depends(get_sql_connector)):
    """Get study details with associated builds and evaluations"""
    try:
        study = get_study(study_id)
        if not study:
            raise HTTPException(status_code=404, detail="Study not found")

        builds = get_study_builds(study_id)
        evaluations = get_study_evaluations(study_id)

        return {
            **study,
            "builds": builds,
            "evaluations": evaluations
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{study_id}/associate")
async def associate_items(study_id: str, body: StudyAssociate, sql_connector=Depends(get_sql_connector)):
    """Associate builds and evaluations with a study"""
    try:
        for build_id in body.build_run_ids:
            add_build_to_study(study_id, build_id)
        for eval_id in body.eval_ids:
            add_evaluation_to_study(study_id, eval_id)
        return {"message": "Associated successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/{study_id}")
async def delete_study_endpoint(study_id: str, sql_connector=Depends(get_sql_connector)):
    """Delete a study"""
    try:
        success = delete_study(study_id)
        if not success:
            raise HTTPException(status_code=404, detail="Study not found")
        return {"message": "Study deleted"}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
