"""
FastAPI Backend for Retrieval Studio
Handles API endpoints, Databricks integration, and job orchestration
"""
from fastapi import FastAPI, HTTPException, Depends, Header, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from typing import Optional, List, Dict, Any
import os
import sys

# Add project paths
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from backend.api import projects, builds, evaluations, leaderboard, metadata
from backend.auth import get_workspace_client, get_vector_search_client, get_sql_connector as get_app_sql_connector
from backend.config import settings

app = FastAPI(
    title="Retrieval Studio API",
    description="API for RAG pipeline evaluation and data preparation",
    version="1.0.0",
    redirect_slashes=True
)

# CORS configuration for React frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure properly for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Explicit route handlers for endpoints without trailing slash (GET/POST don't follow redirects)
# These must be defined BEFORE routers to ensure they match first
from backend.models.schemas import ProjectCreate, ProjectResponse, BuildJobCreate, BuildJobResponse

@app.get("/api/projects", response_model=List[ProjectResponse], include_in_schema=False)
async def list_projects_no_trailing_slash(sql_connector=Depends(get_app_sql_connector)):
    """List projects handler for /api/projects (without trailing slash)"""
    try:
        from utils.state import get_all_projects
        import logging
        logging.info(f"Fetching projects from {settings.CATALOG}.{settings.SCHEMA}")
        projects = get_all_projects(sql_connector, settings.CATALOG, settings.SCHEMA)
        logging.info(f"Found {len(projects)} projects")
        return projects
    except Exception as e:
        import traceback
        error_detail = f"Failed to list projects: {str(e)}\n{traceback.format_exc()}"
        raise HTTPException(status_code=500, detail=error_detail)

@app.post("/api/projects", response_model=ProjectResponse, include_in_schema=False)
async def create_project_no_trailing_slash(
    project: ProjectCreate,
    sql_connector=Depends(get_app_sql_connector)
):
    """Create project handler for /api/projects (without trailing slash)"""
    import uuid
    from utils.state import create_project, initialize_tables
    try:
        # Ensure tables are initialized first
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

@app.post("/api/builds", response_model=BuildJobResponse, include_in_schema=False)
async def create_build_job_no_trailing_slash(
    build_request: BuildJobCreate,
    w=Depends(get_workspace_client),
    sql_connector=Depends(get_app_sql_connector)
):
    """Create build job handler for /api/builds (without trailing slash)"""
    try:
        from utils.state import create_run, update_run_state, get_project, get_run
        from utils.jobs import submit_build_job
        
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
        import traceback
        error_detail = f"Failed to submit build job: {str(e)}\n{traceback.format_exc()}"
        raise HTTPException(status_code=500, detail=error_detail)

# Include routers (after explicit routes)
app.include_router(projects.router, prefix="/api/projects", tags=["projects"])
app.include_router(builds.router, prefix="/api/builds", tags=["builds"])
app.include_router(evaluations.router, prefix="/api/evaluations", tags=["evaluations"])
app.include_router(leaderboard.router, prefix="/api/leaderboard", tags=["leaderboard"])
app.include_router(metadata.router, prefix="/api/metadata", tags=["metadata"])

# Serve static files from React build
FRONTEND_BUILD_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "frontend", "dist")
if os.path.exists(FRONTEND_BUILD_DIR):
    app.mount("/assets", StaticFiles(directory=os.path.join(FRONTEND_BUILD_DIR, "assets")), name="assets")

@app.get("/api/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy"}

@app.post("/api/init")
async def initialize_database(sql_connector=Depends(get_app_sql_connector)):
    """Initialize database tables"""
    try:
        from utils.state import initialize_tables
        initialize_tables(sql_connector, settings.CATALOG, settings.SCHEMA)
        return {"status": "success", "message": "Database tables initialized"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to initialize database: {str(e)}")

@app.get("/api/config")
async def get_config():
    """Get application configuration"""
    return {
        "catalog": settings.CATALOG,
        "schema": settings.SCHEMA,
        "notebook_paths": {
            "build": settings.BUILD_NOTEBOOK_PATH,
            "eval": settings.EVAL_NOTEBOOK_PATH
        }
    }

# Serve React SPA - must be after API routes
@app.get("/")
async def serve_root():
    """Serve React app"""
    index_path = os.path.join(FRONTEND_BUILD_DIR, "index.html")
    if os.path.exists(index_path):
        return FileResponse(index_path)
    return {"message": "Retrieval Studio API", "version": "1.0.0", "docs": "/docs"}

# Catch-all route for SPA - only matches GET requests, excludes API routes
@app.get("/{full_path:path}")
async def serve_spa(full_path: str):
    """Serve React SPA for all non-API routes"""
    # Explicitly skip API routes, docs, and openapi
    if (full_path.startswith("api/") or 
        full_path.startswith("docs") or 
        full_path.startswith("openapi") or
        full_path == "favicon.ico"):
        raise HTTPException(status_code=404, detail="Not found")
    
    # Try to serve static file first
    file_path = os.path.join(FRONTEND_BUILD_DIR, full_path)
    if os.path.exists(file_path) and os.path.isfile(file_path):
        return FileResponse(file_path)
    
    # Fall back to index.html for SPA routing
    index_path = os.path.join(FRONTEND_BUILD_DIR, "index.html")
    if os.path.exists(index_path):
        return FileResponse(index_path)
    
    raise HTTPException(status_code=404, detail="Not found")
