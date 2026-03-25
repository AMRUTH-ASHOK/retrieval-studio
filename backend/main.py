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
import uuid

# Add project paths
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from backend.api import projects, builds, evaluations, leaderboard, metadata, uploads, cleanup, studies
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
        from utils.postgres_state import get_all_projects
        import logging
        logging.info(f"Fetching projects from Postgres")
        projects = get_all_projects()
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
    from utils.postgres_state import create_project, initialize_tables
    try:
        # Ensure tables are initialized first
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

@app.post("/api/builds", response_model=BuildJobResponse, include_in_schema=False)
async def create_build_job_no_trailing_slash(
    build_request: BuildJobCreate,
    w=Depends(get_workspace_client),
    sql_connector=Depends(get_app_sql_connector)
):
    """Create build job handler for /api/builds (without trailing slash)"""
    try:
        print(f"[DEBUG MAIN] === BUILD JOB SUBMISSION START ===")
        print(f"[DEBUG MAIN] Project ID: {build_request.project_id}")
        from utils.postgres_state import create_build as create_run, update_build_state as update_run_state, get_project, get_build as get_run
        from utils.jobs import submit_build_job

        # Get project details
        print(f"[DEBUG MAIN] Step 1: Getting project...")
        project = get_project(build_request.project_id)
        if not project:
            print(f"[DEBUG MAIN] ❌ Project not found")
            raise HTTPException(status_code=404, detail="Project not found")
        print(f"[DEBUG MAIN] ✅ Project found: {project.get('project_name')}")
        
        # Prepare config with catalog and schema
        print(f"[DEBUG MAIN] Step 2: Preparing config...")
        config = build_request.config.model_dump()
        config["catalog"] = settings.CATALOG
        config["schema"] = settings.SCHEMA
        config["project_name"] = project["project_name"]
        config["project_id"] = build_request.project_id
        print(f"[DEBUG MAIN] Config prepared")

        # Create run record first - generate UUID
        print(f"[DEBUG MAIN] Step 3: Creating build record...")
        run_id = str(uuid.uuid4())
        print(f"[DEBUG MAIN] Generated run_id: {run_id}")
        created_build = create_run(
            run_id=run_id,
            project_id=build_request.project_id,
            project_name=project["project_name"],
            config=config
        )
        print(f"[DEBUG MAIN] ✅ Build record created")

        # Submit the build job
        print(f"[DEBUG MAIN] Step 4: Submitting job to Databricks...")
        job_run_id = submit_build_job(
            w=w,
            notebook_path=settings.BUILD_NOTEBOOK_PATH,
            run_id=run_id,
            config=config
        )
        print(f"[DEBUG MAIN] ✅ Job submitted: {job_run_id}")

        # Update run with job_run_id
        print(f"[DEBUG MAIN] Step 5: Updating build with job_run_id...")
        update_run_state(
            run_id=run_id,
            state="RUNNING",
            job_run_id=job_run_id
        )
        print(f"[DEBUG MAIN] ✅ Build updated")

        # Get the created run
        print(f"[DEBUG MAIN] Step 6: Retrieving final build record...")
        run = get_run(run_id)
        print(f"[DEBUG MAIN] === BUILD JOB SUBMISSION SUCCESS ===")
        return run

    except HTTPException:
        print(f"[DEBUG MAIN] === BUILD JOB FAILED (HTTPException) ===")
        raise
    except Exception as e:
        print(f"[DEBUG MAIN] === BUILD JOB FAILED ===")
        print(f"[DEBUG MAIN] Error type: {type(e).__name__}")
        print(f"[DEBUG MAIN] Error: {e}")
        import traceback
        traceback.print_exc()
        error_detail = f"Failed to submit build job: {str(e)}\n{traceback.format_exc()}"
        raise HTTPException(status_code=500, detail=error_detail)

from backend.models.schemas import EvaluationCreate, EvaluationResponse

@app.post("/api/evaluations", response_model=EvaluationResponse, include_in_schema=False)
async def create_evaluation_no_trailing_slash(
    eval_request: EvaluationCreate,
    w=Depends(get_workspace_client),
    sql_connector=Depends(get_app_sql_connector)
):
    """Submit evaluation job handler for /api/evaluations (without trailing slash)"""
    try:
        from utils.jobs import submit_eval_job, get_job_run_status, get_job_url
        from utils.postgres_state import get_build as get_run, update_build_state as update_run_state
        import uuid

        # Get build run to extract project_name
        build_run = get_run(eval_request.run_id)
        if not build_run:
            raise HTTPException(status_code=404, detail="Build run not found")
        
        project_name = build_run.get("project_name", "default")
        top_k = eval_request.top_k or 10
        dataset_type = eval_request.dataset_type or "delta_table"
        auto_generate = eval_request.auto_generate_queries or False
        use_golden = eval_request.use_golden_dataset or False
        generate_golden = eval_request.generate_golden_dataset or False

        # Validate required parameters based on mode
        corpus_tables_list = None

        if use_golden:
            if not eval_request.golden_dataset_table:
                raise HTTPException(status_code=400, detail="golden_dataset_table is required when use_golden_dataset is true")
        elif auto_generate:
            try:
                from backend.utils.build_results import construct_build_results, extract_corpus_tables, extract_corpus_table

                build_config = build_run.get("config", {})
                project_name = build_run.get("project_name")

                if not project_name:
                    raise HTTPException(status_code=400, detail="Project name not found in build record.")

                build_sources = build_config.get("sources", [])
                if build_sources:
                    build_results = construct_build_results(
                        project_name=project_name,
                        strategies=[],
                        catalog=settings.CATALOG,
                        schema=settings.SCHEMA,
                        sources=build_sources
                    )
                    corpus_tables_list = extract_corpus_tables(build_results)
                    if corpus_tables_list:
                        eval_request.corpus_table = corpus_tables_list[0]["table"]
                else:
                    strategies = list(build_config.get("strategies", {}).keys()) or ["baseline"]
                    build_results = construct_build_results(
                        project_name=project_name,
                        strategies=strategies,
                        catalog=settings.CATALOG,
                        schema=settings.SCHEMA
                    )
                    eval_request.corpus_table = extract_corpus_table(build_results)

            except Exception as e:
                if isinstance(e, HTTPException):
                    raise
                raise HTTPException(status_code=400, detail=f"Failed to construct corpus table: {str(e)}")
        else:
            if not eval_request.queries_table:
                raise HTTPException(status_code=400, detail="queries_table is required when auto_generate_queries is false")

        build_parent_run_id = build_run.get("build_parent_run_id")
        eval_id = str(uuid.uuid4())

        notebook_path = settings.EVAL_NOTEBOOK_PATH

        job_run_id = submit_eval_job(
            w=w,
            notebook_path=notebook_path,
            build_run_id=eval_request.run_id,
            eval_id=eval_id,
            build_parent_run_id=build_parent_run_id,
            queries_table=eval_request.queries_table,
            corpus_table=eval_request.corpus_table,
            corpus_tables=corpus_tables_list,
            project_name=project_name,
            catalog=settings.CATALOG,
            schema=settings.SCHEMA,
            dataset_type=dataset_type,
            top_k=top_k,
            auto_generate_queries=auto_generate,
            num_queries=eval_request.num_queries or 50,
            query_style=eval_request.query_style or "keyword",
            compare_query_types=eval_request.compare_query_types or False,
            judge_model_endpoint=eval_request.judge_model_endpoint,
            generate_golden_dataset=generate_golden,
            use_golden_dataset=use_golden,
            golden_dataset_table=eval_request.golden_dataset_table,
            golden_dataset_id=eval_request.golden_dataset_id,
            golden_strategy=eval_request.golden_strategy,
            golden_query_type=eval_request.golden_query_type,
            golden_top_k=eval_request.golden_top_k
        )
        
        # Get job run details to return timestamps
        job_run_status = get_job_run_status(w, job_run_id)
        job_url = get_job_url(w, job_run_id)

        # Create evaluation record in Postgres
        from utils.postgres_state import create_evaluation, update_evaluation_state
        evaluation = create_evaluation(
            eval_id=eval_id,
            run_id=eval_request.run_id,
            project_id=build_run.get("project_id"),
            queries_table=eval_request.queries_table,
            corpus_table=eval_request.corpus_table,
            dataset_type=dataset_type,
            top_k=top_k,
            auto_generate_queries=auto_generate,
            num_queries=eval_request.num_queries,
            query_style=eval_request.query_style,
            compare_query_types=eval_request.compare_query_types,
            judge_model_endpoint=eval_request.judge_model_endpoint
        )

        # Update evaluation with job details
        evaluation = update_evaluation_state(
            eval_id=eval_id,
            state="RUNNING",
            job_run_id=job_run_id,
            job_url=job_url
        )

        # Return evaluation details
        return {
            "eval_id": eval_id,
            "run_id": eval_id,  # Frontend expects run_id for status polling
            "state": "RUNNING",
            "job_id": str(job_run_id),
            "job_url": job_url,
            "created_at": evaluation.get("created_at"),
            "updated_at": evaluation.get("updated_at")
        }
    except HTTPException:
        raise
    except Exception as e:
         import traceback
         error_detail = f"Failed to submit evaluation job: {str(e)}\n{traceback.format_exc()}"
         raise HTTPException(status_code=500, detail=error_detail)

# Include routers (after explicit routes)
app.include_router(projects.router, prefix="/api/projects", tags=["projects"])
app.include_router(builds.router, prefix="/api/builds", tags=["builds"])
app.include_router(evaluations.router, prefix="/api/evaluations", tags=["evaluations"])
app.include_router(leaderboard.router, prefix="/api/leaderboard", tags=["leaderboard"])
app.include_router(metadata.router, prefix="/api/metadata", tags=["metadata"])
app.include_router(uploads.router, prefix="/api/uploads", tags=["uploads"])
app.include_router(cleanup.router, prefix="/api/cleanup", tags=["cleanup"])
app.include_router(studies.router, prefix="/api/studies", tags=["studies"])

# Serve static files from React build
FRONTEND_BUILD_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "frontend", "dist")
print(f"[DEBUG] FRONTEND_BUILD_DIR: {FRONTEND_BUILD_DIR}")
print(f"[DEBUG] FRONTEND_BUILD_DIR exists: {os.path.exists(FRONTEND_BUILD_DIR)}")
if os.path.exists(FRONTEND_BUILD_DIR):
    assets_dir = os.path.join(FRONTEND_BUILD_DIR, "assets")
    print(f"[DEBUG] Assets directory: {assets_dir}")
    print(f"[DEBUG] Assets directory exists: {os.path.exists(assets_dir)}")
    if os.path.exists(assets_dir):
        print(f"[DEBUG] Assets directory contents: {os.listdir(assets_dir)}")
        app.mount("/assets", StaticFiles(directory=assets_dir), name="assets")
    else:
        print(f"[DEBUG] Assets directory not found - static files will not be served properly")
else:
    print(f"[DEBUG] Frontend build directory not found")

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
