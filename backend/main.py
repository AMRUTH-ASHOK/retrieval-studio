"""
FastAPI Backend for Retrieval Studio
Handles API endpoints, Databricks integration, and job orchestration
"""
from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from contextlib import asynccontextmanager
import os
import sys

# Add project paths
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from backend.api import projects, builds, evaluations, leaderboard, metadata, uploads, cleanup, studies
from backend.auth import get_sql_connector as get_app_sql_connector
from backend.config import settings


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Run startup tasks before serving requests."""
    try:
        from utils.postgres_state import initialize_tables
        initialize_tables()
    except Exception as e:
        print(f"⚠️  Startup migration warning (non-fatal): {e}")
    yield


app = FastAPI(
    title="Retrieval Studio API",
    description="API for RAG pipeline evaluation and data preparation",
    version="1.0.0",
    redirect_slashes=True,
    lifespan=lifespan
)

# CORS configuration for React frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure properly for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
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
