"""
Configuration management for FastAPI backend
"""
from pydantic_settings import BaseSettings
from typing import Optional
import os


class Settings(BaseSettings):
    """Application settings"""
    
    # Databricks Configuration - automatically injected by Databricks Apps
    DATABRICKS_HOST: Optional[str] = os.environ.get("DATABRICKS_HOST")
    DATABRICKS_CLIENT_ID: Optional[str] = os.environ.get("DATABRICKS_CLIENT_ID")
    DATABRICKS_CLIENT_SECRET: Optional[str] = os.environ.get("DATABRICKS_CLIENT_SECRET")
    DATABRICKS_HTTP_PATH: Optional[str] = os.environ.get("DATABRICKS_HTTP_PATH")
    
    # Catalog and Schema
    CATALOG: str = os.environ.get("CATALOG", "aamruthcatalog")
    SCHEMA: str = os.environ.get("SCHEMA", "retrieval_studio")
    
    # Notebook Paths - configurable via environment
    # Use workspace-relative paths (without user email) for portability
    BUILD_NOTEBOOK_PATH: str = os.environ.get(
        "BUILD_NOTEBOOK_PATH",
        "/Workspace/retrieval-studio/notebooks/build_notebook_v2"
    )
    EVAL_NOTEBOOK_PATH: str = os.environ.get(
        "EVAL_NOTEBOOK_PATH",
        "/Workspace/retrieval-studio/notebooks/eval_notebook"
    )
    
    # API Configuration
    API_PREFIX: str = "/api"
    DEBUG: bool = os.environ.get("DEBUG", "false").lower() == "true"
    
    class Config:
        env_file = ".env"
        case_sensitive = True


settings = Settings()
