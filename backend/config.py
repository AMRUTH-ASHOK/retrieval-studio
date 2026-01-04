"""
Configuration management for FastAPI backend
"""
from pydantic_settings import BaseSettings
from typing import Optional
import os
import sys

# Ensure we can import from retrieval_core
# This assumes the backend is running with the project root in sys.path (standard for uvicorn)
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from retrieval_core.configs import config as core_config

class Settings(BaseSettings):
    """Application settings"""
    
    # Databricks Configuration
    DATABRICKS_HOST: Optional[str] = core_config.DATABRICKS_HOST
    DATABRICKS_CLIENT_ID: Optional[str] = os.environ.get("DATABRICKS_CLIENT_ID")
    DATABRICKS_CLIENT_SECRET: Optional[str] = os.environ.get("DATABRICKS_CLIENT_SECRET")
    DATABRICKS_HTTP_PATH: Optional[str] = core_config.DATABRICKS_HTTP_PATH
    
    # Catalog and Schema
    CATALOG: str = core_config.CATALOG
    SCHEMA: str = core_config.SCHEMA
    
    # Notebook Paths
    BUILD_NOTEBOOK_PATH: str = core_config.BUILD_NOTEBOOK_PATH
    EVAL_NOTEBOOK_PATH: str = core_config.EVAL_NOTEBOOK_PATH
    
    # API Configuration
    API_PREFIX: str = "/api"
    DEBUG: bool = os.environ.get("DEBUG", "false").lower() == "true"
    
    class Config:
        env_file = ".env"
        case_sensitive = True


settings = Settings()
