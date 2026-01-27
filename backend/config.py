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
    DATABRICKS_HOST: Optional[str] = os.environ.get("DATABRICKS_HOST")
    DATABRICKS_CLIENT_ID: Optional[str] = os.environ.get("DATABRICKS_CLIENT_ID")
    DATABRICKS_CLIENT_SECRET: Optional[str] = os.environ.get("DATABRICKS_CLIENT_SECRET")
    DATABRICKS_HTTP_PATH: Optional[str] = os.environ.get("DATABRICKS_HTTP_PATH")

    # Lakebase Postgres Configuration
    LAKEBASE_INSTANCE_NAME: str = os.environ.get("LAKEBASE_INSTANCE_NAME", "retrieval-studio-db")
    USE_POSTGRES: bool = os.environ.get("USE_POSTGRES", "true").lower() == "true"

    # Catalog and Schema
    CATALOG: str = core_config.UC_CATALOG
    SCHEMA: str = core_config.RAW_SCHEMA

    # Unity Catalog Volume for data uploads
    # Files are uploaded to this volume before build jobs run
    DATA_VOLUME_NAME: str = core_config.DATA_VOLUME_NAME

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
