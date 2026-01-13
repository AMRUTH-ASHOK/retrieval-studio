"""
Authentication and client initialization for Databricks
"""
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from databricks.vector_search.client import VectorSearchClient
from fastapi import Header, HTTPException
from typing import Optional
import os

from backend.config import settings
import sys
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from utils.sql_connector import get_sql_connector as util_get_sql_connector


def get_workspace_client() -> WorkspaceClient:
    """
    Initialize WorkspaceClient with app authorization
    Uses service principal credentials from environment
    """
    try:
        cfg = Config()
        return WorkspaceClient(config=cfg)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to initialize WorkspaceClient: {e}"
        )


def get_vector_search_client() -> VectorSearchClient:
    """
    Initialize VectorSearchClient with service principal credentials
    """
    try:
        client_id = os.environ.get("DATABRICKS_CLIENT_ID")
        client_secret = os.environ.get("DATABRICKS_CLIENT_SECRET")
        
        if not client_id or not client_secret:
            raise ValueError("DATABRICKS_CLIENT_ID and DATABRICKS_CLIENT_SECRET required")
        
        cfg = Config()
        return VectorSearchClient(
            workspace_url=cfg.host,
            service_principal_client_id=client_id,
            service_principal_client_secret=client_secret
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to initialize VectorSearchClient: {e}"
        )


def get_sql_connector():
    """Get SQL connector with app authorization"""
    try:
        return util_get_sql_connector(
            catalog=settings.CATALOG,
            schema=settings.SCHEMA,
            use_user_auth=False
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to initialize SQL connector: {e}"
        )


def get_user_sql_connector(
    x_forwarded_access_token: Optional[str] = Header(None)
):
    """Get SQL connector with user authorization"""
    if not x_forwarded_access_token:
        # Fallback to app authorization
        return get_sql_connector()
    
    try:
        return util_get_sql_connector(
            catalog=settings.CATALOG,
            schema=settings.SCHEMA,
            use_user_auth=True,
            user_token=x_forwarded_access_token
        )
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to initialize user SQL connector: {e}"
        )
