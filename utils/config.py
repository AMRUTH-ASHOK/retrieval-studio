"""
Configuration Manager for Retrieval Studio
Handles automatic configuration, notebook path detection, and defaults
"""
import os
import sys
from typing import Optional
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config


class AppConfig:
    """Manages app configuration with automatic detection and sensible defaults"""
    
    def __init__(self):
        """Initialize configuration with auto-detection"""
        self.cfg = Config()
        
        # Initialize WorkspaceClient (may fail in some contexts, handle gracefully)
        try:
            self.w = WorkspaceClient(config=self.cfg)
        except Exception:
            self.w = None
        
        # Get app location (where the app is running from)
        self.app_location = self._detect_app_location()
        
        # Get current user workspace path
        self.user_workspace_path = self._get_user_workspace_path()
        
        # Notebook paths (relative to app location)
        self.build_notebook_path = self._get_notebook_path("build_notebook")
        self.eval_notebook_path = self._get_notebook_path("eval_notebook")
        
    def _detect_app_location(self) -> str:
        """Detect where the app is running from"""
        # In Databricks Apps, the app runs from its workspace path
        # Try to get from environment or use current working directory
        app_path = os.environ.get("DATABRICKS_APP_PATH", "")
        if app_path:
            return app_path
        
        # Fallback: try to detect from __file__ if available
        try:
            # This will be the app.py location
            app_dir = os.path.dirname(os.path.abspath(__file__))
            # Go up one level to get the app root
            app_root = os.path.dirname(app_dir)
            return app_root
        except:
            return ""
    
    def _get_user_workspace_path(self) -> str:
        """Get current user's workspace path"""
        try:
            if self.w is None:
                return "/Workspace/Users"
            current_user = self.w.current_user.me()
            username = current_user.user_name
            return f"/Workspace/Users/{username}"
        except Exception:
            # Fallback if we can't get user
            return "/Workspace/Users"
    
    def _get_notebook_path(self, notebook_name: str) -> str:
        """
        Get notebook path - hardcoded to specific user workspace location
        """
        # Hardcoded paths
        hardcoded_paths = {
            "build_notebook": "/Workspace/Users/amruth.ashok@databricks.com/retrieval-studio/retrieval-studio/notebooks/build_notebook",
            "eval_notebook": "/Workspace/Users/amruth.ashok@databricks.com/retrieval-studio/retrieval-studio/notebooks/eval_notebook"
        }
        
        # Check hardcoded paths first
        if notebook_name in hardcoded_paths:
            return hardcoded_paths[notebook_name]
        
        # Fallback: Check environment variable
        env_key = f"{notebook_name.upper()}_PATH"
        env_path = os.environ.get(env_key, "")
        if env_path:
            return env_path
        
        # Final fallback: construct path
        return f"/Workspace/Users/amruth.ashok@databricks.com/retrieval-studio/retrieval-studio/notebooks/{notebook_name}"
    
    def get_catalog(self) -> str:
        """Get catalog name from environment or default"""
        return os.environ.get("CATALOG", "main")
    
    def get_schema(self) -> str:
        """Get schema name from environment or default"""
        return os.environ.get("SCHEMA", "retrieval_studio")


# Global config instance (will be initialized in app.py)
_config_instance: Optional[AppConfig] = None


def get_config() -> AppConfig:
    """Get global config instance"""
    global _config_instance
    if _config_instance is None:
        _config_instance = AppConfig()
    return _config_instance


def init_config() -> AppConfig:
    """Initialize and return config instance"""
    global _config_instance
    _config_instance = AppConfig()
    return _config_instance
