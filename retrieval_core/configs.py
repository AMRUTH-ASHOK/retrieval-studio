import os

class Config:
    """
    Centralized configuration for Retrieval Studio.
    Accessible by both Backend API and Databricks Notebooks.
    """
    
    # -------------------------------------------------------------------------
    # Catalog & Schema
    # -------------------------------------------------------------------------
    # Defaults should match your reliable working environment
    CATALOG = os.environ.get("CATALOG", "main")
    SCHEMA = os.environ.get("SCHEMA", "retrieval_studio")

    # -------------------------------------------------------------------------
    # Databricks Environment (Injected by Databricks Apps)
    # -------------------------------------------------------------------------
    DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST")
    DATABRICKS_HTTP_PATH = os.environ.get("DATABRICKS_HTTP_PATH")
    
    # -------------------------------------------------------------------------
    # Project Paths
    # -------------------------------------------------------------------------
    # Base path for the project in the workspace
    # NOTE: This path is critical for import resolution and notebook locations
    BASE_PATH = "/Workspace/Users/amruth.ashok@databricks.com/retrieval-studio/retrieval-studio"
    
    # Notebook Paths
    # We use explicit environment variables if set (e.g. from app.yaml), otherwise default to calculated paths
    BUILD_NOTEBOOK_PATH = os.environ.get(
        "BUILD_NOTEBOOK_PATH", 
        f"{BASE_PATH}/notebooks/build_notebook_v2"
    )
    
    EVAL_NOTEBOOK_PATH = os.environ.get(
        "EVAL_NOTEBOOK_PATH", 
        f"{BASE_PATH}/notebooks/eval_notebook"
    )
    
    # -------------------------------------------------------------------------
    # MLflow
    # -------------------------------------------------------------------------
    # Base path for experiments
    EXPERIMENT_BASE_PATH = f"{BASE_PATH}/experiments"

    @classmethod
    def get_experiment_name(cls, project_name: str) -> str:
        """Generate a consistent MLflow experiment path for a project."""
        from re import sub
        safe_name = sub(r"[^a-zA-Z0-9_\-]", "_", project_name.strip())
        return f"{cls.EXPERIMENT_BASE_PATH}/{safe_name}"

# Global instance
config = Config()
