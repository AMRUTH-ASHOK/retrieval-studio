import os
import re

class Config:
    # UC layout
    UC_CATALOG = os.environ.get("UC_CATALOG", "retrievalstudio")
    RAW_SCHEMA = os.environ.get("RAW_SCHEMA", "raw")
    CHUNKS_SCHEMA = os.environ.get("CHUNKS_SCHEMA", "chunks")
    INDEXES_SCHEMA = os.environ.get("INDEXES_SCHEMA", "indexes")
    
    # Volume for storing uploaded data files
    # Format: /Volumes/<catalog>/<schema>/<volume_name>
    DATA_VOLUME_NAME = os.environ.get("DATA_VOLUME_NAME", "retrieval_data")
    
    @classmethod
    def get_data_volume_path(cls) -> str:
        """Get the full path to the data volume"""
        return f"/Volumes/{cls.UC_CATALOG}/{cls.RAW_SCHEMA}/{cls.DATA_VOLUME_NAME}"
    
    @classmethod
    def get_upload_path(cls, project_name: str, run_id: str) -> str:
        """Get the upload path for a specific build run"""
        safe_project = cls._safe_name(project_name).lower()
        return f"{cls.get_data_volume_path()}/uploads/{safe_project}/{run_id}"

    # Workspace paths
    BASE_PATH = os.environ.get(
        "BASE_PATH",
        "/Workspace/Users/amruth.ashok@databricks.com/retrieval-studio/retrieval-studio"
    )

    BUILD_NOTEBOOK_PATH = os.environ.get("BUILD_NOTEBOOK_PATH", f"{BASE_PATH}/notebooks/build_notebook_v2")
    EVAL_NOTEBOOK_PATH = os.environ.get("EVAL_NOTEBOOK_PATH", f"{BASE_PATH}/notebooks/eval_notebook")

    # IMPORTANT: add leading "/" (your original was missing it)
    EXPERIMENT_BASE_PATH = os.environ.get(
        "EXPERIMENT_BASE_PATH",
        "/Workspace/Users/amruth.ashok@databricks.com/retrieval-studio/experiments"
    )

    @staticmethod
    def _safe_name(name: str) -> str:
        return re.sub(r"[^a-zA-Z0-9_\-]", "_", (name or "").strip()) or "default"

    @classmethod
    def get_experiment_name(cls, project_name: str) -> str:
        return f"{cls.EXPERIMENT_BASE_PATH}/{cls._safe_name(project_name)}"

    @classmethod
    def fq_schema(cls, schema: str) -> str:
        return f"{cls.UC_CATALOG}.{schema}"

    @classmethod
    def fq_table(cls, schema: str, table: str) -> str:
        return f"{cls.UC_CATALOG}.{schema}.{table}"

    # Stable per-project tables
    @classmethod
    def chunks_table(cls, project_name: str, strategy: str, source_name: str = None) -> str:
        p = cls._safe_name(project_name).lower()
        s = cls._safe_name(strategy).lower()
        if source_name:
            src = cls._safe_name(source_name).lower()
            return cls.fq_table(cls.CHUNKS_SCHEMA, f"rs_chunks_{p}_{src}_{s}")
        return cls.fq_table(cls.CHUNKS_SCHEMA, f"rs_chunks_{p}_{s}")

    @classmethod
    def chunks_indexable_table(cls, project_name: str, strategy: str, source_name: str = None) -> str:
        return cls.chunks_table(project_name, strategy, source_name) + "__indexable"

    @classmethod
    def index_name(cls, project_name: str, strategy: str, source_name: str = None) -> str:
        p = cls._safe_name(project_name).lower()
        s = cls._safe_name(strategy).lower()
        if source_name:
            src = cls._safe_name(source_name).lower()
            return cls.fq_table(cls.INDEXES_SCHEMA, f"rs_index_{p}_{src}_{s}")
        return cls.fq_table(cls.INDEXES_SCHEMA, f"rs_index_{p}_{s}")

    @classmethod
    def index_registry_table(cls) -> str:
        return cls.fq_table(cls.INDEXES_SCHEMA, "rs_index_registry")

    @classmethod
    def eval_results_table(cls) -> str:
        return cls.fq_table(cls.RAW_SCHEMA, "rs_eval_results")

    @classmethod
    def golden_dataset_table(cls, project_name: str) -> str:
        p = cls._safe_name(project_name).lower()
        return cls.fq_table(cls.RAW_SCHEMA, f"rs_golden_{p}")

config = Config()
