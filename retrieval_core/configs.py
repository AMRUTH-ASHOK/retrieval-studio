import os
import re

class Config:
    # UC layout
    UC_CATALOG = os.environ.get("UC_CATALOG", "retrievalstudio")
    RAW_SCHEMA = os.environ.get("RAW_SCHEMA", "raw")
    CHUNKS_SCHEMA = os.environ.get("CHUNKS_SCHEMA", "chunks")
    INDEXES_SCHEMA = os.environ.get("INDEXES_SCHEMA", "indexes")

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
    def chunks_table(cls, project_name: str, strategy: str) -> str:
        p = cls._safe_name(project_name).lower()
        s = cls._safe_name(strategy).lower()
        return cls.fq_table(cls.CHUNKS_SCHEMA, f"rs_chunks_{p}_{s}")

    @classmethod
    def chunks_indexable_table(cls, project_name: str, strategy: str) -> str:
        return cls.chunks_table(project_name, strategy) + "__indexable"

    @classmethod
    def index_name(cls, project_name: str, strategy: str) -> str:
        p = cls._safe_name(project_name).lower()
        s = cls._safe_name(strategy).lower()
        return cls.fq_table(cls.INDEXES_SCHEMA, f"rs_index_{p}_{s}")

    @classmethod
    def index_registry_table(cls) -> str:
        return cls.fq_table(cls.INDEXES_SCHEMA, "rs_index_registry")

    @classmethod
    def eval_results_table(cls) -> str:
        return cls.fq_table(cls.RAW_SCHEMA, "rs_eval_results")

config = Config()
