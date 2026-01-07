from databricks.vector_search.client import VectorSearchClient
import time

DEFAULT_COLUMNS = [
    "chunk_id",
    "chunk_text",
    "metadata_json",
    "strategy",
    "chunk_type",
    "doc_id",
    "doc_name",
    "parent_chunk_id",
    "run_id",
    "project",
]

def create_vs_index(
    vs_client: VectorSearchClient,
    endpoint_name: str,
    index_name: str,
    source_table_name: str,
    embedding_model_endpoint_name: str,
    primary_key: str = "chunk_id",
    embedding_source_column: str = "chunk_text",
    pipeline_type: str = "TRIGGERED",
):
    """
    Create (or ensure) a Delta Sync Vector Search index.
    Assumes the index does not exist; if your environment needs idempotency,
    wrap in a try/get_index and skip if exists.
    """
    vs_client.create_delta_sync_index(
        endpoint_name=endpoint_name,
        index_name=index_name,
        primary_key=primary_key,
        index_spec={
            "source_table": source_table_name,
            "pipeline_type": pipeline_type,
            "embedding_source_columns": [
                {
                    "name": embedding_source_column,
                    "embedding_model_endpoint_name": embedding_model_endpoint_name,
                }
            ],
        },
    )

def wait_for_index(
    vs_client: VectorSearchClient,
    endpoint_name: str,
    index_name: str,
    timeout_minutes: int = 30,
    check_interval_seconds: int = 10
) -> bool:
    start_time = time.time()
    timeout_seconds = timeout_minutes * 60

    while time.time() - start_time < timeout_seconds:
        try:
            idx = vs_client.get_index(endpoint_name=endpoint_name, index_name=index_name)
            # Different SDK versions expose status differently
            status = getattr(idx, "status", None)
            if isinstance(status, dict) and status.get("ready"):
                return True
            if hasattr(status, "ready") and status.ready:
                return True
        except Exception:
            pass

        time.sleep(check_interval_seconds)

    return False

def query_index(
    vs_client: VectorSearchClient,
    endpoint_name: str,
    index_name: str,
    query_text: str,
    k: int = 10,
    filters: dict = None,
    columns: list = None,
) -> list:
    """
    Returns: List[Dict] with keys matching `columns`.
    """
    cols = columns or DEFAULT_COLUMNS

    # Preferred pattern across SDK versions: get_index(...).similarity_search(...)
    try:
        idx = vs_client.get_index(endpoint_name=endpoint_name, index_name=index_name)
        res = idx.similarity_search(
            query_text=query_text,
            columns=cols,
            num_results=k,
            filters=filters
        )
        # Many SDKs return {"result": {"data_array": [...], "manifest": {"columns": [...]}}}
        result = res.get("result", {})
        data_array = result.get("data_array", [])
        manifest_cols = (result.get("manifest", {}) or {}).get("columns", cols)

        out = []
        for row in data_array:
            # row may be list or dict depending on version
            if isinstance(row, dict):
                out.append(row)
            else:
                out.append({manifest_cols[i]: row[i] for i in range(min(len(manifest_cols), len(row)))})
        return out

    except Exception as e:
        raise RuntimeError(f"Vector Search query failed: {e}")
