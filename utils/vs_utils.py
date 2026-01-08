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
    columns_to_sync: list | None = None,
    model_endpoint_name_for_query: str | None = None,
):
    """
    Create a Delta Sync Vector Search index with Databricks-managed embeddings.

    NOTE: Official SDK does NOT use 'index_spec'. It uses explicit parameters.
    """
    # Optional idempotency: if index already exists, just return it
    try:
        return vs_client.get_index(endpoint_name=endpoint_name, index_name=index_name)
    except Exception:
        pass

    return vs_client.create_delta_sync_index(
        endpoint_name=endpoint_name,
        source_table_name=source_table_name,
        index_name=index_name,
        pipeline_type=pipeline_type,  # "TRIGGERED" or "CONTINUOUS" (storage-optimized supports TRIGGERED only)
        primary_key=primary_key,
        embedding_source_column=embedding_source_column,
        embedding_model_endpoint_name=embedding_model_endpoint_name,
        columns_to_sync=columns_to_sync,  # optional
        model_endpoint_name_for_query=model_endpoint_name_for_query,  # optional
    )
    # Matches the official example/signature in Databricks docs. :contentReference[oaicite:1]{index=1}


def wait_for_index(
    vs_client: VectorSearchClient,
    endpoint_name: str,
    index_name: str,
    timeout_minutes: int = 30,
    check_interval_seconds: int = 10,
) -> bool:
    """
    Prefer SDK helper wait method when available; fall back to polling describe/status.
    """
    timeout_seconds = timeout_minutes * 60
    start_time = time.time()

    idx = vs_client.get_index(endpoint_name=endpoint_name, index_name=index_name)

    # Best path: SDK exposes wait_until_ready()
    # (VectorSearchIndex.wait_until_ready exists in the official docs.) :contentReference[oaicite:2]{index=2}
    if hasattr(idx, "wait_until_ready"):
        try:
            idx.wait_until_ready(timeout=timeout_seconds)
            return True
        except Exception:
            # fall back to polling below
            pass

    while time.time() - start_time < timeout_seconds:
        try:
            # describe() is usually the most stable way to get status across versions
            if hasattr(idx, "describe"):
                desc = idx.describe()
                status = (desc or {}).get("status", {}) or {}
                if status.get("ready") is True:
                    return True
            else:
                # last resort: older object shapes
                info = vs_client.get_index(endpoint_name=endpoint_name, index_name=index_name)
                status = getattr(info, "status", None)
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
    filters: dict | None = None,
    columns: list | None = None,
    query_type: str = "ANN",
) -> list:
    """
    Query a vector search index

    Args:
        vs_client: VectorSearchClient instance
        endpoint_name: Vector Search endpoint name
        index_name: Index name (catalog.schema.index)
        query_text: Query text
        k: Number of results to return
        filters: Optional filters dictionary
        columns: Columns to retrieve (defaults to DEFAULT_COLUMNS)
        query_type: Type of search - kept for API compatibility but currently unused
                   (query behavior is determined by index configuration)

    Returns:
        List[Dict] with keys matching `columns`

    Note:
        The query_type parameter is accepted for API compatibility but the search
        behavior is determined by how the index was configured (semantic, keyword, or hybrid).
        To use different search modes, configure separate indexes or use index-level settings.
    """
    cols = columns or DEFAULT_COLUMNS

    try:
        idx = vs_client.get_index(endpoint_name=endpoint_name, index_name=index_name)

        # Execute search - behavior determined by index configuration
        res = idx.similarity_search(
            query_text=query_text,
            columns=cols,
            num_results=k,
            filters=filters,
        )

        result = res.get("result", {}) if isinstance(res, dict) else {}
        data_array = result.get("data_array", [])
        manifest_cols = (result.get("manifest", {}) or {}).get("columns", cols)

        out = []
        for row in data_array:
            if isinstance(row, dict):
                out.append(row)
            else:
                out.append({manifest_cols[i]: row[i] for i in range(min(len(manifest_cols), len(row)))})
        return out

    except Exception as e:
        raise RuntimeError(f"Vector Search query failed: {e}")
