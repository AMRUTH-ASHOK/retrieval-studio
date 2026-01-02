from databricks.vector_search.client import VectorSearchClient
import time

def create_vs_index(
    vs_client: VectorSearchClient,
    index_name: str,
    source_table: str,
    embedding_endpoint: str,
    primary_key: str = "chunk_id",
    vs_endpoint_name: str = None
):
    """
    Create Vector Search index
    
    Args:
        vs_client: VectorSearchClient instance
        index_name: Name for the index
        source_table: Full table name (catalog.schema.table)
        embedding_endpoint: Model serving endpoint for embeddings
        primary_key: Primary key column name
        embedding_vector_column: Column name containing embeddings
        vs_endpoint_name: Vector Search endpoint name
        **kwargs: Additional index configuration
    """
    # Use the correct API method: create_delta_sync_index
    # Note: Actual API may vary - adjust based on your Databricks version
    try:
        # Try the delta sync index creation API
        vs_client.create_delta_sync_index(
            index_name=index_name,
            primary_key=primary_key,
            index_spec={
                "embedding_source_columns": [
                    {
                        "name": "chunk_text",
                        "embedding_model_endpoint_name": embedding_endpoint
                    }
                ],
                "source_table": source_table,
                "pipeline_type": "TRIGGERED"
            },
            endpoint_name=vs_endpoint_name
        )
    except AttributeError:
        # Fallback: try alternative API format
        # This is a placeholder - adjust based on actual Vector Search API
        raise NotImplementedError(
            "Vector Search API needs to be implemented based on your Databricks version. "
            "Please check the VectorSearchClient API documentation."
        )

def wait_for_index(
    vs_client: VectorSearchClient,
    index_name: str,
    timeout_minutes: int = 30,
    check_interval_seconds: int = 10
) -> bool:
    """
    Wait for index to be ready
    
    Returns:
        True if ready, False if timeout
    """
    start_time = time.time()
    timeout_seconds = timeout_minutes * 60
    
    while time.time() - start_time < timeout_seconds:
        try:
            index = vs_client.get_index(index_name)
            # Check if index is ready - API may return different formats
            if hasattr(index, 'status'):
                status = index.status
                if isinstance(status, dict) and status.get("ready"):
                    return True
                elif hasattr(status, 'ready') and status.ready:
                    return True
        except Exception:
            # Index might not exist yet, continue polling
            pass
        
        time.sleep(check_interval_seconds)
    
    return False

def query_index(
    vs_client: VectorSearchClient,
    index_name: str,
    query_text: str,
    embedding_endpoint: str,
    k: int = 10,
    filters: dict = None
) -> list:
    """
    Query Vector Search index
    
    Args:
        vs_client: VectorSearchClient instance
        index_name: Index name
        query_text: Query text
        embedding_endpoint: Model serving endpoint for embeddings
        k: Number of results to return
        filters: Optional filters
    
    Returns:
        List of retrieved chunks with scores
    """
    # Query index
    # Note: Actual API may vary - this is a placeholder implementation
    try:
        results = vs_client.query_index(
            index_name=index_name,
            query_text=query_text,
            columns=["chunk_id", "chunk_text", "metadata"],
            num_results=k,
            filters=filters
        )
        return results.get("result", {}).get("data_array", [])
    except Exception as e:
        # Fallback for different API versions
        # This would need to be adjusted based on actual Vector Search API
        raise NotImplementedError(f"Vector Search query API needs to be implemented: {e}")

