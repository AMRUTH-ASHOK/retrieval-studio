"""
Build results construction utilities

Constructs build results (chunks_table, index_name) without querying Databricks API.
Uses the same deterministic naming as the build notebook.
"""
import sys
import os

# Add project root to path to import retrieval_core
project_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from retrieval_core.configs import config as core_config
from typing import Dict, Any, List


def construct_build_results(
    project_name: str,
    strategies: List[str],
    catalog: str,
    schema: str = None
) -> Dict[str, Any]:
    """
    Construct build results without querying Databricks API

    Uses the same deterministic naming logic as the build notebook:
    - Chunks table: {catalog}.chunks.rs_chunks_{project_name}_{strategy}
    - Index name: {catalog}.indexes.rs_index_{project_name}_{strategy}

    Args:
        project_name: Project name
        strategies: List of strategy names (e.g., ["baseline", "semantic"])
        catalog: Unity Catalog name
        schema: Schema name (optional, not used for chunks/indexes)

    Returns:
        Dictionary with results per strategy:
        {
            "baseline": {
                "chunks_table": "catalog.chunks.rs_chunks_project1_baseline",
                "index_name": "catalog.indexes.rs_index_project1_baseline"
            },
            ...
        }
    """
    # Set catalog in config (same as notebook does via widgets)
    type(core_config).UC_CATALOG = catalog
    type(core_config).CHUNKS_SCHEMA = "chunks"
    type(core_config).INDEXES_SCHEMA = "indexes"

    results = {}
    for strategy in strategies:
        results[strategy] = {
            "chunks_table": core_config.chunks_table(project_name, strategy),
            "index_name": core_config.index_name(project_name, strategy)
        }

    return results


def extract_corpus_table(build_results: Dict[str, Any], preferred_strategies: List[str] = None) -> str:
    """
    Extract corpus_table from build results

    Args:
        build_results: Results dict from construct_build_results()
        preferred_strategies: Strategy preference order (default: baseline, semantic, structured)

    Returns:
        Corpus table name

    Raises:
        ValueError: If no chunks_table found in results
    """
    if preferred_strategies is None:
        preferred_strategies = ['baseline', 'semantic', 'structured']

    # Try preferred strategies first
    for strategy in preferred_strategies:
        if strategy in build_results and isinstance(build_results[strategy], dict):
            corpus_table = build_results[strategy].get("chunks_table")
            if corpus_table:
                return corpus_table

    # If no preferred strategy found, use first available
    for strategy, result in build_results.items():
        if isinstance(result, dict) and result.get("chunks_table"):
            return result["chunks_table"]

    raise ValueError("No chunks_table found in build results")
