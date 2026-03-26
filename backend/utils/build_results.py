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

from retrieval_core.configs import Config
from typing import Dict, Any, List


def construct_build_results(
    project_name: str,
    strategies: List[str],
    catalog: str,
    schema: str = None,
    sources: List[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Construct build results without querying Databricks API.

    Supports both per-source builds (new) and legacy builds (old).
    Uses a local Config copy to avoid mutating the global singleton.
    """
    safe_name = Config._safe_name

    def fq_table(schema_name: str, table: str) -> str:
        return f"{catalog}.{schema_name}.{table}"

    def chunks_table(proj: str, strategy: str, source_name: str = None) -> str:
        p = safe_name(proj).lower()
        s = safe_name(strategy).lower()
        if source_name:
            src = safe_name(source_name).lower()
            return fq_table("chunks", f"rs_chunks_{p}_{src}_{s}")
        return fq_table("chunks", f"rs_chunks_{p}_{s}")

    def index_name(proj: str, strategy: str, source_name: str = None) -> str:
        p = safe_name(proj).lower()
        s = safe_name(strategy).lower()
        if source_name:
            src = safe_name(source_name).lower()
            return fq_table("indexes", f"rs_index_{p}_{src}_{s}")
        return fq_table("indexes", f"rs_index_{p}_{s}")

    results = {}

    if sources:
        for source in sources:
            source_name = source.get("source_name", "default")
            source_strategies = source.get("strategies", {})
            for strategy in source_strategies:
                key = f"{source_name}__{strategy}"
                results[key] = {
                    "source_name": source_name,
                    "strategy": strategy,
                    "chunks_table": chunks_table(project_name, strategy, source_name),
                    "index_name": index_name(project_name, strategy, source_name)
                }
    else:
        for strategy in strategies:
            results[strategy] = {
                "chunks_table": chunks_table(project_name, strategy),
                "index_name": index_name(project_name, strategy)
            }

    return results


def extract_corpus_tables(build_results: Dict[str, Any]) -> List[Dict[str, str]]:
    """
    Extract ALL corpus tables from build results (for multi-corpus eval).

    Returns list of {table, source_name} dicts.
    """
    tables = []
    seen = set()
    for key, result in build_results.items():
        if isinstance(result, dict) and result.get("chunks_table"):
            table = result["chunks_table"]
            if table not in seen:
                seen.add(table)
                tables.append({
                    "table": table,
                    "source_name": result.get("source_name", key)
                })
    return tables


def extract_corpus_table(build_results: Dict[str, Any], preferred_strategies: List[str] = None) -> str:
    """
    Extract single corpus_table from build results (legacy compat).
    """
    if preferred_strategies is None:
        preferred_strategies = ['baseline', 'semantic', 'structured']

    for strategy in preferred_strategies:
        if strategy in build_results and isinstance(build_results[strategy], dict):
            corpus_table = build_results[strategy].get("chunks_table")
            if corpus_table:
                return corpus_table

    for key, result in build_results.items():
        if isinstance(result, dict) and result.get("chunks_table"):
            return result["chunks_table"]

    raise ValueError("No chunks_table found in build results")
