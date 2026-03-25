# Databricks notebook source
# MAGIC %md
# MAGIC # Retrieval Studio - Resource Cleanup
# MAGIC
# MAGIC Deletes discarded vector search indexes and drops their delta tables.

# COMMAND ----------
# MAGIC %pip install databricks-vectorsearch "databricks-sdk>=0.61.0" --quiet
# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
dbutils.widgets.text("cleanup_config", "{}")
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")

# COMMAND ----------
import json, traceback
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------
import os, sys

current_dir = os.getcwd()
parent_dir = os.path.dirname(current_dir)
project_root = None
if os.path.isdir(os.path.join(current_dir, "retrieval_core")):
    project_root = current_dir
elif os.path.isdir(os.path.join(parent_dir, "retrieval_core")):
    project_root = parent_dir
else:
    p = current_dir
    for _ in range(4):
        if os.path.isdir(os.path.join(p, "retrieval_core")):
            project_root = p
            break
        p = os.path.dirname(p)
    if not project_root:
        project_root = parent_dir

if project_root not in sys.path:
    sys.path.insert(0, project_root)

from retrieval_core.configs import config as core_config

catalog_override = dbutils.widgets.get("catalog")
schema_override = dbutils.widgets.get("schema")

if catalog_override:
    type(core_config).UC_CATALOG = catalog_override
if schema_override:
    type(core_config).RAW_SCHEMA = schema_override

from databricks.vector_search.client import VectorSearchClient

vs_client = VectorSearchClient()

# COMMAND ----------
cleanup_config = json.loads(dbutils.widgets.get("cleanup_config") or "{}")
indexes_to_delete = cleanup_config.get("indexes", [])
project_id = cleanup_config.get("project_id", "")

print(f"[INFO] Cleanup job started")
print(f"[INFO] Project ID: {project_id}")
print(f"[INFO] Indexes to delete: {len(indexes_to_delete)}")

# COMMAND ----------
index_registry = core_config.index_registry_table()

results = []

for idx_info in indexes_to_delete:
    idx_name = idx_info.get("index_name", "")
    chunks_table = idx_info.get("chunks_table", "")
    vs_endpoint = idx_info.get("vs_endpoint", "")
    selection_id = idx_info.get("selection_id", "")

    print(f"\n{'='*60}")
    print(f"Deleting: {idx_name}")
    print(f"  Chunks table: {chunks_table}")
    print(f"  VS endpoint: {vs_endpoint}")

    entry = {
        "selection_id": selection_id,
        "index_name": idx_name,
        "chunks_table": chunks_table,
        "status": "pending"
    }

    # Delete VS index
    try:
        vs_client.delete_index(endpoint_name=vs_endpoint, index_name=idx_name)
        print(f"  [OK] VS index deleted")
    except Exception as e:
        error_msg = str(e).lower()
        if "not found" in error_msg or "does not exist" in error_msg:
            print(f"  [SKIP] VS index already deleted or not found")
        else:
            print(f"  [ERROR] Failed to delete VS index: {e}")
            entry["status"] = "error"
            entry["error"] = f"VS index deletion failed: {str(e)}"
            results.append(entry)
            continue

    # Drop delta table
    try:
        spark.sql(f"DROP TABLE IF EXISTS {chunks_table}")
        print(f"  [OK] Delta table dropped")
    except Exception as e:
        print(f"  [WARNING] Failed to drop delta table: {e}")

    # Drop indexable table if it exists (for parent_child strategy)
    indexable_table = chunks_table + "__indexable"
    try:
        if spark.catalog.tableExists(indexable_table):
            spark.sql(f"DROP TABLE IF EXISTS {indexable_table}")
            print(f"  [OK] Indexable table dropped")
    except Exception:
        pass

    # Remove from index registry
    try:
        spark.sql(f"DELETE FROM {index_registry} WHERE index_name = '{idx_name}'")
        print(f"  [OK] Registry entry removed")
    except Exception as e:
        print(f"  [WARNING] Failed to remove registry entry: {e}")

    # Update status in Postgres
    try:
        from utils.postgres_state import update_index_selection_status
        if selection_id:
            update_index_selection_status(selection_id, "deleted")
            print(f"  [OK] Selection status updated to 'deleted'")
    except Exception as e:
        print(f"  [WARNING] Failed to update selection status: {e}")

    entry["status"] = "deleted"
    results.append(entry)
    print(f"  [DONE] Cleanup complete for {idx_name}")

# COMMAND ----------
print(f"\n{'='*60}")
print(f"CLEANUP SUMMARY")
print(f"{'='*60}")
deleted = sum(1 for r in results if r["status"] == "deleted")
errors = sum(1 for r in results if r["status"] == "error")
print(f"  Deleted: {deleted}")
print(f"  Errors: {errors}")
print(f"  Total: {len(results)}")

dbutils.notebook.exit(json.dumps({
    "project_id": project_id,
    "results": results,
    "deleted": deleted,
    "errors": errors
}))
