# Databricks notebook source
# MAGIC %md
# MAGIC # Retrieval Studio - Build Job (MVP)
# MAGIC Orchestrates data loading, chunking strategies, Delta writes, and optional Vector Search index creation.

# COMMAND ----------

# MAGIC %pip install databricks-vectorsearch mlflow --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.text("run_id", "")
dbutils.widgets.text("config", "{}")
dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "retrieval_studio")

# COMMAND ----------

import json
import sys
import os
import re
import uuid
import traceback
from pyspark.sql import SparkSession
from databricks.vector_search.client import VectorSearchClient

spark = SparkSession.builder.getOrCreate()
vs_client = VectorSearchClient()

# Add project root to Python path so we can import core and utils modules
# According to Databricks docs (https://docs.databricks.com/aws/en/ldp/import-workspace-files):
# - For Git folders, you must prepend /Workspace/ to the path
# - Use os.path.abspath() for reliability
# - The root directory of a Git folder is automatically appended when running notebooks,
#   but for jobs we need to manually add it
module_path = ".."
sys.path.append(os.path.abspath(module_path))

# COMMAND ----------

# Imports from your project
from core.strategies import get_strategy
from core.data_types import get_data_type_handler, Document
from utils.vs_utils import create_vs_index, wait_for_index  # if wait_for_index exists

# COMMAND ----------

def safe_ident(s: str) -> str:
    """Make a string safe for table/index suffixes."""
    s = s.strip()
    s = s.replace("-", "_")
    s = re.sub(r"[^a-zA-Z0-9_]", "_", s)
    return s[:120]  # keep it reasonable

def ensure_schema(catalog: str, schema: str):
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

# COMMAND ----------

# Parse parameters
run_id = dbutils.widgets.get("run_id") or str(uuid.uuid4())
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
config_json = dbutils.widgets.get("config") or "{}"

config = json.loads(config_json)

print(f"Run ID: {run_id}")
print(json.dumps(config, indent=2))

ensure_schema(catalog, schema)

# COMMAND ----------

# Step 1: Load Documents
print("=" * 80)
print("STEP 1: Loading Documents")
print("=" * 80)

data_type = config.get("data_type", "pdf")
data_config = config.get("data_config", {})

documents = []

try:
    handler = get_data_type_handler(data_type)

    if data_type == "delta_table":
        table_name = data_config.get("table_name", "")
        text_column = data_config.get("text_column", "text")
        id_column = data_config.get("id_column")  # optional
        max_rows = int(data_config.get("max_rows", 2000))  # safety for MVP

        df = spark.table(table_name).select(
            *([id_column] if id_column else []),
            text_column
        ).limit(max_rows)

        for row in df.toLocalIterator():  # streaming, safer than collect()
            row_dict = row.asDict()
            doc_id = str(row_dict.get(id_column)) if id_column else str(uuid.uuid4())
            text = row_dict.get(text_column)
            if text is None:
                continue

            documents.append(
                Document(
                    doc_id=doc_id,
                    doc_name=f"{table_name}:{doc_id}",
                    text=str(text),
                    metadata={"source_table": table_name},
                    data_type="delta_table",
                )
            )

    else:
        # Other types handled by your handler (pdf, uc_volume, plain_text, csv, json, etc.)
        documents = handler.load_documents(data_config)

    if not documents:
        raise ValueError("No documents were loaded. Check your data configuration.")

    print(f"✅ Loaded {len(documents)} documents")
    print(f"Sample: {documents[0].doc_name} (chars={len(documents[0].text)})")

except Exception as e:
    print(f"❌ Error loading documents: {e}")
    traceback.print_exc()
    raise

# COMMAND ----------

# Step 2: Process strategies
print("\n" + "=" * 80)
print("STEP 2: Chunk + Write + (Optional) Index")
print("=" * 80)

strategies_config = config.get("strategies", {})  # {strategy_name: params}
embedding_model_endpoint = config.get("embedding_model_endpoint")
vs_endpoint_name = config.get("vs_endpoint_name")
create_index = bool(config.get("create_index", True))
wait_for_ready = bool(config.get("wait_for_index", False))

run_suffix = safe_ident(run_id)

print(f"Strategies: {list(strategies_config.keys())}")
print(f"Create index: {create_index}, wait: {wait_for_ready}")
print(f"VS endpoint: {vs_endpoint_name}")
print(f"Embedding endpoint: {embedding_model_endpoint}")

strategy_results = {}

# Convert Document objects to dicts (your strategy interface)
doc_dicts = [
    {"doc_id": d.doc_id, "doc_name": d.doc_name, "text": d.text, "metadata": d.metadata}
    for d in documents
]

for strategy_name, strategy_params in strategies_config.items():
    print(f"\n{'='*60}\nStrategy: {strategy_name}\n{'='*60}")
    print(json.dumps(strategy_params, indent=2))

    try:
        strategy = get_strategy(strategy_name, **(strategy_params or {}))

        # Chunk
        chunks = strategy.chunk(doc_dicts)
        print(f"✅ Chunks created: {len(chunks)}")

        # Write chunks table
        chunks_table = f"{catalog}.{schema}.rl_chunks_{safe_ident(strategy_name)}_{run_suffix}"
        print(f"💾 Writing chunks table: {chunks_table}")

        chunk_rows = []
        for c in chunks:
            # Keep metadata simple and queryable (map<string,string> preferred)
            # If c.metadata has non-string values, stringify them.
            meta = {k: (v if isinstance(v, str) else json.dumps(v)) for k, v in (c.metadata or {}).items()}

            chunk_rows.append({
                "chunk_id": c.chunk_id,
                "doc_id": c.doc_id,
                "doc_name": c.doc_name,
                "chunk_text": c.chunk_text,
                "chunk_index": int(getattr(c, "chunk_index", 0)),
                "metadata": meta,  # map type
                "parent_chunk_id": getattr(c, "parent_chunk_id", None),
                "run_id": run_id,
                "strategy": strategy_name,
            })

        chunks_df = spark.createDataFrame(chunk_rows)
        (chunks_df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(chunks_table))

        index_name = None

        # Create Vector Search index (Delta Sync with embeddings computed by Databricks)
        if create_index and embedding_model_endpoint and vs_endpoint_name:
            index_name = f"{catalog}.{schema}.rl_index_{safe_ident(strategy_name)}_{run_suffix}"
            print(f"🔍 Creating Vector Search index: {index_name}")

            create_vs_index(
                vs_client=vs_client,
                endpoint_name=vs_endpoint_name,
                index_name=index_name,
                primary_key="chunk_id",
                source_table_name=chunks_table,
                embedding_source_column="chunk_text",
                embedding_model_endpoint_name=embedding_model_endpoint,
            )

            if wait_for_ready:
                wait_for_index(vs_client, vs_endpoint_name, index_name)

            print("✅ Index created")

        strategy_results[strategy_name] = {
            "status": "SUCCESS" if index_name else "CHUNKS_CREATED",
            "num_chunks": len(chunks),
            "chunks_table": chunks_table,
            "index_name": index_name,
        }

    except Exception as e:
        print(f"❌ Strategy failed: {strategy_name}: {e}")
        traceback.print_exc()
        strategy_results[strategy_name] = {
            "status": "FAILED",
            "error": str(e),
        }

# COMMAND ----------

# Step 3: MLflow logging (build run)
print("\n" + "=" * 80)
print("STEP 3: MLflow Logging")
print("=" * 80)

try:
    import mlflow

    project_name = config.get("project_name", "default")
    experiment_name = f"/Workspace/RetrievalStudio/{catalog}.{schema}/{project_name}"
    mlflow.set_experiment(experiment_name)

    with mlflow.start_run(run_name=f"build_{run_id[:8]}") as run:
        # Params must be strings
        mlflow.log_param("run_id", run_id)
        mlflow.log_param("data_type", str(data_type))
        mlflow.log_param("num_documents", str(len(documents)))
        mlflow.log_param("strategies", json.dumps(list(strategies_config.keys())))
        mlflow.log_param("vs_endpoint", str(vs_endpoint_name))
        mlflow.log_param("embedding_endpoint", str(embedding_model_endpoint))
        mlflow.log_param("create_index", str(create_index))

        # Metrics
        for s, res in strategy_results.items():
            if res.get("status") in ("SUCCESS", "CHUNKS_CREATED"):
                mlflow.log_metric(f"{safe_ident(s)}_num_chunks", float(res.get("num_chunks", 0)))

        # Artifacts
        mlflow.log_dict(config, "config.json")
        mlflow.log_dict(strategy_results, "strategy_results.json")

        # Helpful tags (easy to find later)
        mlflow.set_tag("retrieval_studio", "build")
        mlflow.set_tag("catalog", catalog)
        mlflow.set_tag("schema", schema)

        print(f"✅ MLflow run: {run.info.run_id}")

except Exception as e:
    print(f"⚠️ MLflow logging skipped: {e}")

# COMMAND ----------

# Summary + exit
print("\n" + "=" * 80)
print("BUILD SUMMARY")
print("=" * 80)

final_state = "SUCCESS" if all(r["status"] != "FAILED" for r in strategy_results.values()) else "PARTIAL_SUCCESS"

print(f"Run: {run_id}")
print(f"Final: {final_state}")
print(json.dumps(strategy_results, indent=2))

dbutils.notebook.exit(json.dumps({
    "run_id": run_id,
    "status": final_state,
    "results": strategy_results
}))
