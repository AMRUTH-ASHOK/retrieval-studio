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

# Robustly find project root
current_dir = os.getcwd()
parent_dir = os.path.dirname(current_dir)

# Check logic for finding the 'retrieval_core' package
project_root = None
if os.path.isdir(os.path.join(current_dir, "retrieval_core")):
    project_root = current_dir
elif os.path.isdir(os.path.join(parent_dir, "retrieval_core")):
    project_root = parent_dir
else:
    # Walk up to find 'retrieval_core'
    p = current_dir
    for _ in range(4):
        if os.path.isdir(os.path.join(p, "retrieval_core")):
            project_root = p
            break
        p = os.path.dirname(p)
    if not project_root:
        project_root = parent_dir

print(f"Using Project Root: {project_root}")
if project_root not in sys.path:
    # Use insert(0) to prioritize our project root over system paths
    sys.path.insert(0, project_root)

# COMMAND ----------

# Imports from your project
# Imports from your project
from retrieval_core.strategies import get_strategy
from retrieval_core.data_types import get_data_type_handler, Document
from retrieval_core.configs import config as core_config
from utils.vs_utils import create_vs_index, wait_for_index

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

# Parse parameters (Defaults from Config)
run_id = dbutils.widgets.get("run_id") or str(uuid.uuid4())
catalog = dbutils.widgets.get("catalog") or core_config.CATALOG
schema = dbutils.widgets.get("schema") or core_config.SCHEMA
config_json = dbutils.widgets.get("config") or "{}"

config = json.loads(config_json)

print(f"Run ID: {run_id}")
print(f"Using Catalog: {catalog}, Schema: {schema}")
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

# Step 2 & 3: Process strategies with Nested MLflow Runs
print("\n" + "=" * 80)
print("STEP 2 & 3: Chunk + Index + Login (Nested Runs)")
print("=" * 80)

strategies_config = config.get("strategies", {})  # {strategy_name: params}
embedding_model_endpoint = config.get("embedding_model_endpoint")
vs_endpoint_name = config.get("vs_endpoint_name")
project_name = config.get("project_name", "default")

run_suffix = safe_ident(run_id)

print(f"Strategies: {list(strategies_config.keys())}")
print(f"VS endpoint: {vs_endpoint_name}")
print(f"Embedding endpoint: {embedding_model_endpoint}")

strategy_results = {}

# Convert Document objects to dicts
doc_dicts = [
    {"doc_id": d.doc_id, "doc_name": d.doc_name, "text": d.text, "metadata": d.metadata}
    for d in documents
]

try:
    import mlflow
    # Use centralized config for experiment path
    experiment_name = core_config.get_experiment_name(project_name)
    
    mlflow.set_experiment(experiment_name)
    
    # Start Parent Run for the Build Job
    with mlflow.start_run(run_name=f"build_{run_id[:8]}") as parent_run:
        print(f"🚀 Started Parent Run: {parent_run.info.run_id}")
        
        # Log Parent Params
        mlflow.log_param("build_run_id", run_id)
        mlflow.log_param("data_type", str(data_type))
        mlflow.log_param("num_documents", str(len(documents)))
        mlflow.log_param("strategies_list", json.dumps(list(strategies_config.keys())))
        mlflow.log_dict(config, "build_config.json")
        mlflow.set_tag("retrieval_studio_type", "build_parent")
        
        # Iterate Strategies (Child Runs)
        for strategy_name, strategy_params in strategies_config.items():
            print(f"\n{'='*60}\nStrategy: {strategy_name}\n{'='*60}")
            
            with mlflow.start_run(run_name=f"strat_{strategy_name}", nested=True) as child_run:
                print(f"  ↳ Started Child Run: {child_run.info.run_id}")
                
                try:
                    # Log Strategy Params
                    mlflow.log_param("strategy_name", strategy_name)
                    mlflow.log_param("build_run_id", run_id)  # Critical for discovery
                    for k, v in (strategy_params or {}).items():
                        mlflow.log_param(f"strat_{k}", v)
                    
                    # 1. Chunking
                    strategy = get_strategy(strategy_name, **(strategy_params or {}))
                    chunks = strategy.chunk(doc_dicts)
                    print(f"  ✅ Chunks created: {len(chunks)}")
                    mlflow.log_metric("num_chunks", len(chunks))
                    
                    # 2. Persist Chunks to Delta
                    chunks_table = f"{catalog}.{schema}.rl_chunks_{safe_ident(strategy_name)}_{run_suffix}"
                    print(f"  💾 Writing chunks table: {chunks_table}")
                    
                    chunk_rows = []
                    for c in chunks:
                        meta = {k: (v if isinstance(v, str) else json.dumps(v)) for k, v in (c.metadata or {}).items()}
                        chunk_rows.append({
                            "chunk_id": c.chunk_id,
                            "doc_id": c.doc_id,
                            "doc_name": c.doc_name,
                            "chunk_text": c.chunk_text,
                            "chunk_index": int(getattr(c, "chunk_index", 0)),
                            "metadata": meta, 
                            "parent_chunk_id": getattr(c, "parent_chunk_id", None),
                            "run_id": run_id,
                            "strategy": strategy_name,
                        })
                    
                    chunks_df = spark.createDataFrame(chunk_rows)
                    (chunks_df.write.format("delta")
                        .mode("overwrite")
                        .option("overwriteSchema", "true")
                        .saveAsTable(chunks_table))
                    
                    mlflow.log_param("chunks_table", chunks_table)
                    
                    # 3. Create Vector Search Index (MANDATORY)
                    index_name = f"{catalog}.{schema}.rl_index_{safe_ident(strategy_name)}_{run_suffix}"
                    print(f"  🔍 Creating Vector Search index: {index_name}")
                    
                    if embedding_model_endpoint and vs_endpoint_name:
                        create_vs_index(
                            vs_client=vs_client,
                            endpoint_name=vs_endpoint_name,
                            index_name=index_name,
                            primary_key="chunk_id",
                            source_table_name=chunks_table,
                            embedding_source_column="chunk_text",
                            embedding_model_endpoint_name=embedding_model_endpoint,
                        )
                        # Log Index Artifacts to Child Run
                        mlflow.log_param("vs_index_name", index_name)
                        mlflow.log_param("vs_endpoint", vs_endpoint_name)
                        mlflow.log_param("embedding_endpoint", embedding_model_endpoint)
                        mlflow.set_tag("retrieval_studio_type", "strategy_child")
                        
                        strategy_results[strategy_name] = {
                            "status": "SUCCESS",
                            "num_chunks": len(chunks),
                            "index_name": index_name,
                            "mlflow_run_id": child_run.info.run_id
                        }
                    else:
                        raise ValueError("Missing embedding_model_endpoint or vs_endpoint_name config")

                except Exception as e:
                    print(f"  ❌ Strategy failed: {strategy_name}: {e}")
                    traceback.print_exc()
                    mlflow.set_tag("status", "FAILED")
                    mlflow.log_text(str(e), "error.txt")
                    strategy_results[strategy_name] = {"status": "FAILED", "error": str(e)}

except Exception as e:
    print(f"❌ MLflow/Job failed: {e}")
    traceback.print_exc()
    raise

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
