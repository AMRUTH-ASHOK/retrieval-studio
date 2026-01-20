# Databricks notebook source
# MAGIC %md
# MAGIC # Retrieval Studio - Build Job (Product)

# COMMAND ----------
# MAGIC %pip install databricks-vectorsearch mlflow "databricks-sdk>=0.61.0" --quiet
# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("config", "{}")
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")

# COMMAND ----------
import json, re, uuid, traceback
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import current_timestamp, col, lit

# COMMAND ----------
import os
import sys

current_dir = os.getcwd()
parent_dir = os.path.dirname(current_dir)
# Check logic for finding the 'retrieval_core' package 
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

print(f"Using Project Root: {project_root}")

if project_root not in sys.path:
    # Use insert(0) to prioritize our project root over system paths
    sys.path.insert(0, project_root)

# COMMAND ----------
from retrieval_core.strategies import get_strategy
from retrieval_core.data_types import get_data_type_handler, Document
from retrieval_core.configs import config as core_config

# Apply catalog/schema overrides from widgets
catalog_override = dbutils.widgets.get("catalog")
schema_override = dbutils.widgets.get("schema")

if catalog_override:
    type(core_config).UC_CATALOG = catalog_override
if schema_override:
    type(core_config).RAW_SCHEMA = schema_override

from databricks.vector_search.client import VectorSearchClient
from utils.vs_utils import create_vs_index, wait_for_index

vs_client = VectorSearchClient()

# COMMAND ----------
def ensure_uc_schemas():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {core_config.fq_schema(core_config.RAW_SCHEMA)}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {core_config.fq_schema(core_config.CHUNKS_SCHEMA)}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {core_config.fq_schema(core_config.INDEXES_SCHEMA)}")

def safe_ident(s: str) -> str:
    s = (s or "").strip().replace("-", "_")
    s = re.sub(r"[^a-zA-Z0-9_]", "_", s)
    return s[:120] or "default"

CHUNKS_SCHEMA = StructType([
    StructField("chunk_id", StringType(), False),
    StructField("doc_id", StringType(), True),
    StructField("doc_name", StringType(), True),
    StructField("chunk_text", StringType(), True),
    StructField("chunk_index", IntegerType(), True),
    StructField("metadata_json", StringType(), True),
    StructField("chunk_type", StringType(), True),
    StructField("parent_chunk_id", StringType(), True),
    StructField("run_id", StringType(), False),
    StructField("project", StringType(), False),
    StructField("strategy", StringType(), False),
    StructField("created_at", StringType(), True),  # replaced with timestamp below
])

# COMMAND ----------
run_id = dbutils.widgets.get("run_id") or str(uuid.uuid4())
config_json = dbutils.widgets.get("config") or "{}"
config = json.loads(config_json)

project_name = config.get("project_name", "default")
data_type = config.get("data_type", "pdf")
data_config = config.get("data_config", {})
strategies_config = config.get("strategies", {})

vs_endpoint_name = config.get("vs_endpoint_name")
embedding_model_endpoint = config.get("embedding_model_endpoint")
create_index_flag = bool(config.get("create_index", True))
wait_ready = bool(config.get("wait_for_index", False))

ensure_uc_schemas()

# COMMAND ----------
# Load documents
handler = get_data_type_handler(data_type)
documents = []

if data_type == "delta_table":
    table_name = data_config.get("table_name", "")
    text_column = data_config.get("text_column", "text")
    id_column = data_config.get("id_column")
    max_rows = int(data_config.get("max_rows", 2000))

    df = spark.table(table_name).select(
        *([id_column] if id_column else []),
        text_column
    ).limit(max_rows)

    for row in df.toLocalIterator():
        rd = row.asDict()
        doc_id = str(rd.get(id_column)) if id_column else str(uuid.uuid4())
        text = rd.get(text_column)
        if text is None:
            continue
        documents.append(Document(
            doc_id=doc_id,
            doc_name=f"{table_name}:{doc_id}",
            text=str(text),
            metadata={"source_table": table_name},
            data_type="delta_table",
        ))
else:
    documents = handler.load_documents(data_config)

if not documents:
    raise ValueError("No documents loaded. Check data_config.")

doc_dicts = [{"doc_id": d.doc_id, "doc_name": d.doc_name, "text": d.text, "metadata": d.metadata} for d in documents]

# COMMAND ----------
# Index registry table
index_registry = core_config.index_registry_table()
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {index_registry} (
  project STRING,
  strategy STRING,
  vs_endpoint STRING,
  index_name STRING,
  source_table STRING,
  embedding_endpoint STRING,
  updated_at TIMESTAMP
)
USING DELTA
""")

# COMMAND ----------
import mlflow
experiment_name = core_config.get_experiment_name(project_name)
experiment = mlflow.set_experiment(experiment_name)

# Log experiment details for visibility
print(f"[INFO] ✓ MLflow Experiment Set")
print(f"[INFO]   - Name: {experiment_name}")
print(f"[INFO]   - ID: {experiment.experiment_id}")
print(f"[INFO]   - Artifact Location: {experiment.artifact_location}")

# Store experiment ID in database for accurate MLflow lookups
try:
    from utils.postgres_state import update_build_state

    update_build_state(
        run_id=run_id,
        state='RUNNING',
        experiment_id=experiment.experiment_id
    )

    print(f"[INFO] ✓ Stored experiment_id={experiment.experiment_id} in builds table for run_id={run_id}")

except Exception as e:
    # Log error but don't fail the build - experiment_id is for convenience
    print(f"[ERROR] ✗ Failed to store experiment_id in database: {e}")
    print(f"[WARNING] Build will continue, but API lookups may use fallback name-based lookup")
    import traceback
    traceback.print_exc()

strategy_results = {}

with mlflow.start_run(run_name=f"build_{run_id[:8]}") as parent_run:
    mlflow.set_tag("rs_role", "build_parent")
    mlflow.log_param("build_run_id", str(run_id))
    mlflow.log_param("project_name", project_name)
    mlflow.log_param("data_type", data_type)
    mlflow.log_param("num_documents", str(len(documents)))
    mlflow.log_param("strategies", json.dumps(list(strategies_config.keys())))
    mlflow.log_dict(config, "build_config.json")

    for strategy_name, strategy_params in strategies_config.items():
        with mlflow.start_run(run_name=f"build_{strategy_name}", nested=True) as child_run:
            mlflow.set_tag("rs_role", "build_strategy")
            mlflow.log_param("build_run_id", str(run_id))
            mlflow.log_param("strategy_name", strategy_name)

            for k, v in (strategy_params or {}).items():
                mlflow.log_param(f"strat_{k}", v if isinstance(v, str) else json.dumps(v))

            try:
                strat = get_strategy(strategy_name, **(strategy_params or {}))
                chunks = strat.chunk(doc_dicts)
                mlflow.log_metric("num_chunks", float(len(chunks)))

                chunks_table = core_config.chunks_table(project_name, strategy_name)

                rows = []
                for c in chunks:
                    meta = c.metadata or {}
                    chunk_type = meta.get("chunk_type")
                    rows.append({
                        "chunk_id": c.chunk_id,
                        "doc_id": c.doc_id,
                        "doc_name": c.doc_name,
                        "chunk_text": c.chunk_text,
                        "chunk_index": int(getattr(c, "chunk_index", 0)),
                        "metadata_json": json.dumps(meta),
                        "chunk_type": chunk_type,
                        "parent_chunk_id": getattr(c, "parent_chunk_id", None),
                        "run_id": run_id,
                        "project": project_name,
                        "strategy": strategy_name,
                        "created_at": None,
                    })

                df = spark.createDataFrame(rows, schema=CHUNKS_SCHEMA).drop("created_at").withColumn("created_at", current_timestamp())

                # first write creates table (partitioned), later writes overwrite run partition
                if spark.catalog.tableExists(chunks_table):
                    (df.write.format("delta")
                       .mode("overwrite")
                       .option("replaceWhere", f"run_id = '{run_id}'")
                       .saveAsTable(chunks_table))
                else:
                    (df.write.format("delta")
                       .mode("overwrite")
                       .partitionBy("run_id")
                       .saveAsTable(chunks_table))
                
                # Enable Change Data Feed (required for STANDARD delta-sync indexes)
                spark.sql(f"ALTER TABLE {chunks_table} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

                # choose index source table
                source_for_index = chunks_table
                if strategy_name == "parent_child":
                    # children-only indexable table
                    indexable_table = core_config.chunks_indexable_table(project_name, strategy_name)
                    children_df = df.filter(col("chunk_type") == lit("child"))

                    if spark.catalog.tableExists(indexable_table):
                        (children_df.write.format("delta")
                           .mode("overwrite")
                           .option("replaceWhere", f"run_id = '{run_id}'")
                           .saveAsTable(indexable_table))
                    else:
                        (children_df.write.format("delta")
                           .mode("overwrite")
                           .partitionBy("run_id")
                           .saveAsTable(indexable_table))
                    
                    spark.sql(f"ALTER TABLE {indexable_table} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")


                    source_for_index = indexable_table

                # stable index name
                index_name = core_config.index_name(project_name, strategy_name)

                if create_index_flag:
                    if not (vs_endpoint_name and embedding_model_endpoint):
                        raise ValueError("Missing vs_endpoint_name or embedding_model_endpoint")

                    create_vs_index(
                        vs_client=vs_client,
                        endpoint_name=vs_endpoint_name,
                        index_name=index_name,
                        source_table_name=source_for_index,
                        embedding_model_endpoint_name=embedding_model_endpoint,
                    )
                    if wait_ready:
                        wait_for_index(vs_client, vs_endpoint_name, index_name)

                # update registry (simple delete+insert)
                spark.sql(f"DELETE FROM {index_registry} WHERE project = '{project_name}' AND strategy = '{strategy_name}'")
                spark.sql(f"""
                  INSERT INTO {index_registry}
                  VALUES ('{project_name}', '{strategy_name}', '{vs_endpoint_name}', '{index_name}', '{source_for_index}', '{embedding_model_endpoint}', current_timestamp())
                """)

                mlflow.log_param("chunks_table", chunks_table)
                mlflow.log_param("index_source_table", source_for_index)
                mlflow.log_param("vs_endpoint", vs_endpoint_name)
                mlflow.log_param("vs_index_name", index_name)
                mlflow.log_param("embedding_endpoint", embedding_model_endpoint)

                strategy_results[strategy_name] = {
                    "status": "SUCCESS",
                    "num_chunks": len(chunks),
                    "chunks_table": chunks_table,
                    "index_source_table": source_for_index,
                    "index_name": index_name,
                    "mlflow_run_id": child_run.info.run_id,
                }

            except Exception as e:
                mlflow.set_tag("status", "FAILED")
                mlflow.log_text(str(e), "error.txt")
                strategy_results[strategy_name] = {"status": "FAILED", "error": str(e), "mlflow_run_id": child_run.info.run_id}

final_state = "SUCCESS" if all(v["status"] != "FAILED" for v in strategy_results.values()) else "PARTIAL_SUCCESS"
dbutils.notebook.exit(json.dumps({"run_id": run_id, "status": final_state, "results": strategy_results}))
