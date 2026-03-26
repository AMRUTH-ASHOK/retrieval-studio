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
project_id = config.get("project_id")

vs_endpoint_name = config.get("vs_endpoint_name")
embedding_model_endpoint = config.get("embedding_model_endpoint")
create_index_flag = bool(config.get("create_index", True))
wait_ready = bool(config.get("wait_for_index", False))

ensure_uc_schemas()

# COMMAND ----------
# Load documents
# 
# Scalable Data Loading:
# - If a volume_path is provided in data_config, files are read directly from 
#   the Unity Catalog Volume. This is the recommended approach for large-scale
#   data processing.
# - Files were uploaded to the Volume by the frontend before triggering this job.
# - The volume_path format is: /Volumes/<catalog>/<schema>/<volume>/uploads/<project>/<upload_id>

def _load_single_source(src_type, src_config):
    """Load documents from a single data source by type and config."""
    src_handler = get_data_type_handler(src_type)
    volume_path = src_config.get("volume_path")

    if volume_path:
        print(f"[INFO] Loading {src_type} documents from UC Volume: {volume_path}")

        # Diagnostic: verify path is accessible
        try:
            from databricks.sdk.runtime import dbutils
            file_pattern = src_config.get("file_pattern", "*.*")
            all_items = dbutils.fs.ls(volume_path)
            print(f"[DEBUG] dbutils.fs.ls('{volume_path}') returned {len(all_items)} items:")
            for item in all_items[:20]:
                print(f"  {'[DIR]' if item.isDir() else '[FILE]'} {item.name} ({item.size} bytes) path={item.path}")
            if len(all_items) > 20:
                print(f"  ... and {len(all_items) - 20} more items")

            import fnmatch
            matched = [f for f in all_items if not f.isDir() and fnmatch.fnmatch(f.name, file_pattern)]
            print(f"[DEBUG] Files matching pattern '{file_pattern}': {len(matched)}")
            if not matched:
                print(f"[WARNING] No files matched pattern '{file_pattern}'. Available file names: {[f.name for f in all_items if not f.isDir()][:20]}")
        except Exception as diag_err:
            print(f"[ERROR] Failed to list volume path '{volume_path}': {diag_err}")

        uc_volume_handler = get_data_type_handler("uc_volume")
        volume_config = {
            "volume_path": volume_path,
            "file_pattern": src_config.get("file_pattern", "*.*"),
            "recursive": src_config.get("recursive", False),
            "text_column": src_config.get("text_column", "text"),
            "id_column": src_config.get("id_column"),
            "has_header": src_config.get("has_header", True),
            "text_field": src_config.get("text_field", "text"),
            "id_field": src_config.get("id_field"),
            "is_array": src_config.get("is_array", True),
            "extract_images": src_config.get("extract_images", False),
            "ocr_enabled": src_config.get("ocr_enabled", False),
        }
        docs = uc_volume_handler.load_documents(volume_config)
        print(f"[INFO] Loaded {len(docs)} documents from UC Volume")
        return docs

    if src_type == "delta_table":
        table_name = src_config.get("table_name", "")
        text_column = src_config.get("text_column", "text")
        id_column = src_config.get("id_column")
        max_rows = int(src_config.get("max_rows", 2000))

        df = spark.table(table_name).select(
            *([id_column] if id_column else []),
            text_column
        ).limit(max_rows)

        docs = []
        for row in df.toLocalIterator():
            rd = row.asDict()
            doc_id = str(rd.get(id_column)) if id_column else str(uuid.uuid4())
            text = rd.get(text_column)
            if text is None:
                continue
            docs.append(Document(
                doc_id=doc_id,
                doc_name=f"{table_name}:{doc_id}",
                text=str(text),
                metadata={"source_table": table_name},
                data_type="delta_table",
            ))
        return docs

    return src_handler.load_documents(src_config)


sources = config.get("sources", [])
if not sources:
    raise ValueError("No sources configured. Build config must include a 'sources' array.")

# COMMAND ----------
# Index registry table (with source_name column)
index_registry = core_config.index_registry_table()
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {index_registry} (
  project STRING,
  source_name STRING,
  strategy STRING,
  vs_endpoint STRING,
  index_name STRING,
  source_table STRING,
  embedding_endpoint STRING,
  updated_at TIMESTAMP
)
USING DELTA
""")

# Add source_name column if missing (backward compat)
existing_cols = [c.name.lower() for c in spark.table(index_registry).schema]
if "source_name" not in existing_cols:
    try:
        spark.sql(f"ALTER TABLE {index_registry} ADD COLUMNS (source_name STRING)")
        print("[INFO] Added source_name column to index registry")
    except Exception as e:
        print(f"[WARNING] ALTER TABLE for source_name skipped: {e}")

# COMMAND ----------
import mlflow
experiment_name = core_config.get_experiment_name(project_name)
experiment = mlflow.set_experiment(experiment_name)

print(f"[INFO] ✓ MLflow Experiment Set")
print(f"[INFO]   - Name: {experiment_name}")
print(f"[INFO]   - ID: {experiment.experiment_id}")
print(f"[INFO]   - Artifact Location: {experiment.artifact_location}")

try:
    from utils.postgres_state import update_build_state, update_project

    update_build_state(
        run_id=run_id,
        state='RUNNING',
        experiment_id=experiment.experiment_id
    )

    print(f"[INFO] ✓ Stored experiment_id={experiment.experiment_id} in builds table for run_id={run_id}")
    if project_id:
        update_project(project_id, experiment_id=experiment.experiment_id)
        print(f"[INFO] ✓ Stored experiment_id={experiment.experiment_id} in projects table for project_id={project_id}")

except Exception as e:
    print(f"[ERROR] ✗ Failed to store experiment_id in database: {e}")
    print(f"[WARNING] Build will continue, but API lookups may use fallback name-based lookup")
    import traceback
    traceback.print_exc()

strategy_results = {}

# Compute total source-strategy combos for logging
all_source_names = [s["source_name"] for s in sources]
total_combos = sum(len(s.get("strategies", {})) for s in sources)
print(f"[INFO] Per-source build: {len(sources)} sources, {total_combos} source-strategy combos")

with mlflow.start_run(run_name=f"build_{run_id[:8]}") as parent_run:
    mlflow.set_tag("rs_role", "build_parent")
    mlflow.log_param("build_run_id", str(run_id))
    mlflow.log_param("project_name", project_name)
    if project_id:
        mlflow.log_param("project_id", project_id)
    mlflow.log_param("num_sources", str(len(sources)))
    mlflow.log_param("source_names", json.dumps(all_source_names))
    mlflow.log_dict(config, "build_config.json")
    try:
        from utils.postgres_state import update_build_state
        update_build_state(run_id=run_id, build_parent_run_id=parent_run.info.run_id)
        print(f"[INFO] ✓ Stored build_parent_run_id={parent_run.info.run_id}")
    except Exception as e:
        print(f"[WARNING] Failed to store build_parent_run_id for run_id={run_id}: {e}")

    for source_idx, source in enumerate(sources):
        source_name = source.get("source_name")
        source_type = source.get("source_type")
        source_config = source.get("config", {})
        source_strategies = source.get("strategies", {})

        if not source_name or not source_type:
            print(f"[WARNING] Skipping source {source_idx}: missing source_name or source_type")
            continue

        if not isinstance(source_strategies, dict) or not source_strategies:
            print(f"[WARNING] Skipping source '{source_name}': no strategies defined")
            continue

        print(f"\n{'='*60}")
        print(f"Source: {source_name} (type={source_type})")
        print(f"Strategies: {list(source_strategies.keys())}")
        print(f"{'='*60}")

        # Load documents for this source
        try:
            documents = _load_single_source(source_type, source_config)
        except Exception as e:
            print(f"[ERROR] Failed to load source '{source_name}': {e}")
            for sn in source_strategies:
                key = f"{source_name}__{sn}"
                strategy_results[key] = {"status": "FAILED", "error": f"Source load failed: {str(e)}"}
            continue

        if not documents:
            print(f"[WARNING] No documents loaded for source '{source_name}', skipping.")
            for sn in source_strategies:
                key = f"{source_name}__{sn}"
                strategy_results[key] = {"status": "FAILED", "error": "No documents loaded"}
            continue

        doc_dicts = [{"doc_id": d.doc_id, "doc_name": d.doc_name, "text": d.text, "metadata": d.metadata} for d in documents]
        print(f"[INFO] Loaded {len(documents)} documents for source '{source_name}'")

        for strategy_name, strategy_params in source_strategies.items():
            result_key = f"{source_name}__{strategy_name}"

            with mlflow.start_run(run_name=f"build_{source_name}_{strategy_name}", nested=True) as child_run:
                mlflow.set_tag("rs_role", "build_strategy")
                mlflow.log_param("build_run_id", str(run_id))
                mlflow.log_param("source_name", source_name)
                mlflow.log_param("source_type", source_type)
                mlflow.log_param("strategy_name", strategy_name)

                for k, v in (strategy_params or {}).items():
                    mlflow.log_param(f"strat_{k}", v if isinstance(v, str) else json.dumps(v))

                try:
                    strat = get_strategy(strategy_name, **(strategy_params or {}))
                    chunks = strat.chunk(doc_dicts)
                    mlflow.log_metric("num_chunks", float(len(chunks)))
                    mlflow.log_metric("num_documents", float(len(documents)))

                    chunks_table = core_config.chunks_table(project_name, strategy_name, source_name)

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

                    spark.sql(f"ALTER TABLE {chunks_table} SET TBLPROPERTIES (delta.enableChangeDataFeed = true)")

                    source_for_index = chunks_table
                    if strategy_name == "parent_child":
                        indexable_table = core_config.chunks_indexable_table(project_name, strategy_name, source_name)
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

                    idx_name = core_config.index_name(project_name, strategy_name, source_name)

                    if create_index_flag:
                        if not (vs_endpoint_name and embedding_model_endpoint):
                            raise ValueError("Missing vs_endpoint_name or embedding_model_endpoint")

                        create_vs_index(
                            vs_client=vs_client,
                            endpoint_name=vs_endpoint_name,
                            index_name=idx_name,
                            source_table_name=source_for_index,
                            embedding_model_endpoint_name=embedding_model_endpoint,
                        )
                        if wait_ready:
                            wait_for_index(vs_client, vs_endpoint_name, idx_name)

                    # Update registry (use parameterized queries via DataFrame API)
                    def _esc(val):
                        return str(val).replace("'", "''") if val else ""

                    spark.sql(f"DELETE FROM {index_registry} WHERE project = '{_esc(project_name)}' AND source_name = '{_esc(source_name)}' AND strategy = '{_esc(strategy_name)}'")
                    spark.sql(f"""
                      INSERT INTO {index_registry}
                      VALUES ('{_esc(project_name)}', '{_esc(source_name)}', '{_esc(strategy_name)}', '{_esc(vs_endpoint_name)}', '{_esc(idx_name)}', '{_esc(source_for_index)}', '{_esc(embedding_model_endpoint)}', current_timestamp())
                    """)

                    mlflow.log_param("chunks_table", chunks_table)
                    mlflow.log_param("index_source_table", source_for_index)
                    mlflow.log_param("vs_endpoint", vs_endpoint_name)
                    mlflow.log_param("vs_index_name", idx_name)
                    mlflow.log_param("embedding_endpoint", embedding_model_endpoint)

                    strategy_results[result_key] = {
                        "status": "SUCCESS",
                        "source_name": source_name,
                        "strategy_name": strategy_name,
                        "num_chunks": len(chunks),
                        "chunks_table": chunks_table,
                        "index_source_table": source_for_index,
                        "index_name": idx_name,
                        "mlflow_run_id": child_run.info.run_id,
                    }

                    # Store index selection record in Postgres
                    try:
                        from utils.postgres_state import create_index_selection
                        create_index_selection(
                            project_id=project_id,
                            build_run_id=run_id,
                            source_name=source_name,
                            strategy_name=strategy_name,
                            index_name=idx_name,
                            chunks_table=chunks_table,
                            vs_endpoint=vs_endpoint_name
                        )
                    except Exception as ix_err:
                        print(f"[WARNING] Failed to store index selection: {ix_err}")

                except Exception as e:
                    mlflow.set_tag("status", "FAILED")
                    mlflow.log_text(str(e), "error.txt")
                    strategy_results[result_key] = {
                        "status": "FAILED",
                        "source_name": source_name,
                        "strategy_name": strategy_name,
                        "error": str(e),
                        "mlflow_run_id": child_run.info.run_id,
                    }

if not strategy_results:
    final_state = "FAILED"
elif all(v.get("status") != "FAILED" for v in strategy_results.values()):
    final_state = "SUCCESS"
else:
    final_state = "PARTIAL_SUCCESS"
dbutils.notebook.exit(json.dumps({"run_id": run_id, "status": final_state, "results": strategy_results}))
