# Databricks notebook source
# MAGIC %md
# MAGIC # Retrieval Studio - Evaluation Job (Product)

# COMMAND ----------
dbutils.widgets.text("build_run_id", "")
dbutils.widgets.text("project_name", "default")
dbutils.widgets.text("queries_table", "")
dbutils.widgets.text("top_k", "10")
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")

# COMMAND ----------
# MAGIC %pip install databricks-vectorsearch mlflow --quiet
# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
import json, time, uuid
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp
from databricks.vector_search.client import VectorSearchClient

spark = SparkSession.builder.getOrCreate()
vs_client = VectorSearchClient()

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

from retrieval_core.configs import config as core_config

# Apply catalog/schema overrides from widgets
catalog_override = dbutils.widgets.get("catalog")
schema_override = dbutils.widgets.get("schema")

if catalog_override:
    type(core_config).UC_CATALOG = catalog_override
if schema_override:
    type(core_config).RAW_SCHEMA = schema_override

from retrieval_core.evaluator import RetrievalEvaluator
from utils.vs_utils import query_index

# COMMAND ----------
build_run_id = dbutils.widgets.get("build_run_id")
project_name = dbutils.widgets.get("project_name") or "default"
queries_table = dbutils.widgets.get("queries_table")
top_k = int(dbutils.widgets.get("top_k") or "10")

if not build_run_id:
    raise ValueError("Missing build_run_id")
if not queries_table:
    raise ValueError("Missing queries_table")

# COMMAND ----------
# Load queries once
qdf = spark.table(queries_table)
if "query_text" not in qdf.columns:
    raise ValueError("queries_table must include query_text")

cols = ["query_text"]
if "expected_chunks" in qdf.columns:
    cols.append("expected_chunks")

query_rows = qdf.select(*cols).collect()

# COMMAND ----------
import mlflow
experiment_name = core_config.get_experiment_name(project_name)
exp = mlflow.set_experiment(experiment_name)

build_child_runs = mlflow.search_runs(
    experiment_ids=[exp.experiment_id],
    filter_string=f"params.build_run_id = '{build_run_id}' AND tags.rs_role = 'build_strategy'",
)

if build_child_runs.empty:
    raise ValueError(f"No build strategy runs found for build_run_id={build_run_id}")

# COMMAND ----------
evaluator = RetrievalEvaluator()

eval_results_table = core_config.eval_results_table()
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {eval_results_table} (
  eval_result_id STRING,
  build_run_id STRING,
  eval_run_id STRING,
  build_child_run_id STRING,
  project STRING,
  strategy STRING,
  query_text STRING,
  metrics STRING,
  created_at TIMESTAMP
)
USING DELTA
""")

# COMMAND ----------
all_rows = []

with mlflow.start_run(run_name=f"eval_{build_run_id[:8]}") as eval_parent:
    mlflow.set_tag("rs_role", "eval_parent")
    mlflow.log_param("build_run_id", build_run_id)
    mlflow.log_param("project_name", project_name)
    mlflow.log_param("queries_table", queries_table)
    mlflow.log_param("top_k", str(top_k))

    for _, r in build_child_runs.iterrows():
        build_child_run_id = r.run_id
        strategy_name = r.get("params.strategy_name")
        index_name = r.get("params.vs_index_name")
        vs_endpoint = r.get("params.vs_endpoint")

        if not (strategy_name and index_name and vs_endpoint):
            continue

        with mlflow.start_run(run_name=f"eval_{strategy_name}", nested=True) as eval_child:
            mlflow.set_tag("rs_role", "eval_strategy")
            mlflow.log_param("build_run_id", build_run_id)
            mlflow.log_param("build_child_run_id", build_child_run_id)
            mlflow.log_param("strategy_name", strategy_name)
            mlflow.log_param("vs_endpoint", vs_endpoint)
            mlflow.log_param("vs_index_name", index_name)

            recalls, ndcgs, latencies = [], [], []

            for qr in query_rows:
                qtext = qr["query_text"]
                expected_raw = qr["expected_chunks"] if "expected_chunks" in qr.asDict() else None

                expected_ids = None
                if expected_raw is not None:
                    if isinstance(expected_raw, str):
                        try:
                            expected_ids = json.loads(expected_raw)
                        except:
                            expected_ids = None
                    else:
                        expected_ids = expected_raw

                t0 = time.time()
                retrieved = query_index(
                    vs_client=vs_client,
                    endpoint_name=vs_endpoint,
                    index_name=index_name,
                    query_text=qtext,
                    k=top_k,
                )
                latency_ms = (time.time() - t0) * 1000.0

                if expected_ids:
                    metrics = evaluator.compute_labeled_metrics(qtext, retrieved, expected_ids, k_values=[10])
                else:
                    metrics = evaluator.compute_judge_metrics(qtext, retrieved, k_values=[10])

                metrics["retrieval_latency_ms"] = latency_ms

                recalls.append(float(metrics.get("recall_at_10", 0.0)))
                ndcgs.append(float(metrics.get("ndcg_at_10", 0.0)))
                latencies.append(float(metrics.get("retrieval_latency_ms", 0.0)))

                all_rows.append({
                    "eval_result_id": str(uuid.uuid4()),
                    "build_run_id": build_run_id,
                    "eval_run_id": eval_child.info.run_id,
                    "build_child_run_id": build_child_run_id,
                    "project": project_name,
                    "strategy": strategy_name,
                    "query_text": qtext,
                    "metrics": json.dumps(metrics),
                })

            mlflow.log_metric("recall_at_10", sum(recalls)/len(recalls) if recalls else 0.0)
            mlflow.log_metric("ndcg_at_10", sum(ndcgs)/len(ndcgs) if ndcgs else 0.0)
            mlflow.log_metric("avg_latency_ms", sum(latencies)/len(latencies) if latencies else 0.0)

if all_rows:
    df = spark.createDataFrame(all_rows).withColumn("created_at", current_timestamp())
    df.write.format("delta").mode("append").saveAsTable(eval_results_table)

dbutils.notebook.exit(json.dumps({
    "build_run_id": build_run_id,
    "experiment": experiment_name,
    "num_strategy_runs": int(len(build_child_runs)),
    "num_eval_rows": len(all_rows),
}))
