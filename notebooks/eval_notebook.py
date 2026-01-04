# Databricks notebook source
# MAGIC %md
# MAGIC # Retrieval Studio - Evaluation Job
# MAGIC This notebook evaluates retrieval quality for each strategy

# COMMAND ----------

# Widget parameters
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("queries_table", "")
dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "retrieval_studio")

# MAGIC %pip install databricks-vectorsearch mlflow --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import json
import sys
import os
import re
from pyspark.sql import SparkSession
from databricks.vector_search.client import VectorSearchClient
import time
import uuid

spark = SparkSession.builder.getOrCreate()
vs_client = VectorSearchClient()

# Robust project root discovery
current_dir = os.getcwd()
parent_dir = os.path.dirname(current_dir)

if os.path.isdir(os.path.join(current_dir, "rs_core")):
    project_root = current_dir
elif os.path.isdir(os.path.join(parent_dir, "rs_core")):
    project_root = parent_dir
else:
    project_root = None
    p = current_dir
    for _ in range(4):
        if os.path.isdir(os.path.join(p, "rs_core")):
            project_root = p
            break
        p = os.path.dirname(p)
    if not project_root:
        project_root = parent_dir

print(f"Using Project Root: {project_root}")
if project_root not in sys.path:
    sys.path.insert(0, project_root)

print(f"Updated sys.path entries: {[p for p in sys.path if 'retrieval' in str(p).lower()]}")

# COMMAND ----------

# Helper function for state management using Spark SQL
def update_run_state_spark(spark, catalog, schema, run_id, state, **kwargs):
    """Update run state using Spark SQL"""
    updates = {"state": f"'{state}'"}
    for key, value in kwargs.items():
        if value is not None:
            if isinstance(value, str):
                escaped_value = value.replace("'", "''")
                updates[key] = f"'{escaped_value}'"
            else:
                updates[key] = str(value)
    
    set_clause = ", ".join([f"{k} = {v}" for k, v in updates.items()])
    set_clause += ", updated_at = current_timestamp()"
    
    query = f"""
        UPDATE {catalog}.{schema}.rl_runs 
        SET {set_clause}
        WHERE run_id = '{run_id}'
    """
    spark.sql(query)

def get_run_status_spark(spark, catalog, schema, run_id):
    """Get run status using Spark SQL"""
    df = spark.sql(f"""
        SELECT * FROM {catalog}.{schema}.rl_runs 
        WHERE run_id = '{run_id}'
    """)
    if df.count() == 0:
        return None
    row = df.first()
    result = row.asDict()
    # Parse config JSON
    if result.get("config"):
        try:
            result["config"] = json.loads(result["config"])
        except:
            result["config"] = {}
    return result

# COMMAND ----------

# COMMAND ----------

from retrieval_core.evaluator import RetrievalEvaluator
from retrieval_core.configs import config as core_config
from utils.mlflow_utils import log_eval_run
from utils.vs_utils import query_index

# COMMAND ----------

def sanitize_identifier(value: str) -> str:
    """Sanitize strings for table/index identifiers."""
    sanitized = re.sub(r"[^a-zA-Z0-9_]", "_", value.strip().lower())
    return sanitized or "default"

# COMMAND ----------

# Parse parameters (Defaults from Config)
run_id = dbutils.widgets.get("run_id")
queries_table = dbutils.widgets.get("queries_table")
catalog = dbutils.widgets.get("catalog") or core_config.CATALOG
schema = dbutils.widgets.get("schema") or core_config.SCHEMA

print(f"Using Catalog: {catalog}, Schema: {schema}")

# COMMAND ----------

# Get run status using Spark SQL
run_status = get_run_status_spark(spark, catalog, schema, run_id)
if not run_status:
    raise ValueError(f"Run {run_id} not found")

config = run_status["config"]
experiment_id = run_status.get("experiment_id", "")
strategies = config.get("strategies", ["baseline"])
project_name = config.get("project_name", "default")
project_key = sanitize_identifier(project_name)

# COMMAND ----------

# Load queries
queries_df = spark.table(queries_table)
queries = [row.query_text for row in queries_df.select("query_text").collect()]

print(f"Loaded {len(queries)} queries for evaluation")

# COMMAND ----------

# Initialize evaluator
evaluator = RetrievalEvaluator(
    embedding_endpoint=config.get("embedding_model_endpoint", ""),
    judge_model_endpoint=config.get("judge_model_endpoint", None)  # Optional LLM judge
)

# COMMAND ----------

# Evaluate strategies by discovering Child Runs from the Build Job
import mlflow
from mlflow.tracking import MlflowClient

client = MlflowClient()
# Use centralized config for experiment path
experiment_name = core_config.get_experiment_name(project_name)
experiment = mlflow.set_experiment(experiment_name)
print(f"Using MLflow Experiment: {experiment.name}")

# Search for Child Runs of the Build Job
# Criteria: 
# 1. tag.build_run_id = run_id (if we logged it) OR
# 2. We can search for the Parent Run first, then find its children. 
# Let's assume we can find them by the "build_run_id" param we logged in build_notebook_v2.py

print(f"Searching for child runs with params.build_run_id = '{run_id}'")
child_runs = mlflow.search_runs(
    experiment_ids=[experiment.experiment_id],
    filter_string=f"params.build_run_id = '{run_id}'",
    run_view_type=mlflow.entities.ViewType.ACTIVE_ONLY
)

if child_runs.empty:
    print(f"⚠️ No child runs found for build_run_id: {run_id}. Checking via parent run name...")
    # Fallback/Debug: try to find parent run first
    parent_runs = mlflow.search_runs(
        experiment_ids=[experiment.experiment_id],
        filter_string=f"tags.retrieval_studio_type = 'build_parent' AND params.build_run_id = '{run_id}'"
    )
    if not parent_runs.empty:
        parent_run_id = parent_runs.iloc[0].run_id
        print(f"Found Parent Run: {parent_run_id}. Searching children via parent_run_id tag not possible directly unless we tagged them.")
        # In build_notebook_v2, we used nested=True, so they are children.
        # But searching by 'tags.mlflow.parentRunId' is the standard way.
        child_runs = mlflow.search_runs(
             experiment_ids=[experiment.experiment_id],
             filter_string=f"tags.mlflow.parentRunId = '{parent_run_id}'"
        )

if child_runs.empty:
    raise ValueError(f"No child runs found for build {run_id}. Cannot evaluate.")

print(f"Found {len(child_runs)} child runs to evaluate.")

all_results = []

for _, run in child_runs.iterrows():
    run_id_mlflow = run.run_id
    strategy_name = run["params.strategy_name"]
    index_name = run.get("params.vs_index_name")
    
    if not index_name:
        print(f"Skipping run {run_id_mlflow} (strategy: {strategy_name}) - No index_name found.")
        continue
        
    print(f"\nEvaluating Run: {run_id_mlflow}")
    print(f"  Strategy: {strategy_name}")
    print(f"  Index: {index_name}")

    # Evaluate retrieval
    strategy_results = []
    
    for query_row in queries_df.collect():
        query_id = query_row.query_id
        query_text = query_row.query_text
        
        try:
            # Retrieve
            start_time = time.time()
            retrieved_chunks = query_index(
                vs_client,
                index_name,
                query_text,
                config.get("embedding_model_endpoint", ""),
                k=config.get("top_k", 10)
            )
            latency_ms = (time.time() - start_time) * 1000
            
            # Compute Metrics
            expected_chunks = None
            if query_row.expected_chunks:
                try:
                    expected_chunks = json.loads(query_row.expected_chunks)
                except: pass
            
            metrics = {}
            if expected_chunks:
                metrics = evaluator.compute_labeled_metrics(query_text, retrieved_chunks, expected_chunks, k_values=[5, 10])
            else:
                 metrics = evaluator.compute_judge_metrics(query_text, retrieved_chunks, k_values=[5, 10])
            
            metrics["retrieval_latency_ms"] = latency_ms
            
            # Prepare result dict
            result = {
                "eval_result_id": str(uuid.uuid4()),
                "run_id": run_id, # Our app's run_id
                "mlflow_run_id": run_id_mlflow, # The specific child run
                "project": project_name,
                "strategy": strategy_name,
                "query_text": query_text,
                "metrics": json.dumps(metrics),
                "created_at": time.time()
            }
            strategy_results.append(result)
            
        except Exception as e:
            print(f"  ❌ Error query {query_id}: {e}")

    # Aggregation
    if strategy_results:
        # Simple aggregation for logging
        avg_recall_10 = sum([json.loads(r["metrics"]).get("recall_at_10", 0) for r in strategy_results]) / len(strategy_results)
        avg_ndcg_10 = sum([json.loads(r["metrics"]).get("ndcg_at_10", 0) for r in strategy_results]) / len(strategy_results)
        avg_latency = sum([r["metrics"] for r in strategy_results], 0) # simplified, fix below
        
        # Re-calc properly
        recalls = [json.loads(r["metrics"]).get("recall_at_10", 0) for r in strategy_results]
        ndcgs = [json.loads(r["metrics"]).get("ndcg_at_10", 0) for r in strategy_results]
        latencies = [json.loads(r["metrics"]).get("retrieval_latency_ms", 0) for r in strategy_results]
        
        avg_recall_10 = sum(recalls) / len(recalls) if recalls else 0
        avg_ndcg_10 = sum(ndcgs) / len(ndcgs) if ndcgs else 0
        avg_latency = sum(latencies) / len(latencies) if latencies else 0
        
        print(f"  ✅ Logging Metrics to Child Run {run_id_mlflow}")
        print(f"     Recall@10: {avg_recall_10:.4f}")
        print(f"     NDCG@10:   {avg_ndcg_10:.4f}")
        
        # Log to EXISTING Child Run
        client.log_metric(run_id_mlflow, "recall_at_10", avg_recall_10)
        client.log_metric(run_id_mlflow, "ndcg_at_10", avg_ndcg_10)
        client.log_metric(run_id_mlflow, "avg_latency_ms", avg_latency)
        
        all_results.extend(strategy_results)
        
    print(f"Completed {strategy_name}")

# COMMAND ----------

# Write results to Delta (optional, but good for raw data backup)
if all_results:
    from pyspark.sql.functions import current_timestamp as spark_current_timestamp
    # Convert list of dicts to RDD/DF - handle potential schema issues by being explicit or using JSON
    # For MVP, simpler to just assume consistent schema
    try:
        results_df = spark.createDataFrame(all_results)
        results_df = results_df.withColumn("created_at", spark_current_timestamp())
        results_df.write.format("delta").mode("append").saveAsTable(f"{catalog}.{schema}.rl_eval_results")
        print(f"Wrote {len(all_results)} evaluation results to Delta (backup)")
    except Exception as e:
        print(f"⚠️ Failed to write to Delta: {e}")

# Generating Leaderboard - DEPRECATED in favor of MLflow UI
# But we can print a simple text summary
print("\n" + "="*80)
print("EVALUATION COMPLETE - View Results in MLflow Experiments UI")
print("="*80)
print(f"Go to Experiment: {experiment_name}")
print("Use 'Compare Runs' to see Leaderboard with Recall/NDCG.")
