# Databricks notebook source
# MAGIC %md
# MAGIC # Retrieval Studio - Evaluation Job
# MAGIC This notebook evaluates retrieval quality for each strategy

# COMMAND ----------

# MAGIC %pip install databricks-vectorsearch mlflow --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# Widget parameters
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("queries_table", "")
dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "retrieval_studio")

# COMMAND ----------

import json
import sys
import os
from pyspark.sql import SparkSession
from databricks.vector_search.client import VectorSearchClient
import time
import uuid

spark = SparkSession.builder.getOrCreate()
vs_client = VectorSearchClient()

# Add core to path
sys.path.append("/Workspace" + os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()))

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

try:
    from core.evaluator import RetrievalEvaluator
    from utils.mlflow_utils import log_eval_run
    from utils.vs_utils import query_index
except ImportError:
    pass

# COMMAND ----------

# Parse parameters
run_id = dbutils.widgets.get("run_id")
queries_table = dbutils.widgets.get("queries_table")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

# Get run status using Spark SQL
run_status = get_run_status_spark(spark, catalog, schema, run_id)
if not run_status:
    raise ValueError(f"Run {run_id} not found")

config = run_status["config"]
experiment_id = run_status.get("experiment_id", "")
strategies = config.get("strategies", ["baseline"])

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

# Evaluate each strategy
all_results = []

for strategy in strategies:
    print(f"Evaluating strategy: {strategy}")
    
    # Get index name (would be stored in run metadata or config)
    index_name = f"rl_index_{strategy}_{run_id[:8]}"
    
    # Evaluate retrieval for this strategy
    strategy_results = []
    
    for query_row in queries_df.collect():
        query_id = query_row.query_id
        query_text = query_row.query_text
        
        try:
            # Retrieve chunks
            start_time = time.time()
            retrieved_chunks = query_index(
                vs_client,
                index_name,
                query_text,
                config.get("embedding_model_endpoint", ""),
                k=config.get("top_k", 10)
            )
            latency_ms = (time.time() - start_time) * 1000
            
            # Compute metrics
            expected_chunks = None
            if query_row.expected_chunks:
                try:
                    expected_chunks = json.loads(query_row.expected_chunks)
                except (json.JSONDecodeError, TypeError):
                    pass
            
            if expected_chunks:
                # Labeled evaluation
                metrics = evaluator.compute_labeled_metrics(
                    query_text,
                    retrieved_chunks,
                    expected_chunks,
                    k_values=[5, 10]
                )
            else:
                # Unlabeled evaluation with LLM judge
                metrics = evaluator.compute_judge_metrics(
                    query_text,
                    retrieved_chunks,
                    k_values=[5, 10]
                )
            
            metrics["retrieval_latency_ms"] = latency_ms
            
            # Store result
            result = {
                "eval_result_id": str(uuid.uuid4()),
                "run_id": run_id,
                "strategy": strategy,
                "mlflow_run_id": "",  # Will be set after logging
                "query_id": query_id,
                "query_text": query_text,
                "retrieved_chunks": json.dumps(retrieved_chunks),
                "metrics": json.dumps(metrics),
                "retrieval_latency_ms": latency_ms
            }
            strategy_results.append(result)
        except Exception as e:
            print(f"Error evaluating query {query_id}: {e}")
            # Store error result
            result = {
                "eval_result_id": str(uuid.uuid4()),
                "run_id": run_id,
                "strategy": strategy,
                "mlflow_run_id": "",
                "query_id": query_id,
                "query_text": query_text,
                "retrieved_chunks": json.dumps([]),
                "metrics": json.dumps({"error": str(e)}),
                "retrieval_latency_ms": 0.0
            }
            strategy_results.append(result)
    
    # Log eval run to MLflow
    # Aggregate metrics
    aggregated_metrics = evaluator.aggregate_metrics(strategy_results)
    
    mlflow_run_id = log_eval_run(
        experiment_id=experiment_id,
        run_id=run_id,
        strategy=strategy,
        metrics=aggregated_metrics,
        leaderboard_path=None,  # Will generate
        examples_path=None,  # Will generate
        state="SUCCESS"
    )
    
    # Update mlflow_run_id in results
    for result in strategy_results:
        result["mlflow_run_id"] = mlflow_run_id
    
    all_results.extend(strategy_results)
    
    print(f"Completed evaluation for {strategy}")

# COMMAND ----------

# Write results to Delta with actual timestamps
if all_results:
    from pyspark.sql.functions import current_timestamp as spark_current_timestamp
    results_df = spark.createDataFrame(all_results)
    results_df = results_df.withColumn("created_at", spark_current_timestamp())
    results_df.write.format("delta").mode("append").saveAsTable(f"{catalog}.{schema}.rl_eval_results")
    print(f"Wrote {len(all_results)} evaluation results to Delta")

# COMMAND ----------

# Generate leaderboard using get_json_object (Spark SQL function)
escaped_run_id = run_id.replace("'", "''")  # Prevent SQL injection
leaderboard_df = spark.sql(f"""
    SELECT 
        strategy,
        AVG(CAST(get_json_object(metrics, '$.recall_at_5') AS DOUBLE)) as avg_recall_at_5,
        AVG(CAST(get_json_object(metrics, '$.recall_at_10') AS DOUBLE)) as avg_recall_at_10,
        AVG(CAST(get_json_object(metrics, '$.ndcg_at_5') AS DOUBLE)) as avg_ndcg_at_5,
        AVG(CAST(get_json_object(metrics, '$.ndcg_at_10') AS DOUBLE)) as avg_ndcg_at_10,
        AVG(retrieval_latency_ms) as avg_latency_ms,
        COUNT(*) as num_queries
    FROM {catalog}.{schema}.rl_eval_results
    WHERE run_id = '{escaped_run_id}'
    GROUP BY strategy
    ORDER BY avg_recall_at_10 DESC
""")

# Save leaderboard - use sanitized table name (replace hyphens with underscores)
safe_run_id = run_id.replace("-", "_")
leaderboard_table = f"{catalog}.{schema}.rl_leaderboard_{safe_run_id}"
leaderboard_df.write.mode("overwrite").saveAsTable(leaderboard_table)

print("Evaluation completed!")
print("\nLeaderboard:")
leaderboard_df.show()

