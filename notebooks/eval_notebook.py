# Databricks notebook source
# MAGIC %md
# MAGIC # Retrieval Studio - Evaluation Job
# MAGIC
# MAGIC This notebook supports comprehensive evaluation features:
# MAGIC 1. **Automated Query Generation** - Generate synthetic queries from your corpus
# MAGIC 2. **LLM-based Relevance Scoring** - Score results without ground truth labels
# MAGIC 3. **Query Type Comparison** - Compare FULL_TEXT, ANN, and HYBRID search
# MAGIC 4. **Rich Analytics** (optional) - Detailed analysis with score distributions, top/bottom queries, etc.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------
dbutils.widgets.text("build_run_id", "")
dbutils.widgets.text("project_name", "default")
dbutils.widgets.text("corpus_table", "")  # Optional: for auto query generation
dbutils.widgets.text("queries_table", "")  # Optional: for manual queries
dbutils.widgets.text("dataset_type", "delta_table")
dbutils.widgets.text("top_k", "10")
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("auto_generate_queries", "false")  # Set to "true" for auto generation
dbutils.widgets.text("num_queries", "50")  # Number of queries to generate
dbutils.widgets.text("query_style", "keyword")  # keyword, natural, or mixed
dbutils.widgets.text("compare_query_types", "false")  # Set to "true" to compare query types
dbutils.widgets.text("judge_model_endpoint", "databricks-meta-llama-3-1-70b-instruct")
dbutils.widgets.text("enable_rich_analytics", "false")  # Set to "false" to skip rich analytics section

# COMMAND ----------
# MAGIC %pip install databricks-vectorsearch mlflow requests pandas openai --quiet
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
    sys.path.insert(0, project_root)

from retrieval_core.configs import config as core_config

# Apply catalog/schema overrides
catalog_override = dbutils.widgets.get("catalog")
schema_override = dbutils.widgets.get("schema")

if catalog_override:
    type(core_config).UC_CATALOG = catalog_override
if schema_override:
    type(core_config).RAW_SCHEMA = schema_override

from retrieval_core.evaluator import RetrievalEvaluator
from retrieval_core.analyzer import EvaluationAnalyzer, compare_evaluations
from retrieval_core.query_generator import QueryGenerator
from utils.vs_utils import query_index

# COMMAND ----------
# MAGIC %md
# MAGIC ## Load or Generate Queries

# COMMAND ----------
build_run_id = dbutils.widgets.get("build_run_id")
project_name = dbutils.widgets.get("project_name") or "default"
auto_generate = dbutils.widgets.get("auto_generate_queries").lower() == "true"
compare_types = dbutils.widgets.get("compare_query_types").lower() == "true"
top_k = int(dbutils.widgets.get("top_k") or "10")
judge_endpoint = dbutils.widgets.get("judge_model_endpoint")
enable_rich_analytics_str = dbutils.widgets.get("enable_rich_analytics") or "true"
enable_rich_analytics = enable_rich_analytics_str.lower() == "true"

if not build_run_id:
    raise ValueError("Missing build_run_id")

if auto_generate:
    # Automated Query Generation
    corpus_table = dbutils.widgets.get("corpus_table")
    if not corpus_table:
        raise ValueError("corpus_table required for auto query generation")

    num_queries = int(dbutils.widgets.get("num_queries") or "50")
    query_style = dbutils.widgets.get("query_style") or "keyword"

    print(f"Generating {num_queries} {query_style} queries from {corpus_table}...")

    generator = QueryGenerator(random_seed=42)

    # Optional: Set few-shot examples for better query generation
    # generator.set_few_shot_examples([
    #     {"document": "Example document text...", "query": "example query"}
    # ])

    # Generate queries
    queries_df = generator.generate_queries(
        corpus_table=corpus_table,
        columns=["chunk_text"],  # Chunks table uses chunk_text column
        num_queries=num_queries,
        style=query_style,
        spark_session=spark
    )

    query_rows = queries_df.select("query_text", "doc_id").collect()
    print(f"Generated {len(query_rows)} queries")

else:
    # Manual Query Dataset
    queries_table = dbutils.widgets.get("queries_table")
    dataset_type = dbutils.widgets.get("dataset_type") or "delta_table"

    if not queries_table:
        raise ValueError("Missing queries_table (or set auto_generate_queries=true)")

    # Load queries
    if dataset_type == "delta_table":
        qdf = spark.table(queries_table)
    elif dataset_type == "csv":
        qdf = spark.read.option("header", "true").option("inferSchema", "true").csv(queries_table)
    elif dataset_type == "excel":
        try:
            qdf = spark.read.format("com.crealytics.spark.excel").option("header", "true").load(queries_table)
        except:
            raise ValueError("Excel file support requires spark-excel library")
    else:
        raise ValueError(f"Unsupported dataset_type: {dataset_type}")

    if "query_text" not in qdf.columns:
        raise ValueError("Dataset must include query_text column")

    cols = ["query_text"]
    if "expected_chunks" in qdf.columns:
        cols.append("expected_chunks")

    query_rows = qdf.select(*cols).collect()
    print(f"Loaded {len(query_rows)} queries")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Find Build Strategy Runs

# COMMAND ----------
import mlflow

# Ensure MLflow is configured for Databricks
# In Databricks, this is usually automatic, but we'll set it explicitly
try:
    # Try to set tracking URI to databricks (if not already set)
    current_uri = mlflow.get_tracking_uri()
    if not current_uri or current_uri == "databricks":
        mlflow.set_tracking_uri("databricks")
    print(f"MLflow tracking URI: {mlflow.get_tracking_uri()}")
except Exception as e:
    print(f"Note: Could not set tracking URI (may already be configured): {e}")

experiment_name = core_config.get_experiment_name(project_name)
exp = mlflow.set_experiment(experiment_name)
print(f"MLflow experiment: {experiment_name} (ID: {exp.experiment_id})")

build_child_runs = mlflow.search_runs(
    experiment_ids=[exp.experiment_id],
    filter_string=f"params.build_run_id = '{build_run_id}' AND tags.rs_role = 'build_strategy'",
)

if build_child_runs.empty:
    raise ValueError(f"No build strategy runs found for build_run_id={build_run_id}")

print(f"Found {len(build_child_runs)} strategies to evaluate")
print("Strategies:", list(build_child_runs["params.strategy_name"]))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Initialize Evaluator with LLM Judge

# COMMAND ----------
evaluator = RetrievalEvaluator(judge_model_endpoint=judge_endpoint)

# Create eval results table
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
  query_type STRING,
  metrics STRING,
  created_at TIMESTAMP
)
USING DELTA
""")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Run Evaluation

# COMMAND ----------
# Determine query types to test
if compare_types:
    query_types = ["FULL_TEXT", "ANN", "HYBRID"]
    print("Comparing query types: FULL_TEXT, ANN, HYBRID")
else:
    query_types = ["ANN"]  # Default to ANN only
    print("Using ANN query type only")

all_rows = []
strategy_results = {}  # Store results for analysis

with mlflow.start_run(run_name=f"eval_{build_run_id[:8]}") as eval_parent:
    mlflow.set_tag("rs_role", "eval_parent")
    mlflow.log_param("build_run_id", build_run_id)
    mlflow.log_param("project_name", project_name)
    mlflow.log_param("top_k", str(top_k))
    mlflow.log_param("auto_generate_queries", str(auto_generate))
    mlflow.log_param("compare_query_types", str(compare_types))

    for _, r in build_child_runs.iterrows():
        build_child_run_id = r.run_id
        strategy_name = r.get("params.strategy_name")
        index_name = r.get("params.vs_index_name")
        vs_endpoint = r.get("params.vs_endpoint")

        if not (strategy_name and index_name and vs_endpoint):
            continue

        print(f"\n{'='*60}")
        print(f"Evaluating strategy: {strategy_name}")
        print(f"{'='*60}")

        # Test each query type
        for query_type in query_types:
            print(f"\n--- Testing {query_type} ---")

            with mlflow.start_run(run_name=f"eval_{strategy_name}_{query_type}", nested=True) as eval_child:
                mlflow.set_tag("rs_role", "eval_strategy")
                mlflow.log_param("build_run_id", build_run_id)
                mlflow.log_param("build_child_run_id", build_child_run_id)
                mlflow.log_param("strategy_name", strategy_name)
                mlflow.log_param("query_type", query_type)
                mlflow.log_param("vs_endpoint", vs_endpoint)
                mlflow.log_param("vs_index_name", index_name)

                recalls, ndcgs, relevances, latencies = [], [], [], []

                for i, qr in enumerate(query_rows):
                    qtext = qr["query_text"]
                    # Convert Spark Row to dict first, then use .get()
                    qr_dict = qr.asDict() if hasattr(qr, 'asDict') else dict(qr)
                    expected_raw = qr_dict.get("expected_chunks")

                    expected_ids = None
                    if expected_raw is not None:
                        if isinstance(expected_raw, str):
                            try:
                                expected_ids = json.loads(expected_raw)
                            except json.JSONDecodeError:
                                # Try to parse as comma-separated string
                                try:
                                    expected_ids = [x.strip() for x in expected_raw.split(",") if x.strip()]
                                except:
                                    expected_ids = None
                        elif isinstance(expected_raw, (list, tuple)):
                            expected_ids = list(expected_raw)
                        else:
                            expected_ids = None
                    
                    # Debug first query
                    if i == 0:
                        print(f"  [DEBUG] Query {i+1}: '{qtext[:50]}...'")
                        print(f"  [DEBUG] Expected IDs type: {type(expected_raw)}, value: {expected_raw}")
                        print(f"  [DEBUG] Parsed expected_ids: {expected_ids}")

                    # Query index with specified query type
                    t0 = time.time()
                    retrieved = query_index(
                        vs_client=vs_client,
                        endpoint_name=vs_endpoint,
                        index_name=index_name,
                        query_text=qtext,
                        k=top_k,
                        query_type=query_type,
                    )
                    latency_ms = (time.time() - t0) * 1000.0

                    # Compute metrics
                    try:
                        if expected_ids:
                            # Use labeled metrics if ground truth available
                            metrics = evaluator.compute_labeled_metrics(qtext, retrieved, expected_ids, k_values=[top_k])
                        else:
                            # Use LLM judge for scoring
                            metrics = evaluator.compute_judge_metrics(qtext, retrieved, k_values=[top_k])
                        
                        # Ensure metrics is a dict
                        if not isinstance(metrics, dict):
                            metrics = {}
                    except Exception as e:
                        print(f"  [ERROR] Failed to compute metrics for query {i+1}: {e}")
                        metrics = {}
                    
                    # Always add latency
                    metrics["retrieval_latency_ms"] = latency_ms

                    # Debug: Print metrics keys for first query
                    if i == 0:
                        print(f"  [DEBUG] Metrics keys: {list(metrics.keys())}")
                        print(f"  [DEBUG] Expected IDs: {expected_ids is not None}")
                        print(f"  [DEBUG] Retrieved chunks: {len(retrieved)}")

                    # Extract key metrics - handle both labeled and judge metrics
                    recall_key = f"recall_at_{top_k}"
                    ndcg_key = f"ndcg_at_{top_k}"
                    relevance_key = f"avg_relevance_at_{top_k}"
                    judge_key = f"judge_score_at_{top_k}"

                    # For labeled metrics (with ground truth)
                    if recall_key in metrics:
                        recalls.append(float(metrics[recall_key]))
                    if ndcg_key in metrics:
                        ndcgs.append(float(metrics[ndcg_key]))
                    
                    # For judge metrics (without ground truth)
                    if relevance_key in metrics:
                        relevances.append(float(metrics[relevance_key]))
                    elif judge_key in metrics:
                        # Use judge_score as relevance if avg_relevance not available
                        relevances.append(float(metrics[judge_key]))
                    
                    # Always append latency
                    latencies.append(latency_ms)
                    
                    # Debug: Print extraction status for first query
                    if i == 0:
                        print(f"  [DEBUG] Recalls list length: {len(recalls)}")
                        print(f"  [DEBUG] NDCGs list length: {len(ndcgs)}")
                        print(f"  [DEBUG] Relevances list length: {len(relevances)}")
                        print(f"  [DEBUG] Latencies list length: {len(latencies)}")

                    # Store row
                    all_rows.append({
                        "eval_result_id": str(uuid.uuid4()),
                        "build_run_id": build_run_id,
                        "eval_run_id": eval_child.info.run_id,
                        "build_child_run_id": build_child_run_id,
                        "project": project_name,
                        "strategy": strategy_name,
                        "query_type": query_type,
                        "query_text": qtext,
                        "expected_chunks": json.dumps(expected_ids) if expected_ids else None,
                        "retrieved_chunks": json.dumps(retrieved),
                        "metrics": json.dumps(metrics),
                    })

                    if (i + 1) % 10 == 0:
                        print(f"  Processed {i+1}/{len(query_rows)} queries...")

                # Log aggregate metrics to MLflow
                # According to MLflow docs: metrics must be numeric (float/int) and logged within active run context
                num_queries_processed = len(query_rows)
                
                # Debug: Print list statuses before logging
                print(f"\n  [DEBUG] Before MLflow logging:")
                print(f"    Recalls: {len(recalls)} items")
                print(f"    NDCGs: {len(ndcgs)} items")
                print(f"    Relevances: {len(relevances)} items")
                print(f"    Latencies: {len(latencies)} items")
                print(f"    Queries processed: {num_queries_processed}")
                print(f"    Active MLflow run: {mlflow.active_run() is not None}")
                if mlflow.active_run():
                    print(f"    Run ID: {mlflow.active_run().info.run_id}")
                
                # Ensure we have at least one query processed
                if num_queries_processed == 0:
                    print(f"  [WARNING] No queries processed for {strategy_name} - {query_type}")
                
                # Prepare all metrics as a dictionary for batch logging (more efficient and reliable)
                metrics_to_log = {}
                
                # Calculate and prepare recall metric
                if recalls and len(recalls) > 0:
                    avg_recall = float(sum(recalls) / len(recalls))
                    metrics_to_log[f"recall_at_{top_k}"] = avg_recall
                    print(f"  ✅ Prepared recall_at_{top_k}: {avg_recall:.4f}")
                else:
                    metrics_to_log[f"recall_at_{top_k}"] = 0.0
                    print(f"  ⚠️  No recall metrics - will log 0.0")
                
                # Calculate and prepare NDCG metric
                if ndcgs and len(ndcgs) > 0:
                    avg_ndcg = float(sum(ndcgs) / len(ndcgs))
                    metrics_to_log[f"ndcg_at_{top_k}"] = avg_ndcg
                    print(f"  ✅ Prepared ndcg_at_{top_k}: {avg_ndcg:.4f}")
                else:
                    metrics_to_log[f"ndcg_at_{top_k}"] = 0.0
                    print(f"  ⚠️  No NDCG metrics - will log 0.0")
                
                # Calculate and prepare relevance/judge metrics
                if relevances and len(relevances) > 0:
                    avg_relevance = float(sum(relevances) / len(relevances))
                    metrics_to_log[f"avg_relevance_at_{top_k}"] = avg_relevance
                    metrics_to_log[f"judge_score_at_{top_k}"] = avg_relevance  # Also log as judge_score for consistency
                    print(f"  ✅ Prepared avg_relevance_at_{top_k}: {avg_relevance:.4f}")
                else:
                    metrics_to_log[f"avg_relevance_at_{top_k}"] = 0.0
                    metrics_to_log[f"judge_score_at_{top_k}"] = 0.0
                    print(f"  ⚠️  No relevance metrics - will log 0.0")
                
                # Calculate and prepare latency metric (should always have data)
                if latencies and len(latencies) > 0:
                    avg_latency = float(sum(latencies) / len(latencies))
                    metrics_to_log["avg_latency_ms"] = avg_latency
                    print(f"  ✅ Prepared avg_latency_ms: {avg_latency:.2f} ms")
                else:
                    metrics_to_log["avg_latency_ms"] = 0.0
                    print(f"  ⚠️  No latency metrics - will log 0.0")
                
                # Add query count (always numeric)
                metrics_to_log["num_queries"] = float(num_queries_processed)
                
                # Log all metrics at once using log_metrics (more efficient and ensures atomicity)
                # CRITICAL: Ensure we're in an active run context
                active_run = mlflow.active_run()
                if not active_run:
                    print(f"  ❌ ERROR: No active MLflow run! Cannot log metrics.")
                    raise RuntimeError("No active MLflow run context")
                
                print(f"  [DEBUG] Active run ID: {active_run.info.run_id}")
                print(f"  [DEBUG] Active run name: {active_run.info.run_name}")
                
                try:
                    # Log metrics - ensure all values are numeric
                    for key, value in metrics_to_log.items():
                        if not isinstance(value, (int, float)):
                            metrics_to_log[key] = float(value) if value is not None else 0.0
                    
                    # Use log_metrics for batch logging
                    mlflow.log_metrics(metrics_to_log)
                    
                    print(f"\n  ✅ Successfully logged {len(metrics_to_log)} metrics to MLflow:")
                    for key, value in metrics_to_log.items():
                        print(f"      {key}: {value}")
                        
                except Exception as e:
                    print(f"\n  ❌ ERROR logging metrics to MLflow: {e}")
                    import traceback
                    traceback.print_exc()
                
                print(f"\n  {query_type} - Summary: Processed {num_queries_processed} queries")
                
                print(f"\n  {query_type} - Summary: Processed {num_queries_processed} queries")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Save Results

# COMMAND ----------
if all_rows:
    df = spark.createDataFrame(all_rows).withColumn("created_at", current_timestamp())
    df.write.format("delta").mode("append").saveAsTable(eval_results_table)
    print(f"Saved {len(all_rows)} result rows to {eval_results_table}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Rich Analytics (Optional)

# COMMAND ----------
# Convert results to pandas for analysis (only if rich analytics enabled)
if enable_rich_analytics and all_rows:
    import pandas as pd
    
    results_pd = pd.DataFrame(all_rows)

    # Analyze by strategy
    print("="*60)
    print("ANALYSIS BY STRATEGY")
    print("="*60)
    
    for strategy_name in results_pd["strategy"].unique():
        strategy_data = results_pd[results_pd["strategy"] == strategy_name]

        print(f"\n{'='*60}")
        print(f"Strategy: {strategy_name}")
        print(f"{'='*60}")

        # Create analyzer
        analyzer = EvaluationAnalyzer(strategy_data)

        # Print summary
        print(analyzer.summary())

        # Score distribution
        print("\nScore Distribution:")
        display(analyzer.score_distribution())

        # Top queries
        print("\nTop 5 Queries:")
        display(analyzer.top_queries(5))

        # Bottom queries
        print("\nBottom 5 Queries:")
        display(analyzer.bottom_queries(5))

        # High relevance examples
        high_rel = analyzer.high_relevance_examples(5)
        if not high_rel.empty:
            print("\nHigh Relevance Examples (score >= 2.5):")
            display(high_rel)
        else:
            print("\nNo high relevance examples found (score >= 2.5).")

        # Low relevance examples
        low_rel = analyzer.low_relevance_examples(5)
        if not low_rel.empty:
            print("\nLow Relevance Examples (score <= 1.0):")
            display(low_rel)
        else:
            print("\nNo low relevance examples found (score <= 1.0).")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Compare Query Types (if enabled)

# COMMAND ----------
if enable_rich_analytics and compare_types and all_rows:
    print("="*60)
    print("QUERY TYPE COMPARISON")
    print("="*60)

    for strategy_name in results_pd["strategy"].unique():
        print(f"\n{'='*60}")
        print(f"Strategy: {strategy_name}")
        print(f"{'='*60}")

        strategy_data = results_pd[results_pd["strategy"] == strategy_name]

        # Split by query type
        results_by_type = {}
        for qt in query_types:
            qt_data = strategy_data[strategy_data["query_type"] == qt]
            if len(qt_data) > 0:
                results_by_type[qt] = qt_data

        if results_by_type:
            # Compare
            comparison = compare_evaluations(results_by_type)
            print("\nComparison Table:")
            display(comparison)

            # Determine winner
            if "avg_relevance_at_10" in comparison.columns:
                best = comparison.loc[comparison["avg_relevance_at_10"].idxmax(), "name"]
                print(f"\nBest query type: {best}")
            elif "recall_at_10" in comparison.columns:
                best = comparison.loc[comparison["recall_at_10"].idxmax(), "name"]
                print(f"\nBest query type: {best}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Summary & Recommendations

# COMMAND ----------
print("="*60)
print("EVALUATION SUMMARY")
print("="*60)
print(f"\nBuild Run: {build_run_id}")
print(f"Project: {project_name}")
if all_rows:
    import pandas as pd
    results_pd = pd.DataFrame(all_rows)
    print(f"Strategies Evaluated: {len(results_pd['strategy'].unique())}")
print(f"Total Queries: {len(query_rows)}")
print(f"Query Types Tested: {', '.join(query_types)}")
print(f"\nResults saved to: {eval_results_table}")
print(f"MLflow Experiment: {experiment_name}")

# Aggregate metrics across all strategies (if rich analytics enabled)
if enable_rich_analytics and all_rows:
    print("\n" + "="*60)
    print("AGGREGATE METRICS ACROSS ALL STRATEGIES")
    print("="*60)
    
    overall_analyzer = EvaluationAnalyzer(results_pd)
    print(overall_analyzer.summary())

# COMMAND ----------
dbutils.notebook.exit(json.dumps({
    "build_run_id": build_run_id,
    "experiment": experiment_name,
    "num_strategy_runs": int(len(build_child_runs)),
    "num_eval_rows": len(all_rows),
    "query_types_tested": query_types,
}))
