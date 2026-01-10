# Databricks notebook source
# MAGIC %md
# MAGIC # Retrieval Studio - Advanced Evaluation (with Auto Query Generation & Query Type Comparison)
# MAGIC
# MAGIC This notebook demonstrates the advanced evaluation features:
# MAGIC 1. **Automated Query Generation** - Generate synthetic queries from your corpus
# MAGIC 2. **LLM-based Relevance Scoring** - Score results without ground truth labels
# MAGIC 3. **Query Type Comparison** - Compare FULL_TEXT, ANN, and HYBRID search
# MAGIC 4. **Rich Analytics** - Detailed analysis with score distributions, top/bottom queries, etc.

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

# COMMAND ----------
# MAGIC %pip install databricks-vectorsearch mlflow requests pandas --quiet
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
        columns=["text"],  # Adjust based on your schema
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
experiment_name = core_config.get_experiment_name(project_name)
exp = mlflow.set_experiment(experiment_name)

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
                    qr_dict = qr.asDict()
                    expected_raw = qr_dict.get("expected_chunks")

                    expected_ids = None
                    if expected_raw is not None:
                        if isinstance(expected_raw, str):
                            try:
                                expected_ids = json.loads(expected_raw)
                            except:
                                expected_ids = None
                        else:
                            expected_ids = expected_raw

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
                    if expected_ids:
                        # Use labeled metrics if ground truth available
                        metrics = evaluator.compute_labeled_metrics(qtext, retrieved, expected_ids, k_values=[top_k])
                    else:
                        # Use LLM judge for scoring
                        metrics = evaluator.compute_judge_metrics(qtext, retrieved, k_values=[top_k])

                    metrics["retrieval_latency_ms"] = latency_ms

                    # Extract key metrics
                    recall_key = f"recall_at_{top_k}"
                    ndcg_key = f"ndcg_at_{top_k}"
                    relevance_key = f"avg_relevance_at_{top_k}"

                    if recall_key in metrics:
                        recalls.append(float(metrics[recall_key]))
                    if ndcg_key in metrics:
                        ndcgs.append(float(metrics[ndcg_key]))
                    if relevance_key in metrics:
                        relevances.append(float(metrics[relevance_key]))
                    latencies.append(latency_ms)

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
                        "metrics": json.dumps(metrics),
                    })

                    if (i + 1) % 10 == 0:
                        print(f"  Processed {i+1}/{len(query_rows)} queries...")

                # Log aggregate metrics to MLflow
                if recalls:
                    mlflow.log_metric(f"recall_at_{top_k}", sum(recalls)/len(recalls))
                if ndcgs:
                    mlflow.log_metric(f"ndcg_at_{top_k}", sum(ndcgs)/len(ndcgs))
                if relevances:
                    mlflow.log_metric(f"avg_relevance_at_{top_k}", sum(relevances)/len(relevances))
                if latencies:
                    mlflow.log_metric("avg_latency_ms", sum(latencies)/len(latencies))

                print(f"  {query_type} - Avg Relevance: {sum(relevances)/len(relevances):.3f}" if relevances else "")

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
# MAGIC ## Rich Analytics

# COMMAND ----------
# Convert results to pandas for analysis
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
    print("\nHigh Relevance Examples (score >= 2.5):")
    display(analyzer.high_relevance_examples(5))

    # Low relevance examples
    print("\nLow Relevance Examples (score <= 1.0):")
    display(analyzer.low_relevance_examples(5))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Compare Query Types (if enabled)

# COMMAND ----------
if compare_types:
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
print(f"Strategies Evaluated: {len(results_pd['strategy'].unique())}")
print(f"Total Queries: {len(query_rows)}")
print(f"Query Types Tested: {', '.join(query_types)}")
print(f"\nResults saved to: {eval_results_table}")
print(f"MLflow Experiment: {experiment_name}")

# Aggregate metrics across all strategies
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
