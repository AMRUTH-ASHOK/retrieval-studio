# Databricks notebook source
# MAGIC %md
# MAGIC # Retrieval Studio - Evaluation Job (Product)

# COMMAND ----------
dbutils.widgets.text("build_run_id", "")
dbutils.widgets.text("project_name", "default")
dbutils.widgets.text("queries_table", "")  # Optional: for manual queries
dbutils.widgets.text("corpus_table", "")  # Optional: for auto query generation
dbutils.widgets.text("dataset_type", "delta_table")
dbutils.widgets.text("top_k", "10")
dbutils.widgets.text("catalog", "")
dbutils.widgets.text("schema", "")
dbutils.widgets.text("auto_generate_queries", "false")  # Set to "true" for auto generation
dbutils.widgets.text("num_queries", "50")  # Number of queries to generate
dbutils.widgets.text("query_style", "keyword")  # keyword, natural, or mixed
dbutils.widgets.text("compare_query_types", "false")  # Set to "true" to compare query types
dbutils.widgets.text("judge_model_endpoint", "")  # Optional: LLM judge endpoint for scoring without ground truth

# COMMAND ----------
# MAGIC %pip install databricks-vectorsearch mlflow requests --quiet
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
auto_generate = dbutils.widgets.get("auto_generate_queries").lower() == "true"
compare_types = dbutils.widgets.get("compare_query_types").lower() == "true"
top_k = int(dbutils.widgets.get("top_k") or "10")
judge_endpoint = dbutils.widgets.get("judge_model_endpoint") or None

if not build_run_id:
    raise ValueError("Missing build_run_id")

# COMMAND ----------
# Load or Generate Queries
if auto_generate:
    # Automated Query Generation
    from retrieval_core.query_generator import QueryGenerator
    
    corpus_table = dbutils.widgets.get("corpus_table")
    if not corpus_table:
        raise ValueError("corpus_table required for auto query generation")
    
    num_queries = int(dbutils.widgets.get("num_queries") or "50")
    query_style = dbutils.widgets.get("query_style") or "keyword"
    
    print(f"Generating {num_queries} {query_style} queries from {corpus_table}...")
    
    generator = QueryGenerator(random_seed=42)
    
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
    
    # Load queries based on dataset type
    if dataset_type == "delta_table":
        qdf = spark.table(queries_table)
    elif dataset_type == "csv":
        # Load CSV file
        qdf = spark.read.option("header", "true").option("inferSchema", "true").csv(queries_table)
    elif dataset_type == "excel":
        # Load Excel file (requires additional library)
        try:
            qdf = spark.read.format("com.crealytics.spark.excel").option("header", "true").load(queries_table)
        except:
            raise ValueError("Excel file support requires spark-excel library. Please install it or convert to CSV.")
    else:
        raise ValueError(f"Unsupported dataset_type: {dataset_type}. Supported types: delta_table, csv, excel")
    
    if "query_text" not in qdf.columns:
        raise ValueError("Dataset must include query_text column")
    
    cols = ["query_text"]
    if "expected_chunks" in qdf.columns:
        cols.append("expected_chunks")
    
    query_rows = qdf.select(*cols).collect()
    print(f"Loaded {len(query_rows)} queries")

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

# COMMAND ----------
# Initialize Evaluator with LLM Judge (if provided)
evaluator = RetrievalEvaluator(judge_model_endpoint=judge_endpoint)
if judge_endpoint:
    print(f"Using LLM judge: {judge_endpoint}")
else:
    print("Using default evaluator (requires ground truth for scoring)")

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
all_rows = []

# Determine query types to test
if compare_types:
    query_types = ["FULL_TEXT", "ANN", "HYBRID"]
    print("Comparing query types: FULL_TEXT, ANN, HYBRID")
else:
    query_types = ["ANN"]  # Default to ANN only
    print("Using ANN query type only")

with mlflow.start_run(run_name=f"eval_{build_run_id[:8]}") as eval_parent:
    mlflow.set_tag("rs_role", "eval_parent")
    mlflow.log_param("build_run_id", build_run_id)
    mlflow.log_param("project_name", project_name)
    mlflow.log_param("top_k", str(top_k))
    mlflow.log_param("auto_generate_queries", str(auto_generate))
    mlflow.log_param("compare_query_types", str(compare_types))
    if judge_endpoint:
        mlflow.log_param("judge_model_endpoint", judge_endpoint)

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

                recalls, ndcgs, latencies = [], [], []

                for qr in query_rows:
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

                    t0 = time.time()
                    retrieved = query_index(
                        vs_client=vs_client,
                        endpoint_name=vs_endpoint,
                        index_name=index_name,
                        query_text=qtext,
                        k=top_k,
                        query_type=query_type if compare_types else None,  # Only use query_type if comparing
                    )
                    latency_ms = (time.time() - t0) * 1000.0

                    if expected_ids:
                        metrics = evaluator.compute_labeled_metrics(qtext, retrieved, expected_ids, k_values=[top_k])
                    else:
                        # Use LLM judge if no ground truth
                        metrics = evaluator.compute_judge_metrics(qtext, retrieved, k_values=[top_k])

                    metrics["retrieval_latency_ms"] = latency_ms

                    # Use recall if available, otherwise use judge score
                    recall_key = f"recall_at_{top_k}"
                    judge_key = f"judge_score_at_{top_k}"
                    if recall_key in metrics:
                        recalls.append(float(metrics.get(recall_key, 0.0)))
                    elif judge_key in metrics:
                        recalls.append(float(metrics.get(judge_key, 0.0)))
                    else:
                        recalls.append(0.0)
                    
                    ndcgs.append(float(metrics.get(f"ndcg_at_{top_k}", 0.0)))
                    latencies.append(float(metrics.get("retrieval_latency_ms", 0.0)))

                    all_rows.append({
                        "eval_result_id": str(uuid.uuid4()),
                        "build_run_id": build_run_id,
                        "eval_run_id": eval_child.info.run_id,
                        "build_child_run_id": build_child_run_id,
                        "project": project_name,
                        "strategy": strategy_name,
                        "query_text": qtext,
                        "query_type": query_type,
                        "metrics": json.dumps(metrics),
                    })

                mlflow.log_metric(f"recall_at_{top_k}", sum(recalls)/len(recalls) if recalls else 0.0)
                mlflow.log_metric(f"ndcg_at_{top_k}", sum(ndcgs)/len(ndcgs) if ndcgs else 0.0)
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
