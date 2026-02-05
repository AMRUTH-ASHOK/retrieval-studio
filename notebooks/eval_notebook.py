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
dbutils.widgets.text("eval_id", "")
dbutils.widgets.text("build_parent_run_id", "")
dbutils.widgets.text("golden_dataset_table", "")
dbutils.widgets.text("golden_dataset_id", "")
dbutils.widgets.text("generate_golden_dataset", "false")
dbutils.widgets.text("use_golden_dataset", "false")
dbutils.widgets.text("golden_strategy", "")
dbutils.widgets.text("golden_query_type", "ANN")
dbutils.widgets.text("golden_top_k", "")
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
dbutils.widgets.text("judge_model_endpoint", "databricks-claude-sonnet-4-5")
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
eval_id = dbutils.widgets.get("eval_id") or str(uuid.uuid4())
build_parent_run_id = dbutils.widgets.get("build_parent_run_id") or ""
golden_dataset_table = dbutils.widgets.get("golden_dataset_table") or ""
golden_dataset_id = dbutils.widgets.get("golden_dataset_id") or ""
generate_golden_dataset = dbutils.widgets.get("generate_golden_dataset").lower() == "true"
use_golden_dataset = dbutils.widgets.get("use_golden_dataset").lower() == "true"
golden_strategy = dbutils.widgets.get("golden_strategy") or ""
golden_query_type = dbutils.widgets.get("golden_query_type") or "ANN"
golden_top_k_str = dbutils.widgets.get("golden_top_k") or ""
project_name = dbutils.widgets.get("project_name") or "default"
auto_generate = dbutils.widgets.get("auto_generate_queries").lower() == "true"
compare_types = dbutils.widgets.get("compare_query_types").lower() == "true"
top_k = int(dbutils.widgets.get("top_k") or "10")
judge_endpoint = dbutils.widgets.get("judge_model_endpoint")
enable_rich_analytics_str = dbutils.widgets.get("enable_rich_analytics") or "true"
enable_rich_analytics = enable_rich_analytics_str.lower() == "true"

if not build_run_id:
    raise ValueError("Missing build_run_id")

if generate_golden_dataset and use_golden_dataset:
    raise ValueError("generate_golden_dataset and use_golden_dataset cannot both be true")

if use_golden_dataset:
    if not golden_dataset_table:
        golden_dataset_table = core_config.golden_dataset_table(project_name)

    gdf = spark.table(golden_dataset_table)

    if "expected_chunks" not in gdf.columns or "query_text" not in gdf.columns:
        raise ValueError("Golden dataset must include query_text and expected_chunks columns")

    if golden_dataset_id:
        gdf = gdf.filter(gdf.golden_dataset_id == golden_dataset_id)
    else:
        # Use the most recent dataset
        latest_id_row = (
            gdf.select("golden_dataset_id", "created_at")
               .orderBy(gdf.created_at.desc())
               .limit(1)
               .collect()
        )
        if latest_id_row:
            golden_dataset_id = latest_id_row[0]["golden_dataset_id"]
            gdf = gdf.filter(gdf.golden_dataset_id == golden_dataset_id)

    query_rows = gdf.select("query_text", "expected_chunks").collect()
    if len(query_rows) == 0:
        raise ValueError(f"No rows found in golden dataset {golden_dataset_table} (id={golden_dataset_id})")
    print(f"Loaded {len(query_rows)} queries from golden dataset {golden_dataset_table} (id={golden_dataset_id})")

elif auto_generate:
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

if not build_parent_run_id:
    try:
        parent_runs = mlflow.search_runs(
            experiment_ids=[exp.experiment_id],
            filter_string=f"params.build_run_id = '{build_run_id}' AND tags.rs_role = 'build_parent'",
            max_results=1,
        )
        if not parent_runs.empty:
            build_parent_run_id = parent_runs.iloc[0]["run_id"]
            print(f"Resolved build_parent_run_id: {build_parent_run_id}")
        else:
            print(f"[WARNING] No build_parent_run_id found for build_run_id={build_run_id}")
    except Exception as e:
        print(f"[WARNING] Failed to resolve build_parent_run_id: {e}")

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

# COMMAND ----------
# MAGIC %md
# MAGIC ## Generate Golden Dataset (Optional)

# COMMAND ----------
if generate_golden_dataset:
    if not golden_dataset_table:
        golden_dataset_table = core_config.golden_dataset_table(project_name)

    if not golden_dataset_id:
        golden_dataset_id = str(uuid.uuid4())

    label_top_k = int(golden_top_k_str or top_k)

    # Pick a strategy to generate golden labels (prefer baseline)
    strategy_name = golden_strategy or "baseline"
    strategy_row = None
    try:
        strategy_row = build_child_runs[build_child_runs["params.strategy_name"] == strategy_name].iloc[0]
    except Exception:
        strategy_row = build_child_runs.iloc[0]
        strategy_name = strategy_row.get("params.strategy_name")

    index_name = strategy_row.get("params.vs_index_name")
    vs_endpoint = strategy_row.get("params.vs_endpoint")

    if not index_name or not vs_endpoint:
        raise ValueError("Missing vs_endpoint or vs_index_name for golden dataset generation")

    print(f"Generating golden dataset using strategy={strategy_name}, query_type={golden_query_type}, k={label_top_k}")

    golden_rows = []
    for i, qr in enumerate(query_rows):
        qr_dict = qr.asDict() if hasattr(qr, "asDict") else dict(qr)
        qtext = qr_dict.get("query_text")
        if not qtext:
            continue

        retrieved = query_index(
            vs_client=vs_client,
            endpoint_name=vs_endpoint,
            index_name=index_name,
            query_text=qtext,
            k=label_top_k,
            query_type=golden_query_type,
        )

        labels = evaluator.label_expected_chunks(qtext, retrieved, max_chunks=label_top_k)
        expected_chunk_ids = labels.get("expected_chunk_ids", [])
        expected_chunks_payload = [
            c.get("chunk_text") for c in (labels.get("expected_chunks", []) or [])
            if isinstance(c, dict) and c.get("chunk_text")
        ]

        golden_rows.append({
            "golden_dataset_id": golden_dataset_id,
            "eval_id": eval_id,
            "project": project_name,
            "build_run_id": build_run_id,
            "strategy": strategy_name,
            "query_text": qtext,
            "expected_chunk_ids": json.dumps(expected_chunk_ids),
            "expected_chunks": json.dumps(expected_chunks_payload),
            "query_type": golden_query_type,
        })

        if (i + 1) % 10 == 0:
            print(f"  Labeled {i+1}/{len(query_rows)} queries...")

    from pyspark.sql.types import StructType, StructField, StringType

    golden_schema = StructType([
        StructField("golden_dataset_id", StringType(), False),
        StructField("eval_id", StringType(), False),
        StructField("project", StringType(), False),
        StructField("build_run_id", StringType(), False),
        StructField("strategy", StringType(), False),
        StructField("query_text", StringType(), False),
        StructField("expected_chunk_ids", StringType(), True),
        StructField("expected_chunks", StringType(), True),
        StructField("query_type", StringType(), True),
    ])

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {golden_dataset_table} (
      golden_dataset_id STRING,
      eval_id STRING,
      project STRING,
      build_run_id STRING,
      strategy STRING,
      query_text STRING,
      expected_chunk_ids STRING,
      expected_chunks STRING,
      query_type STRING,
      created_at TIMESTAMP
    )
    USING DELTA
    """)

    for column in ["expected_chunk_ids", "expected_chunks", "golden_dataset_id", "eval_id", "query_type"]:
        try:
            spark.sql(f"ALTER TABLE {golden_dataset_table} ADD COLUMNS ({column} STRING)")
        except Exception:
            pass

    if golden_rows:
        gdf = spark.createDataFrame(golden_rows, schema=golden_schema).withColumn("created_at", current_timestamp())
        gdf.write.format("delta").mode("append").saveAsTable(golden_dataset_table)
        print(f"Saved golden dataset to {golden_dataset_table} (id={golden_dataset_id}) with {len(golden_rows)} rows")

        query_rows = gdf.select("query_text", "expected_chunks").collect()

# Create eval results table
eval_results_table = core_config.eval_results_table()
spark.sql(f"""
CREATE TABLE IF NOT EXISTS {eval_results_table} (
  eval_result_id STRING,
  eval_id STRING,
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

# Add new columns if they don't exist (for backwards compatibility)
try:
    spark.sql(f"ALTER TABLE {eval_results_table} ADD COLUMNS (eval_id STRING)")
    print(f"Added eval_id column to {eval_results_table}")
except Exception as e:
    error_str = str(e).lower()
    if ("already exists" in error_str or 
        "cannot resolve" in error_str or 
        "field_already_exists" in error_str or
        "42710" in str(e)):
        print(f"eval_id column already exists in {eval_results_table} - skipping")
    else:
        print(f"Note: Could not add eval_id column (may already exist): {e}")

try:
    spark.sql(f"ALTER TABLE {eval_results_table} ADD COLUMNS (expected_chunks STRING, retrieved_chunks STRING)")
    print(f"Added expected_chunks and retrieved_chunks columns to {eval_results_table}")
except Exception as e:
    error_str = str(e).lower()
    if ("already exists" in error_str or 
        "cannot resolve" in error_str or 
        "field_already_exists" in error_str or
        "42710" in str(e)):  # SQLSTATE 42710 = duplicate column
        print(f"Columns already exist in {eval_results_table} - skipping")
    else:
        print(f"Note: Could not add columns (may already exist): {e}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Run Evaluation

# COMMAND ----------
def start_eval_parent_run(run_name: str, parent_run_id: str):
    if parent_run_id:
        try:
            return mlflow.start_run(run_name=run_name, nested=True, parent_run_id=parent_run_id)
        except TypeError:
            run = mlflow.start_run(run_name=run_name)
            mlflow.set_tag("mlflow.parentRunId", parent_run_id)
            return run
    return mlflow.start_run(run_name=run_name)

# Determine query types to test
if compare_types:
    query_types = ["FULL_TEXT", "ANN", "HYBRID"]
    print("Comparing query types: FULL_TEXT, ANN, HYBRID")
else:
    query_types = ["ANN"]  # Default to ANN only
    print("Using ANN query type only")

all_rows = []
strategy_results = {}  # Store results for analysis

metric_k_values = sorted({k for k in [5, 10, top_k] if k <= top_k})

with start_eval_parent_run(run_name=f"eval_{build_run_id[:8]}", parent_run_id=build_parent_run_id) as eval_parent:
    mlflow.set_tag("rs_role", "eval_parent")
    mlflow.log_param("build_run_id", build_run_id)
    if build_parent_run_id:
        mlflow.log_param("build_parent_run_id", build_parent_run_id)
    mlflow.log_param("project_name", project_name)
    mlflow.log_param("eval_id", eval_id)
    mlflow.set_tag("rs_eval_id", eval_id)
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
                if build_parent_run_id:
                    mlflow.log_param("build_parent_run_id", build_parent_run_id)
                mlflow.log_param("eval_id", eval_id)
                mlflow.set_tag("rs_eval_id", eval_id)
                mlflow.log_param("build_child_run_id", build_child_run_id)
                mlflow.log_param("strategy_name", strategy_name)
                mlflow.log_param("query_type", query_type)
                mlflow.log_param("vs_endpoint", vs_endpoint)
                mlflow.log_param("vs_index_name", index_name)
                mlflow.set_tag("strategy", strategy_name)
                mlflow.set_tag("query_type", query_type)

                recalls_by_k = {k: [] for k in metric_k_values}
                precisions_by_k = {k: [] for k in metric_k_values}
                ndcgs_by_k = {k: [] for k in metric_k_values}
                latencies = []

                for i, qr in enumerate(query_rows):
                    qtext = qr["query_text"]
                    # Convert Spark Row to dict first, then use .get()
                    qr_dict = qr.asDict() if hasattr(qr, 'asDict') else dict(qr)
                    expected_raw = qr_dict.get("expected_chunks")

                    expected_texts = []
                    if expected_raw is not None:
                        if isinstance(expected_raw, str):
                            try:
                                parsed = json.loads(expected_raw)
                                expected_raw = parsed
                            except json.JSONDecodeError:
                                expected_texts = [expected_raw]

                        if isinstance(expected_raw, (list, tuple)):
                            if expected_raw and isinstance(expected_raw[0], dict):
                                expected_texts = [
                                    str(e.get("chunk_text") or e.get("text") or "")
                                    for e in expected_raw
                                    if isinstance(e, dict) and (e.get("chunk_text") or e.get("text"))
                                ]
                            else:
                                expected_texts = [str(e) for e in expected_raw if e]
                        elif isinstance(expected_raw, dict):
                            if "expected_chunks" in expected_raw:
                                nested = expected_raw.get("expected_chunks")
                                if isinstance(nested, (list, tuple)):
                                    if nested and isinstance(nested[0], dict):
                                        expected_texts = [
                                            str(e.get("chunk_text") or e.get("text") or "")
                                            for e in nested
                                            if isinstance(e, dict) and (e.get("chunk_text") or e.get("text"))
                                        ]
                                    else:
                                        expected_texts = [str(e) for e in nested if e]
                                elif isinstance(nested, str):
                                    expected_texts = [nested]
                    
                    # Debug first query
                    if i == 0:
                        print(f"  [DEBUG] Query {i+1}: '{qtext[:50]}...'")
                        print(f"  [DEBUG] Expected raw type: {type(expected_raw)}, value: {expected_raw}")
                        print(f"  [DEBUG] Parsed expected_texts: {expected_texts}")

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
                        metrics = evaluator.compute_labeled_metrics_by_text(
                            qtext,
                            retrieved,
                            expected_texts,
                            k_values=metric_k_values
                        )
                        
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
                        print(f"  [DEBUG] Expected texts: {len(expected_texts)}")
                        print(f"  [DEBUG] Retrieved chunks: {len(retrieved)}")

                    # Extract key metrics - handle both labeled and judge metrics
                    for k in metric_k_values:
                        recall_key = f"recall_at_{k}"
                        precision_key = f"precision_at_{k}"
                        ndcg_key = f"ndcg_at_{k}"
                        if recall_key in metrics:
                            recalls_by_k[k].append(float(metrics[recall_key]))
                        if precision_key in metrics:
                            precisions_by_k[k].append(float(metrics[precision_key]))
                        if ndcg_key in metrics:
                            ndcgs_by_k[k].append(float(metrics[ndcg_key]))
                    
                    # Always append latency
                    latencies.append(latency_ms)
                    
                    # Debug: Print extraction status for first query
                    if i == 0:
                        print(f"  [DEBUG] Recalls list lengths: { {k: len(v) for k, v in recalls_by_k.items()} }")
                        print(f"  [DEBUG] Precisions list lengths: { {k: len(v) for k, v in precisions_by_k.items()} }")
                        print(f"  [DEBUG] NDCGs list lengths: { {k: len(v) for k, v in ndcgs_by_k.items()} }")
                        print(f"  [DEBUG] Latencies list length: {len(latencies)}")

                    # Store row
                    all_rows.append({
                        "eval_result_id": str(uuid.uuid4()),
                        "eval_id": eval_id,
                        "build_run_id": build_run_id,
                        "eval_run_id": eval_child.info.run_id,
                        "build_child_run_id": build_child_run_id,
                        "project": project_name,
                        "strategy": strategy_name,
                        "query_type": query_type,
                        "query_text": qtext,
                        "expected_chunks": json.dumps(expected_texts) if expected_texts else None,
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
                print(f"    Recalls: { {k: len(v) for k, v in recalls_by_k.items()} }")
                print(f"    Precisions: { {k: len(v) for k, v in precisions_by_k.items()} }")
                print(f"    NDCGs: { {k: len(v) for k, v in ndcgs_by_k.items()} }")
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
                
                # Calculate and prepare recall/NDCG/relevance metrics for each k
                for k in metric_k_values:
                    recalls = recalls_by_k.get(k, [])
                    precisions = precisions_by_k.get(k, [])
                    ndcgs = ndcgs_by_k.get(k, [])

                    if recalls:
                        avg_recall = float(sum(recalls) / len(recalls))
                        metrics_to_log[f"recall_at_{k}"] = avg_recall
                        print(f"  ✅ Prepared recall_at_{k}: {avg_recall:.4f}")
                    else:
                        metrics_to_log[f"recall_at_{k}"] = 0.0
                        print(f"  ⚠️  No recall metrics for k={k} - will log 0.0")

                    if precisions:
                        avg_precision = float(sum(precisions) / len(precisions))
                        metrics_to_log[f"precision_at_{k}"] = avg_precision
                        print(f"  ✅ Prepared precision_at_{k}: {avg_precision:.4f}")
                    else:
                        metrics_to_log[f"precision_at_{k}"] = 0.0
                        print(f"  ⚠️  No precision metrics for k={k} - will log 0.0")

                    if ndcgs:
                        avg_ndcg = float(sum(ndcgs) / len(ndcgs))
                        metrics_to_log[f"ndcg_at_{k}"] = avg_ndcg
                        print(f"  ✅ Prepared ndcg_at_{k}: {avg_ndcg:.4f}")
                    else:
                        metrics_to_log[f"ndcg_at_{k}"] = 0.0
                        print(f"  ⚠️  No NDCG metrics for k={k} - will log 0.0")

                
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

# COMMAND ----------
# MAGIC %md
# MAGIC ## Save Results

# COMMAND ----------
if all_rows:
    from pyspark.sql.types import StructType, StructField, StringType

    # Define explicit schema to avoid type inference issues
    schema = StructType([
        StructField("eval_result_id", StringType(), False),
        StructField("eval_id", StringType(), False),
        StructField("build_run_id", StringType(), False),
        StructField("eval_run_id", StringType(), False),
        StructField("build_child_run_id", StringType(), False),
        StructField("project", StringType(), False),
        StructField("strategy", StringType(), False),
        StructField("query_type", StringType(), False),
        StructField("query_text", StringType(), False),
        StructField("expected_chunks", StringType(), True),  # Can be None
        StructField("retrieved_chunks", StringType(), False),
        StructField("metrics", StringType(), False),
    ])

    df = spark.createDataFrame(all_rows, schema=schema).withColumn("created_at", current_timestamp())
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
        # Relevance examples rely on judge scores; skip for labeled-only mode.

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
            if "recall_at_10" in comparison.columns:
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
