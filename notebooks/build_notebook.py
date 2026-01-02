# Databricks notebook source
# MAGIC %md
# MAGIC # Retrieval Studio - Build Job
# MAGIC This notebook chunks documents and creates Vector Search indexes

# COMMAND ----------

# MAGIC %pip install databricks-vectorsearch mlflow --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

# Widget parameters
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("config", "{}")
dbutils.widgets.text("catalog", "main")
dbutils.widgets.text("schema", "retrieval_studio")

# COMMAND ----------

import json
import sys
import os
from pyspark.sql import SparkSession
from databricks.vector_search.client import VectorSearchClient
import uuid

spark = SparkSession.builder.getOrCreate()
vs_client = VectorSearchClient()

# Add core to path - adjust based on workspace structure
sys.path.append("/Workspace" + os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()))

# COMMAND ----------

# Helper function for state management using Spark SQL
def update_run_state_spark(spark, catalog, schema, run_id, state, **kwargs):
    """Update run state using Spark SQL"""
    updates = {"state": f"'{state}'"}
    for key, value in kwargs.items():
        if value is not None:
            if isinstance(value, str):
                # Escape single quotes in strings
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

# COMMAND ----------

try:
    from core.strategies import BaselineStrategy, StructuredStrategy, ParentChildStrategy
    from utils.mlflow_utils import create_or_get_experiment, log_build_run
    from utils.vs_utils import create_vs_index, wait_for_index
except ImportError:
    import importlib.util
    pass

# COMMAND ----------

# Parse parameters
run_id = dbutils.widgets.get("run_id")
config_json = dbutils.widgets.get("config")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

config = json.loads(config_json)

# COMMAND ----------

# Helper function to load PDFs (placeholder - would use pymupdf or pdfplumber)
def load_pdfs_from_path(source_path: str) -> list:
    """Load PDFs from path and extract text"""
    documents = []
    return documents

def load_documents_from_table(table_name: str) -> list:
    """Load documents from Delta table"""
    df = spark.table(table_name)
    documents = []
    for row in df.collect():
        row_dict = row.asDict() if hasattr(row, 'asDict') else dict(row)
        documents.append({
            "doc_id": row_dict.get("doc_id", str(uuid.uuid4())),
            "doc_name": row_dict.get("doc_name", "unknown"),
            "text": row_dict.get("text", "")
        })
    return documents

# COMMAND ----------

# Update state to RUNNING using Spark SQL
try:
    update_run_state_spark(spark, catalog, schema, run_id, "RUNNING")
    print("✅ Updated run state to RUNNING")
except Exception as e:
    print(f"Warning: Could not update run state: {e}")

# COMMAND ----------

try:
    # Create or get MLflow experiment
    project_name = config.get("project_name", "default")
    experiment_name = f"/RetrievalStudio/{catalog}.{schema}/{project_name}"
    experiment_id = create_or_get_experiment(experiment_name)
    
    # Update run with experiment_id using Spark SQL
    update_run_state_spark(spark, catalog, schema, run_id, "RUNNING", experiment_id=experiment_id)
    
    # Load documents
    source_type = config.get("source_type", "pdf")
    source_path = config.get("source_path", "")
    
    documents = []
    if source_type == "pdf" or source_type == "pdf_upload":
        # Load PDFs from Volume or DBFS
        documents = load_pdfs_from_path(source_path)
    elif source_type == "uc_volume":
        # Load from Unity Catalog Volume
        documents = load_pdfs_from_path(source_path)
    elif source_type == "table" or source_type == "delta_table":
        # Load from Delta table
        table_name = config.get("source_table", source_path)
        documents = load_documents_from_table(table_name)
    
    if not documents:
        raise ValueError("No documents loaded from source")
    
    # Apply chunking strategies
    strategies = config.get("strategies", ["baseline"])
    index_names = {}
    chunks_tables = {}
    
    for strategy_name in strategies:
        print(f"Processing strategy: {strategy_name}")
        
        # Get strategy instance
        if strategy_name == "baseline":
            strategy = BaselineStrategy(
                chunk_size=config.get("chunk_size", 512),
                overlap=config.get("overlap", 50)
            )
        elif strategy_name == "structured":
            strategy = StructuredStrategy(
                preserve_hierarchy=config.get("preserve_hierarchy", True),
                max_chunk_size=config.get("max_chunk_size", 1024)
            )
        elif strategy_name == "parent_child":
            strategy = ParentChildStrategy(
                parent_size=config.get("parent_size", 2048),
                child_size=config.get("child_size", 256)
            )
        else:
            print(f"Unknown strategy: {strategy_name}, skipping")
            continue
        
        # Chunk documents
        chunks = strategy.chunk(documents)
        print(f"Created {len(chunks)} chunks for strategy {strategy_name}")
        
        # Convert to DataFrame with actual timestamps
        from pyspark.sql.functions import current_timestamp as spark_current_timestamp
        
        chunks_data = []
        for c in chunks:
            chunks_data.append({
                "chunk_id": c.chunk_id,
                "doc_id": c.doc_id,
                "doc_name": c.doc_name,
                "chunk_text": c.chunk_text,
                "chunk_index": c.chunk_index,
                "metadata": json.dumps(c.metadata),
                "parent_chunk_id": c.parent_chunk_id if c.parent_chunk_id else None
            })
        
        chunks_df = spark.createDataFrame(chunks_data)
        chunks_df = chunks_df.withColumn("created_at", spark_current_timestamp())
        
        # Write to Delta table
        chunks_table = f"{catalog}.{schema}.rl_chunks_{strategy_name}"
        chunks_df.write.format("delta").mode("overwrite").saveAsTable(chunks_table)
        chunks_tables[strategy_name] = chunks_table
        print(f"Wrote chunks to {chunks_table}")
        
        # Create Vector Search index
        index_name = f"rl_index_{strategy_name}_{run_id[:8]}"
        embedding_endpoint = config.get("embedding_model_endpoint", "")
        vs_endpoint_name = config.get("vs_endpoint_name", None)
        
        if embedding_endpoint:
            print(f"Creating Vector Search index: {index_name}")
            create_vs_index(
                vs_client,
                index_name=index_name,
                source_table=chunks_table,
                embedding_endpoint=embedding_endpoint,
                vs_endpoint_name=vs_endpoint_name
            )
            
            # Wait for index to be ready
            print(f"Waiting for index {index_name} to be ready...")
            ready = wait_for_index(vs_client, index_name, timeout_minutes=30)
            if ready:
                print(f"Index {index_name} is ready")
            else:
                print(f"Warning: Index {index_name} did not become ready within timeout")
            
            index_names[strategy_name] = index_name
        else:
            print(f"Skipping index creation for {strategy_name} (no embedding endpoint)")
    
    # Log build run to MLflow
    log_build_run(
        experiment_id=experiment_id,
        run_id=run_id,
        config=config,
        chunks_table=chunks_tables.get("baseline") or list(chunks_tables.values())[0] if chunks_tables else None,
        index_names=index_names,
        state="SUCCESS"
    )
    
    # Update state to SUCCESS using Spark SQL
    update_run_state_spark(spark, catalog, schema, run_id, "SUCCESS")
    
    print(f"Build completed successfully!")
    print(f"Run ID: {run_id}")
    print(f"Experiment ID: {experiment_id}")
    print(f"Indexes created: {index_names}")

except Exception as e:
    import traceback
    error_msg = str(e)
    traceback.print_exc()
    
    # Update state to FAILED using Spark SQL
    update_run_state_spark(spark, catalog, schema, run_id, "FAILED", error_message=error_msg)
    
    # Log error to MLflow if experiment exists
    try:
        # Get run status using Spark SQL
        run_status_df = spark.sql(f"SELECT experiment_id FROM {catalog}.{schema}.rl_runs WHERE run_id = '{run_id}'")
        if run_status_df.count() > 0:
            exp_id = run_status_df.first()["experiment_id"]
            if exp_id:
                log_build_run(
                    experiment_id=exp_id,
                    run_id=run_id,
                    config=config,
                    state="FAILED"
                )
    except Exception:
        pass
    
    raise

