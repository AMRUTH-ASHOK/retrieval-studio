# Databricks notebook source
# MAGIC %md
# MAGIC # Retrieval Studio - Build Job V2
# MAGIC This notebook supports flexible data types and multiple chunking strategies

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

# Add core to path
workspace_path = os.path.dirname(dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get())
sys.path.append("/Workspace" + workspace_path)
sys.path.append("/Workspace" + os.path.dirname(workspace_path))

# COMMAND ----------

# Helper functions for state management using Spark SQL (not sql_connector)
def update_run_state_spark(spark, catalog, schema, run_id, state, **kwargs):
    """Update run state using Spark SQL"""
    updates = {"state": f"'{state}'"}
    for key, value in kwargs.items():
        if value is not None:
            if isinstance(value, str):
                updates[key] = f"'{value}'"
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

# Import modules
try:
    from core.strategies import get_strategy, get_all_strategies
    from core.data_types import get_data_type_handler
    from utils.mlflow_utils import create_or_get_experiment, log_build_run
    from utils.vs_utils import create_vs_index, wait_for_index
except ImportError as e:
    print(f"Warning: Could not import modules: {e}")
    print("Attempting to import from local paths...")

# COMMAND ----------

# Parse parameters
run_id = dbutils.widgets.get("run_id")
config_json = dbutils.widgets.get("config")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

config = json.loads(config_json)

print(f"Run ID: {run_id}")
print(f"Config: {json.dumps(config, indent=2)}")

# COMMAND ----------

# Update state to RUNNING
try:
    update_run_state_spark(spark, catalog, schema, run_id, "RUNNING")
    print("✅ Updated run state to RUNNING")
except Exception as e:
    print(f"Warning: Could not update run state: {e}")

# COMMAND ----------

# Step 1: Load Documents based on Data Type
print("=" * 80)
print("STEP 1: Loading Documents")
print("=" * 80)

data_type = config.get("data_type", "pdf")
data_config = config.get("data_config", {})

print(f"Data Type: {data_type}")

try:
    # Get the appropriate data type handler
    handler = get_data_type_handler(data_type)
    
    # For delta_table, we need to pass spark instead of sql_connector
    if data_type == "delta_table":
        # Load using Spark directly
        table_name = data_config.get("table_name", "")
        text_column = data_config.get("text_column", "text")
        id_column = data_config.get("id_column")
        
        df = spark.table(table_name)
        documents = []
        for row in df.collect():
            row_dict = row.asDict()
            from core.data_types import Document
            doc = Document(
                doc_id=str(row_dict.get(id_column, uuid.uuid4())) if id_column else str(uuid.uuid4()),
                doc_name=f"row_{uuid.uuid4()}",
                text=str(row_dict.get(text_column, '')),
                metadata={"source_table": table_name},
                data_type="delta_table"
            )
            documents.append(doc)
    else:
        # Load documents using handler
        documents = handler.load_documents(data_config)
    
    print(f"\n✅ Successfully loaded {len(documents)} documents")
    if documents:
        print(f"Sample document: {documents[0].doc_name}")
        print(f"Text length: {len(documents[0].text)} characters")
    
except Exception as e:
    print(f"❌ Error loading documents: {e}")
    import traceback
    traceback.print_exc()
    update_run_state_spark(spark, catalog, schema, run_id, "FAILED", error_message=str(e))
    raise

# COMMAND ----------

# Step 2: Process each strategy
print("\n" + "=" * 80)
print("STEP 2: Processing Chunking Strategies")
print("=" * 80)

strategies_config = config.get("strategies", {})
embedding_model_endpoint = config.get("embedding_model_endpoint")
vs_endpoint_name = config.get("vs_endpoint_name")
create_index = config.get("create_index", True)

print(f"Strategies to process: {list(strategies_config.keys())}")
print(f"Embedding Model: {embedding_model_endpoint}")
print(f"VS Endpoint: {vs_endpoint_name}")

strategy_results = {}

for strategy_name, strategy_params in strategies_config.items():
    print(f"\n{'=' * 60}")
    print(f"Processing Strategy: {strategy_name}")
    print(f"{'=' * 60}")
    print(f"Parameters: {json.dumps(strategy_params, indent=2)}")
    
    try:
        # Get strategy instance
        strategy = get_strategy(strategy_name, **strategy_params)
        
        # Convert Document objects to dicts for chunking
        doc_dicts = [
            {
                "doc_id": doc.doc_id,
                "doc_name": doc.doc_name,
                "text": doc.text,
                "metadata": doc.metadata
            }
            for doc in documents
        ]
        
        # Chunk documents
        print(f"\n📄 Chunking {len(doc_dicts)} documents...")
        chunks = strategy.chunk(doc_dicts)
        print(f"✅ Created {len(chunks)} chunks")
        
        # Create chunks table
        table_name = f"{catalog}.{schema}.rl_chunks_{strategy_name}_{run_id.replace('-', '_')}"
        print(f"\n💾 Saving chunks to: {table_name}")
        
        # Convert chunks to DataFrame
        chunk_data = []
        for chunk in chunks:
            chunk_data.append({
                "chunk_id": chunk.chunk_id,
                "doc_id": chunk.doc_id,
                "doc_name": chunk.doc_name,
                "chunk_text": chunk.chunk_text,
                "chunk_index": chunk.chunk_index,
                "metadata": json.dumps(chunk.metadata),
                "parent_chunk_id": chunk.parent_chunk_id,
                "run_id": run_id,
                "strategy": strategy_name
            })
        
        chunks_df = spark.createDataFrame(chunk_data)
        
        # Save to Delta table
        chunks_df.write.format("delta").mode("overwrite").saveAsTable(table_name)
        print(f"✅ Saved {len(chunk_data)} chunks to Delta table")
        
        # Create Vector Search Index if requested
        if create_index and embedding_model_endpoint and vs_endpoint_name:
            print(f"\n🔍 Creating Vector Search Index...")
            
            index_name = f"{catalog}.{schema}.rl_index_{strategy_name}_{run_id.replace('-', '_')}"
            
            try:
                # Create index
                index = create_vs_index(
                    vs_client=vs_client,
                    endpoint_name=vs_endpoint_name,
                    index_name=index_name,
                    primary_key="chunk_id",
                    source_table_name=table_name,
                    embedding_source_column="chunk_text",
                    embedding_model_endpoint_name=embedding_model_endpoint
                )
                
                print(f"✅ Vector Search Index created: {index_name}")
                
                strategy_results[strategy_name] = {
                    "status": "SUCCESS",
                    "chunks_table": table_name,
                    "index_name": index_name,
                    "num_chunks": len(chunks)
                }
                
            except Exception as e:
                print(f"⚠️ Warning: Could not create index: {e}")
                strategy_results[strategy_name] = {
                    "status": "CHUNKS_CREATED",
                    "chunks_table": table_name,
                    "index_name": None,
                    "num_chunks": len(chunks),
                    "index_error": str(e)
                }
        else:
            strategy_results[strategy_name] = {
                "status": "CHUNKS_CREATED",
                "chunks_table": table_name,
                "index_name": None,
                "num_chunks": len(chunks)
            }
        
        print(f"✅ Strategy '{strategy_name}' completed successfully")
        
    except Exception as e:
        print(f"❌ Error processing strategy '{strategy_name}': {e}")
        import traceback
        traceback.print_exc()
        
        strategy_results[strategy_name] = {
            "status": "FAILED",
            "error": str(e)
        }

# COMMAND ----------

# Step 3: Log results to MLflow
print("\n" + "=" * 80)
print("STEP 3: Logging Results")
print("=" * 80)

try:
    import mlflow
    
    project_name = config.get("project_name", "default")
    experiment_name = f"/Workspace/RetrievalStudio/{catalog}.{schema}/{project_name}"
    
    mlflow.set_experiment(experiment_name)
    
    with mlflow.start_run(run_name=f"build_{run_id[:8]}") as mlflow_run:
        # Log parameters
        mlflow.log_param("run_id", run_id)
        mlflow.log_param("data_type", data_type)
        mlflow.log_param("num_documents", len(documents))
        mlflow.log_param("strategies", list(strategies_config.keys()))
        
        # Log metrics for each strategy
        for strategy_name, result in strategy_results.items():
            if result.get("status") in ["SUCCESS", "CHUNKS_CREATED"]:
                mlflow.log_metric(f"{strategy_name}_num_chunks", result.get("num_chunks", 0))
        
        # Log artifacts
        mlflow.log_dict(config, "config.json")
        mlflow.log_dict(strategy_results, "strategy_results.json")
        
        print(f"✅ Results logged to MLflow: {mlflow_run.info.run_id}")

except Exception as e:
    print(f"⚠️ Warning: Could not log to MLflow: {e}")

# COMMAND ----------

# Step 4: Update final state
print("\n" + "=" * 80)
print("STEP 4: Updating Run State")
print("=" * 80)

# Determine overall status
all_successful = all(
    result.get("status") in ["SUCCESS", "CHUNKS_CREATED"]
    for result in strategy_results.values()
)

if all_successful:
    final_state = "SUCCESS"
    print("✅ All strategies completed successfully!")
else:
    final_state = "PARTIAL_SUCCESS"
    print("⚠️ Some strategies failed")

# Update database using Spark SQL
try:
    update_run_state_spark(spark, catalog, schema, run_id, final_state)
    print(f"✅ Run state updated to: {final_state}")
except Exception as e:
    print(f"⚠️ Warning: Could not update run state: {e}")

# COMMAND ----------

# Summary
print("\n" + "=" * 80)
print("BUILD JOB SUMMARY")
print("=" * 80)
print(f"Run ID: {run_id}")
print(f"Data Type: {data_type}")
print(f"Documents Loaded: {len(documents)}")
print(f"Strategies Processed: {len(strategy_results)}")
print(f"\nResults:")
for strategy_name, result in strategy_results.items():
    print(f"\n  {strategy_name}:")
    print(f"    Status: {result.get('status')}")
    if result.get('num_chunks'):
        print(f"    Chunks: {result.get('num_chunks')}")
    if result.get('chunks_table'):
        print(f"    Table: {result.get('chunks_table')}")
    if result.get('index_name'):
        print(f"    Index: {result.get('index_name')}")
    if result.get('error'):
        print(f"    Error: {result.get('error')}")

print("\n" + "=" * 80)
print(f"FINAL STATE: {final_state}")
print("=" * 80)

# Return results as JSON for programmatic access
dbutils.notebook.exit(json.dumps({
    "run_id": run_id,
    "status": final_state,
    "results": strategy_results
}))
