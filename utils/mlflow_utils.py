import mlflow
from mlflow.tracking import MlflowClient
import json
import os

def create_or_get_experiment(experiment_name: str) -> str:
    """
    Create MLflow experiment or get existing one
    
    Args:
        experiment_name: Experiment name (e.g., "/RetrievalStudio/main.retrieval_studio/project_name")
    
    Returns:
        Experiment ID (string)
    """
    mlflow.set_tracking_uri("databricks")
    
    try:
        experiment = mlflow.get_experiment_by_name(experiment_name)
        if experiment:
            return experiment.experiment_id
    except Exception:
        pass
    
    # Create new experiment
    experiment_id = mlflow.create_experiment(experiment_name)
    return experiment_id

def log_build_run(
    experiment_id: str,
    run_id: str,
    config: dict,
    chunks_table: str = None,
    index_names: dict = None,
    state: str = "SUCCESS"
):
    """
    Log build run to MLflow
    
    Args:
        experiment_id: MLflow experiment ID
        run_id: Run identifier
        config: Build configuration
        chunks_table: Name of chunks table
        index_names: Dict mapping strategy to index name
        state: Run state (SUCCESS, FAILED, etc.)
    """
    with mlflow.start_run(run_name="build", experiment_id=experiment_id):
        # Log parameters
        mlflow.log_params({
            "run_id": run_id,
            "strategies": ",".join(config.get("strategies", [])),
            "chunk_size": str(config.get("chunk_size", 512)),
            "overlap": str(config.get("overlap", 50)),
            "source_type": config.get("source_type", "pdf"),
            "embedding_model_endpoint": config.get("embedding_model_endpoint", ""),
            "vs_endpoint_name": config.get("vs_endpoint_name", ""),
        })
        
        # Log tags
        mlflow.set_tags({
            "run_id": run_id,
            "state": state,
            "type": "build",
        })
        
        if chunks_table:
            mlflow.set_tag("chunks_table", chunks_table)
        
        if index_names:
            mlflow.set_tag("index_names", json.dumps(index_names))
        
        # Log config as artifact
        config_path = "/tmp/build_config.json"
        with open(config_path, "w") as f:
            json.dump(config, f, indent=2)
        mlflow.log_artifact(config_path)
        
        # Store run_id for later reference
        mlflow.set_tag("retrieval_studio_run_id", run_id)

def log_eval_run(
    experiment_id: str,
    run_id: str,
    strategy: str,
    metrics: dict,
    leaderboard_path: str = None,
    examples_path: str = None,
    state: str = "SUCCESS"
) -> str:
    """
    Log eval run to MLflow
    
    Args:
        experiment_id: MLflow experiment ID
        run_id: Run identifier
        strategy: Strategy name (baseline, structured, parent_child)
        metrics: Dictionary of metrics
        leaderboard_path: Path to leaderboard CSV file
        examples_path: Path to examples JSON file
        state: Run state
    
    Returns:
        MLflow run ID
    """
    with mlflow.start_run(run_name=f"eval_{strategy}", experiment_id=experiment_id) as run:
        mlflow_run_id = run.info.run_id
        
        # Log metrics
        mlflow.log_metrics(metrics)
        
        # Log parameters
        mlflow.log_params({
            "run_id": run_id,
            "strategy": strategy,
        })
        
        # Log tags
        mlflow.set_tags({
            "run_id": run_id,
            "strategy": strategy,
            "state": state,
            "type": "eval",
            "retrieval_studio_run_id": run_id,
        })
        
        # Log artifacts
        if leaderboard_path and os.path.exists(leaderboard_path):
            mlflow.log_artifact(leaderboard_path)
        
        if examples_path and os.path.exists(examples_path):
            mlflow.log_artifact(examples_path)
        
        return mlflow_run_id

def get_experiment_runs(experiment_id: str) -> list:
    """Get all runs in an experiment"""
    client = MlflowClient()
    runs = client.search_runs(experiment_ids=[experiment_id])
    
    return [
        {
            "run_id": run.info.run_id,
            "run_name": run.info.run_name,
            "status": run.info.status,
            "start_time": run.info.start_time,
            "end_time": run.info.end_time,
            "metrics": run.data.metrics,
            "params": run.data.params,
            "tags": run.data.tags,
        }
        for run in runs
    ]

