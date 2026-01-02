"""
Pydantic models for API request/response
"""
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List
from datetime import datetime


class ProjectCreate(BaseModel):
    project_name: str
    description: Optional[str] = None


class ProjectResponse(BaseModel):
    project_id: str
    project_name: str
    description: Optional[str] = None
    created_at: datetime
    updated_at: datetime


class BuildJobConfig(BaseModel):
    data_type: str
    data_config: Dict[str, Any]
    strategies: Dict[str, Dict[str, Any]]
    embedding_model_endpoint: str
    vs_endpoint_name: str
    create_index: bool = True


class BuildJobCreate(BaseModel):
    project_id: str
    config: BuildJobConfig


class BuildJobResponse(BaseModel):
    run_id: str
    project_id: str
    state: str
    job_id: Optional[str] = None
    config: Dict[str, Any]
    created_at: datetime
    updated_at: datetime


class EvaluationCreate(BaseModel):
    run_id: str
    queries_table: str


class EvaluationResponse(BaseModel):
    eval_id: str
    run_id: str
    state: str
    job_id: Optional[str] = None
    created_at: datetime
    updated_at: datetime


class LeaderboardEntry(BaseModel):
    strategy: str
    avg_recall_at_5: Optional[float] = None
    avg_recall_at_10: Optional[float] = None
    avg_ndcg_at_5: Optional[float] = None
    avg_ndcg_at_10: Optional[float] = None
    avg_latency_ms: Optional[float] = None
    num_queries: int


class DataTypeInfo(BaseModel):
    name: str
    display_name: str
    input_schema: Dict[str, Any]
    compatible_strategies: List[str]


class StrategyInfo(BaseModel):
    name: str
    display_name: str
    description: str
    parameters: Dict[str, Any]
