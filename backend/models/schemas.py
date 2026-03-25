"""
Pydantic models for API request/response
"""
from pydantic import BaseModel, Field, model_validator
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


class SourceConfig(BaseModel):
    source_name: str
    source_type: str
    config: Dict[str, Any]
    strategies: Dict[str, Dict[str, Any]]

    @model_validator(mode='after')
    def validate_source(self) -> 'SourceConfig':
        import re
        valid_types = {'pdf', 'csv', 'json', 'text', 'docx', 'delta_table', 'uc_volume'}
        if self.source_type not in valid_types:
            raise ValueError(f"source_type must be one of {valid_types}, got '{self.source_type}'")
        if not re.match(r'^[a-zA-Z0-9_\-]+$', self.source_name):
            raise ValueError(f"source_name must be alphanumeric with underscores/hyphens, got '{self.source_name}'")
        if not self.strategies:
            raise ValueError(f"At least one strategy is required for source '{self.source_name}'")
        return self


class BuildJobConfig(BaseModel):
    sources: List[SourceConfig]
    embedding_model_endpoint: str
    vs_endpoint_name: str
    create_index: bool = True

    @model_validator(mode='after')
    def validate_sources(self) -> 'BuildJobConfig':
        if not self.sources:
            raise ValueError("At least one source is required")
        names = [s.source_name for s in self.sources]
        if len(names) != len(set(names)):
            raise ValueError("Source names must be unique within a build")
        return self


class BuildJobCreate(BaseModel):
    project_id: str
    config: BuildJobConfig


class BuildJobResponse(BaseModel):
    run_id: str
    project_id: str
    state: str
    job_id: Optional[str] = None
    job_url: Optional[str] = None
    config: Dict[str, Any]
    created_at: datetime
    updated_at: datetime


class EvaluationCreate(BaseModel):
    run_id: str
    queries_table: Optional[str] = None  # Required if auto_generate_queries is false
    corpus_table: Optional[str] = None  # Required if auto_generate_queries is true
    dataset_type: Optional[str] = "delta_table"
    top_k: Optional[int] = 10
    auto_generate_queries: Optional[bool] = False
    num_queries: Optional[int] = 50  # Number of queries to generate
    query_style: Optional[str] = "keyword"  # keyword, natural, or mixed
    compare_query_types: Optional[bool] = False  # Compare FULL_TEXT, ANN, HYBRID
    judge_model_endpoint: Optional[str] = None  # LLM judge endpoint for scoring without ground truth
    generate_golden_dataset: Optional[bool] = False
    use_golden_dataset: Optional[bool] = False
    golden_dataset_table: Optional[str] = None
    golden_dataset_id: Optional[str] = None
    golden_strategy: Optional[str] = None
    golden_query_type: Optional[str] = None
    golden_top_k: Optional[int] = None


class EvaluationResponse(BaseModel):
    eval_id: str
    run_id: str
    state: str
    job_id: Optional[str] = None
    job_url: Optional[str] = None
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
