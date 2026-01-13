# Retrieval Studio

A Databricks application for systematic evaluation and optimization of Retrieval-Augmented Generation (RAG) pipelines.

## Overview

Retrieval Studio provides a complete inner loop for improving retrieval quality in RAG systems by allowing you to:
- Experiment with different document chunking strategies
- Auto-generate synthetic evaluation queries
- Measure retrieval quality with multiple metrics
- Compare strategies and optimize for your use case

## Architecture

### Technology Stack

**Backend:**
- **FastAPI** - REST API framework
- **Databricks SDK** - Job submission, Vector Search, MLflow integration
- **Lakebase PostgreSQL** - Transactional state management (projects, builds, evaluations)
- **Delta Lake** - OLAP storage (chunks, evaluation results)
- **MLflow** - Experiment tracking and metrics

**Frontend:**
- **React 18** + **TypeScript** - UI framework
- **Material-UI** - Component library
- **Vite** - Build tool
- **Axios** - HTTP client

**Infrastructure:**
- **Databricks Serverless Compute** - All jobs run serverless (no cluster management)
- **Unity Catalog** - Data governance and organization
- **Vector Search** - Similarity search for retrieval

### System Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         Frontend (React)                         │
│  ┌──────────┬──────────┬──────────┬──────────┬──────────────┐  │
│  │ Projects │  Build   │ Evaluate │  Review  │ ProjectDetails│  │
│  └──────────┴──────────┴──────────┴──────────┴──────────────┘  │
└───────────────────────────┬─────────────────────────────────────┘
                            │ REST API
┌───────────────────────────▼─────────────────────────────────────┐
│                      Backend (FastAPI)                           │
│  ┌────────────────────────────────────────────────────────────┐ │
│  │ API Routers: /projects /builds /evaluations /leaderboard  │ │
│  └───────────┬────────────────────────────────────────────────┘ │
└──────────────┼──────────────────────────────────────────────────┘
               │
       ┌───────┴────────┬──────────────┬─────────────┐
       ▼                ▼              ▼             ▼
┌─────────────┐  ┌─────────────┐  ┌──────────┐  ┌─────────────┐
│  Lakebase   │  │  Databricks │  │  Delta   │  │   MLflow    │
│ PostgreSQL  │  │    Jobs     │  │   Lake   │  │ Experiments │
└─────────────┘  └─────────────┘  └──────────┘  └─────────────┘
  (Projects,        (Serverless     (Chunks,       (Metrics,
   Builds,           Notebooks)      Indexes)       Params)
   Evaluations)
```

### Data Flow

#### 1. Build Pipeline
```
User → Frontend (Build Page) → Backend API → Databricks Job
                                                    ↓
                                          build_notebook_v2.py
                                                    ↓
                            ┌───────────────────────┴──────────────────────┐
                            ▼                       ▼                      ▼
                    Load Documents          Apply Chunking         Create Indexes
                    (Delta Table)           (Baseline/           (Vector Search)
                                           Semantic/
                                           Structured)
                                                    ↓
                                    Store in Delta + Register Index
                                                    ↓
                                        Return Results to Backend
                                                    ↓
                                        Update PostgreSQL State
```

#### 2. Evaluation Pipeline
```
User → Frontend (Evaluate Page) → Backend API → Extract Corpus Table
                                                 from Build Results
                                                         ↓
                                                 Databricks Job
                                                         ↓
                                                 eval_notebook.py
                                                         ↓
                            ┌────────────────────────────┴──────────────────┐
                            ▼                            ▼                  ▼
                    Generate Queries            Query Vector Search    Score Results
                    (or Load Existing)          (Top-K Retrieval)      (Recall, NDCG,
                                                                        LLM Judge)
                                                         ↓
                                            Log Metrics to MLflow
                                                         ↓
                                        Store Results in Delta Lake
                                                         ↓
                                        Update PostgreSQL State
```

### Directory Structure

```
retrieval-studio/
├── backend/                    # FastAPI backend
│   ├── api/                   # API endpoints
│   │   ├── projects.py        # Project CRUD + MLflow integration
│   │   ├── builds.py          # Build job lifecycle
│   │   ├── evaluations.py     # Evaluation job submission
│   │   ├── leaderboard.py     # Strategy rankings
│   │   └── metadata.py        # Data types & strategies
│   ├── models/
│   │   └── schemas.py         # Pydantic models
│   ├── auth.py                # Databricks authentication
│   ├── config.py              # Environment & settings
│   └── main.py                # FastAPI app + SPA serving
├── frontend/                   # React + TypeScript
│   ├── src/
│   │   ├── pages/             # UI pages
│   │   ├── services/          # API client wrappers
│   │   ├── components/        # Reusable components
│   │   ├── context/           # State management
│   │   └── types/             # TypeScript interfaces
├── retrieval_core/            # Core RAG logic
│   ├── strategies.py          # Chunking strategies
│   ├── evaluator.py           # Retrieval evaluation
│   ├── data_types.py          # Data type handlers
│   ├── query_generator.py     # Synthetic query generation
│   ├── analyzer.py            # Results analysis
│   └── configs.py             # Unity Catalog paths
├── utils/                      # Shared utilities
│   ├── jobs.py                # Databricks job submission
│   ├── postgres_state.py      # PostgreSQL operations
│   ├── postgres_connector.py  # DB connection pooling
│   ├── vs_utils.py            # Vector Search helpers
│   ├── query_builder.py       # SQL query building
│   └── mlflow_utils.py        # MLflow tracking
├── notebooks/                  # Databricks job notebooks
│   ├── build_notebook_v2.py   # Data prep + chunking + indexing
│   └── eval_notebook.py       # Query + retrieve + evaluate
└── database/                   # Database schemas
    └── postgres_schema.sql    # PostgreSQL table definitions
```

## Implementation Details

### State Management

**PostgreSQL (Lakebase) - OLTP:**
```sql
projects       -- Project metadata
builds         -- Build job tracking
evaluations    -- Evaluation job tracking
job_runs       -- Databricks job status
```

**Delta Lake - OLAP:**
```
{catalog}.chunks.rs_chunks_{project}_{strategy}     -- Document chunks
{catalog}.indexes.rs_index_{project}_{strategy}     -- Vector Search metadata
{catalog}.raw.rs_eval_results                       -- Evaluation results
```

**MLflow - Experiment Tracking:**
- Experiments: One per project (name pattern: `{project_name}_experiment`)
- Runs: Tagged with `rs_role` (build_strategy, eval_strategy)
- Metrics: recall@k, ndcg@k, precision@k, latency
- Params: strategy, chunk_size, top_k, etc.

### Chunking Strategies

**1. Baseline Strategy**
```python
class BaselineStrategy:
    chunk_size: int = 512
    overlap: int = 50

    # Fixed-size chunks with overlap
    # Fast, simple, general-purpose
```

**2. Semantic Strategy**
```python
class SemanticStrategy:
    window_size: int = 3
    min_chunk_size: int = 256

    # Sentence-boundary aware
    # Preserves semantic meaning
```

**3. Structured Strategy**
```python
class StructuredStrategy:
    preserve_hierarchy: bool = True
    max_chunk_size: int = 1024

    # Section-aware chunking
    # Preserves document structure
```

### Evaluation Metrics

**With Ground Truth:**
```python
Recall@k    = |relevant_retrieved| / |total_relevant|
Precision@k = |relevant_retrieved| / k
NDCG@k      = DCG@k / IDCG@k
```

**Without Ground Truth (LLM Judge):**
```python
Score: 0-3 scale
  3 = Highly relevant
  2 = Somewhat relevant
  1 = Loosely related
  0 = Irrelevant
```

### API Endpoints

**Projects:**
```
GET    /api/projects                   List all projects
POST   /api/projects                   Create project
GET    /api/projects/{id}              Get project
DELETE /api/projects/{id}              Delete project + builds + evals
GET    /api/projects/{id}/mlflow       Get MLflow experiment URL
GET    /api/projects/{id}/mlflow/runs  Get MLflow runs
```

**Builds:**
```
POST   /api/builds                     Submit build job
GET    /api/builds/{run_id}            Get build details
GET    /api/builds/{run_id}/status     Poll job status
GET    /api/builds/{run_id}/results    Get notebook output
GET    /api/builds/project/{id}        List builds for project
```

**Evaluations:**
```
POST   /api/evaluations                Submit evaluation job
GET    /api/evaluations/{eval_id}/status     Poll job status
GET    /api/evaluations/{eval_id}/results    Get metrics
GET    /api/evaluations/build/{build_id}     List evals for build
```

**Leaderboard:**
```
GET    /api/leaderboard/{run_id}              Get rankings
GET    /api/leaderboard/project/{id}/aggregate  Aggregate rankings
```

### Job Submission

All jobs use **Databricks Serverless Compute** (no cluster configuration needed):

```python
w.jobs.submit(
    run_name=f"RetrievalStudio-Build-{run_id[:8]}",
    tasks=[Task(
        task_key="build",
        notebook_task=NotebookTask(
            notebook_path="/Workspace/.../build_notebook_v2",
            base_parameters={
                "run_id": run_id,
                "config": json.dumps(config)
            }
        ),
        timeout_seconds=3600
    )]
)
```

### Vector Search Integration

```python
# Create index
vs_client.create_index(
    endpoint_name="vs-endpoint-default",
    index_name=f"{catalog}.indexes.rs_index_{project}_{strategy}",
    primary_key="chunk_id",
    index_type="DELTA_SYNC",
    columns=[
        {"name": "chunk_text", "delta_type": "text"},
        {"name": "chunk_id", "delta_type": "text"}
    ]
)

# Query index
results = vs_client.query_index(
    index_name=index_name,
    query_vector=embedding,
    columns=["chunk_id", "chunk_text", "doc_id"],
    num_results=top_k,
    filters=[]
)
```

## Configuration

### Environment Variables

```bash
# Databricks
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=dapi...

# Unity Catalog
CATALOG=retrievalstudio
SCHEMA=raw

# Notebooks
BUILD_NOTEBOOK_PATH=/Workspace/Users/your-email/notebooks/build_notebook_v2
EVAL_NOTEBOOK_PATH=/Workspace/Users/your-email/notebooks/eval_notebook

# PostgreSQL (Lakebase)
POSTGRES_HOST=your-postgres-host.cloud.databricks.com
POSTGRES_PORT=5432
POSTGRES_DATABASE=default
POSTGRES_USER=your-username
POSTGRES_PASSWORD=your-password
```

### Database Setup

1. Create PostgreSQL database in Lakebase
2. Run schema creation:
```sql
-- See database/postgres_schema.sql
CREATE TABLE projects (...);
CREATE TABLE builds (...);
CREATE TABLE evaluations (...);
CREATE TABLE job_runs (...);
```

3. Verify tables:
```python
from utils.postgres_state import initialize_tables
initialize_tables()
```

## Deployment

### Backend

```bash
cd backend
pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 8000
```

### Frontend

```bash
cd frontend
npm install
npm run build
# Backend serves static files from frontend/dist
```

### Databricks Setup

1. Upload notebooks to Databricks Workspace:
   - `notebooks/build_notebook_v2.py`
   - `notebooks/eval_notebook.py`

2. Create Vector Search endpoint (if not exists):
```python
from databricks.vector_search.client import VectorSearchClient
vs_client = VectorSearchClient()
vs_client.create_endpoint(name="vs-default")
```

3. Ensure Unity Catalog is configured:
```sql
CREATE CATALOG IF NOT EXISTS retrievalstudio;
CREATE SCHEMA IF NOT EXISTS retrievalstudio.raw;
CREATE SCHEMA IF NOT EXISTS retrievalstudio.chunks;
CREATE SCHEMA IF NOT EXISTS retrievalstudio.indexes;
```

## Development

### Running Locally

```bash
# Terminal 1 - Backend
cd backend
python -m uvicorn main:app --reload --port 8000

# Terminal 2 - Frontend
cd frontend
npm run dev
```

### Testing

```bash
# Backend tests
cd backend
pytest

# Frontend tests
cd frontend
npm test
```

## Key Features

### Auto-Extract Corpus Table
When submitting evaluations with auto-generated queries, the system automatically extracts the corpus table from build results. No manual configuration needed.

### Cascading Deletes
Deleting a project automatically removes all associated builds and evaluations from PostgreSQL. Delta tables and MLflow experiments are preserved for audit purposes.

### Real-time Job Monitoring
Frontend polls job status every 5 seconds and displays live updates with job URLs.

### Strategy Comparison
Leaderboard aggregates metrics across all queries to rank strategies by:
- Recall@5, Recall@10
- NDCG@5, NDCG@10
- Average latency

## Troubleshooting

### Common Issues

**1. "no results to fetch" error when deleting:**
- Fixed: Added `fetch="none"` to DELETE queries
- All DELETE operations now properly skip result fetching

**2. "corpus_table is required" error:**
- Fixed: Frontend no longer sends empty corpus_table
- Backend auto-extracts from build results

**3. MLflow URL not working:**
- Fixed: Now uses `/ml/experiments/{experiment_id}` format
- Falls back to search URL if experiment doesn't exist

**4. Evaluation history not showing:**
- Fixed: Shows "Evaluation History (0)" even when empty
- Provides "Run First Evaluation" button for successful builds

## License

MIT

## Contributors

Built for systematic RAG optimization on Databricks.
