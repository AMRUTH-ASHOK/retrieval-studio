# Retrieval Studio - Technical Implementation

This document provides detailed technical information about the architecture, design patterns, and implementation details of Retrieval Studio.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Technology Stack](#technology-stack)
3. [Database Design](#database-design)
4. [Backend Implementation](#backend-implementation)
5. [Frontend Implementation](#frontend-implementation)
6. [MLflow Integration](#mlflow-integration)
7. [Data Processing Pipeline](#data-processing-pipeline)
8. [Job Orchestration](#job-orchestration)
9. [State Management](#state-management)
10. [Security & Authentication](#security--authentication)
11. [Performance Optimizations](#performance-optimizations)
12. [Error Handling](#error-handling)

---

## Architecture Overview

### High-Level System Design

```
┌─────────────────────────────────────────────────────────────────┐
│                         Frontend (React)                         │
│  ┌──────────┬──────────┬──────────┬──────────┬──────────────┐  │
│  │ Projects │  Build   │ Evaluate │  Review  │ ProjectDetails│  │
│  └──────────┴──────────┴──────────┴──────────┴──────────────┘  │
└───────────────────────────┬─────────────────────────────────────┘
                            │ REST API (JSON)
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
  (OLTP State)    (Serverless      (OLAP Data)   (Tracking)
                   Compute)
```

### Design Principles

1. **Separation of Concerns**: Frontend handles UI, backend handles business logic, notebooks handle data processing
2. **Idempotency**: All operations can be safely retried
3. **Asynchronous Processing**: Long-running jobs executed via Databricks
4. **Immutable Artifacts**: Chunks and indexes versioned by run_id
5. **Comprehensive Logging**: Every operation logged for debugging
6. **Backward Compatibility**: Fallback mechanisms for schema evolution

---

## Technology Stack

### Frontend

| Component | Technology | Version | Purpose |
|-----------|-----------|---------|---------|
| Framework | React | 18.x | UI library |
| Language | TypeScript | 5.x | Type safety |
| Build Tool | Vite | 5.x | Fast dev server & bundling |
| Styling | TailwindCSS | 3.x | Utility-first CSS |
| HTTP Client | Axios | 1.x | API communication |
| State | React Context | Built-in | Global state management |
| Routing | React Router | 6.x | Client-side routing |
| Charts | Recharts | 2.x | Data visualization |

### Backend

| Component | Technology | Version | Purpose |
|-----------|-----------|---------|---------|
| Framework | FastAPI | 0.104+ | REST API framework |
| Language | Python | 3.10+ | Backend logic |
| Server | Uvicorn | 0.24+ | ASGI server |
| Validation | Pydantic | 2.4+ | Data validation |
| DB Driver | psycopg2 | 2.9+ | PostgreSQL connection |
| SDK | Databricks SDK | 0.61+ | Job submission, Vector Search |
| Tracking | MLflow | 2.8+ | Experiment tracking |
| PDF | PyMuPDF (fitz) | 1.23+ | PDF text extraction |

### Infrastructure

| Component | Technology | Purpose |
|-----------|-----------|---------|
| Database (OLTP) | Lakebase PostgreSQL | Application state (projects, builds, evaluations) |
| Database (OLAP) | Delta Lake | Chunks, evaluation results |
| Compute | Databricks Serverless | No cluster management, auto-scaling |
| Vector Search | Databricks Vector Search | Similarity search |
| Experiment Tracking | MLflow | Metrics, parameters, artifacts |
| Data Governance | Unity Catalog | Access control, lineage |

---

## Database Design

### PostgreSQL Schema (Lakebase)

#### Projects Table

```sql
CREATE TABLE projects (
    project_id VARCHAR(50) PRIMARY KEY,              -- UUID
    project_name VARCHAR(255) NOT NULL,              -- User-friendly name
    description TEXT,                                -- Optional description
    catalog VARCHAR(100),                            -- Unity Catalog catalog
    db_schema VARCHAR(100),                          -- Unity Catalog schema
    vs_endpoint_name VARCHAR(255),                   -- Vector Search endpoint
    embedding_model_endpoint VARCHAR(255),           -- Embedding model
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(255)
);

CREATE INDEX idx_projects_created_at ON projects(created_at DESC);
```

**Purpose**: Stores project metadata and configuration

#### Builds Table

```sql
CREATE TABLE builds (
    run_id VARCHAR(50) PRIMARY KEY,                  -- UUID
    project_id VARCHAR(50) NOT NULL REFERENCES projects(project_id) ON DELETE CASCADE,
    project_name VARCHAR(255) NOT NULL,              -- Denormalized for queries
    experiment_id VARCHAR(100),                      -- MLflow experiment ID (CRITICAL!)
    state VARCHAR(20) NOT NULL DEFAULT 'PENDING',    -- PENDING, RUNNING, SUCCESS, FAILED
    job_id BIGINT,                                   -- Databricks job ID
    job_run_id BIGINT,                               -- Databricks job run ID
    eval_job_run_id BIGINT,                          -- Evaluation job run ID
    job_url TEXT,                                    -- Link to Databricks job
    config JSONB,                                    -- Build configuration (data_type, strategies, etc.)
    results JSONB,                                   -- Build results (IMPORTANT: stores notebook output)
    error_message TEXT,                              -- Error details if failed
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(255)
);

CREATE INDEX idx_builds_project_id ON builds(project_id);
CREATE INDEX idx_builds_state ON builds(state);
CREATE INDEX idx_builds_created_at ON builds(created_at DESC);
CREATE INDEX idx_builds_project_created ON builds(project_id, created_at DESC);
-- Index on experiment_id intentionally skipped (timeout issues, optional for performance)
```

**Key Fields:**
- `experiment_id`: **CRITICAL** - Stores the MLflow experiment ID to avoid name-based lookup mismatches
- `config`: JSONB storing: `{data_type, data_config, strategies, vs_endpoint_name, embedding_model_endpoint}`
- `results`: JSONB storing build output: `{strategy_name: {status, num_chunks, chunks_table, index_name}}`

**Why results in PostgreSQL?**
- Databricks multi-task jobs don't support output retrieval via API
- Storing in PostgreSQL allows immediate access without parsing logs
- Used by evaluation pipeline to extract corpus tables

#### Evaluations Table

```sql
CREATE TABLE evaluations (
    eval_id VARCHAR(50) PRIMARY KEY,                 -- UUID
    run_id VARCHAR(50) NOT NULL REFERENCES builds(run_id) ON DELETE CASCADE,
    project_id VARCHAR(50) NOT NULL REFERENCES projects(project_id) ON DELETE CASCADE,
    state VARCHAR(20) NOT NULL DEFAULT 'PENDING',    -- PENDING, RUNNING, SUCCESS, FAILED
    job_id BIGINT,                                   -- Databricks job ID
    job_run_id BIGINT,                               -- Databricks job run ID
    job_url TEXT,                                    -- Link to Databricks job
    queries_table VARCHAR(255),                      -- Delta table with queries (or NULL for auto-gen)
    corpus_table VARCHAR(255),                       -- Delta table with corpus (auto-extracted)
    dataset_type VARCHAR(20),                        -- delta_table, auto_generate, etc.
    top_k INTEGER DEFAULT 10,                        -- Number of results to retrieve
    auto_generate_queries BOOLEAN DEFAULT FALSE,     -- Whether to auto-generate queries
    num_queries INTEGER,                             -- Number of queries to generate
    query_style VARCHAR(20),                         -- specific, broad, contextual
    compare_query_types BOOLEAN DEFAULT FALSE,       -- Test FULL_TEXT, ANN, HYBRID
    judge_model_endpoint VARCHAR(255),               -- LLM judge endpoint (if no ground truth)
    error_message TEXT,                              -- Error details if failed
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(255)
);

CREATE INDEX idx_evaluations_run_id ON evaluations(run_id);
CREATE INDEX idx_evaluations_project_id ON evaluations(project_id);
CREATE INDEX idx_evaluations_state ON evaluations(state);
CREATE INDEX idx_evaluations_created_at ON evaluations(created_at DESC);
```

**Purpose**: Tracks evaluation job submissions and configurations

#### Job_Runs Table

```sql
CREATE TABLE job_runs (
    job_run_id BIGINT PRIMARY KEY,                   -- Databricks job run ID
    run_id VARCHAR(50),                              -- Foreign key to builds or evaluations
    job_type VARCHAR(20) NOT NULL,                   -- 'build' or 'eval'
    state VARCHAR(20) NOT NULL,                      -- Job state from Databricks API
    result_state VARCHAR(20),                        -- SUCCESS, FAILED, TIMEOUT
    job_url TEXT,                                    -- Link to Databricks job
    start_time BIGINT,                               -- Unix timestamp (ms)
    end_time BIGINT,                                 -- Unix timestamp (ms)
    setup_duration BIGINT,                           -- Setup time (ms)
    execution_duration BIGINT,                       -- Execution time (ms)
    cleanup_duration BIGINT,                         -- Cleanup time (ms)
    last_checked_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_job_runs_run_id ON job_runs(run_id);
CREATE INDEX idx_job_runs_state ON job_runs(state);
CREATE INDEX idx_job_runs_last_checked ON job_runs(last_checked_at DESC);
```

**Purpose**: Detailed job execution tracking for status polling

### Delta Lake Schema

#### Chunks Tables

**Pattern**: `{catalog}.chunks.rs_chunks_{project}_{strategy}`

```python
# Schema
chunk_id: STRING          # UUID
doc_id: STRING            # Original document ID
doc_name: STRING          # Document name
chunk_text: STRING        # The actual chunk content
chunk_index: INTEGER      # Position in document
metadata_json: STRING     # JSON metadata
chunk_type: STRING        # For parent-child: 'parent' or 'child'
parent_chunk_id: STRING   # For parent-child: reference to parent
run_id: STRING            # Build run ID (for partitioning)
project: STRING           # Project name
strategy: STRING          # Strategy name
created_at: TIMESTAMP     # Creation timestamp
```

**Partitioning**: By `run_id` for efficient overwrite

**Usage**:
- Source table for Vector Search delta-sync indexes
- Queryable for debugging chunk quality
- Immutable per run_id (overwrites entire partition)

#### Index Registry Table

**Pattern**: `{catalog}.indexes.rs_index_registry`

```sql
CREATE TABLE IF NOT EXISTS {catalog}.indexes.rs_index_registry (
    project STRING,
    strategy STRING,
    vs_endpoint STRING,
    index_name STRING,
    source_table STRING,
    embedding_endpoint STRING,
    updated_at TIMESTAMP
) USING DELTA;
```

**Purpose**: Track which indexes exist for each project/strategy

#### Evaluation Results Table

**Pattern**: `{catalog}.raw.rs_eval_results`

```python
# Schema (written by eval notebook)
eval_id: STRING              # Evaluation UUID
query_id: STRING             # Query UUID
query_text: STRING           # The query
strategy: STRING             # Chunking strategy tested
search_type: STRING          # FULL_TEXT, ANN, HYBRID
doc_id: STRING               # Retrieved document ID
rank: INTEGER                # Rank in results (1-K)
score: FLOAT                 # Similarity score
relevance: INTEGER           # Ground truth (0-3)
recall_at_k: FLOAT           # Recall@K for this query
ndcg_at_k: FLOAT             # NDCG@K for this query
precision_at_k: FLOAT        # Precision@K for this query
latency_ms: FLOAT            # Query latency
created_at: TIMESTAMP        # Timestamp
```

**Usage**: Aggregated for leaderboard and comparison tables

---

## Backend Implementation

### Directory Structure

```
backend/
├── api/                          # API route modules
│   ├── projects.py              # Project CRUD + MLflow integration
│   ├── builds.py                # Build job lifecycle
│   ├── evaluations.py           # Evaluation job submission
│   ├── leaderboard.py           # Strategy rankings
│   └── metadata.py              # Data types & strategies metadata
├── models/
│   └── schemas.py               # Pydantic models for request/response
├── auth.py                      # Databricks OAuth + SQL connectors
├── config.py                    # Environment variables & settings
└── main.py                      # FastAPI app initialization + SPA serving
```

### FastAPI Application Structure

**`backend/main.py`**:

```python
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from api import projects, builds, evaluations, leaderboard, metadata

app = FastAPI(title="Retrieval Studio API", version="1.0.0")

# CORS for local development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Restrict in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount API routers
app.include_router(projects.router, prefix="/api/projects", tags=["projects"])
app.include_router(builds.router, prefix="/api/builds", tags=["builds"])
app.include_router(evaluations.router, prefix="/api/evaluations", tags=["evaluations"])
app.include_router(leaderboard.router, prefix="/api/leaderboard", tags=["leaderboard"])
app.include_router(metadata.router, prefix="/api/metadata", tags=["metadata"])

# Serve frontend static files (Databricks Apps deployment)
@app.on_event("startup")
def mount_spa():
    app.mount("/", StaticFiles(directory="frontend/dist", html=True), name="spa")
```

### API Endpoint Patterns

#### Standard CRUD Pattern

```python
# Example: projects.py
from fastapi import APIRouter, Depends, HTTPException
from backend.models.schemas import ProjectCreate, ProjectResponse
from utils.postgres_state import create_project, get_project, delete_project

router = APIRouter()

@router.post("/", response_model=ProjectResponse)
async def create_new_project(project: ProjectCreate):
    """Create a new project"""
    try:
        project_id = str(uuid.uuid4())
        created = create_project(
            project_id=project_id,
            project_name=project.project_name,
            description=project.description,
            ...
        )
        return created
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/{project_id}", response_model=ProjectResponse)
async def get_project_by_id(project_id: str):
    """Get project by ID"""
    project = get_project(project_id)
    if not project:
        raise HTTPException(status_code=404, detail="Project not found")
    return project

@router.delete("/{project_id}")
async def delete_project_by_id(project_id: str):
    """Delete project (cascades to builds and evaluations)"""
    success = delete_project(project_id)
    if not success:
        raise HTTPException(status_code=404, detail="Project not found")
    return {"success": True, "message": f"Project {project_id} deleted"}
```

#### Job Submission Pattern

```python
# Example: builds.py
from utils.jobs import submit_build_job
from utils.postgres_state import create_build, update_build_state

@router.post("/", response_model=BuildResponse)
async def submit_build_job_endpoint(build_request: BuildRequest):
    """Submit a build job to Databricks"""

    # 1. Create build record in PostgreSQL
    run_id = str(uuid.uuid4())
    build = create_build(
        run_id=run_id,
        project_id=build_request.project_id,
        project_name=build_request.project_name,
        config=build_request.dict()
    )

    # 2. Submit Databricks job
    try:
        job_run_id, job_url = submit_build_job(
            run_id=run_id,
            config=build_request.dict()
        )

        # 3. Update build record with job details
        update_build_state(
            run_id=run_id,
            state="RUNNING",
            job_run_id=job_run_id,
            job_url=job_url
        )

        return {
            "run_id": run_id,
            "job_run_id": job_run_id,
            "job_url": job_url,
            "state": "RUNNING"
        }

    except Exception as e:
        # Update state to FAILED
        update_build_state(run_id=run_id, state="FAILED", error_message=str(e))
        raise HTTPException(status_code=500, detail=str(e))
```

#### Status Polling Pattern

```python
@router.get("/{run_id}/status")
async def get_build_status(run_id: str):
    """Poll build job status from Databricks"""

    build = get_build(run_id)
    if not build:
        raise HTTPException(status_code=404, detail="Build not found")

    job_run_id = build.get("job_run_id")
    if not job_run_id:
        return {"state": build["state"], "message": "Job not yet submitted"}

    # Query Databricks for current status
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()

    try:
        job_run = w.jobs.get_run(job_run_id)
        state = job_run.state.life_cycle_state.value
        result_state = job_run.state.result_state.value if job_run.state.result_state else None

        # Update local state if changed
        if result_state and build["state"] != result_state:
            update_build_state(run_id=run_id, state=result_state)

        return {
            "state": state,
            "result_state": result_state,
            "job_url": build["job_url"],
            "start_time": job_run.start_time,
            "end_time": job_run.end_time
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to get job status: {e}")
```

### Database Connection Pooling

**`utils/postgres_connector.py`**:

```python
from psycopg2 import pool
from databricks.sdk import WorkspaceClient
import time

class LakehousePostgresConnector:
    def __init__(self):
        self.w = WorkspaceClient()
        self._token = None
        self._token_last_refresh = 0
        self._token_ttl = 900  # 15 minutes
        self._pool = None
        self._initialize_pool()

    def _get_fresh_token(self):
        """Get OAuth token from Databricks"""
        return self.w.dbutils.notebook.entry_point.getDbutils() \
                   .notebook().getContext().apiToken().get()

    def _ensure_valid_token(self):
        """Refresh token if expired"""
        current_time = time.time()
        if (self._token is None or
            current_time - self._token_last_refresh > self._token_ttl):
            self._token = self._get_fresh_token()
            self._token_last_refresh = current_time
            # Recreate pool with new token
            if self._pool:
                self._pool.closeall()
            self._initialize_pool()

    def _initialize_pool(self):
        """Create connection pool"""
        self._ensure_valid_token()

        self._pool = pool.ThreadedConnectionPool(
            minconn=2,
            maxconn=20,
            host=settings.POSTGRES_HOST,
            port=settings.POSTGRES_PORT,
            database=settings.POSTGRES_DATABASE,
            user=settings.POSTGRES_USER,
            password=self._token,  # OAuth token as password
            sslmode='require',
            connect_timeout=10
        )

    @contextmanager
    def get_connection(self):
        """Context manager for connections"""
        self._ensure_valid_token()
        conn = None
        try:
            conn = self._pool.getconn()
            yield conn
            conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            raise
        finally:
            if conn:
                self._pool.putconn(conn)

    def execute(self, query, params=None, fetch="all"):
        """Execute query with automatic connection management"""
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)

            if fetch == "one":
                result = cursor.fetchone()
                return dict(zip([desc[0] for desc in cursor.description], result)) if result else None
            elif fetch == "all":
                results = cursor.fetchall()
                return [dict(zip([desc[0] for desc in cursor.description], row)) for row in results]
            else:  # fetch="none" for INSERT/UPDATE/DELETE
                return None
```

**Key Features:**
- OAuth token auto-refresh every 15 minutes
- Connection pooling (2-20 connections)
- Context manager for transaction safety
- Automatic reconnection on token expiry

---

## Frontend Implementation

### Component Architecture

```
frontend/src/
├── pages/                        # Top-level page components
│   ├── Projects.tsx             # Project list + create
│   ├── Build.tsx                # Build configuration wizard
│   ├── Evaluate.tsx             # Evaluation submission
│   ├── Review.tsx               # Results comparison
│   └── ProjectDetails.tsx       # Project history
├── components/                   # Reusable UI components
│   ├── ui/                      # Base UI components
│   │   ├── Button.tsx
│   │   ├── Card.tsx
│   │   ├── Input.tsx
│   │   └── Select.tsx
│   └── review/                  # Review-specific components
│       ├── BuildSelector.tsx
│       ├── EvaluationSelector.tsx
│       ├── BestPerformers.tsx
│       ├── MetricsBarCharts.tsx
│       └── ComparisonTable.tsx
├── services/                     # API client wrappers
│   ├── projects.ts
│   ├── builds.ts
│   ├── evaluations.ts
│   └── api.ts                   # Axios instance
├── context/                      # Global state
│   └── ProjectContext.tsx       # Selected project state
├── types/                        # TypeScript interfaces
│   └── index.ts                 # Shared types
└── utils/                        # Utility functions
    └── metricsAggregation.ts    # Metrics calculations
```

### State Management Pattern

**Global State (ProjectContext)**:

```typescript
// context/ProjectContext.tsx
import React, { createContext, useContext, useState, useEffect } from 'react';
import { Project } from '../types';

interface ProjectContextType {
  selectedProject: Project | null;
  selectedProjectId: string | null;
  setSelectedProject: (project: Project | null) => void;
}

const ProjectContext = createContext<ProjectContextType | undefined>(undefined);

export const ProjectProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const [selectedProject, setSelectedProject] = useState<Project | null>(null);

  // Persist to localStorage
  useEffect(() => {
    if (selectedProject) {
      localStorage.setItem('selectedProjectId', selectedProject.project_id);
    }
  }, [selectedProject]);

  // Restore from localStorage
  useEffect(() => {
    const savedId = localStorage.getItem('selectedProjectId');
    if (savedId) {
      projectsApi.getById(savedId).then(setSelectedProject);
    }
  }, []);

  return (
    <ProjectContext.Provider value={{
      selectedProject,
      selectedProjectId: selectedProject?.project_id || null,
      setSelectedProject
    }}>
      {children}
    </ProjectContext.Provider>
  );
};

export const useProject = () => {
  const context = useContext(ProjectContext);
  if (!context) throw new Error('useProject must be used within ProjectProvider');
  return context;
};
```

**Local State (Component-level)**:

```typescript
// pages/Build.tsx
const [dataSources, setDataSources] = useState<DataSource[]>([
  { id: crypto.randomUUID(), config: {} }
]);
const [selectedStrategies, setSelectedStrategies] = useState<Set<string>>(new Set());
const [isSubmitting, setIsSubmitting] = useState(false);

// Add data source
const addDataSource = () => {
  setDataSources(prev => [...prev, { id: crypto.randomUUID(), config: {} }]);
};

// Update data source field
const updateDataSource = (id: string, field: string, value: any) => {
  setDataSources(prev =>
    prev.map(source =>
      source.id === id
        ? { ...source, config: { ...source.config, [field]: value } }
        : source
    )
  );
};
```

### API Client Pattern

**`services/api.ts`** (Axios instance):

```typescript
import axios from 'axios';

const api = axios.create({
  baseURL: import.meta.env.VITE_API_URL || 'http://localhost:8000/api',
  timeout: 30000,
  headers: {
    'Content-Type': 'application/json',
  },
});

// Request interceptor
api.interceptors.request.use(
  (config) => {
    // Add auth token if needed
    const token = localStorage.getItem('auth_token');
    if (token) {
      config.headers.Authorization = `Bearer ${token}`;
    }
    return config;
  },
  (error) => Promise.reject(error)
);

// Response interceptor
api.interceptors.response.use(
  (response) => response.data,
  (error) => {
    if (error.response) {
      console.error('API Error:', error.response.data);
      throw new Error(error.response.data.detail || 'API request failed');
    }
    throw error;
  }
);

export default api;
```

**`services/builds.ts`** (Typed API wrapper):

```typescript
import api from './api';
import { BuildJob, BuildRequest, BuildResponse } from '../types';

export const buildsApi = {
  submit: async (buildRequest: BuildRequest): Promise<BuildResponse> => {
    return api.post('/builds', buildRequest);
  },

  getById: async (runId: string): Promise<BuildJob> => {
    return api.get(`/builds/${runId}`);
  },

  getStatus: async (runId: string): Promise<{ state: string; job_url: string }> => {
    return api.get(`/builds/${runId}/status`);
  },

  getByProject: async (projectId: string): Promise<BuildJob[]> => {
    return api.get(`/builds/project/${projectId}`);
  },

  // Polling helper
  pollStatus: async (runId: string, interval: number = 5000): Promise<BuildJob> => {
    return new Promise((resolve, reject) => {
      const poll = setInterval(async () => {
        try {
          const status = await buildsApi.getStatus(runId);

          if (status.state === 'SUCCESS' || status.state === 'FAILED') {
            clearInterval(poll);
            const build = await buildsApi.getById(runId);
            resolve(build);
          }
        } catch (error) {
          clearInterval(poll);
          reject(error);
        }
      }, interval);
    });
  },
};
```

### Dynamic Form Rendering

**Multi-source data input (Build.tsx)**:

```typescript
{dataSources.map((source, index) => (
  <Card key={source.id} className="p-4 border-2">
    <div className="flex justify-between items-center mb-4">
      <h4 className="font-medium">Source {index + 1}</h4>
      {dataSources.length > 1 && (
        <Button
          variant="ghost"
          onClick={() => removeDataSource(source.id)}
          icon={<Trash2 className="w-4 h-4" />}
        >
          Remove
        </Button>
      )}
    </div>

    {/* Render fields dynamically based on data type schema */}
    {selectedDataTypeInfo?.fields.map((field) => (
      <div key={field.name} className="mb-4">
        <label className="block text-sm font-medium mb-1">
          {field.display_name}
          {field.required && <span className="text-red-500">*</span>}
        </label>

        {field.type === 'text' && (
          <Input
            type="text"
            value={source.config[field.name] || ''}
            onChange={(e) => updateDataSource(source.id, field.name, e.target.value)}
            placeholder={field.placeholder}
          />
        )}

        {field.type === 'textarea' && (
          <textarea
            className="w-full border rounded-md p-2"
            rows={6}
            value={source.config[field.name] || ''}
            onChange={(e) => updateDataSource(source.id, field.name, e.target.value)}
            placeholder={field.placeholder}
          />
        )}

        {field.type === 'number' && (
          <Input
            type="number"
            value={source.config[field.name] || field.default}
            onChange={(e) => updateDataSource(source.id, field.name, parseInt(e.target.value))}
          />
        )}
      </div>
    ))}
  </Card>
))}

<Button onClick={addDataSource} icon={<Plus />}>
  Add Another {selectedDataTypeInfo?.display_name || 'Source'}
</Button>
```

---

## MLflow Integration

### Experiment Naming Convention

**Pattern**: `/Workspace/Users/{user_email}/retrieval-studio/experiments/{project_name}`

**Generated in**: `retrieval_core/configs.py`

```python
class RetrievalStudioConfig:
    EXPERIMENT_BASE_PATH = "/Workspace/Users/{user_email}/retrieval-studio/experiments"

    @classmethod
    def get_experiment_name(cls, project_name: str) -> str:
        """Generate MLflow experiment name for project"""
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        user_email = w.current_user.me().user_name

        safe_project = project_name.replace(" ", "_").lower()
        return f"{cls.EXPERIMENT_BASE_PATH.format(user_email=user_email)}/{safe_project}"
```

### Run Hierarchy & Tagging

**Build Runs** (2 levels):

```python
# Parent run
with mlflow.start_run(run_name=f"build_{run_id[:8]}") as parent_run:
    mlflow.set_tag("rs_role", "build_parent")
    mlflow.log_param("build_run_id", run_id)
    mlflow.log_param("project_name", project_name)
    mlflow.log_param("num_documents", len(documents))

    # Child run per strategy
    for strategy_name in strategies:
        with mlflow.start_run(run_name=f"build_{strategy_name}", nested=True) as child_run:
            mlflow.set_tag("rs_role", "build_strategy")
            mlflow.log_param("build_run_id", run_id)
            mlflow.log_param("strategy_name", strategy_name)
            mlflow.log_metric("num_chunks", num_chunks)
```

**Evaluation Runs** (3 levels):

```python
# Parent run
with mlflow.start_run(run_name=f"eval_{eval_id[:8]}") as parent_run:
    mlflow.set_tag("rs_role", "eval_parent")
    mlflow.log_param("eval_id", eval_id)
    mlflow.log_param("build_run_id", build_run_id)

    # Strategy-level child run
    for strategy in strategies:
        with mlflow.start_run(run_name=f"eval_{strategy}", nested=True) as strategy_run:
            mlflow.set_tag("rs_role", "eval_strategy")
            mlflow.log_param("strategy_name", strategy)

            # Query-level grandchild run (optional, for detailed tracking)
            for query in queries:
                with mlflow.start_run(run_name=f"query_{query_id}", nested=True):
                    mlflow.set_tag("rs_role", "eval_query")
                    mlflow.log_metric("recall_at_10", recall)
                    mlflow.log_metric("ndcg_at_10", ndcg)
```

### Experiment ID Storage (CRITICAL)

**Problem**: Name-based experiment lookup is fragile (renames, path changes)

**Solution**: Store `experiment.experiment_id` in `builds.experiment_id` column

**Implementation** (`notebooks/build_notebook_v2.py`):

```python
experiment_name = core_config.get_experiment_name(project_name)
experiment = mlflow.set_experiment(experiment_name)

# Store experiment ID in database
try:
    from utils.postgres_state import update_build_state

    update_build_state(
        run_id=run_id,
        state='RUNNING',
        experiment_id=experiment.experiment_id  # Store the ID!
    )

    print(f"[INFO] ✓ Stored experiment_id={experiment.experiment_id}")
except Exception as e:
    print(f"[WARNING] Failed to store experiment_id: {e}")
    # Continue anyway - fallback to name-based lookup
```

**API Usage** (`backend/api/projects.py`):

```python
@router.get("/{project_id}/mlflow/runs")
async def get_mlflow_runs(project_id: str):
    """Get MLflow runs using stored experiment_id"""

    # Query builds table for experiment_id
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT experiment_id
        FROM builds
        WHERE project_id = %s
          AND state = 'SUCCESS'
          AND experiment_id IS NOT NULL
        ORDER BY created_at DESC
        LIMIT 1
    """, (project_id,))

    result = cursor.fetchone()

    if result and result[0]:
        # Use stored experiment ID (preferred)
        experiment_id = result[0]
        experiment = client.get_experiment(experiment_id)
    else:
        # Fallback to name-based lookup (backward compatibility)
        experiment_name = core_config.get_experiment_name(project_name)
        experiment = client.get_experiment_by_name(experiment_name)

    # Search runs in this experiment
    runs = mlflow.search_runs(
        experiment_ids=[experiment.experiment_id],
        filter_string="tags.rs_role = 'eval_strategy'",
        order_by=["start_time DESC"]
    )

    return {"runs": runs.to_dict('records')}
```

### Metrics Aggregation

**Frontend Aggregation** (`utils/metricsAggregation.ts`):

```typescript
export function aggregateByStrategy(runs: MLflowRun[]) {
  const strategyMap = new Map<string, MetricsSummary>();

  runs.forEach(run => {
    const strategy = run.params?.strategy_name || 'unknown';

    if (!strategyMap.has(strategy)) {
      strategyMap.set(strategy, {
        strategy,
        recall_at_5: [],
        recall_at_10: [],
        ndcg_at_5: [],
        ndcg_at_10: [],
        latency: [],
      });
    }

    const summary = strategyMap.get(strategy)!;
    summary.recall_at_5.push(run.metrics?.recall_at_5 || 0);
    summary.recall_at_10.push(run.metrics?.recall_at_10 || 0);
    // ... collect all metrics
  });

  // Calculate averages
  return Array.from(strategyMap.values()).map(summary => ({
    strategy: summary.strategy,
    avg_recall_at_5: average(summary.recall_at_5),
    avg_recall_at_10: average(summary.recall_at_10),
    avg_ndcg_at_5: average(summary.ndcg_at_5),
    avg_ndcg_at_10: average(summary.ndcg_at_10),
    avg_latency: average(summary.latency),
  }));
}
```

---

## Data Processing Pipeline

### Build Pipeline Architecture

```
User Submit Build
       │
       ▼
Backend creates build record (PostgreSQL)
       │
       ▼
Submit Databricks Serverless Job
       │
       ▼
build_notebook_v2.py
       │
       ├──────────────────┬──────────────────┬──────────────────┐
       ▼                  ▼                  ▼                  ▼
Load Documents    Apply Strategy 1   Apply Strategy 2   Apply Strategy N
(data_types.py)    (strategies.py)   (strategies.py)    (strategies.py)
       │                  │                  │                  │
       ▼                  ▼                  ▼                  ▼
    List[Document]    List[Chunk]      List[Chunk]        List[Chunk]
       │                  │                  │                  │
       └──────────────────┴──────────────────┴──────────────────┘
                          │
                          ▼
            Write to Delta Tables (partitioned by run_id)
            {catalog}.chunks.rs_chunks_{project}_{strategy}
                          │
                          ▼
            Create/Update Vector Search Indexes
            (delta-sync, auto-embedding)
                          │
                          ▼
            Update Index Registry
                          │
                          ▼
            Store Results in PostgreSQL (builds.results)
            {strategy: {status, num_chunks, chunks_table, index_name}}
                          │
                          ▼
            Update builds.state = 'SUCCESS'
            Update builds.experiment_id = experiment.experiment_id
```

### Data Type Handlers

**Interface** (`retrieval_core/data_types.py`):

```python
class DataTypeHandler(ABC):
    @abstractmethod
    def load_documents(self, config: Dict[str, Any]) -> List[Document]:
        """Load documents from data source"""
        pass

    @abstractmethod
    def get_metadata_schema(self) -> Dict[str, Any]:
        """Return JSON schema for configuration"""
        pass
```

**Example: PDF Handler**:

```python
class PDFDataType(DataTypeHandler):
    def load_documents(self, config: Dict[str, Any]) -> List[Document]:
        """Extract text from PDF files using PyMuPDF"""
        import fitz  # PyMuPDF

        documents = []
        uploaded_files = config.get("uploaded_files", [])

        for file_info in uploaded_files:
            file_content = file_info.get("content")  # base64 encoded
            file_name = file_info.get("name")

            # Decode and extract text
            pdf_bytes = base64.b64decode(file_content)
            pdf_doc = fitz.open(stream=pdf_bytes, filetype="pdf")

            text_parts = []
            for page_num in range(pdf_doc.page_count):
                page = pdf_doc[page_num]
                page_text = page.get_text()
                if page_text.strip():
                    text_parts.append(page_text)

            pdf_doc.close()
            full_text = "\n\n--- Page Break ---\n\n".join(text_parts)

            doc = Document(
                doc_id=str(uuid.uuid4()),
                doc_name=file_name,
                text=full_text,
                metadata={
                    "source_type": "pdf",
                    "num_pages": pdf_doc.page_count,
                    "file_size": len(pdf_bytes)
                },
                data_type="pdf"
            )
            documents.append(doc)

        return documents
```

**Example: Multiple Text Handler**:

```python
class TextDataType(DataTypeHandler):
    def load_documents(self, config: Dict[str, Any]) -> List[Document]:
        """Load text documents (supports multiple entries)"""
        documents = []
        text_entries = config.get("text_entries", [])

        if text_entries:
            # New format: multiple text entries
            for idx, entry in enumerate(text_entries):
                text_content = entry.get("text_content", "")
                doc_name = entry.get("document_name", f"text_document_{idx + 1}")

                if text_content.strip():
                    doc = Document(
                        doc_id=str(uuid.uuid4()),
                        doc_name=doc_name,
                        text=text_content.strip(),
                        metadata={"source_type": "text", "entry_index": idx},
                        data_type="text"
                    )
                    documents.append(doc)
        else:
            # Old format: single text entry (backward compatibility)
            text_content = config.get("text_content", "")
            doc_name = config.get("document_name", "text_document")

            if text_content.strip():
                doc = Document(
                    doc_id=str(uuid.uuid4()),
                    doc_name=doc_name,
                    text=text_content.strip(),
                    metadata={"source_type": "text"},
                    data_type="text"
                )
                documents.append(doc)

        return documents
```

### Chunking Strategies

**Base Interface** (`retrieval_core/strategies.py`):

```python
class ChunkingStrategy(ABC):
    @abstractmethod
    def chunk(self, documents: List[Dict[str, Any]]) -> List[Chunk]:
        """Split documents into chunks"""
        pass

    @abstractmethod
    def get_metadata_schema(self) -> Dict[str, Any]:
        """Return JSON schema for parameters"""
        pass
```

**Example: Baseline Strategy**:

```python
class BaselineStrategy(ChunkingStrategy):
    def __init__(self, chunk_size: int = 512, overlap: int = 50):
        self.chunk_size = chunk_size
        self.overlap = overlap

    def chunk(self, documents: List[Dict[str, Any]]) -> List[Chunk]:
        """Fixed-size chunks with overlap"""
        chunks = []

        for doc in documents:
            text = doc['text']
            doc_id = doc['doc_id']
            doc_name = doc['doc_name']

            # Split into chunks
            start = 0
            chunk_index = 0

            while start < len(text):
                end = start + self.chunk_size
                chunk_text = text[start:end]

                chunk = Chunk(
                    chunk_id=str(uuid.uuid4()),
                    doc_id=doc_id,
                    doc_name=doc_name,
                    chunk_text=chunk_text,
                    chunk_index=chunk_index,
                    metadata={
                        "chunk_size": self.chunk_size,
                        "overlap": self.overlap,
                        "start_pos": start,
                        "end_pos": end
                    }
                )
                chunks.append(chunk)

                start += (self.chunk_size - self.overlap)
                chunk_index += 1

        return chunks
```

**Example: Parent-Child Strategy**:

```python
class ParentChildStrategy(ChunkingStrategy):
    def __init__(self, parent_chunk_size: int = 2048, child_chunk_size: int = 512, overlap: int = 50):
        self.parent_chunk_size = parent_chunk_size
        self.child_chunk_size = child_chunk_size
        self.overlap = overlap

    def chunk(self, documents: List[Dict[str, Any]]) -> List[Chunk]:
        """Two-level hierarchy: parent chunks contain child chunks"""
        chunks = []

        for doc in documents:
            text = doc['text']

            # Create parent chunks
            parent_start = 0
            parent_index = 0

            while parent_start < len(text):
                parent_end = parent_start + self.parent_chunk_size
                parent_text = text[parent_start:parent_end]
                parent_id = str(uuid.uuid4())

                # Parent chunk
                parent_chunk = Chunk(
                    chunk_id=parent_id,
                    doc_id=doc['doc_id'],
                    doc_name=doc['doc_name'],
                    chunk_text=parent_text,
                    chunk_index=parent_index,
                    metadata={"chunk_type": "parent"}
                )
                chunks.append(parent_chunk)

                # Create child chunks within parent
                child_start = 0
                child_index = 0

                while child_start < len(parent_text):
                    child_end = child_start + self.child_chunk_size
                    child_text = parent_text[child_start:child_end]

                    child_chunk = Chunk(
                        chunk_id=str(uuid.uuid4()),
                        doc_id=doc['doc_id'],
                        doc_name=doc['doc_name'],
                        chunk_text=child_text,
                        chunk_index=child_index,
                        parent_chunk_id=parent_id,  # Link to parent
                        metadata={"chunk_type": "child"}
                    )
                    chunks.append(child_chunk)

                    child_start += (self.child_chunk_size - self.overlap)
                    child_index += 1

                parent_start += self.parent_chunk_size
                parent_index += 1

        return chunks
```

### Vector Search Index Creation

**`utils/vs_utils.py`**:

```python
from databricks.vector_search.client import VectorSearchClient

def create_vs_index(
    vs_client: VectorSearchClient,
    endpoint_name: str,
    index_name: str,
    source_table_name: str,
    embedding_model_endpoint_name: str,
):
    """Create or update Vector Search index"""

    try:
        # Try to get existing index
        index = vs_client.get_index(endpoint_name, index_name)
        print(f"[INFO] Index {index_name} already exists, syncing...")

        # Trigger sync if source table changed
        index.sync()

    except Exception:
        # Index doesn't exist, create it
        print(f"[INFO] Creating index {index_name}...")

        index = vs_client.create_index(
            endpoint_name=endpoint_name,
            index_name=index_name,
            primary_key="chunk_id",
            index_type="DELTA_SYNC",  # Auto-sync with Delta table
            columns=[
                {
                    "name": "chunk_id",
                    "delta_type": "string"
                },
                {
                    "name": "chunk_text",
                    "delta_type": "string"
                },
                {
                    "name": "doc_id",
                    "delta_type": "string"
                },
                {
                    "name": "doc_name",
                    "delta_type": "string"
                }
            ],
            source_table_name=source_table_name,
            embedding_model_endpoint_name=embedding_model_endpoint_name
        )

        print(f"[INFO] ✓ Created index {index_name}")

    return index

def wait_for_index(vs_client: VectorSearchClient, endpoint_name: str, index_name: str, timeout: int = 600):
    """Wait for index to be ready"""
    import time

    start_time = time.time()

    while time.time() - start_time < timeout:
        index = vs_client.get_index(endpoint_name, index_name)
        status = index.describe().get("status", {}).get("state")

        if status == "ONLINE":
            print(f"[INFO] ✓ Index {index_name} is online")
            return True

        print(f"[INFO] Index status: {status}, waiting...")
        time.sleep(10)

    raise TimeoutError(f"Index {index_name} did not become ready within {timeout}s")
```

---

## Job Orchestration

### Job Submission Pattern

**`utils/jobs.py`**:

```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import Task, NotebookTask, Source

def submit_build_job(run_id: str, config: Dict[str, Any]) -> Tuple[int, str]:
    """Submit build job to Databricks"""

    w = WorkspaceClient()

    # Prepare notebook parameters
    params = {
        "run_id": run_id,
        "config": json.dumps(config),
        "catalog": config.get("catalog", settings.CATALOG),
        "schema": config.get("schema", settings.SCHEMA)
    }

    # Submit serverless job
    job_run = w.jobs.submit(
        run_name=f"RetrievalStudio-Build-{run_id[:8]}",
        tasks=[
            Task(
                task_key="build",
                notebook_task=NotebookTask(
                    notebook_path=settings.BUILD_NOTEBOOK_PATH,
                    source=Source.WORKSPACE,
                    base_parameters=params
                ),
                timeout_seconds=3600,  # 1 hour
                # Serverless compute - no cluster configuration needed!
            )
        ]
    )

    job_run_id = job_run.run_id
    job_url = f"{settings.DATABRICKS_HOST}/#job/{job_run_id}"

    print(f"[INFO] Submitted build job {job_run_id}")
    print(f"[INFO] Job URL: {job_url}")

    return job_run_id, job_url
```

### Job Status Polling

**Backend polling endpoint**:

```python
@router.get("/{run_id}/status")
async def get_build_status(run_id: str):
    """Poll Databricks job status"""

    build = get_build(run_id)
    job_run_id = build.get("job_run_id")

    if not job_run_id:
        return {"state": "PENDING", "message": "Job not yet submitted"}

    w = WorkspaceClient()
    job_run = w.jobs.get_run(job_run_id)

    state = job_run.state.life_cycle_state.value  # PENDING, RUNNING, TERMINATING, TERMINATED
    result_state = job_run.state.result_state.value if job_run.state.result_state else None  # SUCCESS, FAILED, TIMEOUT

    # Update local state if terminal
    if result_state and build["state"] != result_state:
        update_build_state(run_id=run_id, state=result_state)

        # Extract results from notebook output if SUCCESS
        if result_state == "SUCCESS":
            try:
                output = w.jobs.get_run_output(job_run_id)
                notebook_output = json.loads(output.notebook_output.result)

                update_build_state(
                    run_id=run_id,
                    state="SUCCESS",
                    results=notebook_output.get("results", {})
                )
            except Exception as e:
                print(f"[WARNING] Failed to parse notebook output: {e}")

    return {
        "state": state,
        "result_state": result_state,
        "job_url": build["job_url"],
        "start_time": job_run.start_time,
        "end_time": job_run.end_time
    }
```

**Frontend polling hook**:

```typescript
// Custom hook for job polling
function useBuildStatus(runId: string | null) {
  const [status, setStatus] = useState<BuildStatus | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    if (!runId) return;

    const pollInterval = setInterval(async () => {
      try {
        const statusData = await buildsApi.getStatus(runId);
        setStatus(statusData);

        // Stop polling if terminal state
        if (statusData.result_state === 'SUCCESS' || statusData.result_state === 'FAILED') {
          clearInterval(pollInterval);
        }
      } catch (err) {
        setError(err.message);
        clearInterval(pollInterval);
      }
    }, 5000);  // Poll every 5 seconds

    return () => clearInterval(pollInterval);
  }, [runId]);

  return { status, error };
}

// Usage in component
const { status, error } = useBuildStatus(buildRunId);

{status && (
  <div className="flex items-center gap-2">
    {status.result_state === 'SUCCESS' && <CheckCircle className="text-green-500" />}
    {status.result_state === 'FAILED' && <XCircle className="text-red-500" />}
    {!status.result_state && <Loader className="animate-spin" />}
    <span>{status.state}</span>
    <a href={status.job_url} target="_blank">View Job</a>
  </div>
)}
```

---

## State Management

### PostgreSQL for OLTP

**Use Cases:**
- Project metadata (name, description, config)
- Build/evaluation job tracking (state, job_id, job_url)
- Build results (chunks_table, index_name per strategy)
- Job status polling

**Why PostgreSQL:**
- ACID transactions for state consistency
- Fast lookups by primary key
- JSONB for flexible schema (config, results)
- Cascading deletes for cleanup
- Connection pooling for performance

### Delta Lake for OLAP

**Use Cases:**
- Document chunks (partitioned by run_id)
- Evaluation results (aggregated for leaderboard)
- Audit trail (immutable, time-travel)

**Why Delta:**
- Scalable for large datasets
- Time-travel for reproducibility
- ACID transactions
- Schema evolution
- Optimized for analytics queries

### Separation Strategy

```
┌─────────────────────────────────────┐
│  PostgreSQL (Lakebase) - OLTP       │
│                                     │
│  • projects (metadata)              │
│  • builds (job tracking + results)  │
│  • evaluations (job tracking)       │
│  • job_runs (polling state)         │
│                                     │
│  Optimized for:                     │
│  - Fast primary key lookups         │
│  - Transactional updates            │
│  - Cascading deletes                │
└─────────────────────────────────────┘

┌─────────────────────────────────────┐
│  Delta Lake - OLAP                  │
│                                     │
│  • chunks tables (partitioned)      │
│  • eval_results (aggregatable)      │
│                                     │
│  Optimized for:                     │
│  - Scalability (billions of rows)   │
│  - Analytics queries (GROUP BY)     │
│  - Time-travel (audit)              │
│  - Vector Search source tables      │
└─────────────────────────────────────┘
```

---

## Security & Authentication

### Databricks OAuth

**Authentication Flow:**

```python
# backend/auth.py
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementExecutionAPI

def get_workspace_client() -> WorkspaceClient:
    """Get authenticated Databricks client"""
    # Uses environment variables:
    # - DATABRICKS_HOST
    # - DATABRICKS_TOKEN (or OAuth)
    return WorkspaceClient()

def get_sql_connector():
    """Get SQL connector for Unity Catalog queries"""
    w = get_workspace_client()
    return w.statement_execution

def get_user_sql_connector():
    """Get SQL connector with user impersonation"""
    w = get_workspace_client()
    # User context from workspace client
    return w.statement_execution
```

### PostgreSQL OAuth Token Rotation

**Token Lifecycle:**

```python
class LakehousePostgresConnector:
    _token_ttl = 900  # 15 minutes

    def _ensure_valid_token(self):
        """Refresh token every 15 minutes"""
        current_time = time.time()

        if (self._token is None or
            current_time - self._token_last_refresh > self._token_ttl):
            # Get fresh token from Databricks
            self._token = self._get_fresh_token()
            self._token_last_refresh = current_time

            # Recreate connection pool with new token
            if self._pool:
                self._pool.closeall()
            self._initialize_pool()
```

**Why OAuth for PostgreSQL:**
- No long-lived credentials
- Automatic token rotation
- Integrated with Databricks RBAC
- Secure connection (SSL required)

---

## Performance Optimizations

### Database Query Optimization

**Use Indexes Strategically:**

```sql
-- Composite index for common query pattern
CREATE INDEX idx_builds_project_created ON builds(project_id, created_at DESC);

-- Query benefits from this index
SELECT * FROM builds
WHERE project_id = '...'
ORDER BY created_at DESC
LIMIT 10;
```

**Use JSONB Operators:**

```sql
-- Efficient JSONB query
SELECT run_id, config->>'data_type' as data_type
FROM builds
WHERE config @> '{"data_type": "pdf"}';
```

**Avoid N+1 Queries:**

```python
# BAD: N+1 queries
builds = get_builds_by_project(project_id)
for build in builds:
    evaluations = get_evaluations_by_build(build['run_id'])  # N queries!

# GOOD: Single JOIN query
query = """
    SELECT b.*, e.*
    FROM builds b
    LEFT JOIN evaluations e ON b.run_id = e.run_id
    WHERE b.project_id = %s
    ORDER BY b.created_at DESC
"""
```

### Frontend Performance

**Lazy Loading Components:**

```typescript
// Lazy load heavy components
const Review = lazy(() => import('./pages/Review'));
const MetricsBarCharts = lazy(() => import('./components/review/MetricsBarCharts'));

// Use Suspense for loading state
<Suspense fallback={<Loader />}>
  <Review />
</Suspense>
```

**Memoization:**

```typescript
// Memoize expensive calculations
const aggregatedMetrics = useMemo(() => {
  return aggregateByStrategy(mlflowRuns);
}, [mlflowRuns]);

// Memoize callback functions
const handleToggleBuild = useCallback((buildId: string) => {
  setSelectedBuilds(prev => {
    const newSet = new Set(prev);
    newSet.has(buildId) ? newSet.delete(buildId) : newSet.add(buildId);
    return newSet;
  });
}, []);
```

**Debouncing:**

```typescript
// Debounce search input
const [searchTerm, setSearchTerm] = useState('');
const debouncedSearch = useDebounce(searchTerm, 300);

useEffect(() => {
  if (debouncedSearch) {
    searchProjects(debouncedSearch);
  }
}, [debouncedSearch]);
```

### Databricks Serverless

**Why Serverless:**
- No cluster startup time (instant execution)
- Auto-scaling based on workload
- Pay only for compute used
- No cluster configuration needed
- Managed by Databricks

**Configuration:**

```python
# NO cluster configuration needed!
job_run = w.jobs.submit(
    run_name="Build Job",
    tasks=[Task(
        task_key="build",
        notebook_task=NotebookTask(
            notebook_path=notebook_path,
            base_parameters=params
        ),
        # Serverless - Databricks manages compute automatically
    )]
)
```

---

## Error Handling

### Backend Error Handling

**Consistent HTTP Exceptions:**

```python
from fastapi import HTTPException

@router.post("/builds")
async def submit_build(build_request: BuildRequest):
    try:
        # Validate input
        if not build_request.project_id:
            raise HTTPException(
                status_code=400,
                detail="project_id is required"
            )

        # Submit job
        job_run_id, job_url = submit_build_job(...)
        return {"job_run_id": job_run_id, "job_url": job_url}

    except ValidationError as e:
        raise HTTPException(status_code=422, detail=str(e))

    except Exception as e:
        # Log error
        import traceback
        traceback.print_exc()

        # Return generic error
        raise HTTPException(
            status_code=500,
            detail=f"Failed to submit build: {str(e)}"
        )
```

**Database Transaction Rollback:**

```python
with connector.get_connection() as conn:
    cursor = conn.cursor()

    try:
        cursor.execute("INSERT INTO projects ...")
        cursor.execute("INSERT INTO builds ...")
        conn.commit()  # Automatic in context manager
    except Exception:
        conn.rollback()  # Automatic in context manager
        raise
```

### Frontend Error Handling

**Try-Catch with User Feedback:**

```typescript
const [error, setError] = useState<string | null>(null);

const handleSubmit = async () => {
  setError(null);
  setLoading(true);

  try {
    await buildsApi.submit(buildRequest);
    navigate('/builds');
  } catch (err) {
    setError(err.message || 'Failed to submit build');
  } finally {
    setLoading(false);
  }
};

// Render error
{error && (
  <div className="bg-red-50 border border-red-200 rounded-md p-4">
    <p className="text-sm text-red-800">{error}</p>
  </div>
)}
```

**Axios Error Interceptor:**

```typescript
api.interceptors.response.use(
  (response) => response.data,
  (error) => {
    // Extract user-friendly error message
    const message = error.response?.data?.detail ||
                   error.message ||
                   'An unexpected error occurred';

    // Log for debugging
    console.error('API Error:', error);

    // Throw user-friendly error
    throw new Error(message);
  }
);
```

### Notebook Error Handling

**Graceful Degradation:**

```python
# Store experiment_id - fail gracefully if database unavailable
try:
    update_build_state(run_id=run_id, experiment_id=experiment.experiment_id)
    print("[INFO] ✓ Stored experiment_id")
except Exception as e:
    print(f"[WARNING] Failed to store experiment_id: {e}")
    # Continue - API will fall back to name-based lookup
```

**Comprehensive Logging:**

```python
print(f"[INFO] Starting build for run_id={run_id}")
print(f"[INFO] Project: {project_name}")
print(f"[INFO] Strategies: {list(strategies_config.keys())}")

try:
    # Process
    print(f"[INFO] Processing strategy: {strategy_name}")
    chunks = strategy.chunk(documents)
    print(f"[INFO] ✓ Generated {len(chunks)} chunks")
except Exception as e:
    print(f"[ERROR] ✗ Strategy {strategy_name} failed: {e}")
    import traceback
    traceback.print_exc()
    # Continue with other strategies
```

---

## Key Design Patterns

### 1. Repository Pattern

**Abstraction over data access:**

```python
# utils/postgres_state.py
def get_project(project_id: str) -> Optional[Dict]:
    """Get project by ID - hides SQL details"""
    connector = get_postgres_connector()
    query = "SELECT * FROM projects WHERE project_id = %s"
    return connector.execute(query, (project_id,), fetch="one")

# Usage in API
@router.get("/{project_id}")
async def get_project_endpoint(project_id: str):
    project = get_project(project_id)  # Simple, testable
    if not project:
        raise HTTPException(status_code=404)
    return project
```

### 2. Factory Pattern

**Strategy instantiation:**

```python
# retrieval_core/strategies.py
def get_strategy(strategy_name: str, **params) -> ChunkingStrategy:
    """Factory for chunking strategies"""
    strategies = {
        "baseline": BaselineStrategy,
        "semantic": SemanticStrategy,
        "structured": StructuredStrategy,
        "parent_child": ParentChildStrategy,
    }

    strategy_class = strategies.get(strategy_name)
    if not strategy_class:
        raise ValueError(f"Unknown strategy: {strategy_name}")

    return strategy_class(**params)
```

### 3. Command Pattern

**Job submission as commands:**

```python
# utils/jobs.py
class BuildJobCommand:
    def __init__(self, run_id: str, config: Dict):
        self.run_id = run_id
        self.config = config

    def execute(self) -> Tuple[int, str]:
        """Execute the build job"""
        return submit_build_job(self.run_id, self.config)

# Usage
command = BuildJobCommand(run_id, config)
job_run_id, job_url = command.execute()
```

### 4. Observer Pattern

**Frontend polling:**

```typescript
// Job status observer
class JobStatusObserver {
  private listeners: ((status: JobStatus) => void)[] = [];

  subscribe(callback: (status: JobStatus) => void) {
    this.listeners.push(callback);
  }

  notify(status: JobStatus) {
    this.listeners.forEach(listener => listener(status));
  }

  async startPolling(runId: string) {
    const interval = setInterval(async () => {
      const status = await buildsApi.getStatus(runId);
      this.notify(status);

      if (status.result_state) {
        clearInterval(interval);
      }
    }, 5000);
  }
}
```

---

## Summary

Retrieval Studio is architected as a modern, scalable RAG optimization platform with:

- **Clean separation** between UI (React), API (FastAPI), and data processing (notebooks)
- **Dual storage** strategy (PostgreSQL for OLTP, Delta for OLAP)
- **Comprehensive MLflow integration** for experiment tracking
- **Serverless compute** for zero cluster management
- **Extensible** data type and strategy systems
- **Production-ready** error handling and logging

The architecture prioritizes:
1. **Developer Experience**: Clear patterns, comprehensive logging
2. **User Experience**: Real-time updates, intuitive workflows
3. **Operational Excellence**: Automatic scaling, OAuth security
4. **Reproducibility**: MLflow tracking, immutable artifacts

---

For deployment instructions, see [DEPLOYMENT_GUIDE.md](./DEPLOYMENT_GUIDE.md).

For user guide, see [README.md](./README.md).
