---
title: 'Retrieval Studio - Per-Source Strategy & Resource Management'
slug: 'retrieval-studio-per-source-strategy-resource-management'
created: '2026-03-25'
status: 'implemented'
stepsCompleted:
  - Per-source strategy assignment (Build UI wizard)
  - Per-source-strategy delta table and VS index creation
  - Word/DOCX data type handler
  - Multi-corpus golden dataset auto-generation
  - Per-source comparison view in Review page
  - LLM-generated strategy comparison explanations
  - Index selection/retention UI
  - Resource cleanup API and UI
  - Study management (DB + API + UI)
  - Database schema (index_selections, studies, study_builds, study_evaluations)
  - Build/evaluation delete from Project Details
  - Binary file download via Databricks SDK Files API (serverless-safe)
  - Query Details per-strategy filter, chunk IDs, metric breakdown with match highlighting
  - Build status inspection (notebook exit value for FAILED/PARTIAL_SUCCESS)
  - Cleanup job status polling via dedicated endpoint
tech_stack:
  - React 18 + TypeScript + Vite
  - TailwindCSS
  - Plotly.js (react-plotly.js)
  - FastAPI + Pydantic
  - Python 3.10+
  - Databricks SDK
  - Lakebase PostgreSQL
  - Delta Lake
  - Vector Search
  - MLflow
  - PyMuPDF (PDF extraction)
  - python-docx (NEW — Word file support)
files_to_modify:
  - retrieval_core/data_types.py
  - retrieval_core/configs.py
  - retrieval_core/strategies.py
  - backend/models/schemas.py
  - backend/api/builds.py
  - backend/api/evaluations.py
  - backend/api/metadata.py
  - backend/api/projects.py
  - frontend/src/pages/Build.tsx
  - frontend/src/pages/Evaluate.tsx
  - frontend/src/pages/Review.tsx
  - frontend/src/pages/ProjectDetails.tsx
  - frontend/src/App.tsx
  - frontend/src/components/Layout.tsx
  - notebooks/build_notebook_v2.py
  - notebooks/eval_notebook.py
  - database/postgres_schema.sql
files_to_create:
  - backend/api/cleanup.py (CREATED)
  - backend/api/studies.py (CREATED)
  - frontend/src/components/review/SourceComparison.tsx (CREATED)
  - notebooks/cleanup_notebook.py (CREATED)
  - database/migrations/002_add_index_selections_and_studies.sql (CREATED)
code_patterns:
  - Registry Pattern (DATA_TYPE_REGISTRY for handlers)
  - Factory Pattern (get_data_type_handler)
  - Schema-driven UI (dynamic form generation from metadata)
  - Polling-based async monitoring (5s intervals)
  - Document abstraction (standardized Document dataclass)
  - MLflow hierarchical runs (parent/child/grandchild)
  - JSONB storage for flexible config
  - Per-source-strategy matrix build pattern (NEW)
  - Index lifecycle management (NEW)
test_patterns:
  - No formal test suite identified
  - Manual testing via UI workflows
  - Integration testing via Databricks jobs
---

# Tech-Spec: Retrieval Studio - Per-Source Strategy & Resource Management

**Created:** 2026-03-25

## Overview

### Problem Statement

Retrieval Studio currently merges ALL data sources into a single document list and applies the SAME set of chunking strategies globally. This prevents users from:

1. **Assigning different strategies per data source** — e.g., using "structured" for PDFs and "semantic" for text files
2. **Getting per-source delta tables and vector search indexes** — currently one table/index per strategy, not per source+strategy combo
3. **Comparing strategies within a specific data source** — e.g., "For my PDF data, which strategy performed best?"
4. **Selecting which indexes to keep** after experimentation
5. **Cleaning up unused resources** (vector search indexes, delta tables) from the app
6. **Using Word/DOCX files** as a data source

### User Scenario (MedRAG Example)

A user creates project "MedRAG" with:
- **Text files** with medical research notes
- **PDF files** with clinical guidelines
- **Word files** stored in a UC Volume
- **Delta tables** with structured patient records

For each data source, they choose different chunking strategies:
- Text files → Baseline + Semantic (2 strategies)
- PDF files → Structured + Parent-Child (2 strategies)

**Expected result: 4 separate delta tables, 4 separate vector search indexes.**

During evaluation:
- Upload a golden dataset OR auto-generate one using an LLM that reads ALL 4 delta tables
- All 4 VS indexes are tested against the golden queries
- Results show **per-source comparison**: "For Text files: Baseline scored 0.82 recall, Semantic scored 0.91 — best is Semantic because..."
- User selects the 2 best indexes to keep
- User triggers cleanup to delete the 2 unwanted indexes and their delta tables

### Solution Summary

1. **Per-source strategy assignment** — UI lets users choose strategies independently per data source
2. **Per-source-strategy build** — Each source+strategy combo produces its own delta table and VS index
3. **Word/DOCX support** — New data type handler
4. **Golden dataset upload** — Support file upload in addition to Delta table path
5. **Multi-corpus auto-generation** — LLM samples from ALL delta tables
6. **Per-source review** — Review page groups results by data source, compares strategies within source
7. **LLM explanations** — Auto-generated "why" explanations for strategy comparisons
8. **Index selection** — UI to mark indexes as "keep" or "discard"
9. **Resource cleanup** — Backend API + UI to delete discarded VS indexes and delta tables

### Scope

**In Scope:**
- Word/DOCX data type handler
- Per-source strategy assignment in Build UI
- Per-source-strategy delta table and VS index creation
- Multi-corpus golden dataset auto-generation
- Golden dataset file upload (CSV/JSON)
- Per-source comparison view in Review page
- LLM-generated strategy comparison explanations
- Index selection/retention UI
- Resource cleanup API and UI
- Study concept (grouping builds + evaluations)
- Database schema updates for source tracking

**Out of Scope:**
- New chunking strategies
- New evaluation metrics
- Authentication/RBAC changes
- CI/CD or deployment automation
- Infrastructure changes (Databricks, PostgreSQL setup)

## Gap Analysis (Current vs Required)

### Gap 1: No Word/DOCX File Support

**Current:** `DATA_TYPE_REGISTRY` has 6 handlers: pdf, delta_table, uc_volume, text, csv, json. The UC Volume handler routes files by extension but only handles `.txt`, `.pdf`, `.csv`, `.json`.

**Required:** Support `.docx` files both as direct upload and from UC Volumes.

**Impact:** Cannot process Word documents at all.

**Fix:** Add `DocxDataType` handler using `python-docx` library. Register in `DATA_TYPE_REGISTRY`. Add routing in `UCVolumeDataType._load_file_from_volume()`.

### Gap 2: No Per-Source Strategy Assignment

**Current:** Build UI (`Build.tsx`) selects strategies globally via `selectedStrategies` state. All sources get the same strategies.

```typescript
// Current: Global strategy selection
const [selectedStrategies, setSelectedStrategies] = useState<string[]>([])
```

**Required:** Each data source entry should have its own strategy selection.

```typescript
// Required: Per-source strategy selection
interface DataSourceEntry {
  id: string
  dataType: string
  config: Record<string, any>
  strategies: string[]  // <-- NEW: per-source strategies
  files: UploadedFileState[]
  uploadedVolumePath: string | null
}
```

**Impact:** Fundamental architecture change in Build UI and backend API contract.

### Gap 3: No Per-Source Delta Tables / Vector Search Indexes

**Current:** Build notebook creates ONE delta table per strategy, merging all source documents:

```python
# Current naming: project + strategy only
chunks_table = core_config.chunks_table(project_name, strategy_name)
# => "catalog.chunks.rs_chunks_medrag_baseline"

index_name = core_config.index_name(project_name, strategy_name)
# => "catalog.indexes.rs_index_medrag_baseline"
```

**Required:** Each source+strategy combo needs its own table and index:

```python
# Required naming: project + source + strategy
chunks_table = core_config.chunks_table(project_name, source_name, strategy_name)
# => "catalog.chunks.rs_chunks_medrag_textfiles_baseline"
# => "catalog.chunks.rs_chunks_medrag_textfiles_semantic"
# => "catalog.chunks.rs_chunks_medrag_pdffiles_structured"
# => "catalog.chunks.rs_chunks_medrag_pdffiles_parent_child"

# 4 tables, 4 indexes
```

**Impact:** Core naming convention change in `configs.py`, build notebook loop restructuring, index registry schema update.

### Gap 4: Auto-Generation Doesn't Span All Corpus Tables

**Current:** Eval notebook accepts single `corpus_table` parameter for auto-generation:

```python
corpus_table = dbutils.widgets.get("corpus_table")
generator.generate_queries(corpus_table=corpus_table, ...)
```

**Required:** When auto-generating, sample from ALL delta tables created during the build to create a comprehensive golden dataset covering all sources.

**Impact:** Change eval notebook to accept list of corpus tables, sample proportionally from each.

### Gap 5: No Per-Data-Source Comparison in Review

**Current:** Review page aggregates metrics by `strategy_name` only. MLflow eval runs are tagged with `strategy_name` but not `source_name`.

**Required:** Group metrics by source, then compare strategies within each source. Show:
- "For Text files: Baseline → Recall 0.82, Semantic → Recall 0.91"
- "For PDF files: Structured → Recall 0.78, Parent-Child → Recall 0.85"

**Impact:** Need `source_name` tag on MLflow runs. New Review component for per-source grouping.

### Gap 6: No "Why" Explanation for Best Strategy

**Current:** Review shows numeric metrics only. No explanation of why one strategy outperforms another.

**Required:** LLM-generated explanation: "Semantic chunking performed 11% better than Baseline for text files because it preserves sentence boundaries, which is critical for medical research notes where context within paragraphs is important."

**Impact:** New backend endpoint that takes metrics + sample data and calls LLM for explanation. New frontend component.

### Gap 7: No Index Selection/Retention Feature

**Current:** No way to mark indexes as "keep" or "discard." All indexes persist after evaluation.

**Required:** After reviewing results, user selects which VS indexes to keep for production use. Selection is stored at project level.

**Impact:** New database column/table for index status. New UI component. New API endpoint.

### Gap 8: No Resource Cleanup

**Current:** `doc.md` states: "What does NOT get deleted: Delta tables with chunks, Vector Search indexes, MLflow experiment runs."

**Required:** Cleanup button that:
1. Deletes discarded vector search indexes via Databricks SDK
2. Drops discarded delta tables via Spark SQL
3. Updates index registry
4. Marks resources as cleaned in database

**Impact:** New cleanup API module, new UI page/modal, Databricks SDK operations for index deletion.

**Databricks API References:**
- Delete VS index: `VectorSearchClient.delete_index(endpoint_name, index_name)` — [Databricks Vector Search SDK](https://docs.databricks.com/api/workspace/vectorsearchindexes/deleteindex)
- Drop Delta table: `spark.sql(f"DROP TABLE IF EXISTS {table_name}")` — standard Spark SQL
- Verify deletion: `VectorSearchClient.get_index(endpoint_name, index_name)` should raise 404

### Gap 9: No Formal Study Concept

**Current:** Builds are independent. Multiple builds per project exist but aren't grouped.

**Required:** A "study" groups a specific set of source+strategy builds + their evaluations. Users can have multiple studies per project (e.g., "Study 1: Initial strategies" → "Study 2: Refined strategies").

**Impact:** New database table for studies. Study-level grouping in UI.

## Implementation Plan

### Phase 1: Core Architecture (Per-Source Build Pipeline)

#### Task 1.1 — Add Word/DOCX Data Type Handler

**Files:** `retrieval_core/data_types.py`, `requirements.txt`

**Changes:**
- Add `python-docx` to requirements
- Create `DocxDataType(DataTypeHandler)` class:
  - `get_name()` → `"docx"`
  - `get_display_name()` → `"Word Documents"`
  - `load_documents()` — extract text from .docx using `python-docx`
  - `get_compatible_strategies()` → `["baseline", "structured", "parent_child", "semantic"]`
- Register in `DATA_TYPE_REGISTRY`
- Add `.docx` routing in `UCVolumeDataType._load_file_from_volume()`
- Add `'docx'` to `FILE_UPLOAD_TYPES` in `Build.tsx`

**Acceptance Criteria:**
- [ ] `.docx` files can be uploaded directly
- [ ] `.docx` files in UC Volumes are auto-detected and processed
- [ ] Text extraction preserves paragraphs and headings
- [ ] Compatible with all 4 main chunking strategies

#### Task 1.2 — Per-Source Strategy Assignment (Backend Schema)

**Files:** `backend/models/schemas.py`, `backend/api/metadata.py`

**Changes:**
- Update `BuildJobConfig` to accept per-source strategies:

```python
class SourceConfig(BaseModel):
    source_name: str  # User-given name, e.g. "clinical_pdfs"
    source_type: str  # pdf, text, docx, delta_table, uc_volume, csv, json
    config: Dict[str, Any]
    strategies: Dict[str, Dict[str, Any]]  # Per-source strategies

class BuildJobConfig(BaseModel):
    # Keep backward compatible fields
    data_type: Optional[str] = None
    data_config: Optional[Dict[str, Any]] = None
    strategies: Optional[Dict[str, Dict[str, Any]]] = None

    # New per-source field
    sources: Optional[List[SourceConfig]] = None

    embedding_model_endpoint: str
    vs_endpoint_name: str
    create_index: bool = True
```

- Validation: if `sources` is provided, `data_type`/`data_config`/`strategies` are ignored
- If `sources` is not provided, legacy flow works unchanged

**Acceptance Criteria:**
- [ ] New `sources` field accepted in API
- [ ] Legacy `data_type` + `strategies` still works
- [ ] Each source has its own strategy list
- [ ] Source names are validated (alphanumeric + underscores)

#### Task 1.3 — Per-Source-Strategy Build Notebook

**Files:** `notebooks/build_notebook_v2.py`, `retrieval_core/configs.py`

**Changes to `configs.py`:**

```python
@classmethod
def chunks_table(cls, project_name: str, strategy: str, source_name: str = None) -> str:
    p = cls._safe_name(project_name).lower()
    s = cls._safe_name(strategy).lower()
    if source_name:
        src = cls._safe_name(source_name).lower()
        return cls.fq_table(cls.CHUNKS_SCHEMA, f"rs_chunks_{p}_{src}_{s}")
    return cls.fq_table(cls.CHUNKS_SCHEMA, f"rs_chunks_{p}_{s}")

@classmethod
def index_name(cls, project_name: str, strategy: str, source_name: str = None) -> str:
    p = cls._safe_name(project_name).lower()
    s = cls._safe_name(strategy).lower()
    if source_name:
        src = cls._safe_name(source_name).lower()
        return cls.fq_table(cls.INDEXES_SCHEMA, f"rs_index_{p}_{src}_{s}")
    return cls.fq_table(cls.INDEXES_SCHEMA, f"rs_index_{p}_{s}")
```

**Changes to build notebook:**

```python
# New per-source flow
sources = config.get("sources", [])
if sources:
    for source in sources:
        source_name = source["source_name"]
        source_type = source["source_type"]
        source_config = source["config"]
        source_strategies = source["strategies"]

        # Load documents for THIS source
        documents = _load_single_source(source_type, source_config)

        # For each strategy assigned to THIS source
        for strategy_name, strategy_params in source_strategies.items():
            with mlflow.start_run(..., nested=True):
                mlflow.set_tag("rs_role", "build_strategy")
                mlflow.log_param("source_name", source_name)
                mlflow.log_param("strategy_name", strategy_name)

                # Chunk documents
                strat = get_strategy(strategy_name, **(strategy_params or {}))
                chunks = strat.chunk(doc_dicts)

                # Per-source-strategy table and index
                chunks_table = core_config.chunks_table(project_name, strategy_name, source_name)
                index_name = core_config.index_name(project_name, strategy_name, source_name)

                # Write delta table and create VS index...
else:
    # Legacy flow (unchanged)
    ...
```

**Index registry update:**

```sql
-- Add source_name column
ALTER TABLE {index_registry} ADD COLUMNS (source_name STRING);
```

**Acceptance Criteria:**
- [ ] Each source+strategy combo creates a separate delta table
- [ ] Each source+strategy combo creates a separate VS index
- [ ] Index registry tracks source_name alongside strategy
- [ ] MLflow runs tagged with both source_name and strategy_name
- [ ] Legacy single-source builds still work unchanged
- [ ] Example: 2 sources × 2 strategies each = 4 delta tables + 4 VS indexes

#### Task 1.4 — Per-Source Build UI

**Files:** `frontend/src/pages/Build.tsx`

**Changes:**
- Restructure wizard steps:
  1. **Add Data Sources** — Add multiple sources, each with a name, type, and config
  2. **Assign Strategies Per Source** — For each source, select compatible strategies
  3. **Configure Endpoints** — Embedding model + VS endpoint (shared)
  4. **Review & Submit**

- Per-source strategy UI:

```typescript
interface DataSourceEntry {
  id: string
  name: string  // NEW: user-given name, e.g. "clinical_pdfs"
  dataType: string
  config: Record<string, any>
  strategies: string[]  // NEW: per-source strategies
  files: UploadedFileState[]
  uploadedVolumePath: string | null
}
```

- Step 2 shows each source as a card with its compatible strategies as checkboxes
- Summary step shows matrix: Sources × Strategies → N tables/indexes to be created

**Acceptance Criteria:**
- [ ] Each source has a user-given name field
- [ ] Each source shows only its compatible strategies
- [ ] Different sources can have different strategies selected
- [ ] Summary shows total tables/indexes to be created (source × strategy matrix)
- [ ] Submit sends `sources` array (not legacy `data_type` + `strategies`)

### Phase 2: Evaluation Enhancements

#### Task 2.1 — Multi-Corpus Auto-Generation

**Files:** `notebooks/eval_notebook.py`, `backend/api/evaluations.py`

**Changes:**
- Accept list of corpus tables instead of single `corpus_table`
- When auto-generating, sample proportionally from each table:

```python
corpus_tables = dbutils.widgets.get("corpus_tables")  # JSON list
tables = json.loads(corpus_tables) if corpus_tables else []

all_queries = []
for table_info in tables:
    table_name = table_info["table"]
    source_name = table_info["source_name"]
    proportion = table_info.get("proportion", 1.0 / len(tables))
    n_queries = int(num_queries * proportion)

    generator = QueryGenerator(random_seed=42)
    queries_df = generator.generate_queries(
        corpus_table=table_name,
        columns=["chunk_text"],
        num_queries=n_queries,
        style=query_style,
        spark_session=spark
    )
    all_queries.extend(queries_df.collect())
```

- Backend resolves ALL corpus tables from build's index registry

**Acceptance Criteria:**
- [ ] Auto-generation samples from ALL delta tables from the build
- [ ] Queries are proportionally distributed across sources
- [ ] Generated golden dataset covers all data sources
- [ ] Single corpus_table still works for legacy builds

#### Task 2.2 — Golden Dataset File Upload

**Files:** `frontend/src/pages/Evaluate.tsx`, `backend/api/evaluations.py`, `backend/api/uploads.py`

**Changes:**
- Add file upload option alongside Delta table path:
  - CSV upload: parse `query_text` and `expected_chunks` columns
  - JSON upload: parse array of `{query_text, expected_chunks}`
- Upload file to UC Volume, then create Delta table from it
- Frontend toggle: "Delta Table" | "Upload File"

**Acceptance Criteria:**
- [ ] CSV file with query_text + expected_chunks columns can be uploaded
- [ ] JSON file with array of query objects can be uploaded
- [ ] Uploaded file is converted to Delta table for evaluation
- [ ] Delta table path option still works

#### Task 2.3 — Evaluation Across All Source-Strategy Indexes

**Files:** `notebooks/eval_notebook.py`

**Changes:**
- Instead of iterating only over strategy names, iterate over source+strategy combos
- Tag each eval child run with both `source_name` and `strategy_name`

```python
for _, r in build_child_runs.iterrows():
    source_name = r.get("params.source_name", "")
    strategy_name = r.get("params.strategy_name")
    index_name = r.get("params.vs_index_name")

    with mlflow.start_run(
        run_name=f"eval_{source_name}_{strategy_name}_{query_type}",
        nested=True
    ) as eval_child:
        mlflow.log_param("source_name", source_name)
        mlflow.log_param("strategy_name", strategy_name)
        # ... run evaluation against this specific index
```

**Acceptance Criteria:**
- [ ] All source+strategy indexes are evaluated
- [ ] Each eval run tagged with source_name and strategy_name
- [ ] Metrics stored per source+strategy combination

### Phase 3: Review & Comparison Enhancements

#### Task 3.1 — Per-Source Comparison View

**Files:** `frontend/src/pages/Review.tsx`, `frontend/src/components/review/SourceComparison.tsx`

**Changes:**
- New `SourceComparison` component that groups eval runs by source_name:

```
┌──────────────────────────────────────────────────────┐
│ Source: clinical_pdfs (PDF)                           │
│ ┌──────────────┬──────────┬──────────┬──────────────┐ │
│ │ Strategy     │ Recall@10│ NDCG@10  │ Latency (ms) │ │
│ ├──────────────┼──────────┼──────────┼──────────────┤ │
│ │ structured   │ 0.78     │ 0.72     │ 45           │ │
│ │ parent_child │ 0.85 ★   │ 0.81 ★   │ 52           │ │
│ └──────────────┴──────────┴──────────┴──────────────┘ │
│ ★ Best: parent_child — [View Explanation]             │
│ ☐ Keep parent_child index  ☐ Keep structured index    │
└──────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────┐
│ Source: research_notes (Text)                         │
│ ┌──────────────┬──────────┬──────────┬──────────────┐ │
│ │ Strategy     │ Recall@10│ NDCG@10  │ Latency (ms) │ │
│ ├──────────────┼──────────┼──────────┼──────────────┤ │
│ │ baseline     │ 0.82     │ 0.71     │ 38           │ │
│ │ semantic     │ 0.91 ★   │ 0.87 ★   │ 48           │ │
│ └──────────────┴──────────┴──────────┴──────────────┘ │
│ ★ Best: semantic — [View Explanation]                 │
│ ☐ Keep semantic index  ☐ Keep baseline index          │
└──────────────────────────────────────────────────────┘
```

- Extract `source_name` from MLflow run params
- Group runs by source, then compare strategies within each source
- Best strategy per source is highlighted

**Acceptance Criteria:**
- [ ] Results grouped by data source
- [ ] Within each source, strategies compared side-by-side
- [ ] Best strategy per source highlighted with star
- [ ] Checkboxes to select indexes to keep

#### Task 3.2 — LLM-Generated Strategy Explanations

**Files:** `backend/api/evaluations.py` (new endpoint), `frontend/src/components/review/StrategyExplanation.tsx`

**Changes:**
- New API endpoint: `POST /api/evaluations/explain`
  - Input: source_name, metrics for each strategy, sample chunks
  - Output: LLM-generated explanation text
- Uses Databricks Foundation Model endpoint (e.g., `databricks-claude-sonnet-4-5`)
- Prompt includes metrics, data source type, strategy descriptions, and sample chunks

```python
prompt = f"""
Compare these chunking strategies for {source_type} data source "{source_name}":

{strategy_metrics_table}

Strategy descriptions:
- {strategy_descriptions}

Sample data from source:
{sample_text[:2000]}

Explain in 2-3 sentences:
1. Which strategy performed best and by how much
2. WHY this strategy works better for this type of data
3. Any trade-offs to consider (e.g., latency vs quality)
"""
```

**Acceptance Criteria:**
- [ ] "View Explanation" button triggers LLM call
- [ ] Explanation covers which strategy won and why
- [ ] Explanation references the specific data source type
- [ ] Explanation mentions relevant trade-offs

### Phase 4: Index Selection & Resource Cleanup

#### Task 4.1 — Index Selection/Retention

**Files:** `database/postgres_schema.sql`, `backend/api/projects.py`, `frontend/src/pages/Review.tsx`

**Changes:**
- New database table:

```sql
CREATE TABLE index_selections (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID REFERENCES projects(project_id),
    build_run_id VARCHAR(255),
    source_name VARCHAR(255),
    strategy_name VARCHAR(255),
    index_name VARCHAR(500),
    chunks_table VARCHAR(500),
    vs_endpoint VARCHAR(255),
    status VARCHAR(50) DEFAULT 'active',  -- active, keep, discard, deleted
    selected_at TIMESTAMP,
    selected_by VARCHAR(255),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);
```

- API endpoints:
  - `GET /api/projects/{project_id}/indexes` — list all indexes with status
  - `PUT /api/projects/{project_id}/indexes/{index_id}/status` — update status (keep/discard)
  - `POST /api/projects/{project_id}/indexes/select` — bulk select/deselect

- Frontend: checkboxes in SourceComparison component to mark keep/discard

**Acceptance Criteria:**
- [ ] All indexes listed per project with current status
- [ ] User can mark indexes as "keep" or "discard"
- [ ] Selection persisted in database
- [ ] Selection UI integrated into Review page per-source comparison

#### Task 4.2 — Resource Cleanup API

**Files:** `backend/api/cleanup.py` (new), `backend/main.py`

**Changes:**
- New API module with endpoints:

```python
@router.post("/projects/{project_id}/cleanup")
async def cleanup_resources(project_id: str, request: CleanupRequest):
    """Delete discarded VS indexes and delta tables"""

    # 1. Get all indexes marked as "discard" for this project
    discarded = get_discarded_indexes(project_id)

    results = []
    for idx in discarded:
        try:
            # Delete VS index
            vs_client = VectorSearchClient()
            vs_client.delete_index(
                endpoint_name=idx.vs_endpoint,
                index_name=idx.index_name
            )

            # Drop Delta table
            # Via Databricks SQL connector or job
            sql_connector.execute(f"DROP TABLE IF EXISTS {idx.chunks_table}")

            # Update registry
            sql_connector.execute(
                f"DELETE FROM {index_registry} WHERE index_name = '{idx.index_name}'"
            )

            # Mark as deleted in database
            update_index_status(idx.id, "deleted")

            results.append({"index": idx.index_name, "status": "deleted"})

        except Exception as e:
            results.append({"index": idx.index_name, "status": "error", "error": str(e)})

    return {"results": results}

@router.get("/projects/{project_id}/cleanup/preview")
async def preview_cleanup(project_id: str):
    """Preview what would be deleted"""
    discarded = get_discarded_indexes(project_id)
    return {
        "indexes_to_delete": [idx.index_name for idx in discarded],
        "tables_to_drop": [idx.chunks_table for idx in discarded],
        "count": len(discarded)
    }
```

**Databricks SDK Reference:**
- `VectorSearchClient.delete_index(endpoint_name, index_name)` — Deletes a vector search index
- Before deletion, verify index exists: `VectorSearchClient.get_index(endpoint_name, index_name)`
- After deletion, the underlying Delta table still exists and must be dropped separately
- Drop table: `spark.sql("DROP TABLE IF EXISTS catalog.schema.table_name")`

**Acceptance Criteria:**
- [ ] Preview shows what will be deleted before confirming
- [ ] VS indexes are deleted via Databricks SDK
- [ ] Delta tables are dropped via SQL
- [ ] Index registry updated
- [ ] Database records marked as "deleted"
- [ ] Errors for individual resources don't block other deletions
- [ ] Cleanup is idempotent (safe to run multiple times)

#### Task 4.3 — Cleanup UI

**Files:** `frontend/src/pages/ProjectDetails.tsx` or `frontend/src/pages/Cleanup.tsx`, `frontend/src/App.tsx`

**Changes:**
- Add "Manage Resources" section to Project Details page (or dedicated Cleanup page)
- Shows all indexes with status: active / keep / discard / deleted
- "Preview Cleanup" button → shows what will be deleted
- "Confirm Cleanup" button → executes deletion with progress
- Add route to `App.tsx` if separate page

**UI Layout:**

```
┌──────────────────────────────────────────────────────────────┐
│ Resource Management — Project: MedRAG                         │
├──────────────────────────────────────────────────────────────┤
│                                                               │
│ ┌─ Active Indexes (4) ─────────────────────────────────────┐ │
│ │ ✅ rs_index_medrag_textfiles_semantic    [Keep] [Discard] │ │
│ │ ❌ rs_index_medrag_textfiles_baseline    [Keep] [Discard] │ │
│ │ ✅ rs_index_medrag_pdffiles_parent_child [Keep] [Discard] │ │
│ │ ❌ rs_index_medrag_pdffiles_structured   [Keep] [Discard] │ │
│ └───────────────────────────────────────────────────────────┘ │
│                                                               │
│ ┌─ Cleanup Preview ──────────────────────────────────────────┐│
│ │ 2 indexes to delete:                                       ││
│ │ • rs_index_medrag_textfiles_baseline                       ││
│ │ • rs_index_medrag_pdffiles_structured                      ││
│ │                                                            ││
│ │ 2 delta tables to drop:                                    ││
│ │ • catalog.chunks.rs_chunks_medrag_textfiles_baseline       ││
│ │ • catalog.chunks.rs_chunks_medrag_pdffiles_structured      ││
│ │                                                            ││
│ │ [Cancel]  [⚠️ Confirm Cleanup — This cannot be undone]      ││
│ └────────────────────────────────────────────────────────────┘│
└──────────────────────────────────────────────────────────────┘
```

**Acceptance Criteria:**
- [ ] All project indexes visible with current status
- [ ] Toggle between "keep" and "discard"
- [ ] Preview shows exactly what will be deleted
- [ ] Confirmation dialog warns about irreversibility
- [ ] Progress indicator during cleanup
- [ ] Success/error status shown per resource after cleanup

### Phase 5: Study Management (Optional Enhancement)

#### Task 5.1 — Study Entity

**Files:** `database/postgres_schema.sql`, `backend/api/projects.py`, `frontend/src/pages/ProjectDetails.tsx`

**Changes:**
- New database table:

```sql
CREATE TABLE studies (
    study_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    project_id UUID REFERENCES projects(project_id),
    study_name VARCHAR(255) NOT NULL,
    description TEXT,
    build_run_ids TEXT[],  -- Array of build run IDs in this study
    eval_ids TEXT[],       -- Array of eval IDs in this study
    status VARCHAR(50) DEFAULT 'active',
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);
```

- A study groups related builds and evaluations
- Project Details page shows studies as tabs/sections
- Each study has its own review scope

**Acceptance Criteria:**
- [ ] Studies can be created within a project
- [ ] Builds and evaluations can be assigned to a study
- [ ] Project Details shows study-level grouping
- [ ] Review page can scope to a specific study

## Implementation Priority

| Phase | Priority | Effort | Description |
|-------|----------|--------|-------------|
| Phase 1 | **P0 — Critical** | Large | Per-source build pipeline (the core architecture change) |
| Phase 2 | **P0 — Critical** | Medium | Evaluation against all source-strategy indexes |
| Phase 3 | **P1 — High** | Medium | Per-source comparison + explanations |
| Phase 4 | **P1 — High** | Medium | Index selection + resource cleanup |
| Phase 5 | **P2 — Nice to have** | Small | Study management |

## Dependencies

### New Python Dependencies
- `python-docx` — Word/DOCX file parsing

### Databricks SDK Methods Used
- `VectorSearchClient.delete_index(endpoint_name, index_name)` — Delete VS index
- `VectorSearchClient.get_index(endpoint_name, index_name)` — Check index exists
- `VectorSearchClient.create_delta_sync_index(...)` — Create VS index (existing)
- Spark SQL `DROP TABLE IF EXISTS` — Drop Delta tables

### Database Migrations Required
1. Add `source_name` column to index registry Delta table
2. Create `index_selections` table in PostgreSQL
3. Create `studies` table in PostgreSQL (Phase 5)

## Testing Strategy

### Manual Testing Workflow
1. Create project "MedRAG"
2. Add 2 sources: "clinical_pdfs" (PDF, strategies: structured, parent_child) + "research_notes" (text, strategies: baseline, semantic)
3. Verify 4 delta tables and 4 VS indexes created
4. Run evaluation with auto-generated golden dataset
5. Verify all 4 indexes evaluated
6. Review results grouped by source
7. Select 2 best indexes (one per source)
8. Run cleanup, verify 2 indexes and 2 tables deleted

### Edge Cases
- Single source with single strategy (legacy behavior)
- Source with no compatible strategies
- Cleanup when VS index already deleted externally
- Very large number of sources (10+) — UI scrolling/pagination
- Source name collisions within project
