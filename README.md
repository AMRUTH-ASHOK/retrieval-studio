# Retrieval Studio

A comprehensive Databricks application for systematically evaluating and optimizing Retrieval-Augmented Generation (RAG) pipelines.

## What is Retrieval Studio?

Retrieval Studio provides a complete **inner loop** for improving retrieval quality in RAG systems. Instead of guessing which chunking strategy works best, you can:

- **Per-source strategy assignment** - Apply different chunking strategies to different data sources (e.g., "structured" for PDFs, "semantic" for text files)
- **Multiple data sources per build** - Combine UC Volumes, PDFs, text files, Delta tables, and Word documents in a single build
- **Auto-generate** synthetic evaluation queries from all your document chunks
- **Measure** retrieval quality with industry-standard metrics (Recall@K, NDCG@K, Precision@K)
- **Per-source comparison** - See which strategy works best for each data source, with LLM-generated explanations
- **Resource management** - Select which VS indexes to keep and clean up the rest
- **Track** all experiments in MLflow with full reproducibility

---

## Quick Start

### Prerequisites

- **Databricks Workspace** with Unity Catalog enabled
- **Lakebase PostgreSQL** instance (for application state)
- **Vector Search** endpoint (creates automatically if needed)
- **Python 3.10+** and **Node.js 18+** (for local development)

### 1. Database Setup

Run the schema creation script in your Lakebase SQL Editor:

```sql
-- Copy and run contents of database/postgres_schema.sql
-- This creates: projects, builds, evaluations, job_runs,
--   index_selections, studies, study_builds, study_evaluations tables
```

### 2. Configure Environment Variables

Create `.env` file in the backend directory:

```bash
# Databricks Configuration
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=dapi...

# Unity Catalog
UC_CATALOG=retrievalstudio
RAW_SCHEMA=raw

# Notebook Paths
BUILD_NOTEBOOK_PATH=/Workspace/Users/your-email/retrieval-studio/notebooks/build_notebook_v2
EVAL_NOTEBOOK_PATH=/Workspace/Users/your-email/retrieval-studio/notebooks/eval_notebook

# PostgreSQL (Lakebase)
POSTGRES_HOST=your-postgres.cloud.databricks.com
POSTGRES_PORT=5432
POSTGRES_DATABASE=default
POSTGRES_USER=your_user
POSTGRES_PASSWORD=your_password
```

### 3. Upload Notebooks to Databricks

Upload these notebooks to your Databricks workspace:
- `notebooks/build_notebook_v2.py` - Build pipeline
- `notebooks/eval_notebook.py` - Evaluation pipeline
- `notebooks/cleanup_notebook.py` - Resource cleanup pipeline

### 4. Deploy the Application

**Option A: Deploy to Databricks Apps (Recommended)**
```bash
# 1. Build the frontend
cd frontend && npm run build && cd ..

# 2. Upload all files to your Databricks workspace folder
# 3. Go to Compute > Apps > your app > Deploy > select workspace folder > Deploy
```

**Option B: Run Locally for Development**
```bash
# Terminal 1 - Backend
cd backend
pip install -r requirements.txt
uvicorn main:app --reload --port 8000

# Terminal 2 - Frontend
cd frontend
npm install
npm run dev
```

---

## How to Use Retrieval Studio

### Step 1: Create a Project

1. Navigate to **Projects** page
2. Click **"Create New Project"**
3. Fill in:
   - **Project Name**: e.g., "Product Documentation RAG"
   - **Description**: Optional description
   - **Catalog/Schema**: Unity Catalog location for data (defaults from env)
   - **Vector Search Endpoint**: Name of your VS endpoint
   - **Embedding Model**: e.g., `databricks-gte-large-en`

4. Click **"Create Project"**

Your project is now ready for builds!

---

### Step 2: Build Your Indexes

The **Build** page uses a 4-step wizard to configure and submit build jobs.

#### Step 2a: Add Data Sources

Add one or more named data sources. Each source has its own type and configuration.

**Supported Data Types:**
- **UC Volume** - Read files from Unity Catalog Volumes (supports txt, pdf, docx, csv, json)
- **Delta Table** - Query existing Delta tables
- **CSV** - Upload CSV files
- **JSON** - Upload JSON files
- **PDF** - Upload PDF documents (text extraction via PyMuPDF)
- **DOCX** - Upload Word documents (text extraction via python-docx)

**Example:** Add a source named "clinical_docs" with type "uc_volume" pointing to `/Volumes/catalog/schema/my_volume`

#### Step 2b: Assign Strategies Per Source

For each data source, independently select which chunking strategies to apply:

- **Baseline** - Fixed-size chunks with overlap (chunk_size: 512, overlap: 50)
- **Semantic** - Sentence-boundary aware chunking
- **Structured** - Section-aware chunking preserving document hierarchy
- **Parent-Child** - Two-level hierarchy with parent context (parent: 2048, child: 512, overlap: 50)
- **Sentence** - Split on sentence boundaries
- **Paragraph** - Split on paragraph boundaries

Each source-strategy combination creates a **separate Delta table** and **Vector Search index**.

#### Step 2c: Configure Endpoints

Set the shared embedding model endpoint and Vector Search endpoint.

#### Step 2d: Review & Submit

Review the source-strategy matrix showing all combinations that will be built, including predicted table names like `{catalog}.chunks.rs_chunks_{project}_{source}_{strategy}`.

**What happens during a build:**
- For each source, documents are loaded (from UC Volume, uploaded files, or Delta table)
- For each strategy assigned to that source, documents are chunked independently
- Each source-strategy combo gets its own Delta table and VS index
- MLflow child runs are tagged with `source_name` and `strategy_name`
- Build status reflects the notebook's actual result (SUCCESS/PARTIAL_SUCCESS/FAILED)

---

### Step 3: Evaluate Retrieval Quality

The **Evaluate** page runs systematic tests on your built indexes.

#### Select a Build

Choose a completed build (auto-selected when navigating from Project Details via "Evaluate This Build").

#### Configure Evaluation

**Evaluation Dataset Options:**

**Option A: Auto-Generate Queries (Recommended)**
- No manual query creation needed
- System generates synthetic queries by sampling from ALL source-specific chunk tables
- Parameters:
  - `Number of Queries`: How many to generate (e.g., 50)
  - `Query Style`: `keyword`, `specific`, `broad`, or `contextual`
  - `Compare Query Types`: Test FULL_TEXT, ANN, and HYBRID search

**Option B: Use Existing Golden Dataset (Delta Table)**
- Provide a Delta table with pre-written queries and expected chunks
- Required columns: `query_text`, `expected_chunks`

**Evaluation Settings:**
- `Top-K`: How many results to retrieve (default: 10)
- `Judge Model`: LLM endpoint for relevance scoring

#### What happens during evaluation:
- Queries are generated or loaded from the golden dataset
- Each query is run against ALL strategy indexes from the build
- Top-K results are retrieved from each Vector Search index
- Metrics are calculated per query per strategy (Recall@K, NDCG@K, Precision@K, Latency)
- MLflow child runs are tagged with `source_name`, `strategy_name`, and `eval_id`
- Results are stored in `{catalog}.{schema}.rs_eval_results` (append mode, keyed by `eval_id`)

---

### Step 4: Review & Compare Results

The **Review** page provides comprehensive analysis and comparison.

#### Select Builds and Evaluations

1. **Select Builds**: Choose one or more builds to compare
2. **Select Evaluations**: Pick specific evaluation runs
3. Click **"Review Selected"**

#### View Results

**Best Performers**
- **Best Build**: Highest average recall across all strategies
- **Best Strategy**: Top-performing chunking approach
- **Fastest**: Lowest average query latency
- **Best Overall**: Balanced score across metrics

**Per-Source Comparison**
- Results grouped by data source
- Strategies compared side-by-side within each source
- Best strategy highlighted with LLM-generated explanation ("Why is X best?")
- Index keep/discard toggle per strategy

**Metrics Bar Charts**
- **By Build**: Compare builds across all metrics
- **By Strategy**: Compare chunking strategies
- **By Evaluation**: See performance trends over time
- Scatter plot: Latency vs Recall trade-off

**Comparison Table**
- Side-by-side metrics for all evaluations
- Sortable by any metric

**Query Details**
- **Strategy filter**: View queries for a specific strategy only
- **Per-query metrics**: Recall, Precision, NDCG, and Latency for each query
- **Expected vs Retrieved chunks**: Side-by-side with chunk IDs, scores, and text
- **Match highlighting**: Green border for retrieved chunks that match expected, red for non-matching
- **Metric breakdown**: Explains how each metric was computed for the query (e.g., "3 of 5 expected found in top 10")

**MLflow Integration**
- Direct link to MLflow experiment
- View all runs, parameters, and metrics
- Full experiment reproducibility

---

### Step 5: Project Details & Management

The **Project Details** page shows comprehensive project history and resource management.

**Build History:**
- All builds with status (SUCCESS/FAILED/PARTIAL_SUCCESS/RUNNING), configuration summary, and job links
- Per-source strategy display showing sources, types, and strategies used
- Delete builds (cascades to evaluations, study associations, and index selections)

**Evaluation History:**
- Nested under each build, showing all evaluation runs
- Delete individual evaluations

**Resource Management:**
- All tracked VS indexes and Delta tables created by builds
- Mark indexes as "keep" or "discard"
- **Cleanup**: Preview and permanently delete discarded resources (VS indexes + Delta tables)

**Studies:**
- Create named studies to group related builds and evaluations
- Organize experiments within a project

**Actions:**
- Evaluate any successful build directly ("Evaluate This Build" passes the build ID)
- Refresh build status (polls Databricks for RUNNING/PENDING builds)
- Delete project (with warning about associated resources)

---

## Understanding the Metrics

### Recall@K
**Measures:** What percentage of relevant documents were retrieved?

```
Recall@10 = (Relevant docs in top 10) / (Total relevant docs)
```

- **Higher is better** (0.0 to 1.0)
- Best for: Ensuring you don't miss important information
- Example: Recall@10 = 0.8 means 80% of relevant docs were found

### NDCG@K (Normalized Discounted Cumulative Gain)
**Measures:** Quality of ranking (are relevant docs ranked higher?)

```
NDCG@10 = DCG@10 / IDCG@10
```

- **Higher is better** (0.0 to 1.0)
- Best for: Ensuring best results appear first
- Penalizes relevant documents appearing lower in results

### Precision@K
**Measures:** What percentage of retrieved documents are relevant?

```
Precision@10 = (Relevant docs in top 10) / 10
```

- **Higher is better** (0.0 to 1.0)
- Best for: Minimizing irrelevant results
- Example: Precision@10 = 0.7 means 7 out of 10 results are relevant

### Latency
**Measures:** Query execution time

- **Lower is better** (milliseconds)
- Important for production performance
- Includes embedding + vector search time

---

## Common Workflows

### Workflow 1: Multi-Source RAG Comparison (Recommended)

```
1. Create Project "MedRAG"
2. Build: Add "clinical_pdfs" (UC Volume, *.pdf) with baseline + structured
         Add "research_notes" (UC Volume, *.txt) with baseline + semantic
3. Evaluate with auto-generated queries (50+ queries)
4. Review → Per-Source Comparison shows:
   - For clinical_pdfs: structured > baseline (why?)
   - For research_notes: semantic > baseline (why?)
5. Keep the 2 winning indexes, discard the rest
6. Cleanup to delete unwanted VS indexes and Delta tables
```

### Workflow 2: Single Source Strategy Comparison

```
1. Create Project
2. Build: Add source with ALL strategies (baseline, semantic, structured, parent-child)
3. Evaluate with auto-generated queries
4. Review → Filter Query Details by strategy to see per-query performance
5. Use winning strategy for production
```

### Workflow 3: Iterate and Improve

```
1. Build → Evaluate → Review (Baseline: Recall@10 = 0.65)
2. Review Query Details → identify failing queries, check chunk match highlighting
3. Adjust chunking parameters or data preprocessing
4. Build → Evaluate → Review (Semantic: Recall@10 = 0.78)
5. Repeat until satisfied
```

---

## Tips & Best Practices

### 🎯 Choosing Strategies

- **Start with Baseline**: Fastest to run, good baseline performance
- **Add Semantic**: If document meaning is critical
- **Add Structured**: If documents have clear sections/headings
- **Try Parent-Child**: For long documents (>2000 tokens)

### 📊 Evaluation Design

- **Use 50+ queries**: Statistically significant results
- **Mix query styles**: Combine specific, broad, and contextual
- **Include edge cases**: Test unusual queries and corner cases
- **Use ground truth when available**: More accurate than LLM judges

### 🚀 Performance Optimization

- **Limit documents during testing**: Use `max_rows` to iterate faster
- **Use Databricks Serverless**: Auto-scaling, no cluster management
- **Monitor MLflow**: Track parameter impact systematically
- **Compare incrementally**: Change one variable at a time

### 🔒 Data Management

- **Use meaningful project names**: Include version or iteration number
- **Document your changes**: Use project descriptions
- **Archive old projects**: Delete unused projects to save storage
- **Export results regularly**: Download CSV from Review page

---

## Troubleshooting

### Build fails with "No documents loaded"

**Cause:** Data source configuration issue

**Fix:**
- For UC Volumes: verify the volume path exists and contains files matching the pattern
- For Delta tables: verify the table exists and has data
- Check column names match configuration
- Ensure the service principal has read permissions
- Check the Databricks job logs for detailed `[DEBUG]` output showing file listing results

### Evaluation shows 0 metrics

**Cause:** MLflow experiment_id mismatch or no runs found

**Fix:**
- Deploy updated code (stores experiment_id in builds table)
- Wipe database and create fresh builds (see DEPLOYMENT_GUIDE.md)
- Check MLflow experiment URL in Review page
- Verify build completed successfully before running evaluation

### Vector Search index not found

**Cause:** Index creation failed or endpoint doesn't exist

**Fix:**
- Verify Vector Search endpoint exists
- Check Unity Catalog permissions
- Review build job logs in Databricks
- Ensure embedding model endpoint is accessible

### "Deadline exceeded" when creating index

**Cause:** Lakebase SQL Editor timeout

**Fix:**
- Use `CREATE INDEX CONCURRENTLY` instead of regular `CREATE INDEX`
- Or skip the index - queries will work (slightly slower)
- See DEPLOYMENT_GUIDE.md for details

### Build shows SUCCESS but actually failed

**Cause:** The app now inspects the notebook's exit value, but older deployments may not

**Fix:**
- Redeploy with latest code (build status endpoint inspects notebook output)
- Check the Databricks job logs for the actual exit message
- PARTIAL_SUCCESS with all combos failed is mapped to FAILED

### Frontend not loading

**Cause:** Frontend bundle not rebuilt after code changes

**Fix:**
- Run `cd frontend && npm run build` to create fresh bundle
- Redeploy to Databricks Apps
- Hard-refresh browser (Cmd+Shift+R) to clear cached JS

---

## Data Sources Supported

### ✅ Text
- Paste text directly into the UI
- Support for multiple text entries with names
- Best for: Quick testing, small documents

### ✅ Delta Table
- Query existing Delta tables
- Specify text column and optional ID column
- Best for: Production data, large corpora

### ✅ CSV
- Upload CSV files
- Auto-detects text columns
- Best for: Exported data, spreadsheets

### ✅ JSON
- Upload JSON files
- Specify text field path
- Best for: Structured data, API responses

### ✅ PDF
- Upload PDF documents
- Text extraction via PyMuPDF
- OCR support (coming soon)
- Best for: Documents, reports, manuals

### DOCX / Word Documents
- Upload Word documents
- Text extraction via python-docx (paragraphs, headings, tables)
- Best for: Institutional documents, reports, SOPs

### UC Volume
- Read files from Unity Catalog Volumes
- Glob pattern support (`*.txt`, `*.pdf`, `*.docx`)
- Binary files downloaded via Databricks SDK Files API (serverless-safe)
- Best for: Large file collections, enterprise data

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                    Frontend (React + TypeScript)                 │
│                                                                   │
│  Projects → Build → Evaluate → Review → Project Details          │
└───────────────────────────┬─────────────────────────────────────┘
                            │ REST API
┌───────────────────────────▼─────────────────────────────────────┐
│                     Backend (FastAPI + Python)                   │
│                                                                   │
│  API: /projects /builds /evaluations /cleanup /studies /uploads   │
└─────┬──────────┬──────────┬──────────┬──────────────────────────┘
      │          │          │          │
      ▼          ▼          ▼          ▼
┌──────────┐ ┌────────┐ ┌──────┐ ┌──────────┐
│Lakebase  │ │Databricks│Delta │ │  MLflow  │
│PostgreSQL│ │   Jobs │ │ Lake │ │Experiments│
└──────────┘ └────────┘ └──────┘ └──────────┘
 (Projects,  (Serverless (Chunks, (Metrics,
  Builds,     Notebooks)  Indexes)  Tracking)
  Evals)
```

**Tech Stack:**
- **Frontend**: React 18, TypeScript, Vite, TailwindCSS, Plotly.js
- **Backend**: FastAPI, Pydantic, Databricks SDK
- **Data**: Lakebase PostgreSQL, Delta Lake, Vector Search
- **Tracking**: MLflow
- **Compute**: Databricks Serverless (no cluster management)
- **File Processing**: PyMuPDF (PDF), python-docx (Word)

---

## Support & Documentation

- **Technical Implementation**: See [IMPLEMENTATION.md](./IMPLEMENTATION.md)
- **Deployment Guide**: See [DEPLOYMENT_GUIDE.md](./DEPLOYMENT_GUIDE.md)
- **API Reference**: See [API.md](./API.md) (if available)
- **Report Issues**: [GitHub Issues](https://github.com/your-org/retrieval-studio/issues)

---

## License

MIT License - See LICENSE file for details

---

## What's Next?

After mastering Retrieval Studio, you can:

1. **Export winning strategies** to production RAG pipelines
2. **Monitor production metrics** using the same evaluation framework
3. **Iterate continuously** as your data evolves
4. **Share results** with your team via MLflow
5. **Build confidence** in your retrieval system before deploying

**Happy optimizing! 🚀**
