# Retrieval Studio

A Databricks-native tool for systematically building, evaluating, and optimizing RAG (Retrieval-Augmented Generation) pipelines — without the guesswork.

---

## The Problem

Building a RAG pipeline is easy. Building a *good* one is hard.

Most teams pick a chunking strategy, run a quick vibe-check, and ship — only to find their LLM is hallucinating because the retrieval layer is returning the wrong chunks. Tuning RAG means juggling data sources, chunking strategies, embedding models, and vector indexes with no clear way to measure what's actually working.

**Retrieval Studio** gives you a structured lab to experiment with and measure retrieval quality — so you can make decisions based on data, not intuition.

---

## What It Does

Retrieval Studio helps you:

- **Build** chunk indexes from your data using different chunking strategies
- **Evaluate** retrieval quality with standard metrics (Recall@K, NDCG@K, Precision@K)
- **Compare** strategies side-by-side to find what works best for your data
- **Manage** your Vector Search indexes and Delta tables as experiments evolve

Everything runs natively on Databricks — your data never leaves your lakehouse.

---

## Key Features

### Projects
Organize your work into projects. Each project ties to a Unity Catalog schema, a Vector Search endpoint, and an embedding model endpoint.

### Build
Configure data sources and chunking strategies, then submit a Databricks job to build your indexes.

- Supports multiple data sources per build (UC Volumes, Delta tables, file uploads)
- Choose from multiple chunking strategies per source: **baseline**, **semantic**, **structured**, **parent-child**, **sentence**, and **paragraph**
- Each source × strategy combination generates its own Delta chunk table and Vector Search index
- All runs are tracked in MLflow

### Evaluate
Measure how well your indexes retrieve the right content.

- **Auto-generate** synthetic queries from your chunk tables
- **Bring your own** golden dataset (a Delta table with `query_text` and `expected_chunks`)
- Compare ANN, HYBRID, and FULL_TEXT search modes
- Configure top-K, judge model, and query style
- Results are written to Delta and tracked in MLflow

### Review
Analyze results and find your best-performing strategy.

- See aggregated metrics across all strategies
- Compare strategies per data source with LLM-generated explanations
- Drill into individual queries to see expected vs. retrieved chunks
- Visual charts and comparison tables

### Resource Management
Keep your workspace clean as you iterate.

- Mark indexes as **keep** or **discard** after reviewing results
- Run cleanup jobs to delete discarded indexes and tables
- Group related builds and evaluations into **studies**

---

## How to Use It

### 1. Create a Project
Go to **Projects** and create a new project. You'll need:
- A Unity Catalog catalog and schema
- A Vector Search endpoint
- An embedding model endpoint

### 2. Build an Index
Go to **Build** and follow the wizard:
1. Add your data sources (UC Volume path, Delta table, or file upload)
2. Assign one or more chunking strategies to each source
3. Configure your embedding endpoint
4. Review the build matrix and submit

Databricks runs the job in the background. You can track progress from the project detail page.

### 3. Run an Evaluation
Once a build completes, go to **Evaluate**:
1. Select the build to evaluate
2. Choose auto-generated queries or upload a golden set
3. Set your top-K, judge model, and search mode
4. Submit and wait for results

### 4. Review Results
Go to **Review** to compare builds and evaluations:
- See which strategy performed best overall
- Compare strategies for each data source
- Inspect individual query results to understand failures

### 5. Iterate
Based on what you find, go back to **Build** with a refined strategy — this time with data to back your decisions.

---

## Requirements

- A Databricks workspace with Unity Catalog enabled
- A Vector Search endpoint
- An embedding model endpoint (e.g., a model served via Databricks Model Serving)
- Lakebase PostgreSQL for app state (configured via environment variables)

---

## Deployment

Retrieval Studio is deployed as a **Databricks App** using `app.yaml`. The backend is a FastAPI service; the frontend is a React app built with Vite.

```bash
# Install dependencies
npm install        # installs frontend deps
pip install -r backend/requirements.txt

# Build frontend
npm run build

# Run locally (set required env vars first)
uvicorn backend.main:app --port 8000
```

Required environment variables are documented in `app.yaml`.
