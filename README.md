# Retrieval Studio

A comprehensive Databricks application for systematically evaluating and optimizing Retrieval-Augmented Generation (RAG) pipelines.

## What is Retrieval Studio?

Retrieval Studio provides a complete **inner loop** for improving retrieval quality in RAG systems. Instead of guessing which chunking strategy works best, you can:

- 📊 **Experiment** with different document chunking strategies (baseline, semantic, structured, parent-child)
- 🤖 **Auto-generate** synthetic evaluation queries from your documents
- 📈 **Measure** retrieval quality with industry-standard metrics (Recall@K, NDCG@K, Precision@K)
- 🏆 **Compare** strategies side-by-side and optimize for your specific use case
- 🔍 **Track** all experiments in MLflow with full reproducibility

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
-- This creates: projects, builds, evaluations, job_runs tables
```

### 2. Configure Environment Variables

Create `.env` file in the backend directory:

```bash
# Databricks Configuration
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=dapi...

# Unity Catalog
CATALOG=retrievalstudio
SCHEMA=raw

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
- `notebooks/build_notebook_v2.py` → Build pipeline
- `notebooks/eval_notebook.py` → Evaluation pipeline

### 4. Deploy the Application

**Option A: Deploy to Databricks Apps (Recommended)**
```bash
# Deploy both frontend and backend to Databricks Apps
databricks apps deploy
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

### Step 2: Build Your Index

The **Build** page is where you prepare your data and create searchable indexes.

#### Choose Your Data Source

**Supported Data Types:**
- 📝 **Text** - Paste text directly (supports multiple entries)
- 📊 **Delta Table** - Query existing Delta tables
- 📄 **CSV** - Upload CSV files
- 📋 **JSON** - Upload JSON files
- 📕 **PDF** - Upload PDF documents (text extraction via PyMuPDF)
- 🗄️ **UC Volume** - Read files from Unity Catalog Volumes

#### Configure Data Source

**Example: Delta Table**
```
Table Name: main.prod.customer_docs
Text Column: content
ID Column: doc_id (optional)
Max Rows: 2000
```

**Example: Multiple Text Entries**
```
Entry 1:
  Document Name: "Product Overview"
  Text Content: "Our platform provides..."

Entry 2:
  Document Name: "Getting Started Guide"
  Text Content: "To begin using..."
```

#### Select Chunking Strategies

Choose one or more strategies to compare:

**🔹 Baseline** - Fixed-size chunks with overlap
- Best for: General-purpose retrieval
- Parameters:
  - `chunk_size`: Characters per chunk (default: 512)
  - `overlap`: Character overlap between chunks (default: 50)

**🔹 Semantic** - Sentence-boundary aware chunking
- Best for: Preserving meaning and context
- Parameters:
  - `window_size`: Sentences per chunk (default: 3)
  - `min_chunk_size`: Minimum characters (default: 256)

**🔹 Structured** - Section-aware chunking
- Best for: Documents with clear structure (headings, sections)
- Parameters:
  - `preserve_hierarchy`: Keep document structure (default: true)
  - `max_chunk_size`: Maximum chunk size (default: 1024)

**🔹 Parent-Child** - Two-level hierarchy
- Best for: Large documents requiring context
- Parameters:
  - `parent_chunk_size`: Size of parent chunks (default: 2048)
  - `child_chunk_size`: Size of child chunks (default: 512)
  - `overlap`: Overlap between chunks (default: 50)

#### Submit Build

1. Review your configuration
2. Click **"Submit Build"**
3. Monitor progress in real-time
4. Job URL links to Databricks job run

**What happens during a build:**
- Documents are loaded and preprocessed
- Each selected strategy chunks the documents
- Chunks are written to Delta tables (`{catalog}.chunks.rs_chunks_{project}_{strategy}`)
- Vector Search indexes are created/updated automatically
- Results are stored in PostgreSQL with experiment_id for MLflow tracking

---

### Step 3: Evaluate Retrieval Quality

The **Evaluate** page runs systematic tests on your built indexes.

#### Select a Build

Choose a completed build from the dropdown. You'll see all strategies that were indexed.

#### Configure Evaluation

**Evaluation Dataset Options:**

**Option A: Auto-Generate Queries (Recommended)**
- ✅ No manual query creation needed
- System generates synthetic queries from your documents
- Parameters:
  - `Number of Queries`: How many to generate (e.g., 50)
  - `Query Style`: `specific`, `broad`, or `contextual`
  - `Compare Query Types`: Test FULL_TEXT, ANN, and HYBRID search

**Option B: Use Existing Queries**
- Provide a Delta table with pre-written queries
- Required columns:
  - `query_id`: Unique identifier
  - `query_text`: The query string
  - `doc_id`: (Optional) Ground truth document ID

**Evaluation Settings:**
- `Top-K`: How many results to retrieve (default: 10)
- `Judge Model`: LLM endpoint for relevance scoring (if no ground truth)

#### Submit Evaluation

1. Click **"Submit Evaluation"**
2. Monitor job progress
3. Results logged to MLflow automatically

**What happens during evaluation:**
- Queries are generated or loaded
- Each query is run against all strategies
- Top-K results are retrieved from Vector Search
- Metrics are calculated (Recall@K, NDCG@K, Precision@K)
- LLM judge scores relevance if no ground truth available
- All metrics logged to MLflow with tags (`rs_role=eval_strategy`)

---

### Step 4: Review & Compare Results

The **Review** page provides comprehensive analysis and comparison.

#### Select Builds and Evaluations

1. **Select Builds**: Choose one or more builds to compare
2. **Select Evaluations**: Pick specific evaluation runs
3. Click **"Review Selected"**

#### View Results

**🏆 Best Performers**
- **Best Build**: Highest average recall across all strategies
- **Best Strategy**: Top-performing chunking approach
- **Fastest**: Lowest average query latency
- **Best Overall**: Balanced score across metrics

**📊 Metrics Bar Charts**
- **By Build**: Compare builds across all metrics
- **By Strategy**: Compare chunking strategies
- **By Evaluation**: See performance trends over time

**📋 Comparison Table**
- Side-by-side metrics for all evaluations
- Sortable by any metric
- Export to CSV for further analysis

**🔗 MLflow Integration**
- Direct link to MLflow experiment
- View all runs, parameters, and metrics
- Full experiment reproducibility

---

### Step 5: Project Details & History

The **Project Details** page shows comprehensive project history.

**What you'll see:**
- 📈 **Evaluation History**: All past evaluations with metrics
- 🔨 **Build History**: All builds with status and configurations
- 📊 **Metrics Over Time**: Track improvement across iterations
- 🔗 **MLflow Experiment Link**: Access detailed tracking

**Actions:**
- Re-run evaluations on existing builds
- Compare historical performance
- Download metrics for reporting

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

### Workflow 1: Find the Best Chunking Strategy

```
1. Create Project
2. Build with ALL strategies (baseline, semantic, structured, parent-child)
3. Evaluate with auto-generated queries (50+ queries)
4. Review → Check "Best Strategy"
5. Use winning strategy for production
```

### Workflow 2: Optimize a Single Strategy

```
1. Create Project
2. Build with baseline (chunk_size=512, overlap=50)
3. Evaluate
4. Build with baseline (chunk_size=1024, overlap=100)
5. Evaluate
6. Review → Compare parameter impact
```

### Workflow 3: Test Different Data Sources

```
1. Create Project "Customer Docs - Delta"
2. Build using Delta table
3. Evaluate

4. Create Project "Customer Docs - PDFs"
5. Build using uploaded PDFs
6. Evaluate

7. Compare projects in MLflow
```

### Workflow 4: Iterate and Improve

```
1. Build → Evaluate → Review (Baseline: Recall@10 = 0.65)
2. Analyze poor queries in MLflow
3. Adjust data preprocessing
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
- Verify Delta table exists and has data
- Check column names match configuration
- Ensure you have permissions to read the table
- Try with a smaller dataset first (`max_rows=100`)

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

### Frontend not loading

**Cause:** Backend not serving static files or CORS issue

**Fix:**
- Ensure `npm run build` was run in frontend/
- Check backend is running on port 8000
- Verify `backend/main.py` has `mount_spa()` call
- Check browser console for errors

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

### ✅ UC Volume
- Read files from Unity Catalog Volumes
- Glob pattern support (`*.txt`, `**/*.pdf`)
- Recursive directory traversal
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
│  API Routes → Job Submission → Status Polling                    │
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
- **Frontend**: React 18, TypeScript, Vite, TailwindCSS
- **Backend**: FastAPI, Pydantic, Databricks SDK
- **Data**: Lakebase PostgreSQL, Delta Lake, Vector Search
- **Tracking**: MLflow
- **Compute**: Databricks Serverless (no cluster management)

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
