# Retrieval Studio - User Guide

Welcome to Retrieval Studio! This guide will help you optimize your RAG (Retrieval-Augmented Generation) pipeline by testing different chunking strategies and measuring retrieval quality.

## What is Retrieval Studio?

Retrieval Studio helps you answer the question: **"Which chunking strategy gives me the best retrieval quality?"**

Instead of manually testing different approaches, Retrieval Studio:
- Automatically chunks your documents using multiple strategies
- Generates evaluation queries (or uses your own)
- Measures retrieval quality with industry-standard metrics
- Compares strategies side-by-side

## Getting Started

### Step 1: Create a Project

A **Project** organizes all your experiments for a specific use case.

1. Navigate to the **Projects** page
2. Click **"New Project"**
3. Enter:
   - **Project Name**: E.g., "customer_support_docs"
   - **Description** (optional): E.g., "Optimizing retrieval for customer support FAQs"
4. Click **"Create Project"**

Your project is now ready!

---

## The Workflow

### Step 2: Run a Build

A **Build** prepares your documents by applying different chunking strategies and creating searchable indexes.

#### 2.1 Navigate to Build Page

1. Click on your project name or **"View Details"**
2. Click **"Create New Build"**

#### 2.2 Configure Your Build

**Select Data Source:**

Choose where your documents are stored:

- **Delta Table**: Most common - your documents in a Unity Catalog table
  - Enter: `catalog.schema.table_name`
  - Must have a column with text content (e.g., `content`, `text`, `document`)
  - Example: `my_catalog.raw.support_articles`

- **CSV File**: Upload a CSV with text data
- **Excel File**: Upload an Excel file with text data

**Select Data Configuration:**

- **Text Column**: Which column contains the document text? (e.g., `content`)
- **ID Column**: Which column uniquely identifies documents? (e.g., `doc_id`)
- **Max Rows** (optional): Limit number of documents for testing (e.g., 1000)

**Select Chunking Strategies:**

Choose which strategies to test (you can select multiple):

1. **Baseline**: Fixed-size chunks (512 characters)
   - Best for: General-purpose, quick testing
   - Pros: Fast, simple, consistent chunk sizes
   - Cons: May split sentences/paragraphs awkwardly

2. **Semantic**: Smart chunking based on meaning
   - Best for: Maintaining context, natural language
   - Pros: Preserves sentence boundaries, maintains coherence
   - Cons: Variable chunk sizes

3. **Structured**: Section-aware chunking
   - Best for: Documents with clear structure (headings, sections)
   - Pros: Preserves document hierarchy
   - Cons: Requires structured documents

**Configure Endpoints:**

- **Embedding Model**: Select your embedding model endpoint
  - Default: `databricks-bge-large-en`
  - This converts text to vectors for similarity search

- **Vector Search Endpoint**: Select your Vector Search endpoint
  - Default: `vs-default`
  - This enables fast similarity search

#### 2.3 Submit Build

1. Review your configuration
2. Click **"Submit Build Job"**
3. Wait for the build to complete (usually 2-10 minutes depending on data size)

**What's happening during the build?**
- Documents are loaded from your source
- Each selected strategy processes the documents into chunks
- Chunks are embedded using the embedding model
- Vector Search indexes are created for fast retrieval
- Everything is stored in Unity Catalog

#### 2.4 Monitor Progress

- The page shows real-time status updates
- View the Databricks job URL to see detailed logs
- When status shows **"SUCCESS"**, you're ready to evaluate!

---

### Step 3: Run an Evaluation

An **Evaluation** tests how well each chunking strategy retrieves relevant documents.

#### 3.1 Navigate to Evaluate Page

1. After build completes, click **"Evaluate This Build"**
2. Or go to **Evaluate** page and select your build run

#### 3.2 Select Build to Evaluate

- Choose the build run you want to evaluate
- The system shows the build ID and strategies included

#### 3.3 Choose Evaluation Mode

You have **TWO options**:

---

**Option 1: Use Existing Queries Dataset**

Use this if you already have evaluation queries with expected results.

1. Select **"Use Existing Queries Dataset"**
2. Choose **Dataset Type**:
   - Delta Table (most common)
   - CSV File
   - Excel File

3. Enter **Dataset Path**:
   - For Delta Table: `catalog.schema.queries_table`
   - For files: `/path/to/queries.csv`

**Required columns in your dataset:**
- `query_text` (required): The search query
- `expected_chunks` (optional): List of chunk IDs that should be retrieved

**Example queries table:**
```
| query_text                          | expected_chunks           |
|-------------------------------------|---------------------------|
| "How do I reset my password?"       | ["chunk_123", "chunk_456"]|
| "What are your business hours?"     | ["chunk_789"]             |
```

---

**Option 2: Auto-Generate Queries** (Recommended for quick testing)

Let the system generate evaluation queries automatically from your documents.

1. Select **"Auto-Generate Queries"**
2. Configure:
   - **Number of Queries**: How many queries to generate (default: 50)
   - **Query Style**:
     - **Keyword**: Short 2-5 word queries (e.g., "password reset")
     - **Natural**: Full questions (e.g., "How do I reset my password?")
     - **Mixed**: Combination of both

**How auto-generation works:**
- The system samples chunks from your corpus
- An LLM generates relevant queries that would retrieve each chunk
- Queries are diverse and representative of real search patterns

**No manual dataset needed!** The system automatically uses chunks from your build.

---

#### 3.4 Configure Advanced Settings

**Top K**: Number of results to retrieve per query (default: 10)
- Higher K = more results, better recall but may include irrelevant items
- Typical values: 5, 10, 20

**Judge Model Endpoint** (optional):
- If you DON'T have ground truth labels, specify an LLM judge endpoint
- The LLM will score relevance of retrieved chunks (0-3 scale)
- Example: `databricks-claude-sonnet-4-5`

**Compare Query Types** (optional):
- Test different Vector Search modes:
  - **ANN** (Approximate Nearest Neighbor): Pure vector similarity
  - **FULL_TEXT**: Keyword-based search
  - **HYBRID**: Combination of both
- Useful for understanding which search type works best

#### 3.5 Submit Evaluation

1. Review your configuration
2. Click **"Submit Evaluation Job"**
3. Wait for evaluation to complete (usually 5-15 minutes)

**What's happening during evaluation?**
- Queries are generated or loaded
- Each query is run against every strategy's index
- Top-K chunks are retrieved for each query
- Metrics are calculated (recall, precision, NDCG)
- Results are logged to MLflow for analysis

---

### Step 4: Review Results

After evaluation completes, review your results to determine the best strategy.

#### 4.1 View MLflow Experiment

1. Go to **Review** page
2. Click **"Open MLflow Experiment"**
3. See all runs with detailed metrics

**Key Metrics to Look At:**

- **Recall@10**: What % of relevant documents did we find?
  - Higher is better (closer to 1.0)
  - Example: 0.85 = found 85% of relevant documents

- **NDCG@10**: How good is the ranking?
  - Higher is better (closer to 1.0)
  - Considers position: relevant items ranked higher score better

- **Precision@10**: What % of returned results are relevant?
  - Higher is better (closer to 1.0)
  - Example: 0.70 = 7 out of 10 results are relevant

- **Latency (ms)**: How fast is retrieval?
  - Lower is better
  - Important for user experience

#### 4.2 Compare Strategies

In the Review page, you'll see runs grouped by strategy:

```
Strategy         Recall@10    NDCG@10    Latency
─────────────────────────────────────────────────
Semantic         0.87         0.78       45ms
Baseline         0.82         0.71       38ms
Structured       0.85         0.75       52ms
```

**Choose the best strategy based on your priorities:**
- Need highest quality? → Highest Recall/NDCG
- Need fastest response? → Lowest Latency
- Need balance? → Consider all metrics

#### 4.3 View Detailed Results

Click on any MLflow run to see:
- All parameters used (chunk_size, strategy, top_k)
- Per-query results
- Visualization of metrics over time
- Artifacts and logs

---

## Managing Projects

### View Project Details

1. Go to **Projects** page
2. Click on any project name
3. See:
   - All build runs for this project
   - Evaluation history for each build
   - Build configurations and status

### Delete Projects

**From Projects List:**
1. Click **"Delete"** button in the Actions column
2. Confirm deletion in the modal

**From Project Details:**
1. Open the project
2. Click **"Delete Project"** (red button at top)
3. Confirm deletion

**What gets deleted:**
- Project metadata
- All build records
- All evaluation records

**What does NOT get deleted:**
- Delta tables with chunks
- Vector Search indexes
- MLflow experiment runs

You'll need to manually clean up these resources in Databricks if desired.

---

## Best Practices

### 1. Start Small

For your first build:
- Use **Max Rows: 100-1000** to test quickly
- Select **one or two strategies** to compare
- Use **Auto-Generate Queries** with 20-50 queries

### 2. Iterate Gradually

Once you understand the workflow:
- Increase document count gradually
- Test all three strategies
- Generate more queries (100-200) for robust results

### 3. Understand Your Use Case

Choose strategies based on your document type:

**Technical Documentation:**
- Use **Structured** strategy
- Preserves section hierarchy
- Maintains code blocks and examples

**Customer Support FAQs:**
- Use **Semantic** strategy
- Maintains natural language flow
- Preserves question-answer pairs

**General Articles/Blogs:**
- Start with **Baseline** for quick results
- Compare with **Semantic** for quality

### 4. Monitor Resource Usage

- Builds create Delta tables and indexes (storage cost)
- Evaluations run queries (compute cost)
- Delete old projects when done experimenting

### 5. Use Meaningful Project Names

Bad: "test1", "experiment_v2"
Good: "customer_support_v1", "technical_docs_prod"

---

## Common Workflows

### Workflow 1: Quick Test

**Goal**: Test if retrieval works at all

1. Create project
2. Run build with:
   - Max rows: 100
   - Strategy: Baseline only
3. Auto-generate 20 queries (keyword style)
4. Review recall@10

**Time**: ~5 minutes

---

### Workflow 2: Compare Strategies

**Goal**: Find best chunking strategy

1. Create project
2. Run build with:
   - Max rows: 1000
   - Strategies: All three (Baseline, Semantic, Structured)
3. Auto-generate 100 queries (mixed style)
4. Compare metrics in MLflow
5. Pick winner

**Time**: ~15 minutes

---

### Workflow 3: Production Optimization

**Goal**: Optimize for production deployment

1. Create project with descriptive name
2. Run build with:
   - Full dataset (no max rows)
   - All strategies
3. Use existing queries dataset (real user queries)
4. Run evaluation with compare_query_types=true
5. Analyze results across all dimensions:
   - Strategy (chunking approach)
   - Query type (ANN vs FULL_TEXT vs HYBRID)
   - Top-K value
6. Choose best combination
7. Deploy winning strategy to production

**Time**: ~30-60 minutes (depending on data size)

---

## Troubleshooting

### Build Fails

**Check:**
- Does your Delta table exist? Run `SELECT * FROM catalog.schema.table LIMIT 10`
- Does the text column contain data?
- Is the embedding model endpoint running?
- Is the Vector Search endpoint available?

**View logs:**
- Click the Databricks job URL from the build page
- Check notebook output for error messages

### Evaluation Fails

**Check:**
- Did the build complete successfully?
- If using existing queries: Does the queries table exist?
- If using LLM judge: Is the endpoint specified and running?

### No Results in Review Page

**Check:**
- Did the evaluation job complete (status = SUCCESS)?
- Click "Open MLflow Experiment" to see if runs are logged
- Refresh the page

### Low Recall Scores

This might be normal! Consider:
- How many relevant documents exist per query?
- Is top_k large enough?
- Try different strategies
- Check if your embedding model is appropriate for your domain

---

## Tips for Success

1. **Understand Your Metrics**
   - Don't just optimize for one metric
   - Balance recall, precision, and latency
   - Consider your specific use case requirements

2. **Test with Real Queries**
   - Auto-generated queries are great for testing
   - But evaluate with real user queries before production

3. **Document Your Experiments**
   - Use project descriptions to note what you're testing
   - Keep track of which strategies work best

4. **Iterate Based on Results**
   - If recall is low, try different strategies
   - If latency is high, consider smaller chunks
   - If precision is low, try different top_k values

5. **Clean Up Regularly**
   - Delete old projects you're done with
   - Keep only production-ready configurations

---

## Getting Help

- Check the MLflow experiment for detailed run information
- View Databricks job logs for error details
- Review your Delta table schema to ensure compatibility

---

## Appendix: Understanding Metrics

### Recall@K
**What it measures**: Did we find the relevant documents?

**Formula**: (# relevant docs in top-K) / (total # relevant docs)

**Example**:
- Query: "How to reset password?"
- Relevant docs: 5 total
- Top-10 results contain: 4 of those 5
- Recall@10 = 4/5 = 0.80

**When to prioritize**: When you must find all relevant information (legal, medical, safety)

---

### Precision@K
**What it measures**: Are the results we returned relevant?

**Formula**: (# relevant docs in top-K) / K

**Example**:
- Query: "How to reset password?"
- Top-10 results: 7 are relevant, 3 are not
- Precision@10 = 7/10 = 0.70

**When to prioritize**: When you want clean results (user experience, reducing noise)

---

### NDCG@K (Normalized Discounted Cumulative Gain)
**What it measures**: Are relevant docs ranked at the top?

**Key insight**: Finding relevant docs is good, ranking them higher is better

**Example**:
- Query returns 3 relevant docs
- Ranked at positions: 1, 2, 3 → High NDCG (best case)
- Ranked at positions: 8, 9, 10 → Low NDCG (found them but ranked poorly)

**When to prioritize**: When ranking matters (users only look at top results)

---

### Latency
**What it measures**: How long does retrieval take?

**Typical values**:
- < 50ms: Excellent
- 50-100ms: Good
- 100-200ms: Acceptable
- \> 200ms: May impact user experience

**When to prioritize**: Real-time applications, user-facing search

---

## Quick Reference

### Recommended Settings

**Small Dataset (< 1K docs):**
```
Build:
- Max Rows: All
- Strategies: Baseline + Semantic
- Top K: 10

Evaluate:
- Mode: Auto-generate
- Queries: 50
- Style: Mixed
```

**Medium Dataset (1K - 100K docs):**
```
Build:
- Max Rows: 10,000 for testing, then full
- Strategies: All three
- Top K: 10

Evaluate:
- Mode: Auto-generate
- Queries: 100-200
- Style: Natural
```

**Large Dataset (> 100K docs):**
```
Build:
- Start with 10K sample
- Strategies: Test all, then pick 1-2
- Top K: 20

Evaluate:
- Mode: Use existing queries (real user data)
- Queries: 500+
- Enable compare_query_types
```

---

**Ready to optimize your RAG pipeline? Start by creating your first project!**
