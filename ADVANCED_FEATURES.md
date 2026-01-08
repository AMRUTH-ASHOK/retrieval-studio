# Advanced Evaluation Features

This document describes the advanced evaluation features added to retrieval-studio, inspired by the autoeval library.

## Table of Contents

1. [LLM-based Relevance Scoring](#llm-based-relevance-scoring)
2. [Rich Analytics](#rich-analytics)
3. [Automated Query Generation](#automated-query-generation)
4. [Query Type Comparison](#query-type-comparison)
5. [Usage Examples](#usage-examples)

---

## 1. LLM-based Relevance Scoring

### Overview

Evaluate retrieval quality **without ground truth labels** using an LLM judge to score relevance on a 0-3 scale.

### How It Works

The evaluator sends each query-chunk pair to an LLM and asks it to rate relevance:
- **3 = Highly Relevant**: Directly and completely answers the query
- **2 = Relevant**: Contains useful information that partially answers
- **1 = Marginally Relevant**: Mentions related concepts but doesn't answer
- **0 = Not Relevant**: No useful information for the query

### Configuration

```python
from retrieval_core.evaluator import RetrievalEvaluator

# Initialize with LLM judge endpoint
evaluator = RetrievalEvaluator(
    judge_model_endpoint="databricks-meta-llama-3-1-70b-instruct"
)

# Compute metrics without ground truth
metrics = evaluator.compute_judge_metrics(
    query_text="what is vector search?",
    retrieved_chunks=retrieved_results,
    k_values=[5, 10]
)
```

### Output Metrics

```python
{
    "judge_score_at_5": 2.4,
    "avg_relevance_at_5": 2.4,
    "relevance_0_count_at_5": 0,
    "relevance_1_count_at_5": 1,
    "relevance_2_count_at_5": 2,
    "relevance_3_count_at_5": 2,
    "judge_score_at_10": 2.1,
    "avg_relevance_at_10": 2.1,
    "relevance_0_count_at_10": 1,
    ...
}
```

### Fallback Mechanism

If the LLM API fails, the evaluator falls back to keyword overlap heuristics.

---

## 2. Rich Analytics

### Overview

The `EvaluationAnalyzer` class provides deep insights into evaluation results.

### Features

#### Summary Statistics
```python
from retrieval_core.analyzer import EvaluationAnalyzer

analyzer = EvaluationAnalyzer(results_df)
print(analyzer.summary())
```

Output:
```
============================================================
EVALUATION SUMMARY
============================================================

Total Queries: 200

Average Metrics:
  avg_relevance_at_10: 2.3456
  ndcg_at_10: 0.8234
  recall_at_10: 0.7500
  avg_latency_ms: 145.23 ms
```

#### Score Distribution
```python
dist = analyzer.score_distribution()
display(dist)
```

| score | count | percentage |
|-------|-------|------------|
| 0     | 5     | 2.5        |
| 1     | 20    | 10.0       |
| 2     | 75    | 37.5       |
| 3     | 100   | 50.0       |

#### Top/Bottom Performing Queries
```python
# Best performing queries
top = analyzer.top_queries(n=5)

# Worst performing queries
bottom = analyzer.bottom_queries(n=5)
```

#### High/Low Relevance Examples
```python
# Examples of highly relevant results (score >= 2.5)
high = analyzer.high_relevance_examples(n=5, min_score=2.5)

# Examples of low relevance results (score <= 1.0)
low = analyzer.low_relevance_examples(n=5, max_score=1.0)
```

#### Compare Multiple Runs
```python
from retrieval_core.analyzer import compare_evaluations

results_dict = {
    "strategy_A": results_df_a,
    "strategy_B": results_df_b,
    "strategy_C": results_df_c,
}

comparison = compare_evaluations(results_dict)
display(comparison)
```

Output:
| name | num_queries | avg_relevance_at_10 | recall_at_10 | avg_latency_ms |
|------|-------------|---------------------|--------------|----------------|
| strategy_C | 200 | 2.45 | 0.85 | 120.5 |
| strategy_A | 200 | 2.30 | 0.78 | 135.2 |
| strategy_B | 200 | 2.10 | 0.72 | 145.8 |

---

## 3. Automated Query Generation

### Overview

Generate synthetic evaluation queries directly from your document corpus using an LLM.

### Benefits
- **No manual dataset creation** required
- **Realistic queries** that match your domain
- **Customizable styles**: keyword, natural language, or mixed

### Usage

#### Basic Example
```python
from retrieval_core.query_generator import QueryGenerator

generator = QueryGenerator(random_seed=42)

# Generate 200 keyword-style queries
queries_df = generator.generate_queries(
    corpus_table="my_catalog.my_schema.documents",
    columns=["text"],
    num_queries=200,
    style="keyword"  # or "natural" or "mixed"
)
```

#### With Few-Shot Examples

Guide the model by providing examples:

```python
generator.set_few_shot_examples([
    {
        "document": "Vector search uses embeddings to find semantically similar content...",
        "query": "how does vector search work"
    },
    {
        "document": "Delta Lake provides ACID transactions on data lakes...",
        "query": "delta lake transactions"
    }
])

queries_df = generator.generate_queries(
    corpus_table="my_catalog.my_schema.documents",
    columns=["text"],
    num_queries=200,
    style="keyword"
)
```

#### Preview Generated Queries

Sample a few documents and see what queries are generated:

```python
samples_df = generator.sample_and_generate_examples(
    corpus_table="my_catalog.my_schema.documents",
    columns=["text"],
    num_samples=10
)

display(samples_df)
```

### Query Styles

1. **Keyword** (2-5 words):
   - `"vector search indexing"`
   - `"databricks clusters cost"`
   - `"python list comprehension"`

2. **Natural** (full questions):
   - `"How does vector search indexing work?"`
   - `"What is the cost of Databricks clusters?"`
   - `"How do I use list comprehension in Python?"`

3. **Mixed** (combination of both):
   - Alternates between keyword and natural styles

---

## 4. Query Type Comparison

### Overview

Test and compare different search methods on the same index:
- **FULL_TEXT**: Keyword-based BM25 search
- **ANN**: Semantic vector search (Approximate Nearest Neighbor)
- **HYBRID**: Combination of both

### Usage

```python
from utils.vs_utils import query_index

# Query with different types
full_text_results = query_index(
    vs_client=vs_client,
    endpoint_name="my_endpoint",
    index_name="my_catalog.schema.index",
    query_text="what is vector search?",
    k=10,
    query_type="FULL_TEXT"
)

ann_results = query_index(
    vs_client=vs_client,
    endpoint_name="my_endpoint",
    index_name="my_catalog.schema.index",
    query_text="what is vector search?",
    k=10,
    query_type="ANN"
)

hybrid_results = query_index(
    vs_client=vs_client,
    endpoint_name="my_endpoint",
    index_name="my_catalog.schema.index",
    query_text="what is vector search?",
    k=10,
    query_type="HYBRID"
)
```

### Comparing Results

```python
from retrieval_core.analyzer import compare_evaluations

results_by_type = {
    "FULL_TEXT": full_text_results_df,
    "ANN": ann_results_df,
    "HYBRID": hybrid_results_df,
}

comparison = compare_evaluations(results_by_type)
display(comparison)
```

### When to Use Each Type

- **FULL_TEXT wins**: Users search with specific terms and keywords
  - Example: Technical documentation, code search
  - Recommendation: Ensure index captures key terminology

- **ANN wins**: Documents have strong semantic content
  - Example: Natural language Q&A, knowledge bases
  - Recommendation: Consider adding a reranker, experiment with different embeddings

- **HYBRID wins**: Use case benefits from both semantic and keyword matching
  - Example: Most general-purpose search applications
  - Recommendation: Tune hybrid weights (alpha parameter) for better balance

---

## 5. Usage Examples

### Example 1: Evaluate with Auto-Generated Queries

```python
# Step 1: Generate queries
from retrieval_core.query_generator import QueryGenerator

generator = QueryGenerator()
queries_df = generator.generate_queries(
    corpus_table="main.docs.knowledge_base",
    columns=["text"],
    num_queries=100,
    style="keyword"
)

# Step 2: Run evaluation with LLM judge
from retrieval_core.evaluator import RetrievalEvaluator

evaluator = RetrievalEvaluator(
    judge_model_endpoint="databricks-meta-llama-3-1-70b-instruct"
)

results = []
for row in queries_df.collect():
    query_text = row.query_text

    # Query index
    retrieved = query_index(
        vs_client=vs_client,
        endpoint_name="my_endpoint",
        index_name="main.indexes.my_index",
        query_text=query_text,
        k=10
    )

    # Score with LLM
    metrics = evaluator.compute_judge_metrics(
        query_text=query_text,
        retrieved_chunks=retrieved,
        k_values=[10]
    )

    results.append({
        "query_text": query_text,
        "metrics": json.dumps(metrics)
    })

# Step 3: Analyze results
import pandas as pd
from retrieval_core.analyzer import EvaluationAnalyzer

results_df = pd.DataFrame(results)
analyzer = EvaluationAnalyzer(results_df)

print(analyzer.summary())
display(analyzer.score_distribution())
display(analyzer.top_queries(10))
```

### Example 2: Compare Query Types Across Strategies

```python
from retrieval_core.analyzer import compare_evaluations

# Collect results for each strategy and query type
results = {}

for strategy in ["baseline", "semantic", "parent_child"]:
    for query_type in ["FULL_TEXT", "ANN", "HYBRID"]:
        key = f"{strategy}_{query_type}"

        # Run evaluation
        strategy_results = evaluate_strategy(
            strategy=strategy,
            query_type=query_type,
            queries=queries_df
        )

        results[key] = strategy_results

# Compare all combinations
comparison = compare_evaluations(results)
display(comparison)

# Find best combination
best = comparison.loc[comparison["avg_relevance_at_10"].idxmax()]
print(f"Best combination: {best['name']} with score {best['avg_relevance_at_10']:.3f}")
```

### Example 3: Using the Advanced Eval Notebook

The `eval_notebook_advanced.py` notebook demonstrates all features:

**Notebook Parameters**:
- `build_run_id`: Build run to evaluate (required)
- `auto_generate_queries`: Set to "true" to auto-generate queries
- `corpus_table`: Source table for query generation (if auto_generate=true)
- `queries_table`: Manual query table (if auto_generate=false)
- `compare_query_types`: Set to "true" to test FULL_TEXT, ANN, HYBRID
- `num_queries`: Number of queries to generate (default: 50)
- `query_style`: "keyword", "natural", or "mixed"
- `top_k`: Number of results per query (default: 10)
- `judge_model_endpoint`: LLM endpoint for relevance scoring

**Run the notebook**:
```bash
# With auto-generated queries and query type comparison
databricks jobs run --notebook-path /path/to/eval_notebook_advanced \
  --parameters build_run_id=abc123 \
  --parameters auto_generate_queries=true \
  --parameters corpus_table=main.docs.knowledge_base \
  --parameters compare_query_types=true \
  --parameters num_queries=100 \
  --parameters query_style=keyword
```

---

## API Reference

### RetrievalEvaluator

```python
class RetrievalEvaluator:
    def __init__(
        self,
        embedding_endpoint: str = None,
        judge_model_endpoint: str = "databricks-meta-llama-3-1-70b-instruct"
    )

    def compute_judge_metrics(
        self,
        query_text: str,
        retrieved_chunks: List[Dict],
        k_values: List[int] = [5, 10]
    ) -> Dict[str, float]
```

### EvaluationAnalyzer

```python
class EvaluationAnalyzer:
    def __init__(self, results_df: pd.DataFrame)

    def summary(self) -> str
    def score_distribution(self) -> pd.DataFrame
    def top_queries(self, n: int = 5) -> pd.DataFrame
    def bottom_queries(self, n: int = 5) -> pd.DataFrame
    def high_relevance_examples(self, n: int = 5, min_score: float = 2.5) -> pd.DataFrame
    def low_relevance_examples(self, n: int = 5, max_score: float = 1.0) -> pd.DataFrame
    def recall_at_k(self, k: int) -> float
```

### QueryGenerator

```python
class QueryGenerator:
    def __init__(
        self,
        model_endpoint: str = "databricks-meta-llama-3-1-70b-instruct",
        random_seed: int = None
    )

    def set_few_shot_examples(self, examples: List[Dict[str, str]])

    def generate_queries(
        self,
        corpus_table: str,
        columns: List[str],
        num_queries: int = 200,
        style: str = "keyword"
    ) -> DataFrame

    def sample_and_generate_examples(
        self,
        corpus_table: str,
        columns: List[str],
        num_samples: int = 20
    ) -> pd.DataFrame
```

### query_index (with query type support)

```python
def query_index(
    vs_client: VectorSearchClient,
    endpoint_name: str,
    index_name: str,
    query_text: str,
    k: int = 10,
    filters: dict = None,
    columns: list = None,
    query_type: str = "ANN"  # NEW: "ANN", "HYBRID", or "FULL_TEXT"
) -> List[Dict]
```

---

## Migration Guide

### From Original eval_notebook.py

**Before** (original notebook):
```python
# Manual queries required
queries_table = dbutils.widgets.get("queries_table")
qdf = spark.table(queries_table)

# Only compute metrics with ground truth
metrics = evaluator.compute_labeled_metrics(query_text, retrieved, expected_ids)

# Basic metrics logging
mlflow.log_metric("recall_at_10", avg_recall)
```

**After** (with new features):
```python
# Option 1: Auto-generate queries
generator = QueryGenerator()
queries_df = generator.generate_queries(corpus_table=corpus, num_queries=200)

# Option 2: Manual queries (still supported)
qdf = spark.table(queries_table)

# Compute metrics WITHOUT ground truth
metrics = evaluator.compute_judge_metrics(query_text, retrieved)

# Rich analytics
analyzer = EvaluationAnalyzer(results_df)
print(analyzer.summary())
display(analyzer.top_queries(10))
display(analyzer.score_distribution())
```

---

## Troubleshooting

### LLM Judge Returns Low Scores

**Issue**: All relevance scores are 0 or 1

**Solutions**:
1. Check that chunks have meaningful text in `chunk_text` field
2. Verify LLM endpoint is accessible: `databricks-meta-llama-3-1-70b-instruct`
3. Review a few examples manually - scores may be accurate!
4. Try a different judge model endpoint

### Query Generation Produces Poor Queries

**Issue**: Generated queries don't match how users actually search

**Solutions**:
1. Provide few-shot examples with `set_few_shot_examples()`
2. Choose appropriate query style: "keyword" for short terms, "natural" for questions
3. Sample and review generated queries first with `sample_and_generate_examples()`
4. Adjust the number of documents sampled

### Query Type Parameter Not Supported

**Issue**: `query_type` parameter causes an error

**Solutions**:
1. The SDK automatically falls back to default search
2. Check Databricks Vector Search SDK version: `pip install --upgrade databricks-vectorsearch`
3. Query type support may vary by index type - check index configuration

---

## Performance Considerations

1. **LLM Judge Scoring**:
   - ~100-200ms per query-chunk pair
   - For 200 queries × 10 chunks = 2000 LLM calls ≈ 3-7 minutes
   - Use batch processing where possible

2. **Query Generation**:
   - ~1-2 seconds per query generated
   - 200 queries ≈ 5-10 minutes
   - Results can be cached to `query_cache_table`

3. **Query Type Comparison**:
   - Testing 3 query types = 3× evaluation time
   - Consider running types in parallel if possible

---

## Next Steps

1. Try the `eval_notebook_advanced.py` notebook with your own data
2. Experiment with different query styles and few-shot examples
3. Compare query types to find the best search method for your use case
4. Use analytics to identify queries that need better document coverage
5. Iterate on chunking strategies based on insights

For questions or issues, please open a GitHub issue!
