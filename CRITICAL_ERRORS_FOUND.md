# 🚨 CRITICAL ERRORS FOUND - WILL BREAK THE APP

## Severity Legend
- 🔴 **CRITICAL**: Will cause immediate failure, app won't run
- 🟡 **HIGH**: Will cause failures in specific scenarios
- 🟠 **MEDIUM**: Will cause degraded functionality

---

## 🔴 CRITICAL ERROR #1: Missing `requests` Dependency

**Location**: `retrieval_core/evaluator.py:4`

**Problem**:
```python
import requests  # Line 4
```

The `evaluator.py` module imports `requests` library, but it's NOT installed in the notebook pip commands.

**Notebooks affected**:
- `notebooks/eval_notebook.py` - Line 15: `%pip install databricks-vectorsearch mlflow --quiet`
- `notebooks/eval_notebook_advanced.py` - Line 31: `%pip install databricks-vectorsearch mlflow --quiet`

**Impact**:
- ❌ Evaluation notebooks will FAIL on import with `ModuleNotFoundError: No module named 'requests'`
- ❌ LLM judge scoring will not work
- ❌ All evaluations using `compute_judge_metrics()` will crash

**How to reproduce**:
```bash
python3 -c "from retrieval_core.evaluator import RetrievalEvaluator"
# ModuleNotFoundError: No module named 'requests'
```

**Fix**:
```python
# In both notebooks, change:
%pip install databricks-vectorsearch mlflow --quiet

# To:
%pip install databricks-vectorsearch mlflow requests --quiet
```

---

## 🔴 CRITICAL ERROR #2: Missing `pandas` Dependency

**Location**: `notebooks/eval_notebook_advanced.py:351`

**Problem**:
```python
import pandas as pd  # Line 351
results_pd = pd.DataFrame(all_rows)  # Line 353
```

The advanced notebook imports and uses `pandas`, but it's NOT installed in the pip command.

**Impact**:
- ❌ Advanced notebook will FAIL with `ModuleNotFoundError: No module named 'pandas'`
- ❌ Rich analytics features will not work
- ❌ Cannot use `EvaluationAnalyzer` class

**How to reproduce**:
Run the advanced notebook - it will crash at line 351.

**Fix**:
```python
# In eval_notebook_advanced.py, change:
%pip install databricks-vectorsearch mlflow --quiet

# To:
%pip install databricks-vectorsearch mlflow requests pandas --quiet
```

---

## 🔴 CRITICAL ERROR #3: Table Schema Mismatch - `query_type` Column

**Locations**:
- `utils/state.py:54-64` - Creates table WITHOUT `query_type`
- `notebooks/eval_notebook_advanced.py:191-204` - Creates table WITH `query_type`
- `notebooks/eval_notebook_advanced.py:315` - Writes data with `query_type`
- `notebooks/eval_notebook.py:203-212` - Writes data WITHOUT `query_type`

**Problem**:

**state.py schema (missing query_type)**:
```sql
CREATE TABLE IF NOT EXISTS rs_eval_results (
  eval_result_id STRING,
  build_run_id STRING,
  eval_run_id STRING,
  build_child_run_id STRING,
  project STRING,
  strategy STRING,
  query_text STRING,
  metrics STRING,          -- No query_type column!
  created_at TIMESTAMP
)
```

**eval_notebook_advanced.py schema (has query_type)**:
```sql
CREATE TABLE IF NOT EXISTS rs_eval_results (
  eval_result_id STRING,
  build_run_id STRING,
  eval_run_id STRING,
  build_child_run_id STRING,
  project STRING,
  strategy STRING,
  query_text STRING,
  query_type STRING,       -- Has query_type column!
  metrics STRING,
  created_at TIMESTAMP
)
```

**Impact**:

**Scenario 1**: App starts first (state.py creates table without query_type)
- ❌ Advanced notebook tries to insert query_type → **COLUMN NOT FOUND ERROR**
- ❌ All advanced evaluations will fail

**Scenario 2**: Advanced notebook runs first (creates table with query_type)
- ⚠️ Original notebook inserts without query_type → NULL values (might be OK)
- ⚠️ But schema inconsistency will cause confusion

**Error message you'll see**:
```
AnalysisException: cannot resolve query_type in INSERT INTO rs_eval_results
```

**Fix Option 1** (Recommended): Add query_type to state.py schema
```python
# In utils/state.py line 54-64:
CREATE TABLE IF NOT EXISTS {catalog_escaped}.raw.rs_eval_results (
  eval_result_id STRING,
  build_run_id STRING,
  eval_run_id STRING,
  build_child_run_id STRING,
  project STRING,
  strategy STRING,
  query_text STRING,
  query_type STRING,        -- ADD THIS
  metrics STRING,
  created_at TIMESTAMP
) USING DELTA
PARTITIONED BY (build_run_id, strategy)
```

**Fix Option 2**: Make query_type optional in notebooks
```python
# In eval_notebook.py line 203-212, add:
"query_type": "ANN",  # Default value

# In eval_notebook_advanced.py, it already has query_type so no change needed
```

---

## 🔴 CRITICAL ERROR #4: Invalid `query_type` Parameter in Vector Search SDK

**Location**: `utils/vs_utils.py:154, 160`

**Problem**:
```python
# Lines 152-160
if query_type == "HYBRID":
    query_params["query_type"] = "HYBRID"
elif query_type == "FULL_TEXT":
    query_params["query_type"] = "FULL_TEXT"
else:  # ANN
    query_params["query_type"] = "ANN"

# Line 163
res = idx.similarity_search(**query_params)
```

**The Databricks Vector Search SDK `similarity_search()` method does NOT accept a `query_type` parameter!**

According to the official Databricks docs:
```python
def similarity_search(
    query_text: str,
    columns: List[str] = None,
    filters: Dict[str, Any] = None,
    num_results: int = 10
) -> Dict
```

There is NO `query_type` parameter.

**Impact**:
- ❌ Any call to `query_index()` with `query_type` will FAIL with `TypeError: unexpected keyword argument 'query_type'`
- ❌ Advanced notebook's query type comparison feature WILL NOT WORK
- ❌ All calls to `query_index(..., query_type="HYBRID")` will crash

**Error message you'll see**:
```
TypeError: similarity_search() got an unexpected keyword argument 'query_type'
```

**Reality Check**:
Different query types (FULL_TEXT, ANN, HYBRID) are typically configured at the **INDEX LEVEL**, not at query time:
- Some indexes support both semantic and keyword search
- The behavior is controlled by index configuration, not query parameters
- You might need to use different methods or different indexes for different types

**Fix Options**:

**Option 1** (Quick Fix): Remove query_type parameter support entirely
```python
# Simply don't pass query_type to similarity_search
res = idx.similarity_search(
    query_text=query_text,
    columns=cols,
    num_results=k,
    filters=filters,
)
# The index configuration determines search behavior
```

**Option 2** (Advanced): Use index-level configuration
- Create separate indexes for FULL_TEXT, ANN, HYBRID
- Route queries to appropriate index based on desired query_type
- This requires major architecture changes

**Option 3** (Conditional): Check SDK version and capabilities
```python
# Try with query_type, fallback if not supported
try:
    res = idx.similarity_search(..., query_type=query_type)
except TypeError:
    # Fall back to default search
    res = idx.similarity_search(...)
```

**Recommendation**: Remove query_type feature for now, or clearly document that it requires specific index configurations.

---

## 🟡 HIGH ERROR #5: Potential Token Retrieval Issue

**Location**: `retrieval_core/evaluator.py:140`

**Problem**:
```python
# Lines 138-141
cfg = Config()
self.w = WorkspaceClient(config=cfg)
self.api_token = cfg.token or self.w.config.token
self.api_url = cfg.host or self.w.config.host
```

**Issues**:
1. `cfg.token` may not exist as an attribute (might be `cfg.auth_token` or retrieved differently)
2. `self.w.config.token` may also not be directly accessible
3. In Databricks notebooks, authentication is usually handled automatically

**Impact**:
- ⚠️ LLM judge may fail to authenticate if token retrieval doesn't work
- ⚠️ Fallback to keyword scoring will happen, but users won't get LLM scores
- ⚠️ Error messages may be cryptic: "API client not configured"

**Testing needed**:
Run this in a Databricks notebook:
```python
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

cfg = Config()
print(f"Has token attr: {hasattr(cfg, 'token')}")
print(f"Has auth_token attr: {hasattr(cfg, 'auth_token')}")

w = WorkspaceClient(config=cfg)
print(f"Has w.config.token: {hasattr(w.config, 'token')}")
print(f"Host: {cfg.host}")
```

**Safer approach**:
```python
def _setup_api_client(self):
    """Setup API client for Databricks Foundation Model API"""
    try:
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.core import Config
        import os

        # Try multiple auth methods
        cfg = Config()
        self.w = WorkspaceClient(config=cfg)

        # Get token - try multiple approaches
        self.api_token = (
            getattr(cfg, 'token', None) or
            getattr(cfg, 'auth_token', None) or
            os.environ.get('DATABRICKS_TOKEN') or
            None
        )

        # Get host
        self.api_url = cfg.host or os.environ.get('DATABRICKS_HOST')

    except Exception as e:
        print(f"Warning: Could not setup API client: {e}")
        self.w = None
        self.api_token = None
        self.api_url = None
```

---

## 🟠 MEDIUM ERROR #6: Inconsistent k_values in Notebooks

**Locations**:
- `notebooks/eval_notebook.py:193, 195` - Uses `k_values=[10]` (hardcoded)
- `notebooks/eval_notebook_advanced.py:287, 290` - Uses `k_values=[top_k]` (parameterized)

**Problem**:
The original notebook always uses k=10, but the advanced notebook uses the top_k parameter. This means:
- Original notebook: Always evaluates at k=10, regardless of top_k parameter
- Advanced notebook: Evaluates at whatever k user specifies

**Impact**:
- ⚠️ Inconsistent metric names in results
- ⚠️ If user sets top_k=5, original notebook still logs `recall_at_10` (misleading)
- ⚠️ Results are not comparable between notebooks

**Example**:
```python
# User sets top_k=5
# Original notebook creates: recall_at_10, ndcg_at_10 (WRONG - should be _at_5)
# Advanced notebook creates: recall_at_5, ndcg_at_5 (CORRECT)
```

**Fix**:
```python
# In eval_notebook.py, change line 193 and 195:
# From:
metrics = evaluator.compute_labeled_metrics(qtext, retrieved, expected_ids, k_values=[10])
metrics = evaluator.compute_judge_metrics(qtext, retrieved, k_values=[10])

# To:
metrics = evaluator.compute_labeled_metrics(qtext, retrieved, expected_ids, k_values=[top_k])
metrics = evaluator.compute_judge_metrics(qtext, retrieved, k_values=[top_k])
```

---

## 🟠 MEDIUM ERROR #7: Query Generator Uses Non-existent `doc_id` Column

**Location**: `retrieval_core/query_generator.py:256`

**Problem**:
```python
# Line 256
queries.append({
    "doc_id": row.get("doc_id", f"doc_{i}"),  # Assumes 'doc_id' column exists
    "query_text": query,
    "source_text": doc_text[:500] + ("..." if len(doc_text) > 500 else "")
})
```

The query generator assumes the corpus table has a `doc_id` column, but:
- Not all tables have this column
- Users might have `id`, `document_id`, `chunk_id`, etc.
- If the column doesn't exist, it falls back to `doc_{i}` which might be OK

**Impact**:
- ⚠️ Generated queries have generic IDs like `doc_0`, `doc_1`
- ⚠️ Can't trace back to original documents easily
- ⚠️ If users want to link queries to source docs, they can't

**Fix**:
```python
# Add doc_id_column parameter
def generate_queries(
    self,
    corpus_table: str,
    columns: List[str],
    num_queries: int = 200,
    style: str = "keyword",
    doc_id_column: str = "doc_id",  # ADD THIS with default
    spark_session = None
) -> DataFrame:
    # ...
    # Then use:
    doc_id = row.get(doc_id_column, f"doc_{i}")
```

---

## Summary of Critical Errors

| # | Error | Severity | Impact | Easy Fix? |
|---|-------|----------|--------|-----------|
| 1 | Missing `requests` dependency | 🔴 CRITICAL | App won't run | ✅ Yes - add to pip |
| 2 | Missing `pandas` dependency | 🔴 CRITICAL | Advanced features won't work | ✅ Yes - add to pip |
| 3 | Table schema mismatch (query_type) | 🔴 CRITICAL | INSERT will fail | ✅ Yes - add column |
| 4 | Invalid query_type parameter in SDK | 🔴 CRITICAL | Query type comparison won't work | ❌ No - remove feature or redesign |
| 5 | Token retrieval issues | 🟡 HIGH | LLM judge may fail | ⚠️ Maybe - needs testing |
| 6 | Inconsistent k_values | 🟠 MEDIUM | Confusing metrics | ✅ Yes - use top_k param |
| 7 | doc_id column assumption | 🟠 MEDIUM | Generated queries have generic IDs | ✅ Yes - add parameter |

---

## Recommended Fix Order

### Phase 1: Critical Fixes (Required for app to run)
1. ✅ Add `requests` to pip install
2. ✅ Add `pandas` to pip install
3. ✅ Add `query_type` column to state.py schema
4. ❌ **Remove or redesign query_type comparison feature** (SDK doesn't support it)

### Phase 2: High Priority Fixes
5. Test token retrieval in Databricks and fix if needed
6. Fix k_values inconsistency in eval_notebook.py

### Phase 3: Nice-to-Have Fixes
7. Add doc_id_column parameter to query generator

---

## Testing Checklist

Before deploying:

- [ ] Test import: `from retrieval_core.evaluator import RetrievalEvaluator`
- [ ] Test import: `from retrieval_core.analyzer import EvaluationAnalyzer`
- [ ] Test import: `from retrieval_core.query_generator import QueryGenerator`
- [ ] Run eval_notebook.py with small dataset
- [ ] Run eval_notebook_advanced.py with auto_generate=false
- [ ] Test LLM judge scoring manually
- [ ] Verify table schema has all columns
- [ ] Test with different top_k values
- [ ] Check MLflow logging works

---

## Quick Fix Script

```python
# Fix #1 & #2: Update notebooks
# In notebooks/eval_notebook.py line 15:
%pip install databricks-vectorsearch mlflow requests --quiet

# In notebooks/eval_notebook_advanced.py line 31:
%pip install databricks-vectorsearch mlflow requests pandas --quiet

# Fix #3: Update state.py schema
# In utils/state.py line 61, add:
query_type STRING,

# Fix #4: Remove query_type from vs_utils.py
# In utils/vs_utils.py, remove lines 152-160 and just use:
res = idx.similarity_search(
    query_text=query_text,
    columns=cols,
    num_results=k,
    filters=filters,
)

# Fix #6: Update eval_notebook.py
# Lines 193, 195: Change k_values=[10] to k_values=[top_k]
```
