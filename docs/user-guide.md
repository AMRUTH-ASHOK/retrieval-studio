# Retrieval Studio User Guide

## Who This Guide Is For

This guide follows **John**, an AI Engineer building a RAG system for a multi-agent application on Databricks. John needs to:
- combine mixed source data (PDF docs, CSV logs, and custom text),
- test chunking strategies,
- evaluate retrieval quality,
- and choose the best build strategy for production.

## End-to-End Workflow

## 1) Create or Select a Project

Go to `Projects`:
1. Click **New Project**.
2. Enter a project name and description.
3. Create and select the project.

Why this matters:
- All builds, evaluations, and comparisons are scoped to a project.
- MLflow tracking is organized per project experiment.

## 2) Build a Retrieval Pipeline

Go to `Build` and complete 4 steps.

### Step 1: Select Data Type

Choose:
- a single type (`pdf`, `csv`, `json`, `text`, `delta_table`, `uc_volume`), or
- **Mixed Sources** (`MIXED`) to combine different source types in one build.

### Step 2: Configure Data Sources

For single type:
- configure one or more sources depending on type.

For Mixed Sources:
- add multiple source cards,
- choose a type per source,
- configure each source independently,
- upload files for file-based sources.

Example mixed setup:
- Source 1: `pdf` (product docs)
- Source 2: `csv` (ticket summaries)
- Source 3: `text` (manual notes)

### Step 3: Select Chunking Strategies

Pick one or more compatible strategies. In mixed mode, available strategies are the intersection across selected source types.

### Step 4: Configure Endpoints and Submit

Provide:
- Embedding model endpoint
- Vector Search endpoint

Submit the build, then monitor job status in-page.

## 3) Evaluate Retrieval Quality

Go to `Evaluate`:
1. Pick a successful build run.
2. Choose evaluation mode:
   - existing query/golden tables, or
   - auto-generated queries.
3. Configure `top_k` and optional query-type comparison.
4. Submit evaluation and wait for completion.

## 4) Review and Compare Results

Go to `Review`:
1. Select scope: `Project`, `Build`, or `Evaluation`.
2. Load metrics.
3. Analyze:
   - best performers cards,
   - bar/scatter charts (latency vs recall),
   - detailed comparison table with sorting/filters,
   - query-level expected vs retrieved chunk inspection.

## 5) Choose the Best Strategy

Use a decision approach:
1. Start with high-level metrics (Recall@10, NDCG@10, latency).
2. Validate with query-level inspection.
3. Prioritize your production goal:
   - quality-first,
   - latency-first,
   - or balanced.

## Decision Guide: Which Chunking Strategy?

- **Baseline**
  - Use when you need predictable and fast setup.
  - Good default for initial benchmarking.

- **Semantic**
  - Use when document meaning/context is critical.
  - Often improves relevance quality for conceptual queries.

- **Structured**
  - Use when content has clear structural boundaries (headings/sections).
  - Helps preserve document hierarchy.

- **Parent-Child**
  - Use when you need broad retrieval context plus focused child chunks.
  - Useful for long technical documents.

## Decision Guide: How to Interpret Metrics

- **Recall@K**
  - Higher is better.
  - Measures whether relevant chunks are retrieved.

- **Precision@K**
  - Higher is better.
  - Measures how much of top-K is relevant.

- **NDCG@K**
  - Higher is better.
  - Captures ranking quality (not just inclusion).

- **Latency**
  - Lower is better.
  - Critical for interactive assistant UX.

Recommended practical thresholds (starting point):
- Recall@10: `>= 0.75` strong
- NDCG@10: `>= 0.70` strong
- Latency: `< 300ms` excellent, `< 500ms` acceptable

## Practical Tuning Workflow

John’s weekly iteration loop:
1. Add new mixed sources (new docs + latest CSV exports).
2. Run baseline + semantic + parent-child.
3. Evaluate with same top-k and query style.
4. Compare in Review scatter/table.
5. Inspect worst queries and failure patterns.
6. Adjust strategy or source preprocessing.
7. Re-run and compare again.

## Best Practices

- Keep builds comparable: change one major variable at a time.
- Use consistent evaluation settings across runs for fair comparison.
- Prefer project-level naming conventions for easy history tracking.
- Use query details to debug metric outliers; aggregate scores alone are not enough.
- For file-heavy sources, use volume-based uploads and keep file organization clean.

## Troubleshooting

- **No results in Review**
  - Ensure evaluation job completed successfully.
  - Reload scope selections and refresh project data.

- **No query details returned**
  - Confirm evaluation results table exists and is populated.
  - Try loading a different evaluation ID.

- **Build fails after submission**
  - Validate data source config fields.
  - Check endpoint names for embedding/vector search.
  - For mixed mode, confirm each source has valid type + config.

- **Too slow**
  - Start with baseline chunking and smaller corpora.
  - Reduce evaluation workload (query count/top-k) during iteration.

## Quick Checklist Before Production

- Best strategy selected with both metric and query-level evidence.
- Latency and quality meet product goals.
- Evaluation repeatability verified on refreshed data.
- Project docs updated with chosen settings and rationale.
