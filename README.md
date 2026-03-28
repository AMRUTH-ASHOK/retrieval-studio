# Retrieval Studio

> Stop guessing why your RAG app gives bad answers. Start measuring.

---

## Sound familiar?

You've built a RAG pipeline. It mostly works. But sometimes it returns completely wrong chunks, misses obvious answers, or hallucinates — and you have no idea why.

So you tweak the chunk size. Maybe try a different splitter. Re-index. Test again with a few queries. It feels a little better? Hard to tell.

This is the loop most teams are stuck in. **Retrieval Studio breaks it.**

---

## What is Retrieval Studio?

Retrieval Studio is a lab for your RAG retrieval layer. It lives in your Databricks workspace and lets you **systematically experiment** with how your data gets chunked and indexed — then **measure** which approach actually performs best.

Instead of guessing, you get numbers: Recall@K, NDCG@K, Precision@K — across every chunking strategy, every data source, every search mode.

---

## Meet Priya

*Priya is an ML engineer at a healthcare company. Her team built a RAG chatbot over clinical guidelines — but doctors keep complaining the answers are vague or miss key details. She suspects the chunking is bad but doesn't know where to start.*

Here's how Priya uses Retrieval Studio:

---

### Step 1 — She opens the app and creates a Project

Priya clicks **New Project**, gives it a name ("Clinical Guidelines v2"), and points it at her Unity Catalog schema and Vector Search endpoint. That's it — the project is her workspace for this experiment.

---

### Step 2 — She sets up a Build

Priya goes to **Build** and adds her data sources: a UC Volume with PDFs of clinical guidelines, and a Delta table with structured drug interaction data.

Then — and this is the key part — she assigns multiple **chunking strategies** to each source. For the PDFs she tries *semantic* and *paragraph*. For the structured table she tries *baseline* and *structured*.

She hits **Submit**. Databricks runs the job. Each source × strategy combination gets its own vector index. She goes to lunch.

---

### Step 3 — She evaluates

Build's done. Priya goes to **Evaluate**, selects her build, and lets the app auto-generate synthetic queries from her chunks. She could also upload a golden set of real queries from her doctors — but the auto-generated ones are good enough to start.

She picks a judge model, sets top-5, and submits. Another job runs. This time she gets coffee.

---

### Step 4 — She sees what's actually happening

Priya opens **Review**. Here's what she finds:

- For the PDF guidelines: *semantic* chunking beats *paragraph* by 18 points on Recall@5
- For the drug interaction table: *structured* chunking wins easily — baseline was splitting rows mid-sentence
- The app even gives her an LLM-generated explanation of *why* semantic chunking works better for her docs

She clicks into a few individual query results to see exactly which chunks were returned vs. what was expected. The misses are obvious now.

---

### Step 5 — She ships with confidence

Priya marks the winning indexes as **Keep**, discards the losers, and runs a cleanup job to remove the unused indexes from her workspace. She goes back to **Build** with one tweak — trying *parent-child* chunking for the PDFs — and runs another round.

Two iterations later, Recall@5 is up 31% from where she started. She has the MLflow runs to prove it.

---

## What you get

| | |
|---|---|
| **Multiple chunking strategies** | Baseline, semantic, structured, parent-child, sentence, paragraph — tested in parallel |
| **Multi-source builds** | Different strategies for different data sources in one build |
| **Auto-generated eval queries** | No golden dataset? The app generates synthetic queries from your chunks |
| **Real metrics** | Recall@K, NDCG@K, Precision@K — not vibes |
| **Side-by-side comparison** | Charts, tables, and LLM explanations of what's working and why |
| **Query-level inspection** | See exactly which chunks were returned vs. expected |
| **MLflow tracking** | Every build and eval run is logged automatically |
| **Clean workspace** | Keep winning indexes, discard the rest, run cleanup |

---

## Getting started

The app is already running in your Databricks workspace. Just open it and:

1. **Create a project** — name it, point it at your catalog and Vector Search endpoint
2. **Build** — add your data sources, pick your chunking strategies, submit
3. **Evaluate** — run metrics against your build
4. **Review** — find your winner
5. **Iterate** — go again with what you learned
