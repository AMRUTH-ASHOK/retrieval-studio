# Retrieval Studio - Databricks App

A complete Databricks App for RAG pipeline evaluation and data preparation with React frontend and FastAPI backend.

## 
### Prerequisites
- Databricks workspace with Apps enabled
- Service principal with permissions
- SQL warehouse or compute endpoint
- Node.js 18+ & Python 3.9+ (for local dev)

### Deploy to Databricks Apps

1. **Configure `app.yaml`**:
   ```yaml
   env:
     - name: CATALOG
       value: "your_catalog"
     - name: SCHEMA
       value: "your_schema"
     - name: DATABRICKS_HTTP_PATH
       value: "/sql/1.0/endpoints/your-endpoint-id"
   ```

2. **Deploy**: `databricks apps create retrieval-studio`

3. **Access** via Databricks Apps URL

### Local Development

**Backend**: `cd backend && pip install -r requirements.txt && uvicorn main:app --reload`

**Frontend**: `cd frontend && npm install && npm run dev`

**Access**: Frontend at http://localhost:3000, API at http://localhost:8000/docs

## 
```
backend/       # FastAPI (12 files: main, config, auth, 5 routes, models)
frontend/      # React (18 files: app, 6 services, 3 components, 5 pages)
core/          # Strategies & data types (existing)
utils/         # Helpers (existing)
notebooks/     # Build & eval notebooks (existing)
```

## 
1. **Projects** - Create/manage projects
 endpoints
3. **Evaluate** - Submit eval jobs to Databricks
4. **Review** - View detailed results
5. **Leaderboard** - Strategy rankings (Recall, NDCG, latency)

## 
```
GET/POST  /api/projects              # Projects
POST/GET  /api/builds                # Builds
POST/GET  /api/evaluations           # Evaluations
GET       /api/leaderboard/{run_id}  # Leaderboard
GET       /api/metadata/*            # Data types & strategies
GET       /docs                      # API documentation
```

## 
**Environment Variables** (in `app.yaml` or `.env`):
```
CATALOG=your_catalog
SCHEMA=your_schema
DATABRICKS_HTTP_PATH=/sql/1.0/endpoints/...
# Auto-injected by Databricks Apps:
# DATABRICKS_HOST, DATABRICKS_CLIENT_ID, DATABRICKS_CLIENT_SECRET
```

**Notebook Paths** (hardcoded in `backend/config.py`):
```
BUILD: /Workspace/Users/amruth.ashok@databricks.com/retrieval-studio/retrieval-studio/notebooks/build_notebook_v2
EVAL:  /Workspace/Users/amruth.ashok@databricks.com/retrieval-studio/retrieval-studio/notebooks/eval_notebook
```

## 
**Frontend**: React 18, TypeScript, Material-UI, Vite, Axios  
**Backend**: FastAPI, Pydantic, Databricks SDK, MLflow  
**Data**: Delta Lake, Vector Search, Unity Catalog

## 
 New Project
 Submit
 Submit
 View results
 See rankings

## 
**Backend won't start**: Check env vars, DATABRICKS_HTTP_PATH, permissions  
**Frontend fails**: Ensure Node 18+, clear node_modules, reinstall  
**Can't connect**: Verify credentials, SQL warehouse running  
**Jobs fail**: Check notebook paths, permissions, MLflow access

##  Verification

 Expected All files present!`: `

## 
 Implementation Complete Cleanup Complete |  | 
---

**Built for Databricks | FastAPI + React**
