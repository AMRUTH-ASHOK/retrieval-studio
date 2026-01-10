# Phase 2: Lakehouse Postgres Deployment Guide

This guide will help you migrate from Delta-based state management to Lakehouse Postgres for better transactional performance.

## 🎯 What's Changing

### Before (Delta Tables):
- ❌ Slow point reads/updates
- ❌ Delta commit latency
- ❌ Poor for concurrent writes
- ❌ Overkill for small state data

### After (Lakehouse Postgres):
- ✅ Sub-millisecond latency
- ✅ ACID transactions
- ✅ Concurrent updates
- ✅ Perfect for web app state

## 📋 Prerequisites

- [ ] Databricks workspace with Lakebase enabled
- [ ] Workspace admin access (to create Lakebase instance)
- [ ] Python 3.9+
- [ ] Databricks SDK >= 0.61.0

## 🚀 Step-by-Step Deployment

### Step 1: Create Lakebase Postgres Instance (5 minutes)

1. **Open Lakebase App:**
   - Navigate to your Databricks workspace
   - Click Apps Switcher (grid icon)
   - Select **"Lakebase"**

2. **Create New Project:**
   - Click **"New project"**
   - Name: `retrieval-studio-db`
   - Postgres Version: **Latest**
   - Click **Create**

3. **Wait for Provisioning:**
   - Takes 1-2 minutes
   - Creates `production` and `development` branches
   - Sets up compute automatically

### Step 2: Apply Database Schema (2 minutes)

1. **Open SQL Editor:**
   - In Lakebase, click **"SQL Editor"**
   - Select instance: `retrieval-studio-db`
   - Select database: `databricks_postgres`

2. **Run Schema:**
   - Open `database/postgres_schema.sql`
   - Copy entire contents
   - Paste into SQL Editor
   - Click **Run** or press `Cmd/Ctrl + Enter`

3. **Verify Tables:**
   ```sql
   SELECT table_name
   FROM information_schema.tables
   WHERE table_schema = 'public'
   ORDER BY table_name;
   ```

   Should show:
   - `builds`
   - `evaluations`
   - `job_runs`
   - `projects`

### Step 3: Test Connection (3 minutes)

1. **Install Dependencies:**
   ```bash
   pip install psycopg2-binary databricks-sdk>=0.61.0
   ```

2. **Run Test Script:**
   ```bash
   cd /path/to/retrieval-studio
   python database/test_postgres_setup.py
   ```

3. **Expected Output:**
   ```
   ✅ PASS  Connection
   ✅ PASS  Tables
   ✅ PASS  Projects
   ✅ PASS  Builds
   ✅ PASS  Performance

   Total: 5/5 tests passed
   🎉 All tests passed!
   ```

### Step 4: Update Backend Imports (5 minutes)

The backend needs to switch from Delta-based `state.py` to Postgres-based `postgres_state.py`.

#### Files to Update:

**1. `backend/api/projects.py`:**
```python
# Old import
from utils.state import create_project, get_project, get_all_projects

# New import
from utils.postgres_state import create_project, get_project, get_all_projects
```

**2. `backend/api/builds.py`:**
```python
# Old import
from utils.state import create_run, update_run_state, get_run

# New import
from utils.postgres_state import create_build, update_build_state, get_build
```

**3. `backend/api/evaluations.py`:**
```python
# Old import
from utils.state import get_run, update_run_state

# New import
from utils.postgres_state import get_build, update_build_state
```

**4. `backend/main.py`:** (if it has direct imports)
```python
# Old import
from utils.state import ...

# New import
from utils.postgres_state import ...
```

### Step 5: Update Dependency Injection (2 minutes)

Update `backend/auth.py` to provide Postgres connector instead of SQL connector for projects/builds/evaluations:

```python
from utils.postgres_connector import get_postgres_connector

def get_postgres_connector_dep():
    """Dependency injection for Postgres connector"""
    return get_postgres_connector()
```

### Step 6: Set Environment Variables

Add to your Databricks App configuration:

```bash
# Required
LAKEBASE_INSTANCE_NAME=retrieval-studio-db

# Optional (defaults shown)
USE_POSTGRES=true
```

### Step 7: Deploy Frontend Changes

Since Phase 1 frontend changes weren't deployed yet, let's do both together:

1. **Build Frontend:**
   ```bash
   cd frontend
   npm run build
   ```

2. **Deploy to Databricks App:**
   - Follow your standard deployment process
   - Or use Databricks CLI if configured

### Step 8: Smoke Test (5 minutes)

After deployment, test the full flow:

1. **Test Projects:**
   - Go to `/projects`
   - Create a new project
   - Should navigate to project details page
   - Verify project appears in Postgres:
     ```sql
     SELECT * FROM projects ORDER BY created_at DESC LIMIT 5;
     ```

2. **Test Build:**
   - Click "Create New Build"
   - Configure and submit
   - Check build appears in project details
   - Verify in Postgres:
     ```sql
     SELECT run_id, project_name, state
     FROM builds
     ORDER BY created_at DESC LIMIT 5;
     ```

3. **Test Evaluation:**
   - Navigate from successful build
   - Submit evaluation
   - Verify in Postgres:
     ```sql
     SELECT eval_id, run_id, state
     FROM evaluations
     ORDER BY created_at DESC LIMIT 5;
     ```

## 📊 Data Migration (Optional)

If you have existing projects/builds in Delta tables:

```python
# Run this script once to migrate
from utils.state import get_all_projects as delta_get_projects
from utils.state import get_project_runs as delta_get_runs
from utils.postgres_state import create_project, create_build
from backend.auth import get_sql_connector
from backend.config import settings

# Get Delta data
sql_connector = get_sql_connector()
delta_projects = delta_get_projects(sql_connector, settings.CATALOG, settings.SCHEMA)

# Migrate to Postgres
for proj in delta_projects:
    try:
        create_project(
            project_id=proj['project_id'],
            project_name=proj['project_name'],
            description=proj.get('description'),
            vs_endpoint_name=proj.get('vs_endpoint_name'),
            embedding_model_endpoint=proj.get('embedding_model_endpoint'),
            created_by=proj.get('created_by')
        )
        print(f"✅ Migrated project: {proj['project_name']}")

        # Migrate builds for this project
        builds = delta_get_runs(sql_connector, settings.CATALOG, settings.SCHEMA, proj['project_id'])
        for build in builds:
            create_build(
                run_id=build['run_id'],
                project_id=build['project_id'],
                project_name=build['project_name'],
                config=build.get('config', {}),
                created_by=build.get('created_by')
            )
            print(f"  ✅ Migrated build: {build['run_id'][:8]}...")

    except Exception as e:
        print(f"❌ Failed to migrate {proj['project_name']}: {e}")
```

## 🔍 Monitoring

### Check Connection Pool:
```python
from utils.postgres_connector import get_postgres_connector

connector = get_postgres_connector()
# Pool manages connections automatically
# Min: 2, Max: 20 connections
```

### Query Performance:
```sql
-- Check slow queries (if query logging enabled)
SELECT
    query,
    mean_exec_time / 1000 as avg_seconds,
    calls
FROM pg_stat_statements
ORDER BY mean_exec_time DESC
LIMIT 10;
```

### Table Sizes:
```sql
SELECT
    tablename,
    pg_size_pretty(pg_total_relation_size('public.' || tablename)) AS size
FROM pg_tables
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size('public.' || tablename) DESC;
```

## 🐛 Troubleshooting

### Issue: "Failed to get Lakebase instance"

**Solution:**
- Verify instance name: `retrieval-studio-db`
- Check permissions (workspace admin)
- Ensure Lakebase is enabled

### Issue: "Missing tables"

**Solution:**
- Run `postgres_schema.sql` in SQL Editor
- Verify database is `databricks_postgres`
- Check you're on correct branch (`production`)

### Issue: "Connection timeout"

**Solution:**
- Check instance is running (not scaled to zero)
- Verify network connectivity
- Check private link configuration

### Issue: "Token expired"

**Solution:**
- Connector auto-refreshes every 15 minutes
- Update Databricks SDK: `pip install --upgrade databricks-sdk`
- Check workspace authentication

## ✅ Validation Checklist

After deployment, verify:

- [ ] Lakebase instance is running
- [ ] All 4 tables exist (projects, builds, evaluations, job_runs)
- [ ] Test script passes all 5 tests
- [ ] Backend imports updated to `postgres_state`
- [ ] Environment variables set
- [ ] Can create new project via UI
- [ ] Project details page shows build history
- [ ] Can navigate Build → Project Details → Evaluate
- [ ] Build status updates in real-time
- [ ] Data persists in Postgres (verify with SQL)

## 📈 Performance Comparison

Expected improvements:

| Operation | Delta (Before) | Postgres (After) |
|-----------|----------------|------------------|
| Get project by ID | 100-500ms | 1-10ms |
| List builds | 200-1000ms | 10-50ms |
| Update build status | 500-2000ms | 5-20ms |
| Create project | 1000-3000ms | 10-50ms |

## 🎉 Success Criteria

You'll know it's working when:

1. ✅ Projects page loads instantly
2. ✅ Project details shows real-time build history
3. ✅ Build/Evaluate pages block without project selection
4. ✅ Status updates appear within seconds
5. ✅ No more Delta commit delays
6. ✅ All data in Postgres tables

## 📚 References

- [Lakebase Postgres Documentation](https://docs.databricks.com/aws/en/oltp/)
- [How to use Lakebase as a transactional data layer for Databricks Apps](https://www.databricks.com/blog/how-use-lakebase-transactional-data-layer-databricks-apps)
- [Use a notebook to access a database instance](https://docs.databricks.com/aws/en/oltp/instances/query/notebook)
- [Get started with Lakebase Postgres](https://docs.databricks.com/aws/en/oltp/projects/get-started)

---

**Questions? Issues?**

Review the troubleshooting section or check Postgres logs in Lakebase SQL Editor.

Good luck with the deployment! 🚀
