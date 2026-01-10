# ✅ Pre-Deployment Checklist & Issues Found

## 🔍 Issues Found & Fixed

### ✅ FIXED Issue #1: Missing psycopg2 Dependency
**Problem**: `backend/requirements.txt` didn't include `psycopg2-binary`
**Fix**: Added `psycopg2-binary>=2.9.0` to requirements.txt
**Status**: ✅ FIXED

### ✅ FIXED Issue #2: Databricks SDK Version Too Old
**Problem**: Required SDK >= 0.61.0 for Lakebase, but requirements.txt had >= 0.12.0
**Fix**: Updated to `databricks-sdk>=0.61.0`
**Status**: ✅ FIXED

### ✅ FIXED Issue #3: Button iconPosition Prop Doesn't Exist
**Problem**: Used `iconPosition="right"` on Button component, but it doesn't support this prop
**Fix**: Changed to simple text "View Details →"
**Status**: ✅ FIXED

## ⚠️ Important Notes

### Backend API Files Need Import Updates
The following files still import from `utils.state` and need to be updated to `utils.postgres_state`:

**Files to update:**
1. `backend/api/projects.py`
2. `backend/api/builds.py`
3. `backend/api/evaluations.py`
4. `backend/main.py` (check if it has direct imports)

**This is REQUIRED before deployment!**

### Frontend Needs Rebuild
The Phase 1 UI changes (ProjectDetails page, navigation guards) need frontend rebuild:
```bash
cd frontend
npm install  # If new dependencies
npm run build
```

## ✅ Pre-Deployment Checklist

### Prerequisites (Do First):
- [ ] **Install psycopg2 locally for testing**
  ```bash
  pip install psycopg2-binary>=2.9.0 databricks-sdk>=0.61.0
  ```

### Step 1: Create Lakebase Instance
- [ ] Open Databricks workspace
- [ ] Navigate to Lakebase app
- [ ] Create new project named `retrieval-studio-db`
- [ ] Wait for provisioning (1-2 minutes)
- [ ] Note down the instance name

### Step 2: Apply Database Schema
- [ ] Open Lakebase SQL Editor
- [ ] Select `retrieval-studio-db` instance
- [ ] Select `databricks_postgres` database
- [ ] Copy entire `database/postgres_schema.sql`
- [ ] Run in SQL Editor
- [ ] Verify 4 tables created:
  ```sql
  SELECT table_name FROM information_schema.tables
  WHERE table_schema = 'public'
  ORDER BY table_name;
  ```
  Should show: builds, evaluations, job_runs, projects

### Step 3: Test Connection
- [ ] Run test script:
  ```bash
  python database/test_postgres_setup.py
  ```
- [ ] Verify all 5 tests pass:
  - ✅ Connection
  - ✅ Tables
  - ✅ Projects
  - ✅ Builds
  - ✅ Performance

### Step 4: Update Backend Imports
**This is CRITICAL - don't skip this!**

#### File 1: `backend/api/projects.py`
- [ ] Find line with: `from utils.state import ...`
- [ ] Replace with: `from utils.postgres_state import ...`
- [ ] Update function names if needed:
  - `create_project` stays same ✅
  - `get_project` stays same ✅
  - `get_all_projects` stays same ✅

#### File 2: `backend/api/builds.py`
- [ ] Find line with: `from utils.state import create_run, update_run_state, get_run`
- [ ] Replace with: `from utils.postgres_state import create_build as create_run, update_build_state as update_run_state, get_build as get_run`
- [ ] OR replace function calls directly:
  - `create_run()` → `create_build()`
  - `update_run_state()` → `update_build_state()`
  - `get_run()` → `get_build()`

#### File 3: `backend/api/evaluations.py`
- [ ] Find lines with: `from utils.state import get_run, update_run_state`
- [ ] Replace with: `from utils.postgres_state import get_build as get_run, update_build_state as update_run_state`

#### File 4: `backend/main.py`
- [ ] Search for `from utils.state import`
- [ ] If found, update same as above
- [ ] If not found, you're good ✅

### Step 5: Set Environment Variables
- [ ] Add to Databricks App config or .env:
  ```bash
  LAKEBASE_INSTANCE_NAME=retrieval-studio-db
  USE_POSTGRES=true
  ```

### Step 6: Install Dependencies
- [ ] Update Python dependencies:
  ```bash
  pip install -r backend/requirements.txt
  ```
- [ ] Verify psycopg2 and databricks-sdk installed:
  ```bash
  python -c "import psycopg2; print(psycopg2.__version__)"
  python -c "from databricks import sdk; print(sdk.__version__)"
  ```

### Step 7: Build Frontend
- [ ] Navigate to frontend:
  ```bash
  cd frontend
  ```
- [ ] Install dependencies (if needed):
  ```bash
  npm install
  ```
- [ ] Build production bundle:
  ```bash
  npm run build
  ```
- [ ] Verify build output in `frontend/dist` or `frontend/build`

### Step 8: Deploy Application
- [ ] Follow your standard Databricks App deployment process
- [ ] Deploy backend with updated dependencies
- [ ] Deploy frontend build
- [ ] Restart application

### Step 9: Post-Deployment Testing
- [ ] **Test 1: Projects Page**
  - [ ] Navigate to `/projects`
  - [ ] Page loads without errors
  - [ ] Can see list of projects (or empty state)

- [ ] **Test 2: Create Project**
  - [ ] Click "New Project"
  - [ ] Fill in project name
  - [ ] Click Create
  - [ ] Should navigate to `/projects/{id}`
  - [ ] Verify in Postgres:
    ```sql
    SELECT * FROM projects ORDER BY created_at DESC LIMIT 1;
    ```

- [ ] **Test 3: Project Details Page**
  - [ ] See project details at top
  - [ ] See build history section
  - [ ] "Create New Build" button visible
  - [ ] If no builds, see empty state

- [ ] **Test 4: Navigation Guards**
  - [ ] Try navigating to `/build` without project
  - [ ] Should see "No Project Selected" message
  - [ ] Try navigating to `/evaluate` without project
  - [ ] Should see "No Project Selected" message

- [ ] **Test 5: Create Build**
  - [ ] From project details, click "Create New Build"
  - [ ] Should navigate to `/build` with project context
  - [ ] Configure and submit build
  - [ ] Should appear in project details after submission
  - [ ] Verify in Postgres:
    ```sql
    SELECT run_id, project_name, state FROM builds
    ORDER BY created_at DESC LIMIT 1;
    ```

- [ ] **Test 6: Status Updates**
  - [ ] Build status should update in real-time
  - [ ] Check status updates quickly (< 1 second)
  - [ ] Verify database reflects current status

## 🚨 Rollback Plan

If deployment fails:

### Quick Rollback:
1. Revert backend imports back to `from utils.state import ...`
2. Keep frontend as-is (Phase 1 changes are safe)
3. Remove Postgres environment variables
4. Restart application

### Full Rollback:
1. Restore previous backend code
2. Restore previous frontend build
3. Remove Postgres environment variables
4. Restart application

## 📊 Expected Performance Improvements

After deployment, you should see:
- ⚡ Project list loads in < 50ms (vs 200-1000ms before)
- ⚡ Project details loads in < 20ms (vs 100-500ms before)
- ⚡ Build status updates in < 10ms (vs 500-2000ms before)
- ⚡ Create project in < 50ms (vs 1000-3000ms before)

## 🐛 Common Issues & Solutions

### "Failed to get Lakebase instance"
- **Check**: Instance name is exactly `retrieval-studio-db`
- **Check**: Instance exists in Lakebase app
- **Check**: You have access permissions

### "Missing tables"
- **Solution**: Run `postgres_schema.sql` in Lakebase SQL Editor
- **Check**: Selected correct database (`databricks_postgres`)

### "ModuleNotFoundError: No module named 'psycopg2'"
- **Solution**: Install dependencies: `pip install -r backend/requirements.txt`
- **Check**: psycopg2-binary is in requirements.txt

### "Can't see new UI"
- **Solution**: Frontend needs rebuild: `cd frontend && npm run build`
- **Solution**: Clear browser cache and hard refresh

### Frontend TypeScript errors
- **Solution**: Run `npm install` to update dependencies
- **Solution**: Check no syntax errors in .tsx files

## ✅ Validation

After deployment is successful, validate:
- [ ] Can create projects (stored in Postgres)
- [ ] Can view project details with build history
- [ ] Can create builds (stored in Postgres)
- [ ] Build/Evaluate pages blocked without project
- [ ] Status updates happen quickly
- [ ] All data persists correctly in Postgres
- [ ] Old Delta tables still work for eval results

## 🎉 Success Criteria

Deployment is successful when:
1. ✅ Projects page loads instantly
2. ✅ Can navigate: Projects → Project Details → Build
3. ✅ Project Details shows build history
4. ✅ Build/Evaluate blocked without project selection
5. ✅ Data in Postgres (verify with SQL queries)
6. ✅ Performance is noticeably faster

---

**Ready to proceed?** Follow the step-by-step guide below! 👇
