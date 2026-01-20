# MLflow Experiment ID Fix - Deployment Guide

## Summary

This fix resolves the MLflow experiment mismatch issue where the Review page showed 0 metrics because the API was looking up the wrong experiment.

**Root Cause:** The API generated experiment names from project names and used name-based lookup, which found the wrong experiment (ID 2 instead of 3958664039298191).

**Solution:** Store the actual experiment_id used during build execution and use it for all subsequent API lookups.

## What Changed

### 1. Database Schema
- **File:** `database/postgres_schema.sql`
- **Change:** Added index on existing `experiment_id` column
- **Impact:** Faster lookups, better documentation

### 2. Build Notebook
- **File:** `notebooks/build_notebook_v2.py`
- **Change:** Now stores `experiment.experiment_id` in builds table after `mlflow.set_experiment()`
- **Impact:** All new builds will have experiment_id automatically

### 3. API Endpoints
- **File:** `backend/api/projects.py`
- **Changes:**
  - `get_mlflow_runs()` - Uses stored experiment_id first, falls back to name-based lookup
  - `get_mlflow_experiment_url()` - Uses stored experiment_id for accurate URLs
- **Impact:** Review page will find metrics correctly

### 4. Backfill Script
- **File:** `utils/backfill_experiment_ids.py`
- **Purpose:** Populate experiment_id for existing builds
- **Impact:** Fixes historical builds

### 5. Migration Script
- **File:** `database/migrations/001_add_experiment_id_index.sql`
- **Purpose:** Adds index to existing databases
- **Impact:** One-time database update

## Deployment Steps

### Step 1: Run Database Migration

Run the migration SQL in your Lakebase SQL Editor:

```bash
# Option 1: Via Lakebase SQL Editor UI
# Copy contents of database/migrations/001_add_experiment_id_index.sql
# Paste and execute in SQL Editor

# Option 2: Via psql (if you have direct access)
psql -h <host> -U <user> -d <database> -f database/migrations/001_add_experiment_id_index.sql
```

**Verify:**
```sql
SELECT
    tablename,
    indexname,
    indexdef
FROM pg_indexes
WHERE tablename = 'builds'
  AND indexname = 'idx_builds_experiment_id';
```

### Step 2: Deploy Code Changes

Deploy the updated application to Databricks Apps:

1. The backend API (`backend/api/projects.py`) will automatically use the new logic
2. The build notebook (`notebooks/build_notebook_v2.py`) will store experiment_id for new builds

### Step 3: Backfill Existing Builds

Run the backfill script to populate experiment_id for existing builds:

```bash
# Test first (dry run - safe)
python utils/backfill_experiment_ids.py

# Review the output, then apply if it looks good
python utils/backfill_experiment_ids.py --apply
```

**Expected Output:**
```
Found X builds without experiment_id
Mode: LIVE UPDATE
================================================================================

[1/X] Build abc123... (state: SUCCESS)
  Project: product_2
  Created: 2026-01-18 10:30:00
  ✓ Found 15 run(s) in experiment 3958664039298191
    Experiment: /Workspace/Users/.../retrieval-studio/experiments/product_2
    Lifecycle: active
  ✓ Updated database

...

Summary:
  Total builds: X
  Updated: Y
  Not found: Z
  Errors: 0

✓ Database updated successfully!
```

## Verification & Testing

### Test 1: Verify Backfill Success

```sql
-- Check how many builds now have experiment_id
SELECT
    COUNT(*) as total_builds,
    COUNT(experiment_id) as builds_with_exp_id,
    COUNT(experiment_id)::float / COUNT(*) * 100 as percentage
FROM builds;

-- Should show high percentage (100% for successful builds)
```

### Test 2: Verify API Uses Stored ID

1. Go to Review page in your app
2. Check backend logs (Databricks Apps logs)
3. Should see:
   ```
   [DEBUG] Found stored experiment_id: 3958664039298191
   [DEBUG] ✓ Retrieved experiment by ID: /Workspace/.../experiments/product_2
   [DEBUG] Using experiment: ... (ID: 3958664039298191)
   ```
4. Should NOT see:
   ```
   [DEBUG] Falling back to name-based lookup
   ```

### Test 3: Verify Metrics Load

1. Go to Review page
2. Select builds and evaluations
3. Click "Review Selected"
4. Should now see metrics (not 0 runs)

### Test 4: Verify New Builds Store ID

1. Create a new build in the app
2. Check the build logs for:
   ```
   [INFO] ✓ MLflow Experiment Set
   [INFO]   - Name: /Workspace/.../experiments/your_project
   [INFO]   - ID: 3958664039298191
   [INFO] ✓ Stored experiment_id=3958664039298191 in builds table
   ```
3. Verify in database:
   ```sql
   SELECT run_id, experiment_id, state
   FROM builds
   ORDER BY created_at DESC
   LIMIT 1;
   ```

### Test 5: Verify MLflow URL

1. Go to Review page
2. Click "Open in MLflow UI" button
3. URL should be: `https://.../ml/experiments/3958664039298191/runs`
4. Should show all your evaluation runs

### Test 6: Test Project Rename Robustness

1. Rename a project in the UI (if supported)
2. Go to Review page for that project
3. Should still show metrics (uses stored ID, ignores new name)

## Troubleshooting

### Issue: Backfill finds 0 runs for a build

**Cause:** Build may have failed before creating MLflow runs

**Action:** This is expected for failed builds. No action needed.

### Issue: API still shows 0 metrics after backfill

**Check:**
1. Did the backfill complete successfully?
2. Are there actually MLflow runs for this build?
   ```python
   import mlflow
   mlflow.search_runs(filter_string="params.build_run_id = 'YOUR_BUILD_ID'")
   ```
3. Check backend logs for which experiment_id is being used

### Issue: Build notebook fails to store experiment_id

**Symptoms:** Build completes but logs show error:
```
[ERROR] ✗ Failed to store experiment_id in database
```

**Cause:** Database connection issue or permission problem

**Impact:** Build still succeeds, but API will fall back to name-based lookup

**Fix:** Check database connectivity and permissions for the service principal

## Architecture Benefits

1. **Robust:** Experiment ID is immutable - won't break if project renamed
2. **Accurate:** Uses actual experiment ID from build, not regenerated name
3. **Backward Compatible:** Falls back to name-based lookup for old builds
4. **Debuggable:** Comprehensive logging at each step
5. **Future-Proof:** Works even if `EXPERIMENT_BASE_PATH` changes
6. **Minimal Changes:** Only 4 files modified, no architectural changes

## Rollback Plan

If something goes wrong:

1. **Code Rollback:**
   - Revert changes to `notebooks/build_notebook_v2.py`
   - Revert changes to `backend/api/projects.py`
   - The API will fall back to name-based lookup

2. **Database Rollback:**
   - The experiment_id column is nullable and optional
   - Removing data is safe: `UPDATE builds SET experiment_id = NULL;`
   - Dropping index is safe: `DROP INDEX idx_builds_experiment_id;`

3. **No Data Loss:**
   - All changes are additive
   - No existing functionality is removed
   - Fallback mechanisms ensure backward compatibility

## Success Criteria

- ✅ All existing builds have experiment_id after backfill (where runs exist)
- ✅ New builds automatically store experiment_id
- ✅ Review page loads metrics correctly for all projects
- ✅ MLflow experiment URL points to correct experiment
- ✅ System works even after renaming projects
- ✅ Comprehensive logging helps debug future issues

## Files Modified

1. `database/postgres_schema.sql` - Added index and documentation
2. `database/migrations/001_add_experiment_id_index.sql` - Migration script
3. `notebooks/build_notebook_v2.py` - Store experiment_id during build
4. `backend/api/projects.py` - Use stored experiment_id for lookups
5. `utils/backfill_experiment_ids.py` - Backfill script for existing builds

## Next Steps

1. ✅ Run migration SQL (Step 1)
2. ✅ Deploy updated code to Databricks Apps (Step 2)
3. ✅ Run backfill script (Step 3)
4. ✅ Verify using tests above
5. ✅ Monitor backend logs for any issues
6. ✅ Create a new build to verify end-to-end flow

---

**Questions or Issues?**

Check the backend logs in Databricks Apps for detailed diagnostic information. All operations include comprehensive logging with `[INFO]`, `[DEBUG]`, `[WARNING]`, and `[ERROR]` prefixes.
