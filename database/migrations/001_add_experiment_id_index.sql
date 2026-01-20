-- Migration: Add index on experiment_id column
-- Date: 2026-01-19
-- Purpose: Improve performance of experiment_id lookups for MLflow integration
--
-- IMPORTANT: This uses CONCURRENTLY to avoid table locking and Lakebase timeout issues
-- The experiment_id column already exists in the builds table (VARCHAR(100))
--
-- Run this in Lakebase SQL Editor:
--   Copy and paste the commands below

-- ==============================================================================
-- Option A: Create index with CONCURRENTLY (RECOMMENDED)
-- ==============================================================================
-- This prevents table locking and avoids "Deadline exceeded" timeouts
-- May take 1-5 minutes depending on table size, but won't block application

CREATE INDEX CONCURRENTLY idx_builds_experiment_id ON builds(experiment_id);

-- Add comment documenting the column's purpose
COMMENT ON COLUMN builds.experiment_id IS 'MLflow experiment ID where runs for this build are stored';

-- Verify the index was created
SELECT
    tablename,
    indexname,
    indexdef
FROM pg_indexes
WHERE tablename = 'builds'
  AND indexname = 'idx_builds_experiment_id';

-- Expected output:
--   tablename | indexname                     | indexdef
--   ----------|-------------------------------|------------------------------------------
--   builds    | idx_builds_experiment_id      | CREATE INDEX idx_builds_experiment_id ...

-- ==============================================================================
-- Option B: If Option A times out (FALLBACK)
-- ==============================================================================
-- Skip the index creation entirely - the queries will work without it
-- Existing composite index idx_builds_project_created will be used instead
-- Only affects performance on very large datasets, not functionality
--
-- Just run this if you skip the index:
-- COMMENT ON COLUMN builds.experiment_id IS 'MLflow experiment ID where runs for this build are stored';

-- ==============================================================================
-- Troubleshooting
-- ==============================================================================
-- If you see "ERROR: relation "idx_builds_experiment_id" already exists"
--   The index already exists - you're done! Skip this migration.
--
-- If you see "Deadline exceeded in the request to a Database Instance"
--   Use Option B (skip index) or contact Databricks support to increase timeout
