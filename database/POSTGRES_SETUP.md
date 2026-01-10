# Lakehouse Postgres Setup Guide

This guide walks you through setting up Lakehouse Postgres for Retrieval Studio.

## Step 1: Create Lakebase Postgres Instance

### Via Databricks UI:

1. Open your Databricks workspace
2. Click on **Apps Switcher** → **Lakebase**
3. Click **"New project"**
4. Enter project details:
   - **Name**: `retrieval-studio-db`
   - **Postgres Version**: Latest (recommended)
5. Click **Create**
6. Wait for compute to provision (1-2 minutes)

This will automatically create:
- `production` and `development` branches
- Default `databricks_postgres` database
- Compute resources

## Step 2: Run Schema Migration

### Via Lakebase SQL Editor:

1. In Lakebase, click **SQL Editor**
2. Select your `retrieval-studio-db` instance
3. Select `databricks_postgres` database
4. Copy the entire contents of `postgres_schema.sql`
5. Paste into SQL Editor
6. Click **Run** or press `Cmd/Ctrl + Enter`
7. Verify all tables were created:
   ```sql
   SELECT table_name FROM information_schema.tables
   WHERE table_schema = 'public'
   ORDER BY table_name;
   ```

You should see:
- `builds`
- `evaluations`
- `job_runs`
- `projects`

## Step 3: Configure Environment Variables

Add to your Databricks App configuration or `.env`:

```bash
# Lakebase Postgres Configuration
LAKEBASE_INSTANCE_NAME=retrieval-studio-db
```

The connector will automatically:
- Detect the instance
- Get OAuth tokens from Databricks SDK
- Manage connection pooling

## Step 4: Test Connection

Run the test script:

```python
from utils.postgres_connector import get_postgres_connector

connector = get_postgres_connector()
if connector.test_connection():
    print("✅ Postgres connection successful!")
else:
    print("❌ Connection failed")
```

## Step 5: Initialize Tables

```python
from utils.postgres_state import initialize_tables

if initialize_tables():
    print("✅ All tables verified")
else:
    print("⚠️  Some tables missing - check setup")
```

## Step 6: Update Backend

The backend has been updated to use Postgres for transactional data:

### What Uses Postgres:
- ✅ Projects (CRUD operations)
- ✅ Builds (status tracking, job URLs)
- ✅ Evaluations (configuration, status)
- ✅ Job runs (detailed tracking)

### What Still Uses Delta:
- ✅ `rs_eval_results` (evaluation metrics - analytics workload)
- ✅ `rs_chunks_*` (document chunks - large datasets)
- ✅ Index registry (vector search metadata)

## Architecture

```
┌─────────────────────────────────────┐
│   Lakehouse Postgres (OLTP)        │
│                                     │
│  • Fast transactional reads/writes │
│  • Application state management    │
│  • Real-time status updates        │
├─────────────────────────────────────┤
│  Tables:                            │
│  - projects                         │
│  - builds                           │
│  - evaluations                      │
│  - job_runs                         │
└─────────────────────────────────────┘
            │
            ▼
┌─────────────────────────────────────┐
│     Delta Tables (Analytics)        │
│                                     │
│  • Large-scale analytics           │
│  • Historical metrics               │
│  • Chunk storage                    │
├─────────────────────────────────────┤
│  Tables:                            │
│  - rs_eval_results                  │
│  - rs_chunks_*                      │
│  - rs_index_registry                │
└─────────────────────────────────────┘
```

## Benefits

### Postgres (for app state):
✅ Sub-millisecond latency for point reads
✅ ACID transactions
✅ Foreign key constraints
✅ Better for concurrent updates
✅ Automatic token rotation
✅ Connection pooling

### Delta (for analytics):
✅ Scalable for large datasets
✅ Time travel
✅ Optimized for analytical queries
✅ Direct integration with notebooks

## Connection Details

### Authentication:
- Uses **OAuth tokens** from Databricks SDK
- Tokens auto-refresh every 15 minutes
- No password management needed

### Connection Pooling:
- Min connections: 2
- Max connections: 20
- Thread-safe for web apps
- Automatic failover

### Security:
- SSL/TLS encryption
- Unity Catalog integration
- Row-level security (if needed)
- Audit logging

## Troubleshooting

### Connection Issues:

**Error: "Failed to get Lakebase instance"**
- Verify instance name is correct
- Check you have access permissions
- Ensure Lakebase is enabled in workspace

**Error: "Connection timeout"**
- Check network connectivity
- Verify instance is running (not scaled to zero)
- Check private link configuration if used

**Error: "Missing tables"**
- Run `postgres_schema.sql` in SQL Editor
- Verify you're connected to correct database
- Check table permissions

### Performance Issues:

**Slow queries:**
- Check indexes exist (run schema creates them)
- Monitor connection pool usage
- Consider read replicas for heavy read workloads

**Token refresh errors:**
- Verify Databricks SDK is updated (>= 0.61.0)
- Check workspace authentication
- Review token expiration settings

## Monitoring

### Check Table Sizes:
```sql
SELECT
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
```

### Check Active Connections:
```sql
SELECT count(*) as connections
FROM pg_stat_activity
WHERE datname = 'databricks_postgres';
```

### Check Recent Builds:
```sql
SELECT run_id, project_name, state, created_at
FROM builds
ORDER BY created_at DESC
LIMIT 10;
```

## Migration from Delta

If you have existing data in Delta tables:

```python
# Example migration script (run once)
from utils.state import get_all_projects as delta_get_projects
from utils.postgres_state import create_project

# Migrate projects
delta_projects = delta_get_projects(sql_connector, catalog, schema)
for proj in delta_projects:
    create_project(
        project_id=proj['project_id'],
        project_name=proj['project_name'],
        description=proj.get('description'),
        created_by=proj.get('created_by')
    )
```

## Next Steps

1. ✅ Create Lakebase instance
2. ✅ Run schema migration
3. ✅ Test connection
4. ✅ Update backend imports
5. ✅ Deploy updated app
6. ✅ Monitor performance

## Support

For issues or questions:
- Check Databricks Lakebase docs: https://docs.databricks.com/aws/en/oltp/
- Review connection logs
- Test with SQL Editor first
- Check workspace permissions
