"""
Backfill experiment_id for existing builds by searching MLflow for their runs

This script populates the experiment_id column for builds that were created before
the experiment_id tracking was implemented.

Run this once after deploying the database migration.

Usage:
    # Dry run (safe - no changes)
    python utils/backfill_experiment_ids.py

    # Actually update database
    python utils/backfill_experiment_ids.py --apply
"""
import mlflow
from mlflow.tracking import MlflowClient
import sys
import os

# Add project root to path
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(current_dir)
if project_root not in sys.path:
    sys.path.insert(0, project_root)


def backfill_experiment_ids(dry_run=True):
    """
    Search MLflow for runs associated with each build and store the experiment_id

    Args:
        dry_run: If True, only print what would be done without updating database
    """
    mlflow.set_tracking_uri("databricks")
    client = MlflowClient()

    # Use proper abstraction
    from utils.postgres_state import get_postgres_connector

    connector = get_postgres_connector()

    # Get all builds without experiment_id
    builds = connector.execute("""
        SELECT run_id, project_id, project_name, state, created_at
        FROM builds
        WHERE experiment_id IS NULL
        ORDER BY created_at DESC
    """)

    total = len(builds)
    print(f"Found {total} builds without experiment_id")
    print(f"Mode: {'DRY RUN' if dry_run else 'LIVE UPDATE'}")
    print("=" * 80)
    print()

    updated = 0
    not_found = 0
    errors = 0

    for idx, build in enumerate(builds, 1):
        run_id = build['run_id']
        project_name = build['project_name']
        state = build['state']
        created_at = build['created_at']

        try:
            print(f"[{idx}/{total}] Build {run_id[:12]}... (state: {state})")
            print(f"  Project: {project_name}")
            print(f"  Created: {created_at}")

            # Search for runs with this build_run_id across ALL experiments
            all_runs = mlflow.search_runs(
                filter_string=f"params.build_run_id = '{run_id}'",
                max_results=5
            )

            if len(all_runs) > 0:
                # Get experiment_id from first run
                experiment_id = str(all_runs.iloc[0]['experiment_id'])

                try:
                    exp = client.get_experiment(experiment_id)
                    experiment_name = exp.name
                    lifecycle = exp.lifecycle_stage
                except Exception as exp_error:
                    experiment_name = "unknown"
                    lifecycle = "unknown"
                    print(f"  ⚠️  Could not retrieve experiment details: {exp_error}")

                print(f"  ✓ Found {len(all_runs)} run(s) in experiment {experiment_id}")
                print(f"    Experiment: {experiment_name}")
                print(f"    Lifecycle: {lifecycle}")

                if not dry_run:
                    # Update builds table using abstraction
                    connector.execute(
                        "UPDATE builds SET experiment_id = %s WHERE run_id = %s",
                        (experiment_id, run_id),
                        fetch="none"
                    )
                    print(f"  ✓ Updated database")
                else:
                    print(f"  → Would update: experiment_id = {experiment_id}")

                updated += 1
            else:
                print(f"  ✗ No MLflow runs found")
                print(f"    Build may have failed before creating runs")
                not_found += 1

        except Exception as e:
            print(f"  ✗ Error: {e}")
            errors += 1
            import traceback
            print(f"  Traceback: {traceback.format_exc()}")

        print()

    print("=" * 80)
    print(f"Summary:")
    print(f"  Total builds: {total}")
    print(f"  Updated: {updated}")
    print(f"  Not found: {not_found}")
    print(f"  Errors: {errors}")
    print()

    if dry_run:
        print(f"This was a DRY RUN. Run with --apply to apply changes.")
        print(f"Command: python utils/backfill_experiment_ids.py --apply")
    else:
        print(f"✓ Database updated successfully!")

    return {
        "total": total,
        "updated": updated,
        "not_found": not_found,
        "errors": errors
    }


if __name__ == "__main__":
    # Run with --apply flag to actually update database
    dry_run = "--apply" not in sys.argv

    if dry_run:
        print("=" * 80)
        print("DRY RUN MODE - No database changes will be made")
        print("=" * 80)
        print()

    try:
        result = backfill_experiment_ids(dry_run=dry_run)

        # Exit with non-zero if there were errors
        if result["errors"] > 0:
            sys.exit(1)
    except Exception as e:
        print(f"Fatal error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
