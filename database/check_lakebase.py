"""
Quick script to check if Lakebase is available in your workspace
"""
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

try:
    print("Connecting to Databricks workspace...")
    w = WorkspaceClient(config=Config())

    print(f"✅ Connected to workspace")
    print(f"   Host: {w.config.host}")

    # Check if database attribute exists
    if hasattr(w, 'database'):
        print("✅ Lakebase API is available in this SDK version")

        # Try to list instances
        try:
            print("\nListing Lakebase instances...")
            instances = w.database.list_database_instances()
            instance_list = list(instances)

            if instance_list:
                print(f"✅ Found {len(instance_list)} Lakebase instance(s):")
                for inst in instance_list:
                    print(f"   - {inst.name} (state: {inst.state})")
            else:
                print("⚠️  No Lakebase instances found")
                print("   You need to create one in the Lakebase app first")
        except Exception as e:
            print(f"❌ Error listing instances: {e}")
            print("   This might mean Lakebase is not enabled in your workspace")
    else:
        print("❌ Lakebase API NOT available")
        print("   Your databricks-sdk version is too old")
        print("   Run: pip install --upgrade databricks-sdk>=0.61.0")

except Exception as e:
    print(f"❌ Connection error: {e}")
    print("\nTroubleshooting:")
    print("1. Make sure you're authenticated: databricks auth login")
    print("2. Check your .databrickscfg file exists")

