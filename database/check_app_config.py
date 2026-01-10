"""
Check if the app can access Lakebase from Databricks Apps environment
Run this via databricks apps execute
"""
import os
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

print("=" * 60)
print("LAKEBASE CONNECTION TEST FROM DATABRICKS APPS")
print("=" * 60)

# Check environment variables
print("\n1. Environment Variables:")
print(f"   LAKEBASE_INSTANCE_NAME: {os.environ.get('LAKEBASE_INSTANCE_NAME', 'NOT SET')}")
print(f"   USE_POSTGRES: {os.environ.get('USE_POSTGRES', 'NOT SET')}")
print(f"   CATALOG: {os.environ.get('CATALOG', 'NOT SET')}")

# Try to connect
print("\n2. Connecting to workspace...")
try:
    w = WorkspaceClient(config=Config())
    print(f"   ✅ Connected to workspace")
    print(f"   Host: {w.config.host}")
except Exception as e:
    print(f"   ❌ Failed to connect: {e}")
    exit(1)

# Check Lakebase instances
print("\n3. Checking Lakebase instances...")
instance_name = os.environ.get('LAKEBASE_INSTANCE_NAME', 'retrieval-studio-db')
try:
    instances = list(w.database.list_database_instances())
    print(f"   Found {len(instances)} instance(s):")
    for inst in instances:
        status = "✅" if inst.name == instance_name else "  "
        print(f"   {status} {inst.name} (state: {inst.state})")

    # Try to get specific instance
    print(f"\n4. Trying to access instance '{instance_name}'...")
    instance = w.database.get_database_instance(name=instance_name)
    print(f"   ✅ Found instance: {instance.name}")
    print(f"   State: {instance.state}")
    print(f"   Host: {instance.read_write_dns}")

except Exception as e:
    print(f"   ❌ Error: {e}")
    print("\n   Troubleshooting:")
    print("   - Verify instance name is correct in app.yaml")
    print("   - Check instance is running (not scaled to zero)")
    print("   - Verify app has permissions to access Lakebase")

# Try to get credentials
print("\n5. Testing credential generation...")
try:
    import uuid
    cred = w.database.generate_database_credential(
        request_id=str(uuid.uuid4()),
        instance_names=[instance_name]
    )
    print(f"   ✅ Successfully generated credentials")
    print(f"   Token length: {len(cred.token)}")
except Exception as e:
    print(f"   ❌ Failed to generate credentials: {e}")

print("\n" + "=" * 60)
print("TEST COMPLETE")
print("=" * 60)
