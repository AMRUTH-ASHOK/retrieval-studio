"""
Test script to verify Lakehouse Postgres setup

Run this after creating the Lakebase instance and running the schema.
"""
import sys
import os

!pip install --upgrade databricks-sdk>=0.61.0
dbutils.library.restartPython()

# Add parent directory to path
current_dir = os.getcwd()
parent_dir = os.path.dirname(current_dir)
# Check logic for finding the 'retrieval_core' package 
project_root = None 
if os.path.isdir(os.path.join(current_dir, "retrieval_core")): 
    project_root = current_dir 
elif os.path.isdir(os.path.join(parent_dir, "retrieval_core")): 
    project_root = parent_dir 
else:
    p = current_dir
    for _ in range(4):
        if os.path.isdir(os.path.join(p, "retrieval_core")):
            project_root = p
            break
        p = os.path.dirname(p)
    if not project_root:
        project_root = parent_dir

print(f"Using Project Root: {project_root}")

if project_root not in sys.path:
    # Use insert(0) to prioritize our project root over system paths
    sys.path.insert(0, project_root)


from utils.postgres_connector import get_postgres_connector
from utils.postgres_state import (
    initialize_tables,
    create_project,
    get_project,
    get_all_projects,
    create_build,
    get_build,
    get_builds_by_project
)
import uuid


def test_connection():
    """Test basic connection"""
    print("\n" + "="*60)
    print("TEST 1: Database Connection")
    print("="*60)

    try:
        connector = get_postgres_connector()
        if connector.test_connection():
            print("✅ Connection successful")
            return True
        else:
            print("❌ Connection failed")
            return False
    except Exception as e:
        print(f"❌ Connection error: {e}")
        return False


def test_tables():
    """Test table existence"""
    print("\n" + "="*60)
    print("TEST 2: Table Schema")
    print("="*60)

    try:
        if initialize_tables():
            print("✅ All required tables exist")
            return True
        else:
            print("❌ Some tables missing")
            return False
    except Exception as e:
        print(f"❌ Table check error: {e}")
        return False


def test_project_operations():
    """Test project CRUD operations"""
    print("\n" + "="*60)
    print("TEST 3: Project Operations")
    print("="*60)

    try:
        # Create test project
        test_project_id = f"test_{uuid.uuid4().hex[:8]}"
        project = create_project(
            project_id=test_project_id,
            project_name="Test Project",
            description="Test project for Postgres setup verification",
            created_by="test_user"
        )
        print(f"✅ Created project: {project['project_id']}")

        # Retrieve project
        retrieved = get_project(test_project_id)
        assert retrieved['project_id'] == test_project_id
        assert retrieved['project_name'] == "Test Project"
        print(f"✅ Retrieved project: {retrieved['project_name']}")

        # List projects
        all_projects = get_all_projects()
        assert len(all_projects) > 0
        print(f"✅ Listed {len(all_projects)} project(s)")

        return True

    except Exception as e:
        print(f"❌ Project operation error: {e}")
        return False


def test_build_operations():
    """Test build CRUD operations"""
    print("\n" + "="*60)
    print("TEST 4: Build Operations")
    print("="*60)

    try:
        # Create test project first
        test_project_id = f"test_{uuid.uuid4().hex[:8]}"
        project = create_project(
            project_id=test_project_id,
            project_name="Build Test Project",
            created_by="test_user"
        )

        # Create test build
        test_run_id = str(uuid.uuid4())
        build = create_build(
            run_id=test_run_id,
            project_id=test_project_id,
            project_name="Build Test Project",
            config={
                "data_type": "pdf",
                "strategies": {"baseline": {}}
            },
            created_by="test_user"
        )
        print(f"✅ Created build: {build['run_id'][:8]}...")

        # Retrieve build
        retrieved = get_build(test_run_id)
        assert retrieved['run_id'] == test_run_id
        assert retrieved['state'] == 'PENDING'
        assert retrieved['config']['data_type'] == 'pdf'
        print(f"✅ Retrieved build with config: {retrieved['config']}")

        # List builds for project
        builds = get_builds_by_project(test_project_id)
        assert len(builds) == 1
        print(f"✅ Listed {len(builds)} build(s) for project")

        return True

    except Exception as e:
        print(f"❌ Build operation error: {e}")
        import traceback
        traceback.print_exc()
        return False


def test_query_performance():
    """Test query performance"""
    print("\n" + "="*60)
    print("TEST 5: Query Performance")
    print("="*60)

    try:
        import time

        # Test simple SELECT
        connector = get_postgres_connector()

        start = time.time()
        result = connector.execute("SELECT COUNT(*) as count FROM projects")
        elapsed = (time.time() - start) * 1000

        print(f"✅ Query executed in {elapsed:.2f}ms")
        print(f"   Found {result[0]['count']} project(s)")

        if elapsed < 100:
            print("   ⚡ Excellent latency!")
        elif elapsed < 500:
            print("   👍 Good latency")
        else:
            print("   ⚠️  High latency - check connection")

        return True

    except Exception as e:
        print(f"❌ Performance test error: {e}")
        return False


def run_all_tests():
    """Run all tests"""
    print("\n" + "="*60)
    print("LAKEHOUSE POSTGRES SETUP VERIFICATION")
    print("="*60)

    tests = [
        ("Connection", test_connection),
        ("Tables", test_tables),
        ("Projects", test_project_operations),
        ("Builds", test_build_operations),
        ("Performance", test_query_performance),
    ]

    results = {}
    for name, test_func in tests:
        try:
            results[name] = test_func()
        except Exception as e:
            print(f"\n❌ {name} test crashed: {e}")
            results[name] = False

    # Summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)

    passed = sum(1 for v in results.values() if v)
    total = len(results)

    for name, result in results.items():
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status}  {name}")

    print(f"\nTotal: {passed}/{total} tests passed")

    if passed == total:
        print("\n🎉 All tests passed! Postgres setup is working correctly.")
        print("\nNext steps:")
        print("  1. Update backend to import from postgres_state instead of state")
        print("  2. Deploy updated application")
        print("  3. Test in production")
    else:
        print("\n⚠️  Some tests failed. Please review the errors above.")
        print("\nCommon issues:")
        print("  - Lakebase instance not created or not running")
        print("  - Schema not applied (run postgres_schema.sql)")
        print("  - Wrong instance name in LAKEBASE_INSTANCE_NAME")
        print("  - Permission issues")

    return passed == total


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
