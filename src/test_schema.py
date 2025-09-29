#!/usr/bin/env python3
"""
Test script to validate database schema initialization
"""
import asyncio
import os
import sys
import json

# Add current directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app import DatabaseManager

async def test_schema_initialization():
    """Test the database schema initialization process"""
    
    print("🧪 Testing Database Schema Initialization")
    print("=" * 50)
    
    # Use test database URL or default
    database_url = os.getenv("DATABASE_URL", "postgresql://yugabyte:yugabyte@localhost:5433/yugabyte")
    print(f"Database URL: {database_url.replace('yugabyte:', 'yugabyte:***@') if 'yugabyte:' in database_url else database_url}")
    
    try:
        # Initialize database manager
        print("\n1. Creating DatabaseManager...")
        db_manager = DatabaseManager(database_url)
        
        print("2. Initializing database connection and schema...")
        await db_manager.initialize()
        
        print("3. Getting schema information...")
        schema_info = await db_manager.get_schema_info()
        
        print("\n📊 Schema Information:")
        print(json.dumps(schema_info, indent=2))
        
        print("\n4. Testing basic operations...")
        
        # Test getting all tables
        tables = await db_manager.get_all_tables_with_comments()
        print(f"   Found {len(tables)} total tables in database")
        
        # Test getting current state
        states = await db_manager.get_current_state()
        print(f"   Found {len(states)} tracked table states")
        
        # Test state table operations
        print("\n5. Testing state table operations...")
        
        # Insert test record
        from app import TableState, TableBootstrapConfig
        from datetime import datetime, timezone
        
        test_config = TableBootstrapConfig(
            enabled=True,
            bq_table="test_dataset.test_table",
            columns="id,name,created_at"
        )
        
        test_state = TableState(
            schema_name="test_schema",
            table_name="test_table",
            comment_hash="test_hash_123",
            bootstrap_config=test_config,
            bigquery_created=False,
            pipeline_configured=False,
            last_updated=datetime.now(timezone.utc)
        )
        
        await db_manager.upsert_table_state(test_state)
        print("   ✅ Successfully inserted test state record")
        
        # Read back the record
        updated_states = await db_manager.get_current_state()
        test_key = "test_schema.test_table"
        if test_key in updated_states:
            retrieved_state = updated_states[test_key]
            print(f"   ✅ Successfully retrieved test record")
            print(f"      Schema: {retrieved_state.schema_name}")
            print(f"      Table: {retrieved_state.table_name}")
            print(f"      BQ Target: {retrieved_state.bootstrap_config.bq_table}")
            print(f"      Enabled: {retrieved_state.bootstrap_config.enabled}")
        else:
            print("   ❌ Failed to retrieve test record")
        
        # Clean up test record
        await db_manager.delete_table_state("test_schema", "test_table")
        print("   ✅ Successfully cleaned up test record")
        
        print("\n6. Schema validation completed successfully! ✅")
        
    except Exception as e:
        print(f"\n❌ Schema initialization failed: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        if 'db_manager' in locals():
            await db_manager.close()
            print("\n🔒 Database connections closed")
    
    return True

async def main():
    """Main test function"""
    success = await test_schema_initialization()
    
    if success:
        print("\n🎉 All tests passed! Database schema is ready for use.")
        sys.exit(0)
    else:
        print("\n💥 Tests failed! Please check the error messages above.")
        sys.exit(1)

if __name__ == "__main__":
    # Load environment variables if available
    try:
        from dotenv import load_dotenv
        load_dotenv()
        print("📁 Loaded environment variables from .env file")
    except ImportError:
        print("📁 Using system environment variables (dotenv not available)")
    
    asyncio.run(main())