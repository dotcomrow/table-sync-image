#!/usr/bin/env python3
"""
Component Test 1: Basic YugabyteDB Connection
Tests if we can connect to YugabyteDB and query basic info
"""
import asyncio
import os
import asyncpg
from urllib.parse import urlparse

async def test_yugabyte_connection():
    """Test basic YugabyteDB connectivity"""
    print("🔌 Testing YugabyteDB Connection...")
    
    database_url = os.getenv("DATABASE_URL", "postgresql://yugabyte@localhost:5433/yugabyte")
    print(f"Database URL: {database_url}")
    
    try:
        conn = await asyncpg.connect(database_url)
        
        # Test basic query
        version = await conn.fetchval("SELECT version()")
        print(f"✅ Connected to YugabyteDB: {version}")
        
        # Test database list
        databases = await conn.fetch("SELECT datname FROM pg_database WHERE datistemplate = false")
        print(f"✅ Available databases: {[db['datname'] for db in databases]}")
        
        # Test CDC capability
        slots = await conn.fetch("SELECT slot_name, slot_type FROM pg_replication_slots")
        print(f"✅ Replication slots: {len(slots)} found")
        for slot in slots:
            print(f"   - {slot['slot_name']} ({slot['slot_type']})")
        
        await conn.close()
        print("✅ YugabyteDB connection test PASSED")
        return True
        
    except Exception as e:
        print(f"❌ YugabyteDB connection test FAILED: {e}")
        return False

if __name__ == "__main__":
    asyncio.run(test_yugabyte_connection())