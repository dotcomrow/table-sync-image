#!/usr/bin/env python3
"""
Test script to verify the JSONB fix works correctly.
"""

import asyncio
import asyncpg
import os
from datetime import datetime

async def test_jsonb_fix():
    """Test that our JSONB insertion fix works"""
    
    # Use the same database URL format from the logs
    database_url = os.environ.get('DATABASE_URL', 'postgresql://postgres:password@localhost:5432/test')
    
    try:
        conn = await asyncpg.connect(database_url)
        
        # Test the exact SQL we're using in the fix
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS test_metadata (
                key VARCHAR(255) PRIMARY KEY,
                value JSONB,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );
        """)
        
        # Test the fixed insertion
        await conn.execute("""
            INSERT INTO test_metadata (key, value) 
            VALUES ('schema_version', 
                jsonb_build_object(
                    'version', '1.0.0',
                    'initialized_at', NOW()::text
                )
            )
            ON CONFLICT (key) DO UPDATE SET 
            value = jsonb_build_object(
                'version', '1.0.0',
                'initialized_at', NOW()::text
            );
        """)
        
        # Verify the data was inserted correctly
        result = await conn.fetchrow("SELECT * FROM test_metadata WHERE key = 'schema_version'")
        
        if result:
            print("✅ JSONB insertion successful!")
            print(f"   Key: {result['key']}")
            print(f"   Value: {result['value']}")
            print(f"   Created: {result['created_at']}")
        else:
            print("❌ No data found after insertion")
            
        # Cleanup
        await conn.execute("DROP TABLE test_metadata")
        await conn.close()
        
        print("✅ Test completed successfully - fix should work!")
        
    except Exception as e:
        print(f"❌ Test failed: {e}")
        return False
        
    return True

if __name__ == "__main__":
    print("Testing JSONB insertion fix...")
    success = asyncio.run(test_jsonb_fix())
    if success:
        print("\n🎉 The fix should resolve the Kubernetes deployment issue!")
    else:
        print("\n⚠️  There may still be issues to resolve.")