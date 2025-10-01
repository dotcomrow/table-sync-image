#!/usr/bin/env python3
"""
Alternative Approach: PostgreSQL Fallback Configuration
If YugabyteDB connector continues to fail, test with PostgreSQL connector as fallback
"""
import asyncio
import aiohttp
import json
import os
import asyncpg
from urllib.parse import urlparse

async def test_postgresql_connector_fallback():
    """Test using PostgreSQL connector as fallback for YugabyteDB"""
    print("🔄 Testing PostgreSQL Connector Fallback...")
    print("Note: This tests if YugabyteDB can work with standard PostgreSQL connector")
    
    connect_url = os.getenv("DEBEZIUM_CONNECTOR_URL", "http://localhost:8083")
    database_url = os.getenv("DATABASE_URL", "postgresql://yugabyte@localhost:5433/yugabyte")
    
    parsed = urlparse(database_url)
    
    # Create test table
    try:
        conn = await asyncpg.connect(database_url)
        await conn.execute("""
            DROP TABLE IF EXISTS public.postgres_fallback_test CASCADE;
            CREATE TABLE public.postgres_fallback_test (
                id SERIAL PRIMARY KEY,
                name VARCHAR(50),
                created_at TIMESTAMP DEFAULT NOW()
            );
        """)
        await conn.execute("""
            INSERT INTO public.postgres_fallback_test (name) VALUES ('fallback_test');
        """)
        await conn.close()
        print("✅ Fallback test table created")
    except Exception as e:
        print(f"❌ Failed to create test table: {e}")
        return False
    
    # PostgreSQL connector configuration
    connector_config = {
        "name": "postgres-fallback-test",
        "config": {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "database.hostname": parsed.hostname,
            "database.port": str(parsed.port),
            "database.user": parsed.username or "yugabyte",
            "database.password": parsed.password or "",
            "database.dbname": parsed.path.lstrip('/'),
            "database.server.name": "postgres-fallback",
            "table.include.list": "public.postgres_fallback_test",
            
            # PostgreSQL specific
            "slot.name": "postgres_fallback_slot",
            "publication.name": "postgres_fallback_pub",
            "publication.autocreate.mode": "filtered",
            
            "snapshot.mode": "never",
            "errors.tolerance": "all"
        }
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            # Delete existing
            try:
                async with session.delete(f"{connect_url}/connectors/postgres-fallback-test") as response:
                    if response.status in [200, 204]:
                        print("🧹 Removed existing fallback connector")
                    await asyncio.sleep(2)
            except:
                pass
            
            # Create connector
            print("🔌 Creating PostgreSQL fallback connector...")
            async with session.post(
                f"{connect_url}/connectors",
                json=connector_config,
                headers={"Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=60)
            ) as response:
                response_text = await response.text()
                
                if response.status == 201:
                    print("✅ PostgreSQL fallback connector created!")
                    
                    # Check status
                    await asyncio.sleep(5)
                    async with session.get(f"{connect_url}/connectors/postgres-fallback-test/status") as status_response:
                        if status_response.status == 200:
                            status_data = await status_response.json()
                            connector_state = status_data.get('connector', {}).get('state', 'UNKNOWN')
                            print(f"✅ Fallback connector state: {connector_state}")
                            
                            if connector_state == "RUNNING":
                                print("✅ PostgreSQL fallback approach WORKS!")
                                print("💡 Recommendation: Consider using PostgreSQL connector for YugabyteDB")
                                return True
                            else:
                                print(f"⚠️  Fallback connector not running: {connector_state}")
                                return False
                else:
                    print(f"❌ PostgreSQL fallback failed: {response.status}")
                    print(f"Response: {response_text}")
                    return False
        
    except Exception as e:
        print(f"❌ PostgreSQL fallback test failed: {e}")
        return False

if __name__ == "__main__":
    asyncio.run(test_postgresql_connector_fallback())