#!/usr/bin/env python3
"""
Component Test 3: Minimal Debezium Connector Test
Creates the absolute minimal YugabyteDB connector to isolate the issue
"""
import asyncio
import aiohttp
import json
import os
import asyncpg
from urllib.parse import urlparse

async def create_minimal_test_table():
    """Create a minimal test table"""
    print("📊 Creating minimal test table...")
    
    database_url = os.getenv("DATABASE_URL", "postgresql://yugabyte@localhost:5433/yugabyte")
    
    try:
        conn = await asyncpg.connect(database_url)
        
        # Drop and recreate test table
        await conn.execute("""
            DROP TABLE IF EXISTS public.minimal_cdc_test CASCADE;
            CREATE TABLE public.minimal_cdc_test (
                id SERIAL PRIMARY KEY,
                name VARCHAR(50),
                created_at TIMESTAMP DEFAULT NOW()
            );
        """)
        
        # Insert one test row
        await conn.execute("""
            INSERT INTO public.minimal_cdc_test (name) VALUES ('test_row');
        """)
        
        await conn.close()
        print("✅ Test table created successfully")
        return True
        
    except Exception as e:
        print(f"❌ Failed to create test table: {e}")
        return False

async def test_minimal_connector():
    """Test minimal YugabyteDB connector configuration"""
    print("🔌 Testing Minimal Debezium Connector...")
    
    # Create test table first
    if not await create_minimal_test_table():
        return False
    
    connect_url = os.getenv("DEBEZIUM_CONNECTOR_URL", "http://localhost:8083")
    database_url = os.getenv("DATABASE_URL", "postgresql://yugabyte@localhost:5433/yugabyte")
    master_addresses = os.getenv("YUGABYTE_MASTER_ADDRESSES", "localhost:7100")
    
    parsed = urlparse(database_url)
    
    # ULTRA MINIMAL configuration - only absolutely required parameters
    connector_config = {
        "name": "minimal-test-connector",
        "config": {
            "connector.class": "io.debezium.connector.yugabytedb.YugabyteDBgRPCConnector",
            "database.hostname": parsed.hostname,
            "database.port": str(parsed.port),
            "database.user": parsed.username or "yugabyte",
            "database.password": parsed.password or "",
            "database.dbname": parsed.path.lstrip('/'),
            "database.server.name": "minimal-test",
            "table.include.list": "public.minimal_cdc_test",
            "database.master.addresses": master_addresses,
            
            # ONLY these minimal parameters
            "snapshot.mode": "never",
            "errors.tolerance": "all"
        }
    }
    
    try:
        async with aiohttp.ClientSession() as session:
            # Delete existing connector
            try:
                async with session.delete(f"{connect_url}/connectors/minimal-test-connector") as response:
                    if response.status in [200, 204]:
                        print("🧹 Removed existing test connector")
                    await asyncio.sleep(2)
            except:
                pass
            
            # Create new connector
            print("🔌 Creating minimal connector...")
            print("Configuration:")
            for key, value in connector_config["config"].items():
                if "password" not in key.lower():
                    print(f"  {key}: {value}")
            
            async with session.post(
                f"{connect_url}/connectors",
                json=connector_config,
                headers={"Content-Type": "application/json"},
                timeout=aiohttp.ClientTimeout(total=60)
            ) as response:
                response_text = await response.text()
                
                if response.status == 201:
                    print("✅ Minimal connector created successfully!")
                    
                    # Check status briefly
                    await asyncio.sleep(5)
                    async with session.get(f"{connect_url}/connectors/minimal-test-connector/status") as status_response:
                        if status_response.status == 200:
                            status_data = await status_response.json()
                            connector_state = status_data.get('connector', {}).get('state', 'UNKNOWN')
                            print(f"✅ Connector state: {connector_state}")
                            
                            tasks = status_data.get('tasks', [])
                            for i, task in enumerate(tasks):
                                task_state = task.get('state', 'UNKNOWN')
                                print(f"✅ Task {i}: {task_state}")
                                
                                if task_state == 'FAILED':
                                    task_trace = task.get('trace', 'No trace available')
                                    print(f"❌ Task failed: {task_trace[:500]}...")
                                    
                                    if "nullpointerexception" in task_trace.lower() and "beforeimage" in task_trace.lower():
                                        print("🚨 CONFIRMED: Before image NullPointerException in minimal config!")
                                        return False
                        
                        print("✅ Minimal connector test PASSED")
                        return True
                else:
                    print(f"❌ Failed to create connector: {response.status}")
                    print(f"Response: {response_text}")
                    
                    if "nullpointerexception" in response_text.lower() and "beforeimage" in response_text.lower():
                        print("🚨 CONFIRMED: Before image NullPointerException in HTTP response!")
                        return False
                    
                    return False
        
    except Exception as e:
        print(f"❌ Minimal connector test FAILED: {e}")
        return False

if __name__ == "__main__":
    asyncio.run(test_minimal_connector())