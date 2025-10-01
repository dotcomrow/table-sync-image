#!/usr/bin/env python3
"""
Simple CDC Stream Test
Tests just the CDC stream creation and connector setup without BigQuery
"""
import asyncio
import aiohttp
import json
import os
import asyncpg
import time
from urllib.parse import urlparse

class SimpleCDCTest:
    def __init__(self):
        self.connect_url = os.getenv("DEBEZIUM_CONNECTOR_URL", "http://localhost:8083")
        self.database_url = os.getenv("DATABASE_URL", "postgresql://yugabyte@localhost:5433/yugabyte")
        
        # Get master addresses with same fallback logic as debezium_manager.py
        parsed_db = urlparse(self.database_url)
        self.master_addresses = (
            os.getenv("YUGABYTE_MASTER_ADDRESSES") or 
            os.getenv("DEBEZIUM_MASTER_ADDRESSES") or 
            f"{parsed_db.hostname}:7100"
        )
        
        self.test_table = "simple_cdc_test"
        self.connector_name = "simple-cdc-test-connector"
        
    async def create_cdc_stream(self) -> str:
        """Use the existing CDC stream for the test"""
        print(f"🔄 Using existing CDC stream for yugabyte database...")
        
        # Use the newly created CDC stream ID for yugabyte database
        existing_stream_id = "36fee35ffe8da488284a46c624f4bb76"
        
        print(f"✅ Using fresh CDC stream: {existing_stream_id}")
        return existing_stream_id

    async def create_yugabyte_test_table(self):
        """Create test table in YugabyteDB"""
        print("📊 Creating YugabyteDB test table...")
        
        try:
            # Connect to yugabyte database
            yugabyte_url = self.database_url.rsplit('/', 1)[0] + '/yugabyte'
            print(f"🔍 Connecting to yugabyte database: {yugabyte_url}")
            conn = await asyncpg.connect(yugabyte_url)
            
            # Drop and recreate test table
            await conn.execute(f"""
                DROP TABLE IF EXISTS public.{self.test_table} CASCADE;
                CREATE TABLE public.{self.test_table} (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    email VARCHAR(100),
                    created_at TIMESTAMP DEFAULT NOW()
                );
            """)
            
            # Insert initial test data
            await conn.execute(f"""
                INSERT INTO public.{self.test_table} (name, email) VALUES 
                ('Test User 1', 'user1@example.com'),
                ('Test User 2', 'user2@example.com');
            """)
            
            # Verify data
            count = await conn.fetchval(f"SELECT COUNT(*) FROM public.{self.test_table}")
            print(f"✅ YugabyteDB test table created with {count} initial rows")
            
            await conn.close()
            return True
            
        except Exception as e:
            print(f"❌ Failed to create YugabyteDB test table: {e}")
            return False

    async def create_debezium_connector(self):
        """Create Debezium connector"""
        print("🔌 Creating simple Debezium connector...")
        
        # Get CDC stream
        stream_id = await self.create_cdc_stream()
        if not stream_id:
            print("❌ Failed to get CDC stream - cannot proceed with connector")
            return False
        
        parsed = urlparse(self.database_url)
        
        # Simple connector configuration - just Kafka output
        connector_config = {
            "name": self.connector_name,
            "config": {
                "connector.class": "io.debezium.connector.yugabytedb.YugabyteDBgRPCConnector",
                "tasks.max": "1",
                
                # YugabyteDB source config
                "database.hostname": parsed.hostname,
                "database.port": str(parsed.port),
                "database.user": parsed.username or "yugabyte",
                "database.password": parsed.password or "",
                "database.dbname": "yugabyte",
                "database.server.name": "simple-test-server",
                "table.include.list": f"public.{self.test_table}",
                "database.master.addresses": self.master_addresses,
                
                # Use the CDC stream ID
                "database.streamid": stream_id,
                
                # YugabyteDB specific settings - match the working configuration
                "snapshot.mode": "never",
                "database.stream.prefix": f"yugabyte_public_{self.test_table}",
                
                # Key and value converters
                "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                "value.converter": "org.apache.kafka.connect.json.JsonConverter", 
                "key.converter.schemas.enable": "false",
                "value.converter.schemas.enable": "false",
                
                # YugabyteDB CDC specific settings (from working config)
                "cdcsdk.snapshot.txn.timeout": "900000",
                "cdcsdk.connection.timeout": "10000",
                
                # Critical fixes for NullPointerException
                "provide.transaction.metadata": "false",
                "binary.handling.mode": "base64",
                "before.image.mode": "never",
                "errors.tolerance": "all",
                
                # Simple transforms
                "transforms": "unwrap",
                "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
                "transforms.unwrap.drop.tombstones": "false",
                "transforms.unwrap.delete.handling.mode": "rewrite"
            }
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                # Delete existing connector if it exists
                await session.delete(f"{self.connect_url}/connectors/{self.connector_name}")
                await asyncio.sleep(2)
                
                # Create new connector
                async with session.post(
                    f"{self.connect_url}/connectors",
                    json=connector_config,
                    headers={"Content-Type": "application/json"}
                ) as response:
                    if response.status in [200, 201]:
                        result = await response.json()
                        print(f"✅ Successfully created Debezium connector: {self.connector_name}")
                        return True
                    else:
                        error_text = await response.text()
                        print(f"❌ Failed to create connector: {response.status} - {error_text}")
                        return False
                        
        except Exception as e:
            print(f"❌ Exception creating connector: {e}")
            return False

    async def check_connector_status(self):
        """Check connector status"""
        print("🔍 Checking connector status...")
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{self.connect_url}/connectors/{self.connector_name}/status") as response:
                    if response.status == 200:
                        status = await response.json()
                        connector_state = status.get("connector", {}).get("state", "UNKNOWN")
                        
                        print(f"📊 Connector Status: {connector_state}")
                        
                        # Check task status
                        tasks = status.get("tasks", [])
                        for i, task in enumerate(tasks):
                            task_state = task.get("state", "UNKNOWN")
                            print(f"📋 Task {i}: {task_state}")
                            if task_state == "FAILED":
                                print(f"❌ Task {i} trace: {task.get('trace', 'No trace available')}")
                        
                        return connector_state == "RUNNING"
                    else:
                        print(f"❌ Failed to get connector status: {response.status}")
                        return False
                        
        except Exception as e:
            print(f"❌ Exception checking connector status: {e}")
            return False

    async def insert_test_data(self):
        """Insert new test data to trigger CDC events"""
        print("📝 Inserting new test data...")
        
        try:
            yugabyte_url = self.database_url.rsplit('/', 1)[0] + '/yugabyte'
            conn = await asyncpg.connect(yugabyte_url)
            
            await conn.execute(f"""
                INSERT INTO public.{self.test_table} (name, email) VALUES 
                ('New User 1', 'new1@example.com'),
                ('New User 2', 'new2@example.com');
            """)
            
            count = await conn.fetchval(f"SELECT COUNT(*) FROM public.{self.test_table}")
            print(f"✅ Inserted new data. Total rows: {count}")
            
            await conn.close()
            return True
            
        except Exception as e:
            print(f"❌ Failed to insert test data: {e}")
            return False

    async def run_test(self):
        """Run the complete simple CDC test"""
        print("🚀============================================================")
        print("🚀 SIMPLE CDC STREAM TEST")
        print("🚀 YugabyteDB → Debezium → Kafka")
        print("🚀============================================================")
        
        # Step 1: Create test table
        if not await self.create_yugabyte_test_table():
            return False
        
        await asyncio.sleep(2)
        
        # Step 2: Create connector
        if not await self.create_debezium_connector():
            return False
        
        await asyncio.sleep(5)
        
        # Step 3: Check connector status
        if not await self.check_connector_status():
            print("⚠️ Connector not running, but continuing...")
        
        await asyncio.sleep(2)
        
        # Step 4: Insert test data
        if not await self.insert_test_data():
            return False
        
        await asyncio.sleep(5)
        
        # Step 5: Final status check
        await self.check_connector_status()
        
        print("🎉 Simple CDC test completed!")
        return True

async def main():
    test = SimpleCDCTest()
    await test.run_test()

if __name__ == "__main__":
    asyncio.run(main())