#!/usr/bin/env python3
"""
End-to-End CDC Pipeline Test
Tests the complete flow: YugabyteDB → Debezium → Kafka → BigQuery
"""
import asyncio
import aiohttp
import json
import os
import asyncpg
import time
from urllib.parse import urlparse
from google.cloud import bigquery
from google.oauth2 import service_account

class EndToEndCDCTest:
    def __init__(self):
        self.connect_url = os.getenv("DEBEZIUM_CONNECTOR_URL", "http://localhost:8083")
        self.database_url = os.getenv("DATABASE_URL", "postgresql://yugabyte@localhost:5433/yugabyte")
        self.master_addresses = os.getenv("YUGABYTE_MASTER_ADDRESSES", "localhost:7100")
        # Use same environment variables as main app
        self.bq_project = os.getenv("BIGQUERY_PROJECT_ID") or os.getenv("GOOGLE_CLOUD_PROJECT")
        self.bq_dataset = os.getenv("BIGQUERY_DATASET", "cdc_test_dataset")
        self.test_table = "e2e_cdc_test"
        self.bq_table = f"{self.bq_dataset}.{self.test_table}"
        self.connector_name = "e2e-cdc-test-connector"
        
        # Initialize BigQuery client
        self.bq_client = None
        self._init_bigquery_client()
        
    def _init_bigquery_client(self):
        """Initialize BigQuery client with service account"""
        if not self.bq_project:
            print(f"❌ No BigQuery project ID found. Checked:")
            print(f"   BIGQUERY_PROJECT_ID: {os.getenv('BIGQUERY_PROJECT_ID')}")
            print(f"   GOOGLE_CLOUD_PROJECT: {os.getenv('GOOGLE_CLOUD_PROJECT')}")
            self.bq_client = None
            return
            
        try:
            service_account_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
            print(f"🔍 Initializing BigQuery for project: {self.bq_project}")
            print(f"🔍 Service account path: {service_account_path}")
            
            if service_account_path and os.path.exists(service_account_path):
                credentials = service_account.Credentials.from_service_account_file(service_account_path)
                self.bq_client = bigquery.Client(credentials=credentials, project=self.bq_project)
                print(f"✅ BigQuery client initialized with service account")
            else:
                # Try default credentials
                self.bq_client = bigquery.Client(project=self.bq_project)
                print(f"✅ BigQuery client initialized with default credentials")
                
            print(f"✅ BigQuery client ready for project: {self.bq_project}")
        except Exception as e:
            print(f"❌ Failed to initialize BigQuery client: {e}")
            self.bq_client = None

    async def create_yugabyte_test_table(self):
        """Create test table in YugabyteDB"""
        print("📊 Creating YugabyteDB test table...")
        
        try:
            conn = await asyncpg.connect(self.database_url)
            
            # Drop and recreate test table
            await conn.execute(f"""
                DROP TABLE IF EXISTS public.{self.test_table} CASCADE;
                CREATE TABLE public.{self.test_table} (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    email VARCHAR(100),
                    age INTEGER,
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                );
            """)
            
            # Insert initial test data
            await conn.execute(f"""
                INSERT INTO public.{self.test_table} (name, email, age) VALUES 
                ('Alice Johnson', 'alice@example.com', 28),
                ('Bob Smith', 'bob@example.com', 35),
                ('Carol Davis', 'carol@example.com', 42);
            """)
            
            # Verify data
            count = await conn.fetchval(f"SELECT COUNT(*) FROM public.{self.test_table}")
            print(f"✅ YugabyteDB test table created with {count} initial rows")
            
            await conn.close()
            return True
            
        except Exception as e:
            print(f"❌ Failed to create YugabyteDB test table: {e}")
            return False

    def create_bigquery_dataset_and_table(self):
        """Create BigQuery dataset and table"""
        print("📊 Creating BigQuery dataset and table...")
        
        if not self.bq_client:
            print("❌ BigQuery client not available")
            return False
        
        try:
            # Create dataset if not exists
            dataset_id = f"{self.bq_project}.{self.bq_dataset}"
            dataset = bigquery.Dataset(dataset_id)
            dataset.location = "US"
            
            try:
                dataset = self.bq_client.create_dataset(dataset, timeout=30)
                print(f"✅ Created BigQuery dataset: {dataset_id}")
            except Exception as e:
                if "already exists" in str(e).lower():
                    print(f"✅ BigQuery dataset already exists: {dataset_id}")
                else:
                    raise e
            
            # Create table schema matching YugabyteDB
            table_id = f"{self.bq_project}.{self.bq_dataset}.{self.test_table}"
            schema = [
                bigquery.SchemaField("id", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("name", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("email", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("age", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("created_at", "TIMESTAMP", mode="NULLABLE"),
                bigquery.SchemaField("updated_at", "TIMESTAMP", mode="NULLABLE"),
                # CDC metadata fields
                bigquery.SchemaField("__op", "STRING", mode="NULLABLE"),
                bigquery.SchemaField("__source_ts_ms", "INTEGER", mode="NULLABLE"),
                bigquery.SchemaField("__deleted", "STRING", mode="NULLABLE"),
            ]
            
            table = bigquery.Table(table_id, schema=schema)
            
            try:
                table = self.bq_client.create_table(table)
                print(f"✅ Created BigQuery table: {table_id}")
            except Exception as e:
                if "already exists" in str(e).lower():
                    # Delete existing table to start fresh
                    self.bq_client.delete_table(table_id)
                    table = self.bq_client.create_table(table)
                    print(f"✅ Recreated BigQuery table: {table_id}")
                else:
                    raise e
            
            return True
            
        except Exception as e:
            print(f"❌ Failed to create BigQuery dataset/table: {e}")
            return False

    async def create_debezium_connector(self):
        """Create Debezium connector with BigQuery sink configuration"""
        print("🔌 Creating end-to-end Debezium connector...")
        
        parsed = urlparse(self.database_url)
        
        # Full connector configuration including BigQuery sink transforms
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
                "database.dbname": parsed.path.lstrip('/'),
                "database.server.name": "e2e-test-server",
                "table.include.list": f"public.{self.test_table}",
                "database.master.addresses": self.master_addresses,
                
                # Simplified configuration (bash-style approach)
                "before.image.mode": "never",
                "snapshot.mode": "initial",  # Start with initial snapshot
                
                # Key and value converters
                "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                "value.converter": "org.apache.kafka.connect.json.JsonConverter", 
                "key.converter.schemas.enable": "false",
                "value.converter.schemas.enable": "false",
                
                # Transforms for BigQuery compatibility
                "transforms": "unwrap,addTopicPrefix",
                "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
                "transforms.unwrap.drop.tombstones": "false",
                "transforms.unwrap.delete.handling.mode": "rewrite",
                
                "transforms.addTopicPrefix.type": "org.apache.kafka.connect.transforms.RegexRouter",
                "transforms.addTopicPrefix.regex": f"e2e-test-server\\.public\\.{self.test_table}",
                "transforms.addTopicPrefix.replacement": f"bigquery-{self.bq_project}-{self.test_table}",
                
                # Error handling
                "errors.tolerance": "all",
                "errors.log.enable": "true",
                "errors.log.include.messages": "true"
            }
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                # Delete existing connector
                try:
                    async with session.delete(f"{self.connect_url}/connectors/{self.connector_name}") as response:
                        if response.status in [200, 204]:
                            print("🧹 Removed existing test connector")
                        await asyncio.sleep(3)
                except:
                    pass
                
                # Create new connector
                print("🔌 Creating CDC connector...")
                print("Configuration summary:")
                for key, value in connector_config["config"].items():
                    if "password" not in key.lower() and "transforms" not in key:
                        print(f"  {key}: {value}")
                
                async with session.post(
                    f"{self.connect_url}/connectors",
                    json=connector_config,
                    headers={"Content-Type": "application/json"},
                    timeout=aiohttp.ClientTimeout(total=90)
                ) as response:
                    response_text = await response.text()
                    
                    if response.status == 201:
                        print("✅ Debezium connector created successfully!")
                        return True
                    else:
                        print(f"❌ Failed to create connector: {response.status}")
                        print(f"Response: {response_text}")
                        
                        if "nullpointerexception" in response_text.lower() and "beforeimage" in response_text.lower():
                            print("🚨 CONFIRMED: Before image NullPointerException!")
                        
                        return False
            
        except Exception as e:
            print(f"❌ Connector creation failed: {e}")
            return False

    async def wait_for_connector_health(self, timeout_seconds=60):
        """Wait for connector to be healthy and running"""
        print(f"⏳ Waiting for connector health (timeout: {timeout_seconds}s)...")
        
        start_time = time.time()
        
        try:
            async with aiohttp.ClientSession() as session:
                while (time.time() - start_time) < timeout_seconds:
                    async with session.get(f"{self.connect_url}/connectors/{self.connector_name}/status") as response:
                        if response.status == 200:
                            status_data = await response.json()
                            connector_state = status_data.get('connector', {}).get('state', 'UNKNOWN')
                            tasks = status_data.get('tasks', [])
                            
                            print(f"🔍 Connector state: {connector_state}")
                            
                            if connector_state == "RUNNING":
                                all_tasks_running = True
                                for i, task in enumerate(tasks):
                                    task_state = task.get('state', 'UNKNOWN')
                                    print(f"🔍 Task {i}: {task_state}")
                                    
                                    if task_state == 'FAILED':
                                        task_trace = task.get('trace', 'No trace available')
                                        print(f"❌ Task failed: {task_trace[:300]}...")
                                        return False
                                    elif task_state != 'RUNNING':
                                        all_tasks_running = False
                                
                                if all_tasks_running:
                                    print("✅ Connector and all tasks are RUNNING!")
                                    return True
                            
                            elif connector_state == "FAILED":
                                print("❌ Connector failed!")
                                return False
                    
                    await asyncio.sleep(5)
                
                print(f"⏰ Timeout waiting for connector health")
                return False
                
        except Exception as e:
            print(f"❌ Error checking connector health: {e}")
            return False

    async def add_test_data_and_monitor(self):
        """Add new data to YugabyteDB and monitor BigQuery for changes"""
        print("📝 Adding new test data to trigger CDC...")
        
        try:
            # Add new rows to YugabyteDB
            conn = await asyncpg.connect(self.database_url)
            
            new_rows = [
                ('David Wilson', 'david@example.com', 29),
                ('Eva Brown', 'eva@example.com', 33),
                ('Frank Miller', 'frank@example.com', 45)
            ]
            
            for name, email, age in new_rows:
                await conn.execute(f"""
                    INSERT INTO public.{self.test_table} (name, email, age) 
                    VALUES ($1, $2, $3)
                """, name, email, age)
                print(f"✅ Added: {name}")
            
            # Update an existing row
            await conn.execute(f"""
                UPDATE public.{self.test_table} 
                SET email = 'alice.updated@example.com', updated_at = NOW()
                WHERE name = 'Alice Johnson'
            """)
            print("✅ Updated Alice's email")
            
            # Delete a row
            await conn.execute(f"""
                DELETE FROM public.{self.test_table} 
                WHERE name = 'Bob Smith'
            """)
            print("✅ Deleted Bob Smith")
            
            # Get final count
            final_count = await conn.fetchval(f"SELECT COUNT(*) FROM public.{self.test_table}")
            print(f"✅ YugabyteDB now has {final_count} rows total")
            
            await conn.close()
            
            return True
            
        except Exception as e:
            print(f"❌ Failed to add test data: {e}")
            return False

    def check_bigquery_data(self, timeout_seconds=120):
        """Check if data appears in BigQuery"""
        print(f"🔍 Monitoring BigQuery for CDC data (timeout: {timeout_seconds}s)...")
        
        if not self.bq_client:
            print("❌ BigQuery client not available")
            return False
        
        start_time = time.time()
        table_id = f"{self.bq_project}.{self.bq_dataset}.{self.test_table}"
        
        try:
            while (time.time() - start_time) < timeout_seconds:
                query = f"""
                    SELECT COUNT(*) as total_rows,
                           COUNT(CASE WHEN __op = 'c' THEN 1 END) as creates,
                           COUNT(CASE WHEN __op = 'u' THEN 1 END) as updates,
                           COUNT(CASE WHEN __op = 'd' THEN 1 END) as deletes
                    FROM `{table_id}`
                """
                
                try:
                    query_job = self.bq_client.query(query)
                    results = list(query_job.result())
                    
                    if results:
                        row = results[0]
                        total = row.total_rows or 0
                        creates = row.creates or 0
                        updates = row.updates or 0
                        deletes = row.deletes or 0
                        
                        print(f"📊 BigQuery: {total} rows (C:{creates}, U:{updates}, D:{deletes})")
                        
                        if total > 0:
                            print("✅ SUCCESS! Data found in BigQuery!")
                            
                            # Show sample data
                            sample_query = f"SELECT * FROM `{table_id}` ORDER BY created_at DESC LIMIT 5"
                            sample_job = self.bq_client.query(sample_query)
                            sample_results = list(sample_job.result())
                            
                            print("📋 Sample BigQuery data:")
                            for row in sample_results:
                                print(f"  ID:{row.id} Name:{row.name} Op:{row.__op}")
                            
                            return True
                    
                except Exception as query_error:
                    print(f"⚠️  Query error (retrying): {query_error}")
                
                print(f"⏳ Waiting for data... ({int(time.time() - start_time)}s elapsed)")
                time.sleep(10)
            
            print(f"⏰ Timeout - no data appeared in BigQuery after {timeout_seconds}s")
            return False
            
        except Exception as e:
            print(f"❌ Error checking BigQuery data: {e}")
            return False

    async def cleanup(self):
        """Clean up test resources"""
        print("🧹 Cleaning up test resources...")
        
        try:
            # Delete Debezium connector
            async with aiohttp.ClientSession() as session:
                async with session.delete(f"{self.connect_url}/connectors/{self.connector_name}") as response:
                    if response.status in [200, 204]:
                        print("✅ Removed Debezium connector")
            
            # Clean up BigQuery table
            if self.bq_client:
                table_id = f"{self.bq_project}.{self.bq_dataset}.{self.test_table}"
                try:
                    self.bq_client.delete_table(table_id)
                    print("✅ Removed BigQuery test table")
                except:
                    pass
            
            # Clean up YugabyteDB table
            conn = await asyncpg.connect(self.database_url)
            await conn.execute(f"DROP TABLE IF EXISTS public.{self.test_table} CASCADE")
            await conn.close()
            print("✅ Removed YugabyteDB test table")
            
        except Exception as e:
            print(f"⚠️  Cleanup error: {e}")

    async def run_full_test(self):
        """Run the complete end-to-end test"""
        print("🚀" + "="*60)
        print("🚀 END-TO-END CDC PIPELINE TEST")
        print("🚀 YugabyteDB → Debezium → Kafka → BigQuery")
        print("🚀" + "="*60)
        
        # Validate required environment variables
        print("🔍 Validating environment configuration...")
        required_vars = {
            "DATABASE_URL": self.database_url,
            "DEBEZIUM_CONNECTOR_URL": self.connect_url,
            "BigQuery Project": self.bq_project,
        }
        
        missing_vars = []
        for var_name, var_value in required_vars.items():
            if not var_value:
                missing_vars.append(var_name)
            else:
                print(f"✅ {var_name}: {var_value}")
        
        if missing_vars:
            print(f"❌ Missing required environment variables: {missing_vars}")
            print("💡 Make sure you have set:")
            print("   - BIGQUERY_PROJECT_ID (or GOOGLE_CLOUD_PROJECT)")
            print("   - DATABASE_URL")
            print("   - DEBEZIUM_CONNECTOR_URL")
            return False
        
        if not self.bq_client:
            print("❌ BigQuery client initialization failed - cannot proceed")
            return False
        
        print("✅ Environment validation passed")
        
        success = False
        
        try:
            # Step 1: Create YugabyteDB test table
            if not await self.create_yugabyte_test_table():
                return False
            
            # Step 2: Create BigQuery dataset and table
            if not self.create_bigquery_dataset_and_table():
                return False
            
            # Step 3: Create Debezium connector
            if not await self.create_debezium_connector():
                return False
            
            # Step 4: Wait for connector to be healthy
            if not await self.wait_for_connector_health():
                return False
            
            # Step 5: Add test data to trigger CDC
            if not await self.add_test_data_and_monitor():
                return False
            
            # Step 6: Check if data appears in BigQuery
            if not self.check_bigquery_data():
                return False
            
            success = True
            
        finally:
            # Always cleanup
            await self.cleanup()
        
        if success:
            print("🎉" + "="*60)
            print("🎉 END-TO-END TEST PASSED!")
            print("🎉 Complete CDC pipeline is working!")
            print("🎉" + "="*60)
        else:
            print("❌" + "="*60)
            print("❌ END-TO-END TEST FAILED")
            print("❌ Check logs above for specific failure point")
            print("❌" + "="*60)
        
        return success

async def run_e2e_test():
    """Main test runner"""
    test = EndToEndCDCTest()
    return await test.run_full_test()

if __name__ == "__main__":
    asyncio.run(run_e2e_test())