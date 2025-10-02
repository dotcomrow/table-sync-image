#!/usr/bin/env python3
"""
Comprehensive E2E Test for YugabyteDB <-> BigQuery Sync Pipeline

This test validates the complete bidirectional sync pipeline:
1. Create test table in YugabyteDB
2. Set up CDC connector (YugabyteDB -> Kafka -> BigQuery)
3. Insert data and verify flow to BigQuery
4. Annotate existing BigQuery table and sync back to YugabyteDB
5. Verify bidirectional sync is working

Based on successful E2E testing session findings.
"""

import asyncio
import json
import subprocess
import time
import requests
import psycopg2
from typing import Dict, List, Optional, Any
import os
from datetime import datetime

class ComprehensiveE2ETest:
    def __init__(self):
        # Kafka Connect configuration
        self.kafka_connect_url = "http://localhost:8083"  # Adjust for your environment
        self.test_database = "yugabyte"
        self.test_schema = "public" 
        self.test_table = "comprehensive_e2e_test"
        self.bigquery_project = "k8s-kafka-774d"
        self.bigquery_dataset = "cdc_test_dataset"
        self.bigquery_table = f"comprehensive_e2e_test"
        
        # YugabyteDB connection
        self.yb_host = "yb-tserver-service.yugabyte.svc.cluster.local"
        self.yb_port = 5433
        self.yb_user = "postgres"
        self.yb_password = ""
        
        # Test state tracking
        self.created_resources = []
        self.test_results = {}
        
    def log(self, message: str, level: str = "INFO"):
        """Enhanced logging with timestamp"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{timestamp}] {level}: {message}")
    
    async def run_comprehensive_test(self) -> bool:
        """Run the complete E2E test suite"""
        self.log("🚀 Starting Comprehensive E2E Test for YugabyteDB <-> BigQuery Sync")
        
        try:
            # Phase 1: Setup and Infrastructure
            if not await self.phase1_setup_infrastructure():
                return False
                
            # Phase 2: YugabyteDB -> BigQuery Pipeline
            if not await self.phase2_yugabyte_to_bigquery():
                return False
                
            # Phase 3: BigQuery -> YugabyteDB Pipeline  
            if not await self.phase3_bigquery_to_yugabyte():
                return False
                
            # Phase 4: Bidirectional Sync Validation
            if not await self.phase4_bidirectional_validation():
                return False
                
            # Phase 5: Cleanup and Summary
            await self.phase5_cleanup_and_summary()
            
            self.log("✅ Comprehensive E2E Test PASSED! Full bidirectional sync working.")
            return True
            
        except Exception as e:
            self.log(f"❌ Comprehensive E2E Test FAILED: {e}", "ERROR")
            await self.emergency_cleanup()
            return False
    
    async def phase1_setup_infrastructure(self) -> bool:
        """Phase 1: Set up test infrastructure"""
        self.log("📋 Phase 1: Setting up test infrastructure")
        
        try:
            # 1.1: Create test table in YugabyteDB
            self.log("Creating test table in YugabyteDB...")
            if not await self.create_yugabyte_test_table():
                return False
                
            # 1.2: Create corresponding BigQuery table
            self.log("Creating BigQuery test table...")
            if not await self.create_bigquery_test_table():
                return False
                
            # 1.3: Verify Kafka Connect is healthy
            self.log("Verifying Kafka Connect health...")
            if not await self.verify_kafka_connect_health():
                return False
                
            # 1.4: Get or create shared CDC stream
            self.log("Setting up shared CDC stream...")
            self.shared_cdc_stream = await self.get_or_create_shared_cdc_stream()
            if not self.shared_cdc_stream:
                return False
                
            self.log("✅ Phase 1 completed successfully")
            return True
            
        except Exception as e:
            self.log(f"❌ Phase 1 failed: {e}", "ERROR")
            return False
    
    async def phase2_yugabyte_to_bigquery(self) -> bool:
        """Phase 2: Test YugabyteDB -> BigQuery pipeline"""
        self.log("📊 Phase 2: Testing YugabyteDB -> BigQuery pipeline")
        
        try:
            # 2.1: Create CDC connector with validated configuration
            self.log("Creating CDC connector with E2E validated config...")
            if not await self.create_cdc_connector():
                return False
                
            # 2.2: Insert test data into YugabyteDB
            self.log("Inserting test data into YugabyteDB...")
            test_records = [
                {"id": 1001, "name": "E2E User 1", "email": "e2e1@test.com", "age": 25},
                {"id": 1002, "name": "E2E User 2", "email": "e2e2@test.com", "age": 30},
                {"id": 1003, "name": "E2E User 3", "email": "e2e3@test.com", "age": 35}
            ]
            
            if not await self.insert_yugabyte_data(test_records):
                return False
                
            # 2.3: Wait for CDC processing
            self.log("Waiting for CDC processing...")
            await asyncio.sleep(30)  # Allow time for CDC pipeline
            
            # 2.4: Create BigQuery sink connector (with lessons learned)
            self.log("Creating BigQuery sink connector...")
            if not await self.create_bigquery_sink_connector():
                return False
                
            # 2.5: Verify data in BigQuery
            self.log("Verifying data reached BigQuery...")
            if not await self.verify_bigquery_data(test_records):
                return False
                
            self.log("✅ Phase 2 completed successfully")
            return True
            
        except Exception as e:
            self.log(f"❌ Phase 2 failed: {e}", "ERROR")
            return False
    
    async def phase3_bigquery_to_yugabyte(self) -> bool:
        """Phase 3: Test BigQuery -> YugabyteDB pipeline"""
        self.log("📤 Phase 3: Testing BigQuery -> YugabyteDB pipeline")
        
        try:
            # 3.1: Simulate BigQuery table annotation (metadata marking for sync)
            self.log("Annotating BigQuery table for sync back to YugabyteDB...")
            if not await self.annotate_bigquery_table():
                return False
                
            # 3.2: Insert new data directly into BigQuery
            self.log("Inserting new data into BigQuery...")
            bigquery_records = [
                {"id": 2001, "name": "BQ User 1", "email": "bq1@test.com", "age": 28},
                {"id": 2002, "name": "BQ User 2", "email": "bq2@test.com", "age": 33}
            ]
            
            if not await self.insert_bigquery_data(bigquery_records):
                return False
                
            # 3.3: Set up BigQuery -> YugabyteDB sync connector
            self.log("Creating BigQuery source connector...")
            if not await self.create_bigquery_source_connector():
                return False
                
            # 3.4: Wait for sync processing
            self.log("Waiting for BigQuery -> YugabyteDB sync...")
            await asyncio.sleep(30)
            
            # 3.5: Verify data in YugabyteDB
            self.log("Verifying data synced to YugabyteDB...")
            if not await self.verify_yugabyte_sync_data(bigquery_records):
                return False
                
            self.log("✅ Phase 3 completed successfully")
            return True
            
        except Exception as e:
            self.log(f"❌ Phase 3 failed: {e}", "ERROR")
            return False
    
    async def phase4_bidirectional_validation(self) -> bool:
        """Phase 4: Validate bidirectional sync"""
        self.log("🔄 Phase 4: Validating bidirectional sync")
        
        try:
            # 4.1: Update data in YugabyteDB and verify it reaches BigQuery
            self.log("Testing YugabyteDB update -> BigQuery...")
            update_data = {"id": 1001, "name": "Updated E2E User 1", "age": 26}
            
            if not await self.update_yugabyte_data(update_data):
                return False
                
            await asyncio.sleep(20)  # Wait for CDC processing
            
            if not await self.verify_bigquery_update(update_data):
                return False
                
            # 4.2: Update data in BigQuery and verify it reaches YugabyteDB
            self.log("Testing BigQuery update -> YugabyteDB...")
            bq_update_data = {"id": 2001, "name": "Updated BQ User 1", "age": 29}
            
            if not await self.update_bigquery_data(bq_update_data):
                return False
                
            await asyncio.sleep(20)  # Wait for sync processing
            
            if not await self.verify_yugabyte_update(bq_update_data):
                return False
                
            self.log("✅ Phase 4 completed successfully")
            return True
            
        except Exception as e:
            self.log(f"❌ Phase 4 failed: {e}", "ERROR")
            return False
    
    async def create_yugabyte_test_table(self) -> bool:
        """Create test table in YugabyteDB"""
        try:
            # Use kubectl exec to create table
            create_table_sql = f"""
            DROP TABLE IF EXISTS {self.test_table};
            CREATE TABLE {self.test_table} (
                id SERIAL PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                email VARCHAR(255) UNIQUE NOT NULL,
                age INTEGER,
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            );
            """
            
            cmd = [
                "tsh", "kubectl", "exec", "-n", "yugabyte", "yb-tserver-0", "--",
                "ysqlsh", "-h", "yb-tserver-service", "-p", "5433", "-U", "postgres",
                "-c", create_table_sql
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            
            if result.returncode == 0:
                self.log("✅ Test table created in YugabyteDB")
                self.created_resources.append(f"yugabyte_table:{self.test_table}")
                return True
            else:
                self.log(f"❌ Failed to create YugabyteDB table: {result.stderr}", "ERROR")
                return False
                
        except Exception as e:
            self.log(f"❌ Exception creating YugabyteDB table: {e}", "ERROR")
            return False
    
    async def create_bigquery_test_table(self) -> bool:
        """Create corresponding table in BigQuery"""
        try:
            # This would use BigQuery API or gcloud CLI
            # For now, we'll assume it needs to be created manually
            self.log("📋 Please ensure BigQuery table exists:")
            self.log(f"   Project: {self.bigquery_project}")
            self.log(f"   Dataset: {self.bigquery_dataset}")  
            self.log(f"   Table: {self.bigquery_table}")
            self.log("   Schema: id INT64, name STRING, email STRING, age INT64, created_at TIMESTAMP, updated_at TIMESTAMP")
            
            # TODO: Implement actual BigQuery table creation
            return True
            
        except Exception as e:
            self.log(f"❌ Exception creating BigQuery table: {e}", "ERROR")
            return False
    
    async def verify_kafka_connect_health(self) -> bool:
        """Verify Kafka Connect is healthy"""
        try:
            # Use kubectl to check Kafka Connect status
            cmd = [
                "tsh", "kubectl", "exec", "-n", "kafka", "kafka-connect-5dd8c95895-4q84c", "--",
                "curl", "-s", "http://localhost:8083/"
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            
            if result.returncode == 0 and "version" in result.stdout:
                self.log("✅ Kafka Connect is healthy")
                return True
            else:
                self.log("❌ Kafka Connect health check failed", "ERROR")
                return False
                
        except Exception as e:
            self.log(f"❌ Exception checking Kafka Connect health: {e}", "ERROR")
            return False
    
    async def get_or_create_shared_cdc_stream(self) -> Optional[str]:
        """Get or create shared CDC stream using validated approach"""
        try:
            # List existing CDC streams
            cmd = [
                "tsh", "kubectl", "exec", "-n", "yugabyte", "yb-tserver-0", "--",
                "yb-admin", "--master_addresses", "yb-master-0.yb-master-service.yugabyte.svc.cluster.local:7100,yb-master-1.yb-master-service.yugabyte.svc.cluster.local:7100,yb-master-2.yb-master-service.yugabyte.svc.cluster.local:7100",
                "list_cdc_streams"
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            
            if result.returncode == 0:
                # Parse output to find ACTIVE streams
                output = result.stdout.strip()
                for line in output.split('\n'):
                    if 'stream_id:' in line and 'ACTIVE' in output:
                        # Extract stream ID - use the first ACTIVE stream found
                        stream_id = line.split(':', 1)[1].strip().strip('"')
                        self.log(f"✅ Found existing ACTIVE CDC stream: {stream_id}")
                        return stream_id
                
                self.log("No ACTIVE CDC streams found, will create new one")
                return await self.create_new_cdc_stream()
            else:
                self.log(f"❌ Failed to list CDC streams: {result.stderr}", "ERROR")
                return None
                
        except Exception as e:
            self.log(f"❌ Exception getting CDC stream: {e}", "ERROR")
            return None
    
    async def create_new_cdc_stream(self) -> Optional[str]:
        """Create new CDC stream"""
        try:
            cmd = [
                "tsh", "kubectl", "exec", "-n", "yugabyte", "yb-tserver-0", "--",
                "yb-admin", "--master_addresses", "yb-master-0.yb-master-service.yugabyte.svc.cluster.local:7100,yb-master-1.yb-master-service.yugabyte.svc.cluster.local:7100,yb-master-2.yb-master-service.yugabyte.svc.cluster.local:7100",
                "create_cdc_stream", "ysql.yugabyte"
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            
            if result.returncode == 0:
                # Extract stream ID from output
                for line in result.stdout.split('\n'):
                    if 'stream_id' in line:
                        stream_id = line.split()[-1].strip()
                        self.log(f"✅ Created new CDC stream: {stream_id}")
                        self.created_resources.append(f"cdc_stream:{stream_id}")
                        return stream_id
                        
                self.log("❌ Could not extract stream ID from output", "ERROR")
                return None
            else:
                self.log(f"❌ Failed to create CDC stream: {result.stderr}", "ERROR")
                return None
                
        except Exception as e:
            self.log(f"❌ Exception creating CDC stream: {e}", "ERROR")
            return None
    
    async def create_cdc_connector(self) -> bool:
        """Create CDC connector with validated E2E configuration"""
        try:
            connector_name = f"comprehensive-e2e-cdc-connector"
            
            # Configuration based on successful E2E test
            config = {
                "name": connector_name,
                "config": {
                    "connector.class": "io.debezium.connector.yugabytedb.YugabyteDBgRPCConnector",
                    "tasks.max": "1",
                    "database.hostname": "yb-tserver-service.yugabyte.svc.cluster.local",
                    "database.port": "5433",
                    "database.user": "postgres",
                    "database.password": "",
                    "database.dbname": self.test_database,
                    "database.server.name": f"comprehensive-e2e-server",
                    "database.master.addresses": "yb-master-0.yb-master-service.yugabyte.svc.cluster.local:7100,yb-master-1.yb-master-service.yugabyte.svc.cluster.local:7100,yb-master-2.yb-master-service.yugabyte.svc.cluster.local:7100",
                    "database.streamid": self.shared_cdc_stream,
                    "database.stream.prefix": f"{self.test_database}_{self.test_schema}_{self.test_table}",
                    "table.include.list": f"{self.test_schema}.{self.test_table}",
                    "snapshot.mode": "never",
                    "before.image.mode": "never",
                    "provide.transaction.metadata": "false",
                    "binary.handling.mode": "base64",
                    "cdcsdk.connection.timeout": "10000",
                    "cdcsdk.snapshot.txn.timeout": "900000",
                    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                    "key.converter.schemas.enable": "false",
                    "value.converter.schemas.enable": "false",
                    "transforms": "unwrap,addTopicPrefix",
                    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
                    "transforms.unwrap.drop.tombstones": "false",
                    "transforms.unwrap.delete.handling.mode": "rewrite",
                    "transforms.addTopicPrefix.type": "org.apache.kafka.connect.transforms.RegexRouter",
                    "transforms.addTopicPrefix.regex": f"comprehensive-e2e-server\\.{self.test_schema}\\.{self.test_table}",
                    "transforms.addTopicPrefix.replacement": f"comprehensive-e2e-cdc-topic",
                    "errors.tolerance": "all",
                    "errors.log.enable": "true",
                    "errors.log.include.messages": "true"
                }
            }
            
            # Create connector via kubectl
            config_json = json.dumps(config)
            cmd = [
                "tsh", "kubectl", "exec", "-n", "kafka", "kafka-connect-5dd8c95895-4q84c", "--",
                "curl", "-s", "-X", "POST", "http://localhost:8083/connectors",
                "-H", "Content-Type: application/json",
                "-d", config_json
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
            
            if result.returncode == 0:
                self.log("✅ CDC connector created successfully")
                self.created_resources.append(f"connector:{connector_name}")
                
                # Wait and check status
                await asyncio.sleep(10)
                return await self.verify_connector_status(connector_name)
            else:
                self.log(f"❌ Failed to create CDC connector: {result.stderr}", "ERROR")
                return False
                
        except Exception as e:
            self.log(f"❌ Exception creating CDC connector: {e}", "ERROR")
            return False
    
    async def verify_connector_status(self, connector_name: str) -> bool:
        """Verify connector is running properly"""
        try:
            cmd = [
                "tsh", "kubectl", "exec", "-n", "kafka", "kafka-connect-5dd8c95895-4q84c", "--",
                "curl", "-s", f"http://localhost:8083/connectors/{connector_name}/status"
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            
            if result.returncode == 0:
                status = json.loads(result.stdout)
                connector_state = status.get('connector', {}).get('state', 'UNKNOWN')
                task_states = [task.get('state', 'UNKNOWN') for task in status.get('tasks', [])]
                
                self.log(f"Connector state: {connector_state}")
                self.log(f"Task states: {task_states}")
                
                if connector_state == "RUNNING" and all(state == "RUNNING" for state in task_states):
                    self.log("✅ Connector is running properly")
                    return True
                else:
                    self.log(f"❌ Connector not in running state", "ERROR")
                    return False
            else:
                self.log(f"❌ Failed to get connector status: {result.stderr}", "ERROR")
                return False
                
        except Exception as e:
            self.log(f"❌ Exception verifying connector status: {e}", "ERROR")
            return False
    
    # Additional methods would continue here...
    # (insert_yugabyte_data, create_bigquery_sink_connector, verify_bigquery_data, etc.)
    
    async def insert_yugabyte_data(self, records: List[Dict]) -> bool:
        """Insert test data into YugabyteDB"""
        try:
            for record in records:
                insert_sql = f"""
                INSERT INTO {self.test_table} (id, name, email, age) 
                VALUES ({record['id']}, '{record['name']}', '{record['email']}', {record['age']})
                ON CONFLICT (id) DO UPDATE SET 
                    name = EXCLUDED.name,
                    email = EXCLUDED.email, 
                    age = EXCLUDED.age,
                    updated_at = NOW();
                """
                
                cmd = [
                    "tsh", "kubectl", "exec", "-n", "yugabyte", "yb-tserver-0", "--",
                    "ysqlsh", "-h", "yb-tserver-service", "-p", "5433", "-U", "postgres",
                    "-c", insert_sql
                ]
                
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
                
                if result.returncode != 0:
                    self.log(f"❌ Failed to insert record {record}: {result.stderr}", "ERROR")
                    return False
                    
            self.log(f"✅ Inserted {len(records)} records into YugabyteDB")
            return True
            
        except Exception as e:
            self.log(f"❌ Exception inserting YugabyteDB data: {e}", "ERROR")
            return False
    
    async def phase5_cleanup_and_summary(self):
        """Phase 5: Cleanup and test summary"""
        self.log("🧹 Phase 5: Cleanup and summary")
        
        # Print test results summary
        self.log("📊 Test Results Summary:")
        self.log("=" * 50)
        
        for phase, result in self.test_results.items():
            status = "✅ PASSED" if result else "❌ FAILED"
            self.log(f"{phase}: {status}")
        
        # Cleanup resources if requested
        cleanup_resources = os.getenv("CLEANUP_TEST_RESOURCES", "false").lower() == "true"
        if cleanup_resources:
            await self.cleanup_test_resources()
        else:
            self.log("💡 Set CLEANUP_TEST_RESOURCES=true to automatically cleanup test resources")
    
    async def cleanup_test_resources(self):
        """Clean up all created test resources"""
        self.log("🧹 Cleaning up test resources...")
        
        for resource in reversed(self.created_resources):  # Cleanup in reverse order
            try:
                resource_type, resource_id = resource.split(":", 1)
                
                if resource_type == "connector":
                    await self.delete_connector(resource_id)
                elif resource_type == "cdc_stream":
                    await self.delete_cdc_stream(resource_id)
                elif resource_type == "yugabyte_table":
                    await self.delete_yugabyte_table(resource_id)
                    
            except Exception as e:
                self.log(f"⚠️  Failed to cleanup {resource}: {e}", "WARN")
    
    async def delete_connector(self, connector_name: str):
        """Delete Kafka Connect connector"""
        try:
            cmd = [
                "tsh", "kubectl", "exec", "-n", "kafka", "kafka-connect-5dd8c95895-4q84c", "--",
                "curl", "-s", "-X", "DELETE", f"http://localhost:8083/connectors/{connector_name}"
            ]
            
            subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            self.log(f"🗑️ Deleted connector: {connector_name}")
            
        except Exception as e:
            self.log(f"⚠️ Failed to delete connector {connector_name}: {e}", "WARN")
    
    async def emergency_cleanup(self):
        """Emergency cleanup on test failure"""
        self.log("🚨 Emergency cleanup due to test failure")
        await self.cleanup_test_resources()


async def main():
    """Main test execution"""
    test = ComprehensiveE2ETest()
    
    print("🚀 Starting Comprehensive E2E Test Suite")
    print("=" * 60)
    
    success = await test.run_comprehensive_test()
    
    if success:
        print("\n🎉 COMPREHENSIVE E2E TEST PASSED!")
        print("✅ Full bidirectional YugabyteDB <-> BigQuery sync is working")
        exit(0)
    else:
        print("\n❌ COMPREHENSIVE E2E TEST FAILED!")
        print("Please check the logs above for details")
        exit(1)


if __name__ == "__main__":
    asyncio.run(main())