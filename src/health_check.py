"""
Health check and monitoring utilities for the table sync application
"""
import asyncio
import time
from typing import Dict, Optional
import asyncpg
from google.cloud import bigquery
import aiohttp
from loguru import logger

class HealthChecker:
    def __init__(self, database_url: str, bq_project_id: str, debezium_url: str):
        self.database_url = database_url
        self.bq_project_id = bq_project_id
        self.debezium_url = debezium_url
        self.bq_client = bigquery.Client(project=bq_project_id) if bq_project_id else None
    
    async def check_yugabyte_connection(self) -> Dict[str, any]:
        """Check YugabyteDB connection and basic functionality"""
        try:
            conn = await asyncpg.connect(self.database_url)
            
            # Test basic query
            result = await conn.fetchval("SELECT version()")
            await conn.close()
            
            return {
                "status": "healthy",
                "message": "YugabyteDB connection successful",
                "version": result,
                "timestamp": time.time()
            }
            
        except Exception as e:
            return {
                "status": "unhealthy",
                "message": f"YugabyteDB connection failed: {str(e)}",
                "timestamp": time.time()
            }
    
    def check_bigquery_connection(self) -> Dict[str, any]:
        """Check BigQuery connection and permissions"""
        if not self.bq_client:
            return {
                "status": "not_configured",
                "message": "BigQuery client not configured",
                "timestamp": time.time()
            }
        
        try:
            # Test basic query
            query = "SELECT 1 as test_value"
            query_job = self.bq_client.query(query)
            results = list(query_job.result())
            
            return {
                "status": "healthy",
                "message": "BigQuery connection successful",
                "project_id": self.bq_project_id,
                "timestamp": time.time()
            }
            
        except Exception as e:
            return {
                "status": "unhealthy",
                "message": f"BigQuery connection failed: {str(e)}",
                "timestamp": time.time()
            }
    
    async def check_debezium_connection(self) -> Dict[str, any]:
        """Check Debezium Connect API availability"""
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{self.debezium_url}/connectors") as response:
                    if response.status == 200:
                        connectors = await response.json()
                        return {
                            "status": "healthy",
                            "message": "Debezium Connect API accessible",
                            "connector_count": len(connectors),
                            "connectors": connectors,
                            "timestamp": time.time()
                        }
                    else:
                        return {
                            "status": "unhealthy",
                            "message": f"Debezium API returned status: {response.status}",
                            "timestamp": time.time()
                        }
                        
        except Exception as e:
            return {
                "status": "unhealthy",
                "message": f"Debezium connection failed: {str(e)}",
                "timestamp": time.time()
            }
    
    async def check_table_sync_state_table(self) -> Dict[str, any]:
        """Check if the table sync state table exists and is accessible"""
        try:
            conn = await asyncpg.connect(self.database_url)
            
            # Check if state table exists
            exists = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'table_sync_state'
                )
            """)
            
            if not exists:
                await conn.close()
                return {
                    "status": "unhealthy",
                    "message": "Table sync state table does not exist",
                    "timestamp": time.time()
                }
            
            # Get count of tracked tables
            count = await conn.fetchval("SELECT COUNT(*) FROM table_sync_state")
            
            await conn.close()
            
            return {
                "status": "healthy",
                "message": "Table sync state table accessible",
                "tracked_tables": count,
                "timestamp": time.time()
            }
            
        except Exception as e:
            return {
                "status": "unhealthy",
                "message": f"State table check failed: {str(e)}",
                "timestamp": time.time()
            }
    
    async def get_comprehensive_health_status(self) -> Dict[str, any]:
        """Get comprehensive health status of all components"""
        health_checks = {
            "yugabyte": await self.check_yugabyte_connection(),
            "bigquery": self.check_bigquery_connection(),
            "debezium": await self.check_debezium_connection(),
            "state_table": await self.check_table_sync_state_table()
        }
        
        # Determine overall status
        all_healthy = all(
            check["status"] in ["healthy", "not_configured"] 
            for check in health_checks.values()
        )
        
        overall_status = "healthy" if all_healthy else "unhealthy"
        
        return {
            "overall_status": overall_status,
            "timestamp": time.time(),
            "components": health_checks
        }

class MetricsCollector:
    def __init__(self, database_url: str):
        self.database_url = database_url
    
    async def get_sync_metrics(self) -> Dict[str, any]:
        """Get metrics about table synchronization"""
        try:
            conn = await asyncpg.connect(self.database_url)
            
            # Get basic counts
            total_tracked = await conn.fetchval("SELECT COUNT(*) FROM table_sync_state")
            enabled_configs = await conn.fetchval("""
                SELECT COUNT(*) FROM table_sync_state 
                WHERE bootstrap_config->>'enabled' = 'true'
            """)
            with_pipelines = await conn.fetchval("""
                SELECT COUNT(*) FROM table_sync_state 
                WHERE pipeline_configured = true
            """)
            with_bq_tables = await conn.fetchval("""
                SELECT COUNT(*) FROM table_sync_state 
                WHERE bigquery_created = true
            """)
            
            # Get recent activity
            recent_updates = await conn.fetchval("""
                SELECT COUNT(*) FROM table_sync_state 
                WHERE last_updated > NOW() - INTERVAL '1 hour'
            """)
            
            await conn.close()
            
            return {
                "total_tracked_tables": total_tracked,
                "enabled_bootstrap_configs": enabled_configs,
                "active_pipelines": with_pipelines,
                "bigquery_tables_created": with_bq_tables,
                "recent_updates_1h": recent_updates,
                "timestamp": time.time()
            }
            
        except Exception as e:
            logger.error(f"Failed to collect sync metrics: {e}")
            return {
                "error": str(e),
                "timestamp": time.time()
            }
    
    async def get_table_details(self) -> Dict[str, any]:
        """Get detailed information about tracked tables"""
        try:
            conn = await asyncpg.connect(self.database_url)
            
            rows = await conn.fetch("""
                SELECT 
                    schema_name,
                    table_name,
                    comment_hash IS NOT NULL as has_comment,
                    bootstrap_config->>'enabled' = 'true' as bootstrap_enabled,
                    bootstrap_config->>'bq' as bigquery_target,
                    bigquery_created,
                    pipeline_configured,
                    last_updated
                FROM table_sync_state
                ORDER BY schema_name, table_name
            """)
            
            await conn.close()
            
            tables = []
            for row in rows:
                tables.append({
                    "schema_name": row["schema_name"],
                    "table_name": row["table_name"],
                    "has_comment": row["has_comment"],
                    "bootstrap_enabled": row["bootstrap_enabled"],
                    "bigquery_target": row["bigquery_target"],
                    "bigquery_created": row["bigquery_created"],
                    "pipeline_configured": row["pipeline_configured"],
                    "last_updated": row["last_updated"].isoformat() if row["last_updated"] else None
                })
            
            return {
                "tables": tables,
                "count": len(tables),
                "timestamp": time.time()
            }
            
        except Exception as e:
            logger.error(f"Failed to get table details: {e}")
            return {
                "error": str(e),
                "timestamp": time.time()
            }

async def main():
    """CLI tool for health checks and metrics"""
    import sys
    import os
    import json
    
    # Load environment variables
    from dotenv import load_dotenv
    load_dotenv()
    
    database_url = os.getenv("DATABASE_URL", "postgresql://yugabyte@localhost:5433/yugabyte")
    bq_project_id = os.getenv("BIGQUERY_PROJECT_ID")
    debezium_url = os.getenv("DEBEZIUM_CONNECTOR_URL", "http://localhost:8083")
    
    health_checker = HealthChecker(database_url, bq_project_id, debezium_url)
    metrics_collector = MetricsCollector(database_url)
    
    if len(sys.argv) < 2:
        print("Usage: python health_check.py [health|metrics|tables]")
        return
    
    command = sys.argv[1]
    
    if command == "health":
        result = await health_checker.get_comprehensive_health_status()
        print(json.dumps(result, indent=2))
        
        # Exit with error code if unhealthy
        if result["overall_status"] != "healthy":
            sys.exit(1)
            
    elif command == "metrics":
        result = await metrics_collector.get_sync_metrics()
        print(json.dumps(result, indent=2))
        
    elif command == "tables":
        result = await metrics_collector.get_table_details()
        print(json.dumps(result, indent=2))
        
    else:
        print(f"Unknown command: {command}")
        print("Available commands: health, metrics, tables")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())