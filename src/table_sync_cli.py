#!/usr/bin/env python3
"""
Table Sync CLI - Command line interface for managing table synchronization
"""
import asyncio
import sys
import json
import os
from typing import Optional
import argparse
from datetime import datetime

# Add current directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from health_check import HealthChecker, MetricsCollector
from app import DatabaseManager, TableBootstrapConfig
import asyncpg

class TableSyncCLI:
    def __init__(self):
        self.database_url = os.getenv("DATABASE_URL", "postgresql://yugabyte@localhost:5433/yugabyte")
        self.bq_project_id = os.getenv("BIGQUERY_PROJECT_ID")
        self.debezium_url = os.getenv("DEBEZIUM_CONNECTOR_URL", "http://localhost:8083")
        
        self.db_manager = None
        self.health_checker = None
        self.metrics_collector = None
    
    async def initialize(self):
        """Initialize CLI components"""
        self.db_manager = DatabaseManager(self.database_url)
        await self.db_manager.initialize()
        
        self.health_checker = HealthChecker(self.database_url, self.bq_project_id, self.debezium_url)
        self.metrics_collector = MetricsCollector(self.database_url)
    
    async def close(self):
        """Clean up resources"""
        if self.db_manager:
            await self.db_manager.close()
    
    async def list_tables(self, schema: Optional[str] = None):
        """List all tables with their sync status"""
        tables_with_comments = await self.db_manager.get_all_tables_with_comments()
        current_states = await self.db_manager.get_current_state()
        
        filtered_tables = tables_with_comments
        if schema:
            filtered_tables = [t for t in tables_with_comments if t['table_schema'] == schema]
        
        print(f"\n📊 Table Sync Status")
        print("=" * 80)
        
        for table in filtered_tables:
            schema_name = table['table_schema']
            table_name = table['table_name']
            comment = table['comment']
            table_key = f"{schema_name}.{table_name}"
            
            # Parse bootstrap config
            bootstrap_config = None
            if comment:
                bootstrap_config = TableBootstrapConfig.from_comment(comment)
            
            # Get current state
            state = current_states.get(table_key)
            
            # Determine status
            if bootstrap_config and bootstrap_config.enabled:
                if state and state.bigquery_created and state.pipeline_configured:
                    status = "🟢 SYNCED"
                elif state and (state.bigquery_created or state.pipeline_configured):
                    status = "🟡 PARTIAL"
                else:
                    status = "🔄 PENDING"
            elif comment:
                status = "⏸️  DISABLED"
            else:
                status = "⚪ NO CONFIG"
            
            print(f"\n{status} {schema_name}.{table_name}")
            
            if bootstrap_config:
                print(f"   Target: {bootstrap_config.bq_table}")
                print(f"   Enabled: {bootstrap_config.enabled}")
                if bootstrap_config.columns:
                    print(f"   Columns: {bootstrap_config.columns}")
            
            if state:
                print(f"   BigQuery Created: {state.bigquery_created}")
                print(f"   Pipeline Active: {state.pipeline_configured}")
                print(f"   Last Updated: {state.last_updated}")
    
    async def add_table_config(self, schema_name: str, table_name: str, bq_dataset: str, 
                              bq_table: str, enabled: bool = True, columns: Optional[str] = None):
        """Add bootstrap configuration to a table"""
        
        config = {
            "bootstrap": {
                "enabled": enabled,
                "bq": f"{bq_dataset}.{bq_table}",
            }
        }
        
        if columns:
            config["bootstrap"]["columns"] = columns
        
        comment_json = json.dumps(config, indent=2)
        
        try:
            async with self.db_manager.pool.acquire() as conn:
                await conn.execute(
                    f"COMMENT ON TABLE {schema_name}.{table_name} IS $1",
                    comment_json
                )
            
            print(f"✅ Added bootstrap configuration to {schema_name}.{table_name}")
            print(f"   Target: {bq_dataset}.{bq_table}")
            print(f"   Enabled: {enabled}")
            
        except Exception as e:
            print(f"❌ Failed to add configuration: {e}")
    
    async def remove_table_config(self, schema_name: str, table_name: str):
        """Remove bootstrap configuration from a table"""
        
        try:
            async with self.db_manager.pool.acquire() as conn:
                await conn.execute(
                    f"COMMENT ON TABLE {schema_name}.{table_name} IS NULL"
                )
            
            print(f"✅ Removed bootstrap configuration from {schema_name}.{table_name}")
            
        except Exception as e:
            print(f"❌ Failed to remove configuration: {e}")
    
    async def show_table_detail(self, schema_name: str, table_name: str):
        """Show detailed information about a specific table"""
        
        table_key = f"{schema_name}.{table_name}"
        
        # Get table info
        async with self.db_manager.pool.acquire() as conn:
            table_info = await conn.fetchrow("""
                SELECT 
                    t.table_schema,
                    t.table_name,
                    obj_description(c.oid) as comment
                FROM information_schema.tables t
                JOIN pg_class c ON c.relname = t.table_name
                JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = t.table_schema
                WHERE t.table_schema = $1 AND t.table_name = $2
            """, schema_name, table_name)
        
        if not table_info:
            print(f"❌ Table {schema_name}.{table_name} not found")
            return
        
        # Get current state
        current_states = await self.db_manager.get_current_state()
        state = current_states.get(table_key)
        
        # Parse comment
        comment = table_info['comment']
        bootstrap_config = None
        if comment:
            bootstrap_config = TableBootstrapConfig.from_comment(comment)
        
        # Get columns
        columns = await self.db_manager.get_table_columns(schema_name, table_name)
        
        print(f"\n📋 Table Details: {schema_name}.{table_name}")
        print("=" * 60)
        
        print(f"\n🏗️  Schema Information:")
        print(f"   Columns: {len(columns)}")
        for col in columns[:5]:  # Show first 5 columns
            nullable = "NULL" if col['is_nullable'] == 'YES' else "NOT NULL"
            print(f"   - {col['column_name']}: {col['data_type']} {nullable}")
        if len(columns) > 5:
            print(f"   ... and {len(columns) - 5} more columns")
        
        print(f"\n🔧 Bootstrap Configuration:")
        if bootstrap_config:
            print(f"   Enabled: {bootstrap_config.enabled}")
            print(f"   BigQuery Target: {bootstrap_config.bq_table}")
            if bootstrap_config.columns:
                print(f"   Column Order: {bootstrap_config.columns}")
        else:
            print("   No bootstrap configuration found")
        
        print(f"\n📊 Sync Status:")
        if state:
            print(f"   Tracked: Yes")
            print(f"   BigQuery Created: {state.bigquery_created}")
            print(f"   Pipeline Configured: {state.pipeline_configured}")
            print(f"   Last Updated: {state.last_updated}")
            print(f"   Comment Hash: {state.comment_hash[:16] if state.comment_hash else 'None'}...")
        else:
            print("   Tracked: No")
    
    async def force_sync(self, schema_name: str, table_name: str):
        """Force immediate synchronization of a table"""
        # This would trigger the sync process manually
        # For now, we'll just update the state to trigger processing
        
        try:
            async with self.db_manager.pool.acquire() as conn:
                await conn.execute("""
                    UPDATE table_sync_state 
                    SET last_updated = NOW() - INTERVAL '1 hour'
                    WHERE schema_name = $1 AND table_name = $2
                """, schema_name, table_name)
            
            print(f"✅ Marked {schema_name}.{table_name} for immediate sync")
            print("   The table will be processed in the next scan cycle")
            
        except Exception as e:
            print(f"❌ Failed to force sync: {e}")

async def main():
    parser = argparse.ArgumentParser(description="Table Sync CLI")
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # List tables command
    list_parser = subparsers.add_parser('list', help='List tables and their sync status')
    list_parser.add_argument('--schema', help='Filter by schema name')
    
    # Add config command
    add_parser = subparsers.add_parser('add', help='Add bootstrap configuration to a table')
    add_parser.add_argument('schema', help='Schema name')
    add_parser.add_argument('table', help='Table name')
    add_parser.add_argument('bq_dataset', help='BigQuery dataset name')
    add_parser.add_argument('bq_table', help='BigQuery table name')
    add_parser.add_argument('--disabled', action='store_true', help='Add config in disabled state')
    add_parser.add_argument('--columns', help='Explicit column order')
    
    # Remove config command
    remove_parser = subparsers.add_parser('remove', help='Remove bootstrap configuration')
    remove_parser.add_argument('schema', help='Schema name')
    remove_parser.add_argument('table', help='Table name')
    
    # Show table detail command
    detail_parser = subparsers.add_parser('detail', help='Show detailed table information')
    detail_parser.add_argument('schema', help='Schema name')
    detail_parser.add_argument('table', help='Table name')
    
    # Force sync command
    sync_parser = subparsers.add_parser('sync', help='Force immediate sync of a table')
    sync_parser.add_argument('schema', help='Schema name')
    sync_parser.add_argument('table', help='Table name')
    
    # Health command
    health_parser = subparsers.add_parser('health', help='Check system health')
    
    # Metrics command  
    metrics_parser = subparsers.add_parser('metrics', help='Show sync metrics')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    # Load environment variables
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass  # dotenv not available, use existing env vars
    
    cli = TableSyncCLI()
    
    try:
        await cli.initialize()
        
        if args.command == 'list':
            await cli.list_tables(args.schema)
            
        elif args.command == 'add':
            await cli.add_table_config(
                args.schema, args.table, args.bq_dataset, args.bq_table,
                enabled=not args.disabled, columns=args.columns
            )
            
        elif args.command == 'remove':
            await cli.remove_table_config(args.schema, args.table)
            
        elif args.command == 'detail':
            await cli.show_table_detail(args.schema, args.table)
            
        elif args.command == 'sync':
            await cli.force_sync(args.schema, args.table)
            
        elif args.command == 'health':
            result = await cli.health_checker.get_comprehensive_health_status()
            print(json.dumps(result, indent=2))
            
        elif args.command == 'metrics':
            result = await cli.metrics_collector.get_sync_metrics()
            print(json.dumps(result, indent=2))
            
    finally:
        await cli.close()

if __name__ == "__main__":
    asyncio.run(main())