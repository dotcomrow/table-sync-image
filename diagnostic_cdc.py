#!/usr/bin/env python3
"""
Comprehensive CDC diagnostics for YugabyteDB
"""
import asyncio
import os
import sys
sys.path.append('/Users/christopherlyons/GitHub/table-sync-image/src')

from debezium_manager import DebeziumConnectorManager
import asyncpg

async def comprehensive_cdc_diagnostics():
    """Run comprehensive CDC diagnostics"""
    print("🔍 Running comprehensive CDC diagnostics...")
    
    # Mock the environment variables to match production
    os.environ["DATABASE_URL"] = "postgresql://vaultadmin:MKmfsgcms9uniFRnB2FCnbW@yb-tserver-service.yugabyte.svc.cluster.local:5433/kafka"
    
    database_name = "mcp"
    schema_name = "mcp_openapi_ro"
    table_name = "mcp_openapi_augmentations"
    
    print(f"Analyzing: {database_name}.{schema_name}.{table_name}")
    print("=" * 60)
    
    # Test database connectivity
    database_url = os.environ["DATABASE_URL"]
    db_url = database_url.rsplit('/', 1)[0] + f'/{database_name}'
    
    try:
        print("1. Testing database connectivity...")
        conn = await asyncpg.connect(db_url)
        
        # Get YugabyteDB version
        version = await conn.fetchval("SELECT version()")
        print(f"✅ Connected to: {version}")
        
        print("\n2. Checking replication slots...")
        slots = await conn.fetch("SELECT * FROM pg_replication_slots")
        if slots:
            print(f"Found {len(slots)} replication slots:")
            for slot in slots:
                print(f"  - {slot['slot_name']}: {slot['slot_type']}, active={slot['active']}")
        else:
            print("❌ No replication slots found")
        
        print("\n3. Checking publications...")
        pubs = await conn.fetch("SELECT * FROM pg_publication")
        if pubs:
            print(f"Found {len(pubs)} publications:")
            for pub in pubs:
                print(f"  - {pub['pubname']}: {pub}")
        else:
            print("❌ No publications found")
        
        print("\n4. Checking table existence and properties...")
        table_info = await conn.fetchrow("""
            SELECT c.relname, c.relkind, c.relhassubclass, n.nspname, c.oid
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace 
            WHERE c.relname = $1 AND n.nspname = $2
        """, table_name, schema_name)
        
        if table_info:
            print(f"✅ Table found: {dict(table_info)}")
            
            # Get table attributes
            attrs = await conn.fetch("""
                SELECT a.attname, t.typname, a.attnotnull
                FROM pg_attribute a
                JOIN pg_type t ON a.atttypid = t.oid
                WHERE a.attrelid = $1 AND a.attnum > 0
                AND NOT a.attisdropped
                ORDER BY a.attnum
            """, table_info['oid'])
            
            print(f"  Table has {len(attrs)} columns:")
            for attr in attrs[:5]:  # Show first 5 columns
                print(f"    {attr['attname']}: {attr['typname']}")
            if len(attrs) > 5:
                print(f"    ... and {len(attrs) - 5} more columns")
                
        else:
            print(f"❌ Table not found: {schema_name}.{table_name}")
            return
        
        print("\n5. Testing truncate operation (with rollback)...")
        try:
            await conn.execute("BEGIN")
            await conn.execute(f"TRUNCATE TABLE {schema_name}.{table_name}")
            await conn.execute("ROLLBACK")
            print("✅ Truncate test succeeded - no CDC detected")
        except Exception as e:
            await conn.execute("ROLLBACK")
            error_str = str(e)
            if "cdc" in error_str.lower():
                print(f"❌ CDC detected via truncate test: {e}")
            else:
                print(f"⚠️  Truncate failed for other reason: {e}")
        
        print("\n6. Checking for stream-related objects...")
        stream_id = f"{database_name}_{schema_name}_{table_name}_stream"
        stream_objects = await conn.fetch("""
            SELECT c.relname, c.relkind, n.nspname
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace 
            WHERE c.relname LIKE $1 OR c.relname LIKE '%cdc%' OR c.relname LIKE '%stream%'
        """, f"%{stream_id}%")
        
        if stream_objects:
            print(f"Found {len(stream_objects)} stream/CDC related objects:")
            for obj in stream_objects:
                print(f"  - {obj['nspname']}.{obj['relname']} ({obj['relkind']})")
        else:
            print("❌ No stream/CDC related objects found")
        
        print("\n7. Checking system catalogs for CDC info...")
        try:
            # Look for any YugabyteDB-specific CDC metadata
            yb_objects = await conn.fetch("""
                SELECT schemaname, tablename, attname, description
                FROM pg_stats 
                WHERE tablename LIKE '%cdc%' OR tablename LIKE '%stream%'
            """)
            if yb_objects:
                print(f"Found YugabyteDB CDC metadata: {len(yb_objects)} entries")
        except Exception as e:
            print(f"System catalog check failed: {e}")
        
        await conn.close()
        
        print("\n8. Checking Debezium connector status...")
        manager = DebeziumConnectorManager("http://kafka-connect.kafka.svc.internal.lan:8083")
        
        # Check if connector exists
        connector_exists = await manager.connector_exists_for_table(database_name, schema_name, table_name)
        print(f"Debezium connector exists: {connector_exists}")
        
        # Run the CDC detection
        cdc_detected = await manager.check_cdc_stream_exists(database_name, schema_name, table_name)
        print(f"CDC stream detected by our method: {cdc_detected}")
        
    except Exception as e:
        print(f"❌ Diagnostics failed: {e}")
        print("This is expected if running outside the Kubernetes environment")

if __name__ == "__main__":
    asyncio.run(comprehensive_cdc_diagnostics())