# Alternative CDC Management Strategy - Plan B

## Problem Analysis
The HTTP 500 error `"Cannot create a replication slot on the same namespace which already has a yb-admin stream on it"` indicates YugabyteDB cluster-level CDC streams that can only be cleaned up with actual yb-admin tool access.

## Plan B: Alternative Approaches (If YugabyteDB Redeploy Doesn't Work)

### Option 1: Aggressive PostgreSQL-Level Cleanup
Instead of trying to clean yb-admin streams, aggressively clean ALL PostgreSQL-level CDC artifacts before every connector creation:

```python
async def nuclear_postgresql_cleanup(self, database_name: str) -> bool:
    """Nuclear option: Clean ALL PostgreSQL CDC artifacts"""
    try:
        import asyncpg
        database_url = os.getenv("DATABASE_URL", "postgresql://yugabyte@localhost:5433/yugabyte")
        base_url = database_url.rsplit('/', 1)[0]
        db_url = f"{base_url}/{database_name}"
        
        conn = await asyncpg.connect(db_url)
        try:
            # 1. Kill all active replication connections
            await conn.execute("""
                SELECT pg_terminate_backend(pid) 
                FROM pg_stat_activity 
                WHERE application_name LIKE '%debezium%' OR query LIKE '%replication%'
            """)
            
            # 2. Drop ALL replication slots (nuclear)
            slots = await conn.fetch("SELECT slot_name FROM pg_replication_slots")
            for slot in slots:
                try:
                    await conn.execute(f"SELECT pg_drop_replication_slot('{slot['slot_name']}')")
                    logger.info(f"Dropped replication slot: {slot['slot_name']}")
                except:
                    pass
            
            # 3. Drop ALL publications (nuclear)  
            publications = await conn.fetch("SELECT pubname FROM pg_publication")
            for pub in publications:
                try:
                    await conn.execute(f"DROP PUBLICATION IF EXISTS {pub['pubname']} CASCADE")
                    logger.info(f"Dropped publication: {pub['pubname']}")
                except:
                    pass
            
            # 4. Wait for YugabyteDB to process changes
            await asyncio.sleep(30)
            
            return True
            
        finally:
            await conn.close()
            
    except Exception as e:
        logger.error(f"Nuclear PostgreSQL cleanup failed: {e}")
        return False
```

### Option 2: Different Connector Configuration
Try different Debezium configurations that might avoid the yb-admin conflict:

```python
# Alternative connector config that might bypass yb-admin conflicts
connector_config = {
    "name": connector_name,
    "config": {
        "connector.class": "io.debezium.connector.yugabytedb.YugabyteDBgRPCConnector",
        "tasks.max": "1",
        
        # YugabyteDB connection
        "database.hostname": self.db_hostname,
        "database.port": self.db_port,
        "database.user": self.db_user,
        "database.password": self.db_password,
        "database.dbname": database_name,
        "database.master.addresses": self.db_master_addresses,
        "database.server.name": f"yugabyte-{database_name}-{schema_name}-v2",
        
        # Try different slot management
        "slot.name": f"debezium_slot_{int(time.time())}",  # Unique slot name
        "publication.name": f"dbz_pub_{int(time.time())}",  # Unique publication
        
        # Different replication approach
        "plugin.name": "yboutput",
        "snapshot.mode": "initial",  # Try different snapshot modes
        "slot.drop.on.stop": "true",  # Auto-cleanup on stop
        
        # Table filtering
        "table.include.list": f"{schema_name}.{table_name}",
        
        # Converters
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter.schemas.enable": "false"
    }
}
```

### Option 3: Connector Recreation Strategy
Instead of trying to fix conflicts, detect them and recreate with different names:

```python
async def create_connector_with_retry_strategy(self, database_name: str, schema_name: str, table_name: str) -> bool:
    """Create connector with automatic retry and rename strategy"""
    
    max_attempts = 5
    
    for attempt in range(max_attempts):
        # Use timestamp to ensure unique connector names
        timestamp = int(time.time())
        connector_name = f"yugabyte-{database_name}-{schema_name}-{table_name}-{timestamp}"
        
        try:
            # Try nuclear PostgreSQL cleanup first
            await self.nuclear_postgresql_cleanup(database_name)
            
            # Create connector with unique identifiers
            success = await self._create_single_connector(connector_name, database_name, schema_name, table_name, timestamp)
            
            if success:
                logger.info(f"✅ Connector created successfully: {connector_name}")
                return True
                
        except Exception as e:
            logger.error(f"Attempt {attempt + 1} failed: {e}")
            
        # Exponential backoff
        wait_time = 2 ** attempt * 15  # 15, 30, 60, 120, 240 seconds
        logger.warning(f"Waiting {wait_time} seconds before retry...")
        await asyncio.sleep(wait_time)
    
    logger.error(f"❌ All {max_attempts} attempts failed for {database_name}.{schema_name}.{table_name}")
    return False
```

### Option 4: YugabyteDB REST API Approach
Try using YugabyteDB's REST API instead of Debezium for CDC:

```python
async def create_yugabyte_cdc_via_api(self, database_name: str, table_name: str) -> bool:
    """Create CDC stream using YugabyteDB REST API"""
    try:
        # YugabyteDB master REST API endpoint
        master_host = self.db_master_addresses.split(':')[0]
        api_url = f"http://{master_host}:7000/api/v1/cdc/stream"
        
        async with aiohttp.ClientSession() as session:
            # Create CDC stream via REST API
            data = {
                "table_id": table_name,
                "database_name": database_name,
                "format": "PROTO"
            }
            
            async with session.post(api_url, json=data) as response:
                if response.status == 200:
                    result = await response.json()
                    stream_id = result.get('stream_id')
                    logger.info(f"✅ Created CDC stream via API: {stream_id}")
                    return True
                else:
                    logger.error(f"❌ API call failed: {response.status}")
                    return False
                    
    except Exception as e:
        logger.error(f"❌ REST API approach failed: {e}")
        return False
```

## Deployment Strategy

1. **First**: Try YugabyteDB redeploy (nuclear option)
2. **If that doesn't work**: Deploy Plan B with aggressive PostgreSQL cleanup
3. **If still failing**: Try different connector configurations
4. **Last resort**: Switch to YugabyteDB REST API approach

## Implementation Priority

1. Nuclear PostgreSQL cleanup (most likely to work)
2. Unique connector naming with timestamps
3. Different Debezium configuration options
4. YugabyteDB REST API fallback

These approaches bypass the yb-admin conflict entirely by working at different levels of the CDC system.