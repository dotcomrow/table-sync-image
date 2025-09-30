"""
Debezium connector management utilities
"""
import json
import aiohttp
from typing import Dict, Optional
from loguru import logger
import os

class DebeziumConnectorManager:
    def __init__(self, connector_url: str):
        self.connector_url = connector_url.rstrip('/')
        self.connectors_endpoint = f"{self.connector_url}/connectors"
        
        # Get configuration from environment
        self.db_hostname = os.getenv("DEBEZIUM_DATABASE_HOSTNAME", "localhost")
        self.db_port = os.getenv("DEBEZIUM_DATABASE_PORT", "5433")
        self.db_user = os.getenv("DEBEZIUM_DATABASE_USER", "yugabyte")
        self.db_password = os.getenv("DEBEZIUM_DATABASE_PASSWORD", "yugabyte")
        self.db_name = os.getenv("DEBEZIUM_DATABASE_NAME", "yugabyte")
    
    async def create_connector(self, database_name: str, schema_name: str, table_name: str, bq_table: str) -> bool:
        """Create a Debezium connector for a YugabyteDB table"""
        
        connector_name = f"yugabyte-{database_name}-{schema_name}-{table_name}"
        
        # Check if connector already exists
        if await self.connector_exists(connector_name):
            logger.info(f"Connector {connector_name} already exists")
            return True
        
        # Check if CDC stream already exists in YugabyteDB
        cdc_exists = await self.check_cdc_stream_exists(database_name, schema_name, table_name)
        if cdc_exists:
            logger.info(f"CDC stream already exists for {database_name}.{schema_name}.{table_name} - creating connector to use existing stream")
        
        connector_config = {
            "name": connector_name,
            "config": {
                "connector.class": "io.debezium.connector.yugabytedb.YugabyteDBgRPCConnector",
                "tasks.max": "1",
                
                # YugabyteDB gRPC connector specific config
                "database.hostname": self.db_hostname,
                "database.port": self.db_port,
                "database.user": self.db_user,
                "database.password": self.db_password,
                "database.dbname": database_name,
                "database.server.name": f"yugabyte-{database_name}-{schema_name}",
                "table.include.list": f"{schema_name}.{table_name}",
                
                # YugabyteDB specific settings - try different stream ID format
                "database.streamid": f"{database_name}_{schema_name}_{table_name}_stream",
                "snapshot.mode": "never",  # We handle initial data separately
                
                # Try without transforms first to see if connector works
                # Key and value converters
                "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                "key.converter.schemas.enable": "false",
                "value.converter.schemas.enable": "false",
                
                # Error handling
                "errors.tolerance": "all",
                "errors.log.enable": "true",
                "errors.log.include.messages": "true"
            }
        }
        
        logger.info(f"Creating connector {connector_name} with config:")
        for key, value in connector_config["config"].items():
            if "password" not in key.lower():
                logger.info(f"  {key}: {value}")
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.connectors_endpoint,
                    json=connector_config,
                    headers={"Content-Type": "application/json"}
                ) as response:
                    response_text = await response.text()
                    
                    if response.status == 201:
                        logger.info(f"Successfully created Debezium connector: {connector_name}")
                        return True
                    else:
                        logger.error(f"Failed to create connector {connector_name}: {response.status}")
                        logger.error(f"Response body: {response_text}")
                        
                        # Try to parse error details
                        try:
                            import json
                            error_data = json.loads(response_text)
                            if "message" in error_data:
                                logger.error(f"Error details: {error_data['message']}")
                        except:
                            pass
                        
                        return False
                        
        except Exception as e:
            logger.error(f"Error creating Debezium connector {connector_name}: {e}")
            logger.error(f"Connector config was: {connector_config}")
            return False
    
    async def delete_connector(self, database_name: str, schema_name: str, table_name: str) -> bool:
        """Delete a Debezium connector"""
        
        connector_name = f"yugabyte-{database_name}-{schema_name}-{table_name}"
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.delete(f"{self.connectors_endpoint}/{connector_name}") as response:
                    if response.status == 204:
                        logger.info(f"Successfully deleted Debezium connector: {connector_name}")
                        return True
                    elif response.status == 404:
                        logger.info(f"Connector {connector_name} not found (already deleted)")
                        return True
                    else:
                        error_text = await response.text()
                        logger.error(f"Failed to delete connector {connector_name}: {response.status} - {error_text}")
                        return False
                        
        except Exception as e:
            logger.error(f"Error deleting Debezium connector {connector_name}: {e}")
            return False
    
    async def connector_exists(self, connector_name: str) -> bool:
        """Check if a connector exists"""
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{self.connectors_endpoint}/{connector_name}") as response:
                    return response.status == 200
                    
        except Exception as e:
            logger.error(f"Error checking connector existence {connector_name}: {e}")
            return False
    
    async def connector_exists_for_table(self, database_name: str, schema_name: str, table_name: str) -> bool:
        """Check if a connector exists for a specific database/schema/table"""
        connector_name = f"yugabyte-{database_name}-{schema_name}-{table_name}"
        return await self.connector_exists(connector_name)
    
    async def check_cdc_stream_exists(self, database_name: str, schema_name: str, table_name: str) -> bool:
        """Check if a CDC stream already exists for the table in YugabyteDB"""
        import asyncpg
        database_url = os.getenv("DATABASE_URL", "postgresql://yugabyte@localhost:5433/yugabyte")
        db_url = database_url.rsplit('/', 1)[0] + f'/{database_name}'
        
        try:
            conn = await asyncpg.connect(db_url)
            try:
                # Check if table is part of any CDC stream
                # YugabyteDB stores CDC stream information in system tables
                stream_check = await conn.fetchval("""
                    SELECT COUNT(*) > 0 FROM pg_class c
                    JOIN pg_namespace n ON n.oid = c.relnamespace 
                    WHERE c.relname = $1 AND n.nspname = $2
                    AND EXISTS (
                        SELECT 1 FROM pg_replication_slots 
                        WHERE slot_name LIKE '%' || $1 || '%'
                    )
                """, table_name, schema_name)
                
                if stream_check:
                    logger.info(f"CDC stream detected for {database_name}.{schema_name}.{table_name}")
                    return True
                
                # Alternative check - look for any replication slots that might be related
                slots = await conn.fetch("""
                    SELECT slot_name, slot_type, active 
                    FROM pg_replication_slots 
                    WHERE slot_name LIKE $1
                """, f"%{table_name}%")
                
                if slots:
                    logger.info(f"Found {len(slots)} replication slots for table {table_name}: {[s['slot_name'] for s in slots]}")
                    return True
                
                return False
                
            finally:
                await conn.close()
                
        except Exception as e:
            logger.warning(f"Could not check CDC stream status for {database_name}.{schema_name}.{table_name}: {e}")
            return False
    
    async def get_connector_status(self, connector_name: str) -> Optional[Dict]:
        """Get connector status"""
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(f"{self.connectors_endpoint}/{connector_name}/status") as response:
                    if response.status == 200:
                        return await response.json()
                    else:
                        return None
                        
        except Exception as e:
            logger.error(f"Error getting connector status {connector_name}: {e}")
            return None
    
    async def list_connectors(self) -> list:
        """List all connectors"""
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.connectors_endpoint) as response:
                    if response.status == 200:
                        return await response.json()
                    else:
                        return []
                        
        except Exception as e:
            logger.error(f"Error listing connectors: {e}")
            return []
    
    async def pause_connector(self, connector_name: str) -> bool:
        """Pause a connector"""
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.put(f"{self.connectors_endpoint}/{connector_name}/pause") as response:
                    if response.status == 202:
                        logger.info(f"Successfully paused connector: {connector_name}")
                        return True
                    else:
                        error_text = await response.text()
                        logger.error(f"Failed to pause connector {connector_name}: {response.status} - {error_text}")
                        return False
                        
        except Exception as e:
            logger.error(f"Error pausing connector {connector_name}: {e}")
            return False
    
    async def resume_connector(self, connector_name: str) -> bool:
        """Resume a connector"""
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.put(f"{self.connectors_endpoint}/{connector_name}/resume") as response:
                    if response.status == 202:
                        logger.info(f"Successfully resumed connector: {connector_name}")
                        return True
                    else:
                        error_text = await response.text()
                        logger.error(f"Failed to resume connector {connector_name}: {response.status} - {error_text}")
                        return False
                        
        except Exception as e:
            logger.error(f"Error resuming connector {connector_name}: {e}")
            return False
    
    async def restart_connector(self, connector_name: str) -> bool:
        """Restart a connector"""
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(f"{self.connectors_endpoint}/{connector_name}/restart") as response:
                    if response.status == 204:
                        logger.info(f"Successfully restarted connector: {connector_name}")
                        return True
                    else:
                        error_text = await response.text()
                        logger.error(f"Failed to restart connector {connector_name}: {response.status} - {error_text}")
                        return False
                        
        except Exception as e:
            logger.error(f"Error restarting connector {connector_name}: {e}")
            return False

class YugabytePublicationManager:
    """Manage YugabyteDB publications for Debezium"""
    
    def __init__(self, db_pool):
        self.db_pool = db_pool
    
    async def create_publication_for_table(self, database_name: str, schema_name: str, table_name: str) -> bool:
        """Create a publication for a specific table"""
        
        publication_name = f"dbz_publication_{database_name}_{schema_name}_{table_name}"
        
        # Need to connect to the specific database
        import asyncpg
        database_url = os.getenv("DATABASE_URL", "postgresql://yugabyte@localhost:5433/yugabyte")
        db_url = database_url.rsplit('/', 1)[0] + f'/{database_name}'
        
        try:
            conn = await asyncpg.connect(db_url)
            try:
                # Check if publication exists
                check_query = """
                SELECT 1 FROM pg_publication WHERE pubname = $1
                """
                exists = await conn.fetchval(check_query, publication_name)
                
                if exists:
                    logger.info(f"Publication {publication_name} already exists")
                    return True
                
                # Create publication for the specific table
                create_query = f"""
                CREATE PUBLICATION {publication_name} FOR TABLE {schema_name}.{table_name}
                """
                await conn.execute(create_query)
                
                logger.info(f"Created publication {publication_name} for {database_name}.{schema_name}.{table_name}")
                return True
            finally:
                await conn.close()
                
        except Exception as e:
            logger.error(f"Error creating publication for {database_name}.{schema_name}.{table_name}: {e}")
            return False
    
    async def drop_publication_for_table(self, database_name: str, schema_name: str, table_name: str) -> bool:
        """Drop a publication for a specific table"""
        
        publication_name = f"dbz_publication_{database_name}_{schema_name}_{table_name}"
        
        # Need to connect to the specific database
        import asyncpg
        database_url = os.getenv("DATABASE_URL", "postgresql://yugabyte@localhost:5433/yugabyte")
        db_url = database_url.rsplit('/', 1)[0] + f'/{database_name}'
        
        try:
            conn = await asyncpg.connect(db_url)
            try:
                # Check if publication exists
                check_query = """
                SELECT 1 FROM pg_publication WHERE pubname = $1
                """
                exists = await conn.fetchval(check_query, publication_name)
                
                if not exists:
                    logger.info(f"Publication {publication_name} does not exist")
                    return True
                
                # Drop publication
                drop_query = f"DROP PUBLICATION IF EXISTS {publication_name}"
                await conn.execute(drop_query)
                
                logger.info(f"Dropped publication {publication_name} from {database_name}")
                return True
            finally:
                await conn.close()
                
        except Exception as e:
            logger.error(f"Error dropping publication for {database_name}.{schema_name}.{table_name}: {e}")
            return False