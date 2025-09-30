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
        
        connector_config = {
            "name": connector_name,
            "config": {
                "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
                "tasks.max": "1",
                "database.hostname": self.db_hostname,
                "database.port": self.db_port,
                "database.user": self.db_user,
                "database.password": self.db_password,
                "database.dbname": database_name,
                "database.server.name": f"yugabyte-{database_name}-{schema_name}",
                "table.include.list": f"{schema_name}.{table_name}",
                "plugin.name": "pgoutput",
                "slot.name": f"debezium_{database_name}_{schema_name}_{table_name}",
                "publication.name": f"dbz_publication_{database_name}_{schema_name}_{table_name}",
                
                # Key and value converters
                "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                "key.converter.schemas.enable": "false",
                "value.converter.schemas.enable": "false",
                
                # Topic routing
                "transforms": "route",
                "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
                "transforms.route.regex": f"yugabyte-{database_name}-{schema_name}\.{schema_name}\.{table_name}",
                "transforms.route.replacement": f"bigquery-{bq_table.replace('.', '-')}",
                
                # Snapshot configuration
                "snapshot.mode": "never",  # We handle initial data separately
                "include.schema.changes": "false",
                
                # Error handling
                "errors.tolerance": "all",
                "errors.log.enable": "true",
                "errors.log.include.messages": "true"
            }
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self.connectors_endpoint,
                    json=connector_config,
                    headers={"Content-Type": "application/json"}
                ) as response:
                    if response.status == 201:
                        logger.info(f"Successfully created Debezium connector: {connector_name}")
                        return True
                    else:
                        error_text = await response.text()
                        logger.error(f"Failed to create connector {connector_name}: {response.status} - {error_text}")
                        return False
                        
        except Exception as e:
            logger.error(f"Error creating Debezium connector {connector_name}: {e}")
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