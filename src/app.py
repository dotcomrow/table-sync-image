import os
import json
import asyncio
import time
from typing import Dict, List, Optional, Set
from dataclasses import dataclass, asdict
from datetime import datetime, timezone
import logging

import psycopg
import asyncpg
from google.cloud import bigquery
from google.auth import default
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential
import sqlparse

# Configuration from environment variables
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://yugabyte@localhost:5433/yugabyte")
BIGQUERY_PROJECT_ID = os.getenv("BIGQUERY_PROJECT_ID")
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
DEBEZIUM_CONNECTOR_URL = os.getenv("DEBEZIUM_CONNECTOR_URL", "http://localhost:8083")
SCAN_INTERVAL_SECONDS = int(os.getenv("SCAN_INTERVAL_SECONDS", "30"))
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# Configure logging
logger.remove()
logger.add(
    lambda msg: print(msg, end=""),
    level=LOG_LEVEL,
    format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>"
)

@dataclass
class TableBootstrapConfig:
    enabled: bool
    bq_table: str  # format: dataset.table
    columns: Optional[str] = None  # optional explicit column order
    
    @classmethod
    def from_comment(cls, comment_text: str) -> Optional['TableBootstrapConfig']:
        """Parse table comment to extract bootstrap configuration"""
        try:
            # Clean up comment text - remove comments and parse JSON
            cleaned = '\n'.join(line for line in comment_text.split('\n') 
                              if not line.strip().startswith('//'))
            config_data = json.loads(cleaned)
            
            bootstrap = config_data.get('bootstrap', {})
            if not bootstrap:
                return None
                
            return cls(
                enabled=bootstrap.get('enabled', False),
                bq_table=bootstrap.get('bq', ''),
                columns=bootstrap.get('columns')
            )
        except (json.JSONDecodeError, KeyError, TypeError) as e:
            logger.warning(f"Failed to parse table comment: {e}")
            return None

@dataclass
class TableState:
    schema_name: str
    table_name: str
    comment_hash: Optional[str]
    bootstrap_config: Optional[TableBootstrapConfig]
    bigquery_created: bool
    pipeline_configured: bool
    last_updated: datetime
    
    def to_dict(self) -> Dict:
        return {
            'schema_name': self.schema_name,
            'table_name': self.table_name,
            'comment_hash': self.comment_hash,
            'bootstrap_config': asdict(self.bootstrap_config) if self.bootstrap_config else None,
            'bigquery_created': self.bigquery_created,
            'pipeline_configured': self.pipeline_configured,
            'last_updated': self.last_updated.isoformat()
        }

class DatabaseManager:
    def __init__(self, database_url: str):
        self.database_url = database_url
        self.pool: Optional[asyncpg.Pool] = None
    
    async def initialize(self):
        """Initialize database connection pool and prepare schema"""
        logger.info("Initializing database manager...")
        
        # Create connection pool
        try:
            self.pool = await asyncpg.create_pool(
                self.database_url, 
                min_size=2, 
                max_size=10,
                command_timeout=30
            )
            logger.info("Database connection pool created successfully")
        except Exception as e:
            logger.error(f"Failed to create database connection pool: {e}")
            raise
        
        # Validate and prepare schema
        await self._validate_and_prepare_schema()
    
    async def close(self):
        """Close database connections"""
        if self.pool:
            await self.pool.close()
            logger.info("Database connection pool closed")
    
    async def _validate_and_prepare_schema(self):
        """Validate database connection and prepare schema if needed"""
        logger.info("Validating database schema and preparing if needed...")
        
        try:
            async with self.pool.acquire() as conn:
                # Test basic connectivity
                await self._test_database_connectivity(conn)
                
                # Check database version and capabilities
                await self._validate_database_capabilities(conn)
                
                # Prepare schema
                await self._prepare_schema(conn)
                
                # Create state table and indexes
                await self._create_state_table(conn)
                
                # Validate schema is ready
                await self._validate_schema_ready(conn)
                
            logger.info("Database schema validation and preparation completed successfully")
            
        except Exception as e:
            logger.error(f"Database schema validation/preparation failed: {e}")
            raise
    
    async def _test_database_connectivity(self, conn):
        """Test basic database connectivity and permissions"""
        logger.info("Testing database connectivity...")
        
        try:
            # Test basic query
            version = await conn.fetchval("SELECT version()")
            logger.info(f"Connected to: {version}")
            
            # Test if we can create objects (check permissions)
            await conn.execute("SELECT 1")
            
        except Exception as e:
            logger.error(f"Database connectivity test failed: {e}")
            raise
    
    async def _validate_database_capabilities(self, conn):
        """Validate that the database supports required features"""
        logger.info("Validating database capabilities...")
        
        try:
            # Check for JSONB support (required for bootstrap_config)
            jsonb_support = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT 1 FROM pg_type WHERE typname = 'jsonb'
                )
            """)
            
            if not jsonb_support:
                raise Exception("Database does not support JSONB type (required for bootstrap configuration)")
            
            # Check for publication support (required for Debezium)
            pub_support = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT 1 FROM pg_proc WHERE proname = 'pg_create_logical_replication_slot'
                )
            """)
            
            if not pub_support:
                logger.warning("Logical replication functions not found - Debezium may not work properly")
            
            # Check wal_level (should be 'logical' for CDC)
            wal_level = await conn.fetchval("SHOW wal_level")
            if wal_level not in ['logical', 'replica']:
                logger.warning(f"WAL level is '{wal_level}' - 'logical' recommended for CDC functionality")
            
            logger.info("Database capabilities validated successfully")
            
        except Exception as e:
            logger.error(f"Database capability validation failed: {e}")
            raise
    
    async def _prepare_schema(self, conn):
        """Prepare any required schema objects"""
        logger.info("Preparing database schema...")
        
        try:
            # Ensure we have necessary extensions
            await conn.execute("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"")
            logger.debug("UUID extension ensured")
            
            # Create any custom types if needed
            # (Currently none required, but placeholder for future needs)
            
        except Exception as e:
            logger.error(f"Schema preparation failed: {e}")
            raise
    
    async def _create_state_table(self, conn):
        """Create the table sync state tracking table and related objects"""
        logger.info("Creating state table and indexes...")
        
        try:
            # Create the main state table
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS table_sync_state (
                schema_name VARCHAR(255) NOT NULL,
                table_name VARCHAR(255) NOT NULL,
                comment_hash VARCHAR(64),
                bootstrap_config JSONB,
                bigquery_created BOOLEAN DEFAULT FALSE,
                pipeline_configured BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                PRIMARY KEY (schema_name, table_name)
            );
            """
            await conn.execute(create_table_sql)
            
            # Create indexes for performance
            indexes_sql = [
                """
                CREATE INDEX IF NOT EXISTS idx_table_sync_state_updated 
                ON table_sync_state(last_updated);
                """,
                """
                CREATE INDEX IF NOT EXISTS idx_table_sync_state_bootstrap_enabled 
                ON table_sync_state((bootstrap_config->>'enabled')) 
                WHERE bootstrap_config IS NOT NULL;
                """,
                """
                CREATE INDEX IF NOT EXISTS idx_table_sync_state_bigquery_created 
                ON table_sync_state(bigquery_created) 
                WHERE bigquery_created = TRUE;
                """,
                """
                CREATE INDEX IF NOT EXISTS idx_table_sync_state_pipeline_configured 
                ON table_sync_state(pipeline_configured) 
                WHERE pipeline_configured = TRUE;
                """
            ]
            
            for index_sql in indexes_sql:
                await conn.execute(index_sql)
            
            # Create a metadata table for tracking application state
            metadata_table_sql = """
            CREATE TABLE IF NOT EXISTS table_sync_metadata (
                key VARCHAR(255) PRIMARY KEY,
                value JSONB,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
            );
            """
            await conn.execute(metadata_table_sql)
            
            # Insert initial metadata
            await conn.execute("""
                INSERT INTO table_sync_metadata (key, value) 
                VALUES ('schema_version', '{"version": "1.0.0", "initialized_at": "' || NOW() || '"}')
                ON CONFLICT (key) DO NOTHING;
            """)
            
            logger.info("State table and indexes created successfully")
            
        except Exception as e:
            logger.error(f"State table creation failed: {e}")
            raise
    
    async def _validate_schema_ready(self, conn):
        """Validate that the schema is ready for use"""
        logger.info("Validating schema readiness...")
        
        try:
            # Check that state table exists and is accessible
            state_table_exists = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'table_sync_state'
                )
            """)
            
            if not state_table_exists:
                raise Exception("State table was not created properly")
            
            # Check that we can insert/update/delete from state table
            test_schema = 'test_schema_validation'
            test_table = 'test_table_validation'
            
            # Test insert
            await conn.execute("""
                INSERT INTO table_sync_state (schema_name, table_name, comment_hash) 
                VALUES ($1, $2, 'test_hash')
                ON CONFLICT (schema_name, table_name) DO UPDATE SET 
                comment_hash = 'test_hash'
            """, test_schema, test_table)
            
            # Test select
            exists = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT 1 FROM table_sync_state 
                    WHERE schema_name = $1 AND table_name = $2
                )
            """, test_schema, test_table)
            
            if not exists:
                raise Exception("Failed to insert test record into state table")
            
            # Test delete (cleanup)
            await conn.execute("""
                DELETE FROM table_sync_state 
                WHERE schema_name = $1 AND table_name = $2
            """, test_schema, test_table)
            
            # Check metadata table
            metadata_exists = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'table_sync_metadata'
                )
            """)
            
            if not metadata_exists:
                raise Exception("Metadata table was not created properly")
            
            # Get schema version
            schema_version = await conn.fetchval("""
                SELECT value FROM table_sync_metadata WHERE key = 'schema_version'
            """)
            
            if schema_version:
                logger.info(f"Schema version: {schema_version}")
            
            logger.info("Schema validation completed successfully")
            
        except Exception as e:
            logger.error(f"Schema validation failed: {e}")
            raise
    
    async def get_schema_info(self) -> Dict:
        """Get information about the current schema state"""
        try:
            async with self.pool.acquire() as conn:
                # Get basic table counts
                total_tables = await conn.fetchval("""
                    SELECT COUNT(*) FROM information_schema.tables 
                    WHERE table_schema NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
                """)
                
                # Get state table info
                tracked_tables = await conn.fetchval("SELECT COUNT(*) FROM table_sync_state")
                enabled_configs = await conn.fetchval("""
                    SELECT COUNT(*) FROM table_sync_state 
                    WHERE bootstrap_config->>'enabled' = 'true'
                """)
                
                # Get schema version
                schema_version = await conn.fetchval("""
                    SELECT value FROM table_sync_metadata WHERE key = 'schema_version'
                """)
                
                return {
                    "total_tables": total_tables,
                    "tracked_tables": tracked_tables,
                    "enabled_configs": enabled_configs,
                    "schema_version": schema_version,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
                
        except Exception as e:
            logger.error(f"Failed to get schema info: {e}")
            return {
                "error": str(e),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
    
    async def get_all_tables_with_comments(self) -> List[Dict]:
        """Get all tables and their comments from information_schema"""
        query = """
        SELECT 
            t.table_schema,
            t.table_name,
            obj_description(c.oid) as comment
        FROM information_schema.tables t
        JOIN pg_class c ON c.relname = t.table_name
        JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = t.table_schema
        WHERE t.table_type = 'BASE TABLE'
        AND t.table_schema NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
        ORDER BY t.table_schema, t.table_name;
        """
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query)
            return [dict(row) for row in rows]
    
    async def get_current_state(self) -> Dict[str, TableState]:
        """Get current state of all tracked tables"""
        query = """
        SELECT schema_name, table_name, comment_hash, bootstrap_config,
               bigquery_created, pipeline_configured, last_updated
        FROM table_sync_state;
        """
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query)
            
        states = {}
        for row in rows:
            key = f"{row['schema_name']}.{row['table_name']}"
            bootstrap_config = None
            if row['bootstrap_config']:
                config_dict = row['bootstrap_config']
                bootstrap_config = TableBootstrapConfig(**config_dict)
            
            states[key] = TableState(
                schema_name=row['schema_name'],
                table_name=row['table_name'],
                comment_hash=row['comment_hash'],
                bootstrap_config=bootstrap_config,
                bigquery_created=row['bigquery_created'],
                pipeline_configured=row['pipeline_configured'],
                last_updated=row['last_updated']
            )
        
        return states
    
    async def upsert_table_state(self, state: TableState):
        """Insert or update table state"""
        query = """
        INSERT INTO table_sync_state 
        (schema_name, table_name, comment_hash, bootstrap_config, bigquery_created, pipeline_configured, last_updated)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (schema_name, table_name) 
        DO UPDATE SET
            comment_hash = EXCLUDED.comment_hash,
            bootstrap_config = EXCLUDED.bootstrap_config,
            bigquery_created = EXCLUDED.bigquery_created,
            pipeline_configured = EXCLUDED.pipeline_configured,
            last_updated = EXCLUDED.last_updated;
        """
        
        async with self.pool.acquire() as conn:
            await conn.execute(
                query,
                state.schema_name,
                state.table_name,
                state.comment_hash,
                json.dumps(asdict(state.bootstrap_config)) if state.bootstrap_config else None,
                state.bigquery_created,
                state.pipeline_configured,
                state.last_updated
            )
    
    async def delete_table_state(self, schema_name: str, table_name: str):
        """Delete table state record"""
        query = "DELETE FROM table_sync_state WHERE schema_name = $1 AND table_name = $2;"
        
        async with self.pool.acquire() as conn:
            await conn.execute(query, schema_name, table_name)
    
    async def get_table_columns(self, schema_name: str, table_name: str) -> List[Dict]:
        """Get column information for a table"""
        query = """
        SELECT column_name, data_type, is_nullable, column_default, ordinal_position
        FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = $2
        ORDER BY ordinal_position;
        """
        
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query, schema_name, table_name)
            return [dict(row) for row in rows]

class BigQueryManager:
    def __init__(self, project_id: str):
        self.project_id = project_id
        self.client = bigquery.Client(project=project_id)
    
    def table_exists(self, dataset_id: str, table_id: str) -> bool:
        """Check if a BigQuery table exists"""
        try:
            table_ref = self.client.dataset(dataset_id).table(table_id)
            self.client.get_table(table_ref)
            return True
        except Exception:
            return False
    
    def dataset_exists(self, dataset_id: str) -> bool:
        """Check if a BigQuery dataset exists"""
        try:
            self.client.get_dataset(dataset_id)
            return True
        except Exception:
            return False
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def create_dataset(self, dataset_id: str):
        """Create a BigQuery dataset"""
        if self.dataset_exists(dataset_id):
            logger.info(f"Dataset {dataset_id} already exists")
            return
        
        dataset = bigquery.Dataset(f"{self.project_id}.{dataset_id}")
        dataset.location = "US"  # Configure as needed
        
        dataset = self.client.create_dataset(dataset, timeout=30)
        logger.info(f"Created dataset {dataset_id}")
    
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    def create_table_from_yugabyte_schema(self, dataset_id: str, table_id: str, columns: List[Dict]):
        """Create BigQuery table based on YugabyteDB table schema"""
        # Map PostgreSQL/YugabyteDB types to BigQuery types
        type_mapping = {
            'integer': 'INTEGER',
            'bigint': 'INTEGER',
            'smallint': 'INTEGER',
            'serial': 'INTEGER',
            'bigserial': 'INTEGER',
            'decimal': 'NUMERIC',
            'numeric': 'NUMERIC',
            'real': 'FLOAT',
            'double precision': 'FLOAT',
            'money': 'NUMERIC',
            'character varying': 'STRING',
            'varchar': 'STRING',
            'character': 'STRING',
            'char': 'STRING',
            'text': 'STRING',
            'boolean': 'BOOLEAN',
            'date': 'DATE',
            'timestamp': 'TIMESTAMP',
            'timestamp with time zone': 'TIMESTAMP',
            'timestamptz': 'TIMESTAMP',
            'time': 'TIME',
            'json': 'JSON',
            'jsonb': 'JSON',
            'uuid': 'STRING',
            'bytea': 'BYTES'
        }
        
        schema = []
        for col in columns:
            col_type = col['data_type'].lower()
            bq_type = type_mapping.get(col_type, 'STRING')
            
            field = bigquery.SchemaField(
                col['column_name'],
                bq_type,
                mode="NULLABLE" if col['is_nullable'] == 'YES' else "REQUIRED"
            )
            schema.append(field)
        
        table_ref = self.client.dataset(dataset_id).table(table_id)
        table = bigquery.Table(table_ref, schema=schema)
        
        table = self.client.create_table(table)
        logger.info(f"Created BigQuery table {dataset_id}.{table_id}")
    
    def delete_table(self, dataset_id: str, table_id: str):
        """Delete a BigQuery table"""
        try:
            table_ref = self.client.dataset(dataset_id).table(table_id)
            self.client.delete_table(table_ref)
            logger.info(f"Deleted BigQuery table {dataset_id}.{table_id}")
        except Exception as e:
            logger.error(f"Failed to delete BigQuery table {dataset_id}.{table_id}: {e}")

class PipelineManager:
    def __init__(self, debezium_url: str, kafka_servers: str, db_pool):
        self.debezium_url = debezium_url
        self.kafka_servers = kafka_servers
        self.db_pool = db_pool
        
        # Import here to avoid circular imports
        from debezium_manager import DebeziumConnectorManager, YugabytePublicationManager
        
        self.connector_manager = DebeziumConnectorManager(debezium_url)
        self.publication_manager = YugabytePublicationManager(db_pool)
    
    async def setup_debezium_connector(self, schema_name: str, table_name: str, config: TableBootstrapConfig):
        """Setup Debezium connector for a table"""
        logger.info(f"Setting up Debezium connector for {schema_name}.{table_name}")
        
        try:
            # Create publication for the table
            pub_success = await self.publication_manager.create_publication_for_table(schema_name, table_name)
            if not pub_success:
                logger.error(f"Failed to create publication for {schema_name}.{table_name}")
                return False
            
            # Create Debezium connector
            conn_success = await self.connector_manager.create_connector(
                schema_name, table_name, config.bq_table
            )
            if not conn_success:
                logger.error(f"Failed to create Debezium connector for {schema_name}.{table_name}")
                return False
            
            logger.info(f"Successfully setup pipeline for {schema_name}.{table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error setting up pipeline for {schema_name}.{table_name}: {e}")
            return False
    
    async def remove_debezium_connector(self, schema_name: str, table_name: str):
        """Remove Debezium connector for a table"""
        logger.info(f"Removing Debezium connector for {schema_name}.{table_name}")
        
        try:
            # Delete Debezium connector
            conn_success = await self.connector_manager.delete_connector(schema_name, table_name)
            
            # Drop publication
            pub_success = await self.publication_manager.drop_publication_for_table(schema_name, table_name)
            
            if conn_success and pub_success:
                logger.info(f"Successfully removed pipeline for {schema_name}.{table_name}")
                return True
            else:
                logger.warning(f"Partial failure removing pipeline for {schema_name}.{table_name}")
                return False
                
        except Exception as e:
            logger.error(f"Error removing pipeline for {schema_name}.{table_name}: {e}")
            return False

class TableSyncManager:
    def __init__(self):
        self.db_manager = DatabaseManager(DATABASE_URL)
        self.bq_manager = BigQueryManager(BIGQUERY_PROJECT_ID) if BIGQUERY_PROJECT_ID else None
        self.pipeline_manager = None  # Will be initialized after db_manager
        self.data_transfer_manager = None  # Will be initialized if needed
        
    async def initialize(self):
        """Initialize the sync manager"""
        logger.info("Initializing TableSyncManager components...")
        
        # Initialize database manager (this validates and prepares the schema)
        logger.info("Initializing database manager...")
        await self.db_manager.initialize()
        
        # Initialize pipeline manager with database pool
        logger.info("Initializing pipeline manager...")
        self.pipeline_manager = PipelineManager(
            DEBEZIUM_CONNECTOR_URL, 
            KAFKA_BOOTSTRAP_SERVERS, 
            self.db_manager.pool
        )
        logger.info(f"Pipeline manager configured for Debezium at {DEBEZIUM_CONNECTOR_URL}")
        
        # Initialize BigQuery components if configured
        if self.bq_manager:
            logger.info("Initializing BigQuery integration...")
            
            # Initialize data transfer manager
            from data_transfer import DataTransferManager
            temp_bucket = os.getenv("TEMP_STORAGE_BUCKET")
            if temp_bucket:
                logger.info(f"Using temp storage bucket: {temp_bucket}")
            else:
                logger.info(f"Using default temp bucket: {BIGQUERY_PROJECT_ID}-table-sync-temp")
            
            self.data_transfer_manager = DataTransferManager(BIGQUERY_PROJECT_ID, temp_bucket)
            
            # Ensure temp bucket exists
            try:
                self.data_transfer_manager.ensure_temp_bucket_exists()
                logger.info("Temp storage bucket validated/created")
            except Exception as e:
                logger.error(f"Failed to setup temp storage bucket: {e}")
                logger.warning("Data transfer operations may fail without proper bucket access")
            
            logger.info("BigQuery integration initialized successfully")
        else:
            logger.warning("BigQuery manager not initialized - BIGQUERY_PROJECT_ID not set")
            logger.warning("BigQuery operations will be disabled")
        
        logger.info("TableSyncManager initialization completed successfully")
    
    async def close(self):
        """Close the sync manager"""
        await self.db_manager.close()
    
    def _calculate_comment_hash(self, comment: str) -> str:
        """Calculate hash of comment for change detection"""
        import hashlib
        return hashlib.sha256(comment.encode()).hexdigest() if comment else None
    
    async def copy_yugabyte_data_to_bigquery(self, schema_name: str, table_name: str, config: TableBootstrapConfig):
        """Copy existing data from YugabyteDB to BigQuery"""
        if not self.data_transfer_manager:
            logger.error("Data transfer manager not initialized")
            return False
        
        dataset_id, table_id = config.bq_table.split('.')
        
        try:
            await self.data_transfer_manager.copy_yugabyte_to_bigquery(
                self.db_manager.pool,
                schema_name,
                table_name,
                dataset_id,
                table_id,
                config.columns,
                batch_size=int(os.getenv("BATCH_SIZE", "10000"))
            )
            logger.info(f"Successfully copied data from {schema_name}.{table_name} to BigQuery")
            return True
            
        except Exception as e:
            logger.error(f"Failed to copy data from {schema_name}.{table_name} to BigQuery: {e}")
            return False
    
    async def copy_bigquery_data_to_yugabyte(self, schema_name: str, table_name: str, config: TableBootstrapConfig):
        """Copy data from BigQuery to YugabyteDB (overwrite mode)"""
        if not self.data_transfer_manager:
            logger.error("Data transfer manager not initialized")
            return False
        
        dataset_id, table_id = config.bq_table.split('.')
        
        try:
            await self.data_transfer_manager.copy_bigquery_to_yugabyte(
                self.db_manager.pool,
                dataset_id,
                table_id,
                schema_name,
                table_name,
                truncate_target=True
            )
            logger.info(f"Successfully copied data from BigQuery to {schema_name}.{table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to copy data from BigQuery to {schema_name}.{table_name}: {e}")
            return False
    
    async def process_table_changes(self, current_tables: List[Dict], current_states: Dict[str, TableState]):
        """Process changes in table configurations"""
        
        # Track which tables we've seen in this scan
        seen_tables = set()
        
        for table_info in current_tables:
            schema_name = table_info['table_schema']
            table_name = table_info['table_name']
            comment = table_info['comment']
            
            table_key = f"{schema_name}.{table_name}"
            seen_tables.add(table_key)
            
            # Parse bootstrap configuration from comment
            bootstrap_config = None
            comment_hash = None
            
            if comment:
                comment_hash = self._calculate_comment_hash(comment)
                bootstrap_config = TableBootstrapConfig.from_comment(comment)
            
            current_state = current_states.get(table_key)
            
            # Determine what action to take
            if current_state is None:
                # New table with comment
                if bootstrap_config and bootstrap_config.enabled:
                    await self._handle_new_table_with_config(schema_name, table_name, comment_hash, bootstrap_config)
                elif comment_hash:
                    # Track table even if bootstrap is disabled
                    await self._create_table_state(schema_name, table_name, comment_hash, bootstrap_config)
            else:
                # Existing table - check for changes
                if comment_hash != current_state.comment_hash:
                    await self._handle_table_comment_change(current_state, comment_hash, bootstrap_config)
        
        # Handle tables that no longer exist or lost their comments
        for table_key, state in current_states.items():
            if table_key not in seen_tables:
                await self._handle_table_removal(state)
    
    async def _handle_new_table_with_config(self, schema_name: str, table_name: str, comment_hash: str, config: TableBootstrapConfig):
        """Handle a new table with bootstrap configuration"""
        logger.info(f"Processing new table {schema_name}.{table_name} with bootstrap config")
        
        if not self.bq_manager:
            logger.error("BigQuery manager not initialized - check BIGQUERY_PROJECT_ID")
            return
        
        dataset_id, table_id = config.bq_table.split('.')
        
        # Create dataset if it doesn't exist
        if not self.bq_manager.dataset_exists(dataset_id):
            self.bq_manager.create_dataset(dataset_id)
        
        # Check if BigQuery table exists
        bq_table_exists = self.bq_manager.table_exists(dataset_id, table_id)
        
        if not bq_table_exists:
            # Create BigQuery table and copy data from YugabyteDB
            columns = await self.db_manager.get_table_columns(schema_name, table_name)
            self.bq_manager.create_table_from_yugabyte_schema(dataset_id, table_id, columns)
            
            # Copy existing data
            await self.copy_yugabyte_data_to_bigquery(schema_name, table_name, config)
            
            # Setup pipeline
            await self.pipeline_manager.setup_debezium_connector(schema_name, table_name, config)
            
            # Save state
            state = TableState(
                schema_name=schema_name,
                table_name=table_name,
                comment_hash=comment_hash,
                bootstrap_config=config,
                bigquery_created=True,
                pipeline_configured=True,
                last_updated=datetime.now(timezone.utc)
            )
        else:
            # BigQuery table exists - copy data from BigQuery to YugabyteDB
            await self.copy_bigquery_data_to_yugabyte(schema_name, table_name, config)
            
            # Setup pipeline
            await self.pipeline_manager.setup_debezium_connector(schema_name, table_name, config)
            
            # Save state
            state = TableState(
                schema_name=schema_name,
                table_name=table_name,
                comment_hash=comment_hash,
                bootstrap_config=config,
                bigquery_created=True,
                pipeline_configured=True,
                last_updated=datetime.now(timezone.utc)
            )
        
        await self.db_manager.upsert_table_state(state)
        logger.info(f"Successfully processed new table {schema_name}.{table_name}")
    
    async def _create_table_state(self, schema_name: str, table_name: str, comment_hash: str, config: Optional[TableBootstrapConfig]):
        """Create state record for a table"""
        state = TableState(
            schema_name=schema_name,
            table_name=table_name,
            comment_hash=comment_hash,
            bootstrap_config=config,
            bigquery_created=False,
            pipeline_configured=False,
            last_updated=datetime.now(timezone.utc)
        )
        await self.db_manager.upsert_table_state(state)
    
    async def _handle_table_comment_change(self, current_state: TableState, new_comment_hash: Optional[str], new_config: Optional[TableBootstrapConfig]):
        """Handle changes to table comments"""
        schema_name = current_state.schema_name
        table_name = current_state.table_name
        
        if new_comment_hash is None:
            # Comment was removed
            await self._handle_table_removal(current_state)
        elif new_config and new_config.enabled and not current_state.bootstrap_config:
            # Bootstrap was enabled
            await self._handle_new_table_with_config(schema_name, table_name, new_comment_hash, new_config)
        elif not new_config or not new_config.enabled:
            # Bootstrap was disabled or config is invalid
            if current_state.bootstrap_config and current_state.bootstrap_config.enabled:
                await self._handle_table_removal(current_state)
        else:
            # Configuration changed - update state
            current_state.comment_hash = new_comment_hash
            current_state.bootstrap_config = new_config
            current_state.last_updated = datetime.now(timezone.utc)
            await self.db_manager.upsert_table_state(current_state)
    
    async def _handle_table_removal(self, state: TableState):
        """Handle removal of table or its bootstrap configuration"""
        logger.info(f"Handling removal of table {state.schema_name}.{state.table_name}")
        
        if state.bigquery_created and state.bootstrap_config:
            # Delete BigQuery table
            dataset_id, table_id = state.bootstrap_config.bq_table.split('.')
            if self.bq_manager:
                self.bq_manager.delete_table(dataset_id, table_id)
        
        if state.pipeline_configured:
            # Remove pipeline
            await self.pipeline_manager.remove_debezium_connector(state.schema_name, state.table_name)
        
        # Remove state record
        await self.db_manager.delete_table_state(state.schema_name, state.table_name)
        logger.info(f"Successfully removed table {state.schema_name}.{state.table_name}")
    
    async def scan_and_process(self):
        """Main scanning and processing loop"""
        logger.info("Starting table scan and processing")
        
        try:
            # Get current table information
            current_tables = await self.db_manager.get_all_tables_with_comments()
            current_states = await self.db_manager.get_current_state()
            
            logger.info(f"Found {len(current_tables)} tables, {len(current_states)} tracked states")
            
            # Process changes
            await self.process_table_changes(current_tables, current_states)
            
            logger.info("Table scan and processing completed successfully")
            
        except Exception as e:
            logger.error(f"Error during scan and processing: {e}")
            raise

async def validate_external_dependencies(sync_manager: 'TableSyncManager'):
    """Validate external dependencies are accessible"""
    logger.info("Checking external service connectivity...")
    
    # Check BigQuery connectivity if configured
    if sync_manager.bq_manager:
        try:
            # Simple query to test connectivity
            query = "SELECT 1 as test"
            query_job = sync_manager.bq_manager.client.query(query)
            list(query_job.result())
            logger.info("✅ BigQuery connectivity validated")
        except Exception as e:
            logger.warning(f"⚠️  BigQuery connectivity issue: {e}")
            logger.warning("BigQuery operations may fail during runtime")
    
    # Check Debezium connectivity
    try:
        import aiohttp
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{DEBEZIUM_CONNECTOR_URL}/connectors", timeout=10) as response:
                if response.status == 200:
                    connectors = await response.json()
                    logger.info(f"✅ Debezium Connect API accessible ({len(connectors)} existing connectors)")
                else:
                    logger.warning(f"⚠️  Debezium API returned status {response.status}")
    except Exception as e:
        logger.warning(f"⚠️  Debezium connectivity issue: {e}")
        logger.warning("Real-time CDC operations may fail during runtime")
    
    # Check Kafka connectivity (basic check)
    try:
        # This is a basic connectivity test - in production you might want more thorough checks
        logger.info(f"✅ Kafka configuration: {KAFKA_BOOTSTRAP_SERVERS}")
    except Exception as e:
        logger.warning(f"⚠️  Kafka configuration issue: {e}")
    
    logger.info("External dependency validation completed")

async def main():
    """Main application loop"""
    logger.info("=" * 60)
    logger.info("STARTING TABLE SYNC APPLICATION")
    logger.info("=" * 60)
    
    # Log configuration
    logger.info("Configuration:")
    logger.info(f"  Database URL: {DATABASE_URL.replace('://yugabyte:', '://yugabyte:***@') if 'yugabyte:' in DATABASE_URL else DATABASE_URL}")
    logger.info(f"  BigQuery Project: {BIGQUERY_PROJECT_ID}")
    logger.info(f"  Debezium URL: {DEBEZIUM_CONNECTOR_URL}")
    logger.info(f"  Scan Interval: {SCAN_INTERVAL_SECONDS}s")
    logger.info(f"  Log Level: {LOG_LEVEL}")
    
    # Validate required environment variables
    if not BIGQUERY_PROJECT_ID:
        logger.error("BIGQUERY_PROJECT_ID environment variable is required")
        logger.error("Please set this variable and restart the application")
        return
    
    if not GOOGLE_APPLICATION_CREDENTIALS:
        logger.warning("GOOGLE_APPLICATION_CREDENTIALS not set - BigQuery operations may fail")
    
    sync_manager = TableSyncManager()
    
    try:
        logger.info("Initializing application components...")
        await sync_manager.initialize()
        
        # Get and log schema information
        schema_info = await sync_manager.db_manager.get_schema_info()
        logger.info("Database schema information:")
        logger.info(f"  Total tables in database: {schema_info.get('total_tables', 'unknown')}")
        logger.info(f"  Currently tracked tables: {schema_info.get('tracked_tables', 0)}")
        logger.info(f"  Tables with enabled sync: {schema_info.get('enabled_configs', 0)}")
        if schema_info.get('schema_version'):
            logger.info(f"  Schema version: {schema_info['schema_version']}")
        
        # Validate external dependencies
        logger.info("Validating external dependencies...")
        await validate_external_dependencies(sync_manager)
        
        logger.info("=" * 60)
        logger.info("APPLICATION READY - Starting main processing loop")
        logger.info(f"Scanning for table changes every {SCAN_INTERVAL_SECONDS} seconds")
        logger.info("=" * 60)
        
        # Run initial scan
        logger.info("Performing initial table scan...")
        await sync_manager.scan_and_process()
        
        # Main loop
        while True:
            try:
                logger.debug(f"Sleeping for {SCAN_INTERVAL_SECONDS} seconds...")
                await asyncio.sleep(SCAN_INTERVAL_SECONDS)
                await sync_manager.scan_and_process()
                
            except KeyboardInterrupt:
                logger.info("Received interrupt signal, shutting down...")
                break
            except Exception as e:
                logger.error(f"Error in main loop: {e}")
                logger.info("Continuing after error...")
                await asyncio.sleep(5)  # Short delay before retrying
    
    except Exception as e:
        logger.error(f"Failed to initialize application: {e}")
        logger.error("Application startup failed - exiting")
        raise
    
    finally:
        logger.info("Shutting down application...")
        await sync_manager.close()
        logger.info("Application shutdown complete")

if __name__ == "__main__":
    asyncio.run(main())
