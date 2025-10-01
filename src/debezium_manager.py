"""
Debezium connector management utilities
"""
import asyncio
import json
import aiohttp
from typing import Dict, Optional
import os

# Configure logging with version information
from logging_config import logger

# Dynamic version detection
from version_utils import APP_VERSION as DEBEZIUM_MANAGER_VERSION

class DebeziumConnectorManager:
    def __init__(self, connector_url: str):
        self.connector_url = connector_url.rstrip('/')
        self.connectors_endpoint = f"{self.connector_url}/connectors"
        
        # Check if we should use shared CDC streams (default: true for reliability)
        self.use_shared_cdc_streams = os.getenv("USE_SHARED_CDC_STREAMS", "true").lower() == "true"
        
        # Parse database configuration from DATABASE_URL environment variable
        database_url = os.getenv("DATABASE_URL", "postgresql://yugabyte@localhost:5433/yugabyte")
        
        # Parse the URL to extract connection details
        import urllib.parse
        parsed = urllib.parse.urlparse(database_url)
        
        self.db_hostname = parsed.hostname or "localhost"
        self.db_port = str(parsed.port or 5433)
        self.db_user = parsed.username or "yugabyte"
        self.db_password = parsed.password or "yugabyte"
        
        logger.info(f"Debezium Manager {DEBEZIUM_MANAGER_VERSION} - connecting to YugabyteDB at {self.db_hostname}:{self.db_port} as user {self.db_user}")
        
        # Get YugabyteDB master addresses from environment
        self.db_master_addresses = os.getenv("YUGABYTE_MASTER_ADDRESSES", f"{self.db_hostname}:7100")
        logger.info(f"YugabyteDB master addresses: {self.db_master_addresses}")
        
        # Allow override via specific environment variables if needed
        self.db_hostname = os.getenv("DEBEZIUM_DATABASE_HOSTNAME", self.db_hostname)
        self.db_port = os.getenv("DEBEZIUM_DATABASE_PORT", self.db_port)
        self.db_user = os.getenv("DEBEZIUM_DATABASE_USER", self.db_user)
        self.db_password = os.getenv("DEBEZIUM_DATABASE_PASSWORD", self.db_password)
        self.db_master_addresses = os.getenv("DEBEZIUM_MASTER_ADDRESSES", self.db_master_addresses)
    
    async def cleanup_all_cdc_streams_on_startup(self) -> bool:
        """Clean up ALL CDC streams across all databases on startup"""
        logger.info("🧹 Starting cleanup of ALL CDC streams across all databases...")
        
        # First, clean up any yb-admin streams that might cause conflicts
        logger.info("🔧 Cleaning up yb-admin CDC streams first...")
        yb_admin_cleanup = await self._cleanup_all_yb_admin_streams()
        if yb_admin_cleanup:
            logger.info("✅ YB-admin stream cleanup completed")
        else:
            logger.warning("⚠️ YB-admin stream cleanup had issues, continuing with PostgreSQL cleanup...")
        
        try:
            import asyncpg
            
            # Get the base database URL and discover all databases  
            database_url = os.getenv("DATABASE_URL", "postgresql://yugabyte@localhost:5433/yugabyte")
            base_url = database_url.rsplit('/', 1)[0]
            
            # Connect to default database to get list of all databases
            conn = await asyncpg.connect(database_url)
            try:
                # Get all databases except system databases
                databases = await conn.fetch("""
                    SELECT datname FROM pg_database 
                    WHERE datname NOT IN ('template0', 'template1', 'postgres', 'system_platform')
                    AND datistemplate = false
                """)
                
                total_cleaned = 0
                for db_record in databases:
                    db_name = db_record['datname']
                    cleaned_count = await self._cleanup_database_cdc_streams(base_url, db_name)
                    total_cleaned += cleaned_count
                
                # Add a longer wait period to let YugabyteDB process the cleanup
                if total_cleaned > 0:
                    logger.info(f"Waiting 10 seconds for YugabyteDB to process CDC cleanup...")
                    import asyncio
                    await asyncio.sleep(10)
                else:
                    logger.info(f"No CDC streams found to clean up, but waiting 5 seconds for YugabyteDB stabilization...")
                    import asyncio
                    await asyncio.sleep(5)
                
                # After cleanup, let's also check if there are any remaining issues
                logger.info(f"🔍 Post-cleanup verification...")
                for database_name in databases:
                    try:
                        db_url = base_url + f'/{database_name}'
                        check_conn = await asyncpg.connect(db_url)
                        try:
                            remaining_pubs = await check_conn.fetchval("SELECT COUNT(*) FROM pg_publication")
                            remaining_slots = await check_conn.fetchval("SELECT COUNT(*) FROM pg_replication_slots")
                            active_conns = await check_conn.fetchval("""
                                SELECT COUNT(*) FROM pg_stat_activity 
                                WHERE pid != pg_backend_pid() AND (
                                    application_name LIKE '%cdc%' OR 
                                    application_name LIKE '%debezium%' OR
                                    query LIKE '%publication%'
                                )
                            """)
                            logger.info(f"   {database_name}: {remaining_pubs} publications, {remaining_slots} slots, {active_conns} CDC connections")
                        finally:
                            await check_conn.close()
                    except Exception as e:
                        logger.debug(f"Could not verify cleanup for {database_name}: {e}")
                
                logger.info(f"✅ Startup CDC cleanup completed: {total_cleaned} streams/publications cleaned across {len(databases)} databases")
                return True
                
            finally:
                await conn.close()
                
        except Exception as e:
            logger.error(f"❌ Failed to cleanup CDC streams on startup: {e}")
            logger.warning("⚠️  Continuing startup despite cleanup failure...")
            return False
    
    async def _cleanup_database_cdc_streams(self, base_url: str, database_name: str) -> int:
        """Clean up all CDC streams in a specific database"""
        try:
            import asyncpg
            db_url = f"{base_url}/{database_name}"
            
            conn = await asyncpg.connect(db_url)
            try:
                cleaned_count = 0
                
                # 1. Drop all publications
                publications = await conn.fetch("SELECT pubname FROM pg_publication")
                for pub in publications:
                    try:
                        await conn.execute(f"DROP PUBLICATION IF EXISTS {pub['pubname']} CASCADE")
                        logger.debug(f"Dropped publication: {pub['pubname']} in {database_name}")
                        cleaned_count += 1
                    except Exception as e:
                        logger.debug(f"Failed to drop publication {pub['pubname']}: {e}")
                
                # 2. Drop all replication slots
                slots = await conn.fetch("SELECT slot_name FROM pg_replication_slots")
                for slot in slots:
                    try:
                        await conn.execute(f"SELECT pg_drop_replication_slot('{slot['slot_name']}')")
                        logger.debug(f"Dropped replication slot: {slot['slot_name']} in {database_name}")
                        cleaned_count += 1
                    except Exception as e:
                        logger.debug(f"Failed to drop replication slot {slot['slot_name']}: {e}")
                
                if cleaned_count > 0:
                    logger.info(f"🧹 Cleaned {cleaned_count} CDC streams/publications in database '{database_name}'")
                
                return cleaned_count
                
            finally:
                await conn.close()
                
        except Exception as e:
            logger.error(f"Failed to cleanup CDC streams in database {database_name}: {e}")
            return 0
    
    async def _cleanup_yb_admin_streams(self, database_name: str) -> bool:
        """Clean up yb-admin CDC streams that are causing conflicts"""
        try:
            import subprocess
            import json
            
            logger.info(f"🧹 Attempting automated yb-admin CDC stream cleanup for database: {database_name}")
            
            # First, list all CDC streams to see what we're dealing with
            list_cmd = [
                "yb-admin", 
                "--master_addresses", self.db_master_addresses,
                "list_cdc_streams"
            ]
            
            logger.info(f"Listing CDC streams: {' '.join(list_cmd)}")
            
            try:
                result = subprocess.run(
                    list_cmd,
                    capture_output=True,
                    text=True,
                    timeout=30
                )
                
                if result.returncode == 0:
                    logger.info(f"📋 Current CDC streams:")
                    logger.info(result.stdout)
                    
                    # Parse the output to find streams related to our database
                    streams_to_delete = []
                    lines = result.stdout.strip().split('\n')
                    
                    for line in lines:
                        # Look for lines that contain database name or stream IDs
                        if database_name.lower() in line.lower() or 'stream_id' in line.lower():
                            # Extract stream ID if present
                            if 'stream_id' in line.lower():
                                # Parse stream ID from output like: "Stream ID: abc123..."
                                parts = line.split()
                                for i, part in enumerate(parts):
                                    if 'stream_id' in part.lower() and i + 1 < len(parts):
                                        stream_id = parts[i + 1].strip(':')
                                        streams_to_delete.append(stream_id)
                                        logger.info(f"🎯 Found stream to delete: {stream_id}")
                    
                    # Delete each conflicting stream
                    deletion_success = True
                    for stream_id in streams_to_delete:
                        delete_cmd = [
                            "yb-admin",
                            "--master_addresses", self.db_master_addresses,
                            "delete_cdc_stream", stream_id
                        ]
                        
                        logger.info(f"Deleting CDC stream: {' '.join(delete_cmd)}")
                        
                        try:
                            delete_result = subprocess.run(
                                delete_cmd,
                                capture_output=True,
                                text=True,
                                timeout=30
                            )
                            
                            if delete_result.returncode == 0:
                                logger.info(f"✅ Successfully deleted CDC stream: {stream_id}")
                            else:
                                logger.error(f"❌ Failed to delete CDC stream {stream_id}: {delete_result.stderr}")
                                deletion_success = False
                                
                        except subprocess.TimeoutExpired:
                            logger.error(f"⏰ Timeout deleting CDC stream: {stream_id}")
                            deletion_success = False
                        except Exception as e:
                            logger.error(f"❌ Exception deleting CDC stream {stream_id}: {e}")
                            deletion_success = False
                    
                    if len(streams_to_delete) == 0:
                        logger.warning(f"🤔 No streams found matching database '{database_name}' - may need manual inspection")
                        # Try a more aggressive approach - delete ALL streams
                        logger.info(f"Attempting to delete ALL CDC streams as last resort...")
                        
                        # Parse all stream IDs from the output
                        all_streams = []
                        for line in lines:
                            if 'stream_id' in line.lower():
                                parts = line.split()
                                for i, part in enumerate(parts):
                                    if 'stream_id' in part.lower() and i + 1 < len(parts):
                                        stream_id = parts[i + 1].strip(':')
                                        all_streams.append(stream_id)
                        
                        # Delete all streams
                        for stream_id in all_streams:
                            delete_cmd = [
                                "yb-admin",
                                "--master_addresses", self.db_master_addresses,
                                "delete_cdc_stream", stream_id
                            ]
                            
                            try:
                                delete_result = subprocess.run(
                                    delete_cmd,
                                    capture_output=True,
                                    text=True,
                                    timeout=30
                                )
                                
                                if delete_result.returncode == 0:
                                    logger.info(f"✅ Deleted CDC stream: {stream_id}")
                                else:
                                    logger.warning(f"⚠️ Could not delete stream {stream_id}: {delete_result.stderr}")
                                    
                            except Exception as e:
                                logger.warning(f"⚠️ Exception deleting stream {stream_id}: {e}")
                    
                    return deletion_success
                    
                else:
                    logger.error(f"❌ Failed to list CDC streams: {result.stderr}")
                    return False
                    
            except subprocess.TimeoutExpired:
                logger.error(f"⏰ Timeout listing CDC streams")
                return False
            except FileNotFoundError:
                logger.error(f"❌ yb-admin command not found - cannot perform automated cleanup")
                logger.error(f"Manual cleanup required using yb-admin tool")
                return False
            except Exception as e:
                logger.error(f"❌ Exception during yb-admin stream listing: {e}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Failed to cleanup yb-admin streams: {e}")
            return False
    
    async def _cleanup_all_yb_admin_streams(self) -> bool:
        """Clean up ALL yb-admin CDC streams at startup"""
        try:
            import subprocess
            
            logger.info(f"🧹 Attempting to clean up ALL yb-admin CDC streams at startup")
            
            # List all CDC streams
            list_cmd = [
                "yb-admin", 
                "--master_addresses", self.db_master_addresses,
                "list_cdc_streams"
            ]
            
            logger.info(f"Listing all CDC streams: {' '.join(list_cmd)}")
            
            try:
                result = subprocess.run(
                    list_cmd,
                    capture_output=True,
                    text=True,
                    timeout=30
                )
                
                if result.returncode == 0:
                    logger.info(f"📋 Found CDC streams:")
                    logger.info(result.stdout)
                    
                    # Parse all stream IDs from the output
                    all_streams = []
                    lines = result.stdout.strip().split('\n')
                    
                    for line in lines:
                        if 'stream_id' in line.lower():
                            parts = line.split()
                            for i, part in enumerate(parts):
                                if 'stream_id' in part.lower() and i + 1 < len(parts):
                                    stream_id = parts[i + 1].strip(':')
                                    all_streams.append(stream_id)
                                    logger.info(f"🎯 Found stream to delete: {stream_id}")
                    
                    if len(all_streams) == 0:
                        logger.info(f"✅ No yb-admin CDC streams found to clean up")
                        return True
                    
                    # Delete all streams
                    deletion_success = True
                    for stream_id in all_streams:
                        delete_cmd = [
                            "yb-admin",
                            "--master_addresses", self.db_master_addresses,
                            "delete_cdc_stream", stream_id
                        ]
                        
                        logger.info(f"Deleting CDC stream: {' '.join(delete_cmd)}")
                        
                        try:
                            delete_result = subprocess.run(
                                delete_cmd,
                                capture_output=True,
                                text=True,
                                timeout=30
                            )
                            
                            if delete_result.returncode == 0:
                                logger.info(f"✅ Successfully deleted CDC stream: {stream_id}")
                            else:
                                logger.warning(f"⚠️ Could not delete CDC stream {stream_id}: {delete_result.stderr}")
                                # Don't fail the entire process for individual stream deletion failures
                                
                        except subprocess.TimeoutExpired:
                            logger.warning(f"⏰ Timeout deleting CDC stream: {stream_id}")
                        except Exception as e:
                            logger.warning(f"⚠️ Exception deleting CDC stream {stream_id}: {e}")
                    
                    logger.info(f"✅ Completed yb-admin CDC stream cleanup (attempted {len(all_streams)} streams)")
                    return True
                    
                else:
                    logger.warning(f"⚠️ Could not list CDC streams: {result.stderr}")
                    return False
                    
            except subprocess.TimeoutExpired:
                logger.warning(f"⏰ Timeout listing CDC streams")
                return False
            except FileNotFoundError:
                logger.warning(f"⚠️ yb-admin command not found - skipping automated cleanup")
                logger.warning(f"This is expected if running outside YugabyteDB environment")
                return True  # Don't fail startup if yb-admin is not available
            except Exception as e:
                logger.warning(f"⚠️ Exception during yb-admin stream listing: {e}")
                return False
                
        except Exception as e:
            logger.warning(f"⚠️ Failed to cleanup all yb-admin streams: {e}")
            return False
    
    async def _get_or_create_shared_cdc_stream(self, database_name: str) -> Optional[str]:
        """Get or create a shared CDC stream for the database using the reliable approach from E2E testing"""
        try:
            # Use the same approach that worked in our E2E testing
            # Check for existing shared stream first
            existing_stream = await self._find_existing_shared_stream(database_name)
            if existing_stream:
                logger.info(f"Found existing shared CDC stream: {existing_stream}")
                return existing_stream
            
            # Create new shared stream with deterministic naming
            stream_name = f"shared_cdc_stream_{database_name}"
            
            import subprocess
            
            # Create CDC stream using yb-admin (same as E2E test)
            create_cmd = [
                "yb-admin",
                "--master_addresses", self.db_master_addresses,
                "create_cdc_stream", "ysql.yugabyte"  # Use the database name from successful test
            ]
            
            logger.info(f"Creating shared CDC stream: {' '.join(create_cmd)}")
            
            try:
                result = subprocess.run(
                    create_cmd,
                    capture_output=True,
                    text=True,
                    timeout=60
                )
                
                if result.returncode == 0 and "CDC Stream ID" in result.stdout:
                    # Extract stream ID from output
                    import re
                    match = re.search(r'CDC Stream ID: ([a-f0-9-]+)', result.stdout)
                    if match:
                        stream_id = match.group(1)
                        logger.info(f"✅ Created shared CDC stream: {stream_id}")
                        return stream_id
                    else:
                        logger.error(f"Could not extract CDC stream ID from: {result.stdout}")
                        return None
                else:
                    logger.error(f"Failed to create CDC stream: {result.stderr}")
                    return None
                    
            except subprocess.TimeoutExpired:
                logger.error(f"Timeout creating CDC stream")
                return None
            except FileNotFoundError:
                logger.error(f"yb-admin command not found - cannot create CDC stream")
                return None
                    
        except Exception as e:
            logger.error(f"Exception creating shared CDC stream: {e}")
            return None
    
    async def _find_existing_shared_stream(self, database_name: str) -> Optional[str]:
        """Find existing shared CDC stream"""
        try:
            import subprocess
            
            # List existing streams
            list_cmd = [
                "yb-admin",
                "--master_addresses", self.db_master_addresses,
                "list_cdc_streams"
            ]
            
            try:
                result = subprocess.run(
                    list_cmd,
                    capture_output=True,
                    text=True,
                    timeout=30
                )
                
                if result.returncode == 0:
                    # Parse CDC stream output to find ACTIVE streams only
                    output = result.stdout.strip()
                    streams = []
                    current_stream = {}
                    
                    for line in output.split('\n'):
                        line = line.strip()
                        if line.startswith('stream_id:'):
                            if current_stream:
                                streams.append(current_stream)
                            current_stream = {'stream_id': line.split(':', 1)[1].strip().strip('"')}
                        elif 'value: "ACTIVE"' in line and current_stream:
                            current_stream['active'] = True
                        elif line.startswith('streams {') or not line:
                            continue
                    
                    # Add the last stream
                    if current_stream:
                        streams.append(current_stream)
                    
                    # Find first ACTIVE stream
                    for stream in streams:
                        if stream.get('active', False):
                            stream_id = stream['stream_id']
                            logger.info(f"Found ACTIVE CDC stream: {stream_id}")
                            return stream_id
                    
                    logger.info(f"No ACTIVE CDC streams found")
                    return None
                else:
                    logger.warning(f"Could not list CDC streams: {result.stderr}")
                    return None
                    
            except (subprocess.TimeoutExpired, FileNotFoundError):
                logger.warning(f"yb-admin not available for listing streams")
                return None
                
        except Exception as e:
            logger.warning(f"Exception finding existing CDC stream: {e}")
            return None
    
    async def create_connector(self, database_name: str, schema_name: str, table_name: str, bq_table: str, cdc_stream_id: Optional[str] = None) -> bool:
        """Create a Debezium connector for a YugabyteDB table using shared CDC stream approach"""
        
        connector_name = f"yugabyte-{database_name}-{schema_name}-{table_name}"
        
        # Check if connector already exists
        if await self.connector_exists(connector_name):
            logger.info(f"Connector {connector_name} already exists")
            return True
        
        # Choose CDC stream approach based on configuration
        if self.use_shared_cdc_streams:
            # SHARED CDC STREAM APPROACH: Use a shared stream for reliability
            # Based on successful E2E testing, we'll use a shared CDC stream approach
            # This avoids the per-table stream creation/deletion cycle that causes NullPointerExceptions
            logger.info(f"Using shared CDC stream approach for better reliability")
            shared_stream_id = await self._get_or_create_shared_cdc_stream(database_name)
            if not shared_stream_id:
                logger.error(f"Failed to get or create shared CDC stream for database {database_name}")
                return False
            
            logger.info(f"Using shared CDC stream: {shared_stream_id} for connector {connector_name}")
        else:
            # LEGACY APPROACH: Let connector auto-create its own stream
            logger.info(f"Using legacy per-connector CDC stream approach")
            # AUTOMATIC CLEANUP: Try to clean up any stale CDC streams before creating connector
            logger.info(f"Performing automatic CDC stream cleanup before creating connector...")
            cleanup_success = await self.cleanup_stale_cdc_stream(database_name, schema_name, table_name)
            if cleanup_success:
                logger.info(f"Automatic CDC stream cleanup completed successfully")
            else:
                logger.warning(f"Automatic CDC stream cleanup had issues, but proceeding with connector creation")
            
            shared_stream_id = None  # Let connector auto-create
        
        # Check if CDC stream already exists in YugabyteDB (after cleanup)
        cdc_exists = await self.check_cdc_stream_exists(database_name, schema_name, table_name)
        if cdc_exists:
            logger.info(f"CDC stream still exists after cleanup for {database_name}.{schema_name}.{table_name} - creating connector to use existing stream")
            # When CDC stream exists, use a different snapshot mode
            logger.info(f"Using 'never' snapshot mode since CDC stream already exists")
        
        # Create connector configuration based on approach (shared vs legacy)
        config_dict = {
            "connector.class": "io.debezium.connector.yugabytedb.YugabyteDBgRPCConnector",
            "tasks.max": "1",
            
            # YugabyteDB gRPC connector specific config
            "database.hostname": self.db_hostname,
            "database.port": self.db_port,
            "database.user": self.db_user,
            "database.password": self.db_password,
            "database.dbname": database_name,
            "database.master.addresses": self.db_master_addresses,
            "database.server.name": f"yugabyte-{database_name}-{schema_name}",
            
            # Use only table.include.list for table filtering (most specific)
            "table.include.list": f"{schema_name}.{table_name}",
            
            # YugabyteDB specific settings based on successful E2E test
            "snapshot.mode": "never",  # Critical: never take snapshots to avoid conflicts
            "database.stream.prefix": f"{database_name}_{schema_name}_{table_name}",
            
            # Key and value converters (same as E2E test)
            "key.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "key.converter.schemas.enable": "false",
            "value.converter.schemas.enable": "false",
            
            # YugabyteDB CDC specific settings
            "cdcsdk.snapshot.txn.timeout": "900000",  # 15 minutes timeout
            "cdcsdk.connection.timeout": "10000",     # 10 seconds connection timeout
            
            # CRITICAL FIXES from E2E test to prevent NullPointerException
            "provide.transaction.metadata": "false",  # Critical: disable transaction metadata
            "binary.handling.mode": "base64",
            "before.image.mode": "never",              # Critical: disable before images
            
            # Error handling - same as E2E test
            "errors.tolerance": "all",
            "errors.log.enable": "true",
            "errors.log.include.messages": "true"
        }
        
        # Configure CDC stream ID based on approach
        if self.use_shared_cdc_streams and shared_stream_id:
            # SHARED CDC STREAM: Use the shared stream ID
            config_dict.update({
                "database.stream.id": shared_stream_id,
                "database.streamid": shared_stream_id,
            })
            logger.info(f"📊 Using shared CDC stream: {shared_stream_id} for connector {connector_name}")
            logger.info(f"📊 Configuration matches successful E2E test setup")
        else:
            # LEGACY: Let connector auto-create its own stream
            logger.info(f"📊 Using legacy approach - connector will auto-create CDC stream")
            logger.info(f"📊 Configuration uses per-connector stream creation")
        
        connector_config = {
            "name": connector_name,
            "config": config_dict
        }
        
        logger.info(f"Creating connector {connector_name} with config:")
        logger.info(f"  🔌 Connecting to YugabyteDB at {self.db_hostname}:{self.db_port}")
        for key, value in connector_config["config"].items():
            if "password" not in key.lower():
                logger.info(f"  {key}: {value}")
        
        # PREFLIGHT CHECK: Verify Kafka Connect is responsive
        logger.info(f"🔍 Checking Kafka Connect service health before creating connector...")
        connect_healthy = await self._check_connect_health()
        if not connect_healthy:
            logger.error(f"❌ Kafka Connect service is not responsive - aborting connector creation")
            return False
        
        # RETRY LOGIC: Try up to 3 times with cleanup between attempts
        max_retries = 3
        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    wait_time = attempt * 10  # 10, 20 seconds backoff
                    logger.info(f"Retrying connector creation (attempt {attempt + 1}/{max_retries}) after {wait_time}s delay...")
                    await asyncio.sleep(wait_time)
                    
                    # Additional cleanup before retry
                    logger.info(f"Performing additional CDC cleanup before retry {attempt + 1}")
                    await self.cleanup_stale_cdc_stream(database_name, schema_name, table_name)
                
                # Log detailed request information
                logger.info(f"🔌 Sending POST request to: {self.connectors_endpoint}")
                logger.info(f"🔌 Request timeout: 120 seconds")
                logger.info(f"🔌 Connector config size: {len(json.dumps(connector_config))} characters")
                
                async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=120)) as session:  # Increased timeout
                    start_time = asyncio.get_event_loop().time()
                    
                    async with session.post(
                        self.connectors_endpoint,
                        json=connector_config,
                        headers={"Content-Type": "application/json"}
                    ) as response:
                        end_time = asyncio.get_event_loop().time()
                        request_duration = end_time - start_time
                        
                        logger.info(f"📡 HTTP request completed in {request_duration:.2f} seconds")
                        logger.info(f"📡 Response status: {response.status}")
                        
                        response_text = await response.text()
                        logger.info(f"📡 Response body length: {len(response_text)} characters")
                        
                        if response.status == 201:
                            logger.info(f"✅ HTTP request succeeded - created Debezium connector: {connector_name} (attempt {attempt + 1})")
                            
                            # Wait for connector to initialize and detect reconfiguration errors
                            logger.info(f"⏳ Waiting 5 seconds for connector initialization...")
                            await asyncio.sleep(5)
                            
                            # Check connector status multiple times to catch reconfiguration errors
                            logger.info(f"🔍 Fetching connector status for {connector_name}...")
                            status = await self.get_connector_status(connector_name)
                            
                            # If connector exists but has issues, wait a bit more for reconfiguration to complete
                            if status:
                                connector_state = status.get('connector', {}).get('state', 'UNKNOWN')
                                if connector_state not in ['RUNNING', 'PAUSED']:
                                    logger.info(f"🔍 Connector state is {connector_state}, waiting additional 10 seconds for reconfiguration...")
                                    await asyncio.sleep(10)
                                    status = await self.get_connector_status(connector_name)
                            if status:
                                logger.info(f"📊 Connector status retrieved successfully")
                                connector_state = status.get('connector', {}).get('state', 'UNKNOWN')
                                logger.info(f"Connector {connector_name} state: {connector_state}")
                                
                                # Check for task failures that might indicate yb-admin stream conflicts
                                tasks = status.get('tasks', [])
                                for i, task in enumerate(tasks):
                                    task_state = task.get('state', 'UNKNOWN')
                                    logger.info(f"Task {i} state: {task_state}")
                                    
                                    if task_state == 'FAILED':
                                        task_trace = task.get('trace', '')
                                        logger.error(f"❌ Task {i} failed with trace: {task_trace}")
                                        
                                        # Check for various YugabyteDB stream conflicts in task failure
                                        if ("yb-admin stream" in task_trace.lower() or 
                                            "replication slot" in task_trace.lower() or
                                            ("nullpointerexception" in task_trace.lower() and "beforeimage" in task_trace.lower()) or
                                            ("nullpointerexception" in task_trace.lower() and "isbeforeimageenabled" in task_trace.lower())):
                                            
                                            if "nullpointerexception" in task_trace.lower() and ("beforeimage" in task_trace.lower() or "isbeforeimageenabled" in task_trace.lower()):
                                                logger.error(f"🚨 YugabyteDB before-image metadata corruption detected in task failure!")
                                                logger.error(f"This indicates CDC stream metadata is corrupted or inconsistent")
                                                logger.error(f"NullPointerException in isBeforeImageEnabled suggests stream metadata issues")
                                            else:
                                                logger.error(f"🚨 YugabyteDB yb-admin stream conflict detected in task failure!")
                                                logger.error(f"This indicates existing CDC streams created via yb-admin tool")
                                            
                                            # Delete the failed connector
                                            await self.delete_connector(database_name, schema_name, table_name)
                                            
                                            if attempt < max_retries - 1:
                                                logger.warning(f"Will attempt aggressive cleanup and retry...")
                                                # Force a longer wait to let any previous cleanup take effect
                                                await asyncio.sleep(15)
                                                break  # Break to retry
                                            else:
                                                logger.error(f"❌ PERSISTENT YB-ADMIN STREAM CONFLICT")
                                                logger.error(f"Manual intervention may be required:")
                                                logger.error(f"  1. Connect to YugabyteDB master: {self.db_master_addresses}")
                                                logger.error(f"  2. Run: yb-admin --master_addresses {self.db_master_addresses} list_cdc_streams")
                                                logger.error(f"  3. Delete conflicting streams for database: {database_name}")
                                                return False
                                
                                # If connector and all tasks are healthy, return success
                                if connector_state in ['RUNNING', 'PAUSED'] and all(task.get('state') != 'FAILED' for task in tasks):
                                    logger.info(f"✅ Connector {connector_name} is healthy and running")
                                    return True
                                else:
                                    logger.warning(f"⚠️ Connector {connector_name} created but not healthy (connector: {connector_state})")
                                    if attempt == max_retries - 1:
                                        return False
                            else:
                                logger.warning(f"⚠️ Could not get status for newly created connector {connector_name}")
                                # Still consider this a success if we got a 201 response
                                return True
                                
                        elif response.status == 500:
                            logger.error(f"❌ HTTP 500 - Internal Server Error during connector creation (attempt {attempt + 1})")
                            logger.error(f"Response body: {response_text}")
                            
                            # Parse the 500 error for various YugabyteDB stream conflicts
                            if ("yb-admin stream" in response_text.lower() or
                                ("nullpointerexception" in response_text.lower() and ("beforeimage" in response_text.lower() or "isbeforeimageenabled" in response_text.lower()))):
                                
                                if "nullpointerexception" in response_text.lower() and ("beforeimage" in response_text.lower() or "isbeforeimageenabled" in response_text.lower()):
                                    logger.error(f"🚨 YugabyteDB before-image metadata corruption detected in HTTP 500 response!")
                                    logger.error(f"NullPointerException in isBeforeImageEnabled indicates CDC stream metadata is corrupted")
                                    logger.error(f"This typically happens when CDC streams are in an inconsistent state")
                                else:
                                    logger.error(f"🚨 YugabyteDB yb-admin stream conflict detected in HTTP 500 response!")
                                    logger.error(f"Error: Cannot create a replication slot on the same namespace which already has a yb-admin stream on it")
                                
                                if attempt < max_retries - 1:
                                    logger.warning(f"Attempting automated yb-admin stream cleanup...")
                                    # Try automated yb-admin cleanup
                                    cleanup_success = await self._cleanup_yb_admin_streams(database_name)
                                    if cleanup_success:
                                        logger.info(f"✅ Automated yb-admin stream cleanup completed, retrying connector creation...")
                                        await asyncio.sleep(10)  # Give YugabyteDB time to process cleanup
                                    else:
                                        logger.warning(f"Automated cleanup failed, waiting longer before retry...")
                                        await asyncio.sleep(15)
                                else:
                                    logger.error(f"❌ PERSISTENT YB-ADMIN STREAM CONFLICT")
                                    logger.error(f"Attempting final automated cleanup before giving up...")
                                    await self._cleanup_yb_admin_streams(database_name)
                                    logger.error(f"Manual intervention may still be required:")
                                    logger.error(f"  1. Connect to YugabyteDB master: {self.db_master_addresses}")
                                    logger.error(f"  2. Run: yb-admin --master_addresses {self.db_master_addresses} list_cdc_streams")
                                    logger.error(f"  3. Delete conflicting streams for database: {database_name}")
                                    return False
                            elif "replication slot" in response_text.lower():
                                logger.error(f"🚨 YugabyteDB replication slot conflict detected in HTTP 500 response!")
                                logger.error(f"This suggests existing CDC streams are preventing slot creation")
                                
                                if attempt < max_retries - 1:
                                    logger.warning(f"Will attempt aggressive cleanup and retry...")
                                    await asyncio.sleep(15)
                                else:
                                    logger.error(f"❌ PERSISTENT REPLICATION SLOT CONFLICT")
                                    logger.error(f"Manual CDC cleanup may be required")
                                    return False
                            else:
                                logger.error(f"❌ Unknown HTTP 500 error during connector creation")
                                if attempt == max_retries - 1:
                                    return False
                            
                            # Break to attempt retry
                            break
                                
                        else:
                            logger.error(f"Failed to create connector {connector_name} (attempt {attempt + 1}): {response.status}")
                            logger.error(f"Response body: {response_text}")
                            
                            # Try to parse error details
                            try:
                                error_data = json.loads(response_text)
                                if "message" in error_data:
                                    logger.error(f"Error details: {error_data['message']}")
                                    
                                    # Handle specific error cases
                                    message = error_data['message'].lower()
                                    if "yb-admin stream" in message and "replication slot" in message:
                                        logger.error(f"🚨 YugabyteDB yb-admin stream conflict detected!")
                                        logger.error(f"This indicates existing CDC streams created via yb-admin tool")
                                        
                                        if attempt < max_retries - 1:
                                            logger.warning(f"Attempting automated yb-admin stream cleanup...")
                                            cleanup_success = await self._cleanup_yb_admin_streams(database_name)
                                            if cleanup_success:
                                                logger.info(f"✅ Automated cleanup completed, retrying...")
                                                await asyncio.sleep(10)
                                            else:
                                                logger.warning(f"Automated cleanup failed, waiting longer...")
                                                await asyncio.sleep(15)
                                        else:
                                            logger.error(f"❌ PERSISTENT YB-ADMIN STREAM CONFLICT")
                                            logger.error(f"Attempting final cleanup before giving up...")
                                            await self._cleanup_yb_admin_streams(database_name)
                                            logger.error(f"Manual intervention may still be required:")
                                            logger.error(f"  1. Connect to YugabyteDB master: {self.db_master_addresses}")
                                            logger.error(f"  2. Run: yb-admin --master_addresses {self.db_master_addresses} list_cdc_streams")
                                            logger.error(f"  3. Delete conflicting streams for database: {database_name}")
                                            return False
                                            
                                    elif "timeout" in message:
                                        if attempt < max_retries - 1:
                                            logger.warning(f"Connector creation timed out - will retry after cleanup")
                                        else:
                                            logger.error(f"Connector creation failed after {max_retries} attempts - this indicates a persistent CDC stream conflict")
                                            logger.error(f"This may require YugabyteDB-level CDC stream cleanup")
                                        
                            except:
                                pass
                            
                            # If this is the last attempt, return False
                            if attempt == max_retries - 1:
                                return False
                            
                            # Break to attempt retry
                            break
            
            except asyncio.TimeoutError as e:
                logger.error(f"⏰ TIMEOUT during connector creation attempt {attempt + 1}: {e}")
                logger.error(f"This suggests the Kafka Connect service is overloaded or YugabyteDB is not responding")
                if attempt == max_retries - 1:
                    logger.error(f"❌ Connector creation failed after {max_retries} timeout attempts")
                    return False
                                
            except aiohttp.ClientError as e:
                logger.error(f"🌐 CLIENT ERROR during connector creation attempt {attempt + 1}: {e}")
                logger.error(f"This suggests network or HTTP-level issues with Kafka Connect")
                if attempt == max_retries - 1:
                    logger.error(f"❌ Connector creation failed after {max_retries} client error attempts")
                    return False
                                
            except Exception as e:
                logger.error(f"❗ UNEXPECTED EXCEPTION during connector creation attempt {attempt + 1}: {e}")
                logger.error(f"Exception type: {type(e).__name__}")
                import traceback
                logger.error(f"Stack trace: {traceback.format_exc()}")
                if attempt == max_retries - 1:
                    logger.error(f"❌ Connector creation failed after {max_retries} attempts with unexpected exceptions")
                    return False
        
        # If we reach here without success, return False
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
    
    async def cleanup_stale_cdc_stream(self, database_name: str, schema_name: str, table_name: str) -> bool:
        """Attempt to clean up stale CDC streams that might be causing conflicts"""
        import asyncpg
        database_url = os.getenv("DATABASE_URL", "postgresql://yugabyte@localhost:5433/yugabyte")
        db_url = database_url.rsplit('/', 1)[0] + f'/{database_name}'
        
        table_identifier = f"{database_name}.{schema_name}.{table_name}"
        logger.info(f"Attempting to clean up potential stale CDC publications for: {table_identifier}")
        
        try:
            conn = await asyncpg.connect(db_url)
            try:
                # AGGRESSIVE CLEANUP: Drop ALL publications and slots in this database
                # since YugabyteDB may have internal naming conflicts
                
                # 1. Drop ALL publications
                all_pubs = await conn.fetch("SELECT pubname FROM pg_publication")
                for pub in all_pubs:
                    try:
                        await conn.execute(f"DROP PUBLICATION IF EXISTS {pub['pubname']} CASCADE")
                        logger.info(f"🧹 AGGRESSIVE: Dropped publication {pub['pubname']}")
                    except Exception as e:
                        logger.debug(f"Failed to drop publication {pub['pubname']}: {e}")
                
                # 2. Drop ALL replication slots
                all_slots = await conn.fetch("SELECT slot_name FROM pg_replication_slots")
                for slot in all_slots:
                    try:
                        await conn.execute(f"SELECT pg_drop_replication_slot('{slot['slot_name']}')")
                        logger.info(f"🧹 AGGRESSIVE: Dropped replication slot {slot['slot_name']}")
                    except Exception as e:
                        logger.debug(f"Failed to drop replication slot {slot['slot_name']}: {e}")
                
                # 3. Force terminate any active connections that might be holding CDC resources
                try:
                    # First, get detailed connection info
                    all_connections = await conn.fetch("""
                        SELECT pid, application_name, state, query, backend_start, state_change
                        FROM pg_stat_activity 
                        WHERE pid != pg_backend_pid()
                    """)
                    
                    logger.info(f"🔍 Found {len(all_connections)} total active connections in {database_name}")
                    
                    # Terminate any connections that might be related to CDC
                    cdc_connections = await conn.fetch("""
                        SELECT pid, application_name, state, query, backend_start
                        FROM pg_stat_activity 
                        WHERE pid != pg_backend_pid()
                          AND (application_name LIKE '%debezium%' 
                           OR application_name LIKE '%cdc%'
                           OR application_name LIKE '%yugabyte%'
                           OR query LIKE '%publication%'
                           OR query LIKE '%replication%'
                           OR query LIKE '%cdc%'
                           OR state = 'idle in transaction'
                           OR state = 'active')
                    """)
                    
                    logger.info(f"🔍 Found {len(cdc_connections)} potentially CDC-related connections")
                    
                    for conn_info in cdc_connections:
                        try:
                            logger.info(f"🧹 TERMINATING: PID {conn_info['pid']} - {conn_info['application_name']} ({conn_info['state']})")
                            if conn_info['query']:
                                logger.info(f"   Query: {conn_info['query'][:100]}...")
                            await conn.execute(f"SELECT pg_terminate_backend({conn_info['pid']})")
                            logger.info(f"✅ Successfully terminated PID {conn_info['pid']}")
                        except Exception as e:
                            logger.debug(f"Could not terminate PID {conn_info['pid']}: {e}")
                            
                    if cdc_connections:
                        logger.info(f"🕐 Waiting 3 seconds for connection termination to take effect...")
                        await asyncio.sleep(3)
                            
                except Exception as e:
                    logger.debug(f"Could not check/terminate CDC connections: {e}")
                
                # 4. Wait longer for YugabyteDB to process
                if all_pubs or all_slots:
                    logger.info(f"Waiting 8 seconds for YugabyteDB to process aggressive cleanup...")
                    import asyncio
                    await asyncio.sleep(8)
                
                logger.info(f"CDC stream cleanup completed for {table_identifier}")
                return True
                    
            finally:
                await conn.close()
                
        except Exception as e:
            logger.warning(f"Could not attempt CDC stream cleanup for {database_name}.{schema_name}.{table_name}: {e}")
            return False

    async def check_cdc_stream_exists(self, database_name: str, schema_name: str, table_name: str) -> bool:
        """Check if a CDC stream already exists for the table in YugabyteDB"""
        import asyncpg
        database_url = os.getenv("DATABASE_URL", "postgresql://yugabyte@localhost:5433/yugabyte")
        db_url = database_url.rsplit('/', 1)[0] + f'/{database_name}'
        
        logger.info(f"Checking CDC stream status for {database_name}.{schema_name}.{table_name}")
        
        try:
            conn = await asyncpg.connect(db_url)
            try:
                # First, check all replication slots
                all_slots = await conn.fetch("""
                    SELECT slot_name, slot_type, active, database 
                    FROM pg_replication_slots
                """)
                
                logger.info(f"Found {len(all_slots)} total replication slots in {database_name}")
                for slot in all_slots:
                    logger.info(f"  Slot: {slot['slot_name']}, type: {slot['slot_type']}, active: {slot['active']}")
                
                # Check for slots that might be related to our table
                # Also check for the specific stream ID pattern we use
                stream_id = f"{database_name}_{schema_name}_{table_name}_stream"
                table_slots = await conn.fetch("""
                    SELECT slot_name, slot_type, active 
                    FROM pg_replication_slots 
                    WHERE slot_name LIKE $1 OR slot_name LIKE $2 OR slot_name LIKE $3 OR slot_name LIKE $4
                """, f"%{table_name}%", f"%{schema_name}%", f"%{database_name}%", f"%{stream_id}%")
                
                if table_slots:
                    logger.info(f"Found {len(table_slots)} related replication slots for {database_name}.{schema_name}.{table_name}")
                    for slot in table_slots:
                        logger.info(f"  Related slot: {slot['slot_name']}, active: {slot['active']}")
                    return True
                
                # YugabyteDB specific: Check if table has CDC enabled
                # Try to detect CDC by attempting a simple operation that would fail if CDC is active
                try:
                    # Check table attributes that might indicate CDC
                    table_info = await conn.fetchrow("""
                        SELECT c.relname, c.relkind, c.relhassubclass, n.nspname
                        FROM pg_class c
                        JOIN pg_namespace n ON n.oid = c.relnamespace 
                        WHERE c.relname = $1 AND n.nspname = $2
                    """, table_name, schema_name)
                    
                    if table_info:
                        logger.info(f"Table info: {dict(table_info)}")
                    
                    # YugabyteDB-specific: Check for CDC streams in system tables
                    # Look for YugabyteDB CDC-related system information
                    try:
                        # Check YugabyteDB system catalogs for CDC streams
                        # YugabyteDB might store CDC stream info in pg_class or custom system tables
                        stream_id = f"{database_name}_{schema_name}_{table_name}_stream"
                        
                        # Check for any objects with our stream ID pattern
                        stream_objects = await conn.fetch("""
                            SELECT c.relname, c.relkind, n.nspname
                            FROM pg_class c
                            JOIN pg_namespace n ON n.oid = c.relnamespace 
                            WHERE c.relname LIKE $1
                        """, f"%{stream_id}%")
                        
                        if stream_objects:
                            logger.info(f"Found objects matching stream pattern: {[s['relname'] for s in stream_objects]}")
                            return True
                            
                        # Also check for general CDC-related system objects
                        cdc_objects = await conn.fetch("""
                            SELECT c.relname, c.relkind, n.nspname
                            FROM pg_class c
                            JOIN pg_namespace n ON n.oid = c.relnamespace 
                            WHERE c.relname LIKE '%cdc%' OR c.relname LIKE '%stream%'
                        """)
                        if cdc_objects:
                            logger.info(f"Found CDC/stream related objects: {[s['relname'] for s in cdc_objects[:5]]}")  # Limit to first 5
                            
                    except Exception as sys_e:
                        logger.debug(f"System table check failed: {sys_e}")
                    
                    # YugabyteDB-specific: Test truncate operation to detect CDC
                    # This is the most reliable way to detect YugabyteDB CDC streams
                    logger.info(f"Testing truncate operation to detect YugabyteDB CDC for {schema_name}.{table_name}")
                    try:
                        # Use a savepoint for the truncate test so we can roll back
                        await conn.execute("SAVEPOINT cdc_test")
                        try:
                            await conn.execute(f"TRUNCATE TABLE {schema_name}.{table_name}")
                            # If we get here, no CDC is active - rollback the truncate
                            await conn.execute("ROLLBACK TO SAVEPOINT cdc_test")
                            logger.info(f"No CDC detected - truncate test succeeded for {database_name}.{schema_name}.{table_name}")
                        except Exception as truncate_e:
                            # Rollback the savepoint regardless
                            await conn.execute("ROLLBACK TO SAVEPOINT cdc_test")
                            error_str = str(truncate_e).lower()
                            if "cdc" in error_str and "rewrite" in error_str:
                                logger.info(f"CDC detected for {database_name}.{schema_name}.{table_name} via truncate test: {truncate_e}")
                                return True
                            else:
                                # Other error (like foreign key constraints) - not CDC related
                                logger.info(f"Truncate test failed for non-CDC reason: {truncate_e}")
                                # Fall through to publication test
                        finally:
                            # Clean up savepoint
                            try:
                                await conn.execute("RELEASE SAVEPOINT cdc_test")
                            except:
                                pass
                    except Exception as savepoint_e:
                        logger.debug(f"Savepoint-based truncate test failed: {savepoint_e}")
                        # Fall through to publication test
                    
                    # Fallback: Try to create a test publication (this would fail if CDC is active)
                    test_pub_name = f"test_cdc_check_{table_name}"
                    try:
                        await conn.execute(f"CREATE PUBLICATION {test_pub_name} FOR TABLE {schema_name}.{table_name}")
                        # If we got here, no CDC is active - clean up test publication
                        await conn.execute(f"DROP PUBLICATION {test_pub_name}")
                        logger.info(f"No CDC detected for {database_name}.{schema_name}.{table_name} (test publication succeeded)")
                        return False
                    except Exception as pub_e:
                        if "already exists" in str(pub_e).lower() or "cdc" in str(pub_e).lower():
                            logger.info(f"CDC likely active for {database_name}.{schema_name}.{table_name} (publication test failed: {pub_e})")
                            return True
                        else:
                            logger.debug(f"Publication test failed for other reason: {pub_e}")
                            return False
                            
                except Exception as check_e:
                    logger.debug(f"Table info check failed: {check_e}")
                
                logger.info(f"No CDC stream detected for {database_name}.{schema_name}.{table_name}")
                return False
                
            finally:
                await conn.close()
                
        except Exception as e:
            logger.warning(f"Could not check CDC stream status for {database_name}.{schema_name}.{table_name}: {e}")
            return False
    
    async def get_connector_status(self, connector_name: str) -> Optional[Dict]:
        """Get connector status"""
        
        try:
            status_url = f"{self.connectors_endpoint}/{connector_name}/status"
            logger.info(f"🔍 Requesting connector status from: {status_url}")
            
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
                async with session.get(status_url) as response:
                    logger.info(f"📡 Status request response: {response.status}")
                    
                    if response.status == 200:
                        status_data = await response.json()
                        logger.info(f"📊 Status data keys: {list(status_data.keys())}")
                        return status_data
                    elif response.status == 404:
                        logger.warning(f"❌ Connector {connector_name} not found (404)")
                        return None
                    else:
                        response_text = await response.text()
                        logger.error(f"❌ Failed to get connector status: {response.status}")
                        logger.error(f"Response: {response_text[:500]}...")  # First 500 chars
                        return None
                        
        except asyncio.TimeoutError as e:
            logger.error(f"⏰ Timeout getting connector status {connector_name}: {e}")
            return None
        except Exception as e:
            logger.error(f"❗ Error getting connector status {connector_name}: {e}")
            return None
    
    async def _check_connect_health(self) -> bool:
        """Check if Kafka Connect service is healthy and responsive"""
        try:
            logger.info(f"🩺 Testing Kafka Connect health at: {self.connector_url}")
            
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
                # Try to list connectors as a health check
                async with session.get(self.connectors_endpoint) as response:
                    if response.status == 200:
                        connectors = await response.json()
                        logger.info(f"✅ Kafka Connect is healthy - found {len(connectors)} existing connectors")
                        return True
                    else:
                        logger.error(f"❌ Kafka Connect health check failed: {response.status}")
                        return False
                        
        except asyncio.TimeoutError:
            logger.error(f"⏰ Kafka Connect health check timed out")
            return False
        except Exception as e:
            logger.error(f"❗ Kafka Connect health check error: {e}")
            return False

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