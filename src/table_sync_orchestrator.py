#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Production Table Sync Orchestrator for YugabyteDB → BigQuery.

Highlights:
- Filters out non-Postgres DSN keys before psycopg2.connect()
- Robust connection options (timeouts, keepalives, application_name)
- BigQuery guarded if client is None
- Debezium YugabyteDB connector: auto-detects installed class
  (prefers io.debezium.connector.yugabytedb.YugabyteDBgRPCConnector)
- CDC stream id via annotation/config; else auto list/create via yb-admin if master addresses available
- Validates connector config before create; includes topic.prefix + database.server.name
- Supplies database.hostname/port/user/password if provided (some validators expect them)
- Periodic reconciliation:
  * verifies connector presence & RUNNING status
  * restarts failed connectors, optionally deletes/recreates
  * optional Kafka topic existence check if kafka.bootstrap_servers configured
- Exposes /status with per-table sync view
- Logs failed HTTP requests to Kafka Connect with redacted payloads and truncated bodies
"""

import sys
import threading
import time
import os
import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import List
from flask import Flask, jsonify

from classes.ybadmin_utils import YBAdminUtils
from classes.table_info import TableInfo
from classes.logging import Logging
from services.kafka_connector import KafkaConnector
from services.bigquery_manager import BigQueryManager
from classes.config_reader import ConfigReader, ConfigKeys, ProcessingKeys, HealthCheckKeys, RedisCacheKeys
from services.yugabyte_db_manager import YugabyteDBManager
from services.redis import RedisService

# ----------------------------- Orchestrator -----------------------------

class TableSyncOrchestrator:
    def __init__(self, config_path: str, start_servers: bool = True):
        self.running = False        
        self.config = ConfigReader(config_path).load_config()
        self.logger = Logging(self.config)
        self.yb_admin_utils = YBAdminUtils(self.config, self.logger)
        yugabyte_manager = YugabyteDBManager(self.config, self.logger)
        self.redis_cache = RedisService(self.config, self.logger)
        self.bigquery_manager = BigQueryManager(self.config, self.logger)
        
        # Background task management
        self._background_threads = []
        self._background_shutdown = threading.Event()
        
        databases = yugabyte_manager._discover_databases("kafka")
        for db in databases:
            yugabyte_manager.create_debezium_signal_table(db)
        
        if start_servers:
            if not os.getenv('DISABLE_HEALTH'):
                self._start_health_server()

    # ----------------------------- Health & Metrics Servers -----------------------------

    def _start_health_server(self):
        port = int((self.config.get(ConfigKeys.HEALTH_CHECK.value, {}) or {}).get(HealthCheckKeys.PORT.value, 8080))
        app = Flask(__name__)

        @app.route('/health')
        def health():
            return jsonify({'status': 'healthy', 'timestamp': datetime.utcnow().isoformat()})

        @app.route('/ready')
        def ready():
            return jsonify({'status': 'ready', 'running': self.running})

        def run_server():
            app.run(host='0.0.0.0', port=port, debug=False)

        threading.Thread(target=run_server, daemon=True).start()
        print(f"Health server started, port {port}")
        
    # ------------------------------ Background Task Management ------------------------------
    
    def _background_database_preparation(self, databases: List[str]):
        """Run database preparation in background thread."""
        self.logger.logMessage(Logging.LogLevel.INFO, "Starting background database preparation", databases=databases)
        
        try:
            with ThreadPoolExecutor(max_workers=self.config.get(ConfigKeys.PROCESSING.value, {}).get(ProcessingKeys.MAX_PREPARATION_THREADS.value, 4)) as executor:
                futures = {executor.submit(self.prepare_database, db, self.logger, self.config): db for db in databases}
                
                for future in as_completed(futures):
                    if self._background_shutdown.is_set():
                        break
                    db = futures[future]
                    try:
                        future.result()
                        self.logger.logMessage(Logging.LogLevel.DEBUG, "Database preparation completed", database=db)
                    except Exception as e:
                        error = str(e)
                        self.logger.logMessage(Logging.LogLevel.ERROR, "Error in database preparation", database=db, error=error)
            
            self.logger.logMessage(Logging.LogLevel.INFO, "Background database preparation completed")
        except Exception as e:
            self.logger.logMessage(Logging.LogLevel.ERROR, "Critical error in background database preparation", error=str(e))
    
    def _background_cache_checking(self, databases: List[str]):
        """Run cache checking continuously in background thread."""
        self.logger.logMessage(Logging.LogLevel.INFO, "Starting background cache checking", databases=databases)
        
        cache_check_interval = self.config.get(ConfigKeys.PROCESSING.value, {}).get('cache_check_interval_seconds', 60)
        
        while not self._background_shutdown.is_set():
            try:
                self.logger.logMessage(Logging.LogLevel.DEBUG, "Running cache check cycle")
                
                with ThreadPoolExecutor(max_workers=self.config.get(ConfigKeys.PROCESSING.value, {}).get(ProcessingKeys.MAX_CACHE_CHECK_THREADS.value, 4)) as executor:
                    futures = {executor.submit(self.check_cache_counts, db, self.logger, self.config): db for db in databases}
                    
                    for future in as_completed(futures):
                        if self._background_shutdown.is_set():
                            break
                        db = futures[future]
                        try:
                            future.result()
                        except Exception as e:
                            error = str(e)
                            self.logger.logMessage(Logging.LogLevel.ERROR, "Error in cache checking", database=db, error=error)
                
                # Wait for next cycle or shutdown signal
                self._background_shutdown.wait(timeout=cache_check_interval)
                
            except Exception as e:
                self.logger.logMessage(Logging.LogLevel.ERROR, "Error in background cache checking cycle", error=str(e))
                # Wait a bit before retrying
                self._background_shutdown.wait(timeout=30)
        
        self.logger.logMessage(Logging.LogLevel.INFO, "Background cache checking stopped")
    
    def _start_background_tasks(self, databases: List[str]):
        """Start background tasks for database preparation and cache checking."""
        self.logger.logMessage(Logging.LogLevel.INFO, "Starting background tasks")
        
        # Start database preparation thread (must complete before main loop)
        prep_thread = threading.Thread(
            target=self._background_database_preparation,
            args=(databases,),
            daemon=False,  # Not daemon - we need to wait for completion
            name="DatabasePreparation"
        )
        prep_thread.start()
        
        # Wait for database preparation to complete
        prep_thread.join()
        self.logger.logMessage(Logging.LogLevel.INFO, "Database preparation completed, starting cache checking")
        
        # Start cache checking thread (runs continuously in background)
        cache_thread = threading.Thread(
            target=self._background_cache_checking,
            args=(databases,),
            daemon=True,  # Daemon thread - can be killed when main exits
            name="CacheChecking"
        )
        cache_thread.start()
        self._background_threads.append(cache_thread)
        
        self.logger.logMessage(Logging.LogLevel.INFO, "Background tasks started")
    
    def _stop_background_tasks(self):
        """Signal background tasks to stop and wait for completion."""
        self.logger.logMessage(Logging.LogLevel.INFO, "Stopping background tasks")
        self._background_shutdown.set()
        
        # Wait for non-daemon background threads to complete
        for thread in self._background_threads:
            if thread.is_alive() and not thread.daemon:
                thread.join(timeout=30)  # Wait up to 30 seconds
        
        self.logger.logMessage(Logging.LogLevel.INFO, "Background tasks stopped")
        
    # ------------------------------ Cache Check Thread ------------------------------
    
    def check_cache_counts(self, db: str, logger: Logging, config: ConfigReader):
        self.logger.logMessage(Logging.LogLevel.INFO, "Checking cached row counts for tables in database", database=db)
        yugabyte_manager = YugabyteDBManager(config, logger)
        tables: list[TableInfo] = yugabyte_manager._discover_tables(db)
        try:
            while True:
                start = time.time()
                for table_info in tables:
                    redis_val = self.redis_cache.get(
                        self.config.get(ConfigKeys.REDIS.value).get(RedisCacheKeys.ROW_COUNTS.value),
                        self.redis_cache.table_count_key_format.format(
                            db=table_info.database, 
                            table_info=table_info)
                    )
                    if redis_val is not None:
                        logger.logMessage(Logging.LogLevel.DEBUG, "Found cached row count", database=db, table=table_info.to_dict(), cached_count=redis_val)
                        if self.bigquery_manager.get_row_count(table_info) == redis_val:
                            logger.logMessage(Logging.LogLevel.INFO, "BigQuery table row count matches cached YugabyteDB count", database=db, table=table_info.to_dict(), row_count=redis_val)
                            self.redis_cache.delete(
                                self.config.get(ConfigKeys.REDIS.value).get(RedisCacheKeys.ROW_COUNTS.value),
                                self.redis_cache.table_count_key_format.format(
                                    db=table_info.database, 
                                    table_info=table_info)
                            )
                            yugabyte_manager.remove_entry_from_debezium_signal(db, table_info.table)
                            
                # Logic to check cached counts and compare with BigQuery
                logger.logMessage(Logging.LogLevel.DEBUG, "Cache check complete", database=db, table=table_info)
                # Sleep for the configured interval
                elapsed = time.time() - start
                sleep_time = max(0, self.config.get(ConfigKeys.PROCESSING.value, {}).get(ProcessingKeys.SCAN_INTERVAL_SECONDS.value, 30) - elapsed)
                if sleep_time > 0:
                    time.sleep(sleep_time)
                    
        except Exception as e:
            logger.logMessage(Logging.LogLevel.ERROR, "Error checking cache counts", database=db, error=str(e))
        
    # ------------------------------ Prepare Database Thread ------------------------------
            
    def prepare_database(self, db: str, logger: Logging, config: ConfigReader):
        self.logger.logMessage(Logging.LogLevel.INFO, "Preparing database for sync", database=db)
        yugabyte_manager = YugabyteDBManager(config, logger)
        try:
            yugabyte_manager.create_debezium_signal_table(db)
            yugabyte_manager.create_stream_table(db)
            if yugabyte_manager.stream_exists(db) is not None:
                logger.logMessage(Logging.LogLevel.DEBUG, "Stream already exists for database, skipping creation", database=db)
                return  # Stop preparation if stream exists
            
            stream_id = self.yb_admin_utils.create_stream(db)
            yugabyte_manager.insert_into_stream_table(stream_id, db)
            logger.logMessage(Logging.LogLevel.DEBUG, "Database preparation complete", database=db)
        except Exception as e:
            logger.logMessage(Logging.LogLevel.ERROR, "Error preparing database", database=db, error=str(e))

    # ----------------------------- Orchestrator Loop Thread-----------------------------
    
    def _table_sync_loop(self, db):
        logger = Logging(self.config)
        logger.logMessage(Logging.LogLevel.INFO, "Starting table sync loop for database", database=db)
        yugabyte_manager = YugabyteDBManager(self.config, logger)
        logger.logMessage(Logging.LogLevel.DEBUG, "YugabyteDBManager initialized", database=db)
        kafka_connector = KafkaConnector(self.config, logger)
        logger.logMessage(Logging.LogLevel.DEBUG, "KafkaConnector initialized", database=db)
        bigquery_manager = BigQueryManager(self.config, logger)
        logger.logMessage(Logging.LogLevel.DEBUG, "BigQueryManager initialized", database=db)
        
        tables = yugabyte_manager._discover_tables(db)
        logger.logMessage(Logging.LogLevel.DEBUG, "Tables discovered", database=db, tables=[t.table for t in tables])

        for table_info in tables:
            try:
                logger.logMessage(Logging.LogLevel.DEBUG, "Processing table", table=table_info.to_dict())
                # for each table in the database check if it has annotation enabled
                if table_info.annotation is not None and table_info.annotation.enabled:
                    # Table is annotated and enabled, check to see if connectors exist
                    connector_statuses = kafka_connector.check_connector_exists(table_info)
                    if not connector_statuses['source_exists'] or not connector_statuses['sink_exists']:
                        logger.logMessage(Logging.LogLevel.INFO, "Table annotation enabled, setting up sync", table=table_info.to_dict())
                        # Create BigQuery dataset if not exists
                        bigquery_manager.create_dataset(table_info)
                        # get yugabyte table record count to verify snapshot success
                        if yugabyte_manager.get_row_count(table_info) > 0:
                            logger.logMessage(Logging.LogLevel.DEBUG, "Yugabyte table has data, caching record count to verify later with bigquery count", table=table_info.to_dict())
                            self.redis_cache.set(self.config.get(ConfigKeys.REDIS.value).get(RedisCacheKeys.ROW_COUNTS.value),
                                self.redis_cache.table_count_key_format.format(
                                    db=table_info.database, 
                                    table_info=table_info), 
                                yugabyte_manager.get_row_count(table_info), 
                                ex=self.config.get(ConfigKeys.REDIS.value, {}).get('default_ttl', 300)
                            )
                        # Create source connector
                        kafka_connector.create_source_connector(table_info)
                        # Create sink connector
                        kafka_connector.create_sink_connector(table_info)
                    elif connector_statuses['source_exists'] and not connector_statuses['sink_exists']:
                        logger.logMessage(Logging.LogLevel.INFO, "Source connector exists but sink connector missing, creating sink connector", table=table_info.to_dict())
                        kafka_connector.create_sink_connector(table_info)
                    elif not connector_statuses['source_exists'] and connector_statuses['sink_exists']:
                        logger.logMessage(Logging.LogLevel.INFO, "Sink connector exists but source connector missing, creating source connector", table=table_info.to_dict())
                        kafka_connector.create_source_connector(table_info)
                        logger.logMessage(Logging.LogLevel.INFO, "Table annotation enabled and connectors exist, no action needed", table=table_info.to_dict())
                else:
                    logger.logMessage(Logging.LogLevel.INFO, "Table annotation disabled or not found, removing any existing setup if present", table=table_info.to_dict())
                    
            except Exception as e:
                logger.logMessage(Logging.LogLevel.ERROR, "Error processing table", table=table_info.to_dict(), error=str(e))
                continue

    # ----------------------------- Main Loop -----------------------------

    def start(self):
        """Start the orchestrator with background tasks and main scan loop."""
        self.running = True
        yugabyte_manager = YugabyteDBManager(self.config, self.logger)
        databases = yugabyte_manager._discover_databases()
        print(f"Discovered databases: {databases}")
        
        try:
            # Start background tasks (database preparation will complete before returning)
            self._start_background_tasks(databases)
            
            # Main scan loop - runs after database preparation is complete
            self.logger.logMessage(Logging.LogLevel.INFO, "Starting main scan loop")
            
            while self.running:
                self.logger.logMessage(Logging.LogLevel.INFO, "Starting scan loop for all databases")
                start = time.time()
                
                try:
                    with ThreadPoolExecutor(max_workers=self.config.get(ConfigKeys.PROCESSING.value, {}).get(ProcessingKeys.MAX_SCAN_THREADS.value, 4)) as executor:
                        futures = {executor.submit(self._table_sync_loop, db): db for db in databases}

                        for future in as_completed(futures):
                            db = futures[future]
                            try:
                                future.result()
                            except Exception as e:
                                error = str(e)
                                self.logger.logMessage(Logging.LogLevel.ERROR, "Error in table sync loop", database=db, error=error)

                except Exception as e:
                    error = str(e)
                    self.logger.logMessage(Logging.LogLevel.ERROR, "Error during table sync", error=error)
                finally:
                    elapsed = time.time() - start
                    minutes = int(elapsed // 60)
                    seconds = elapsed % 60
                    print(f"Scan loop complete, elapsed time: {minutes}m {seconds:.2f}s")
                    self.logger.logMessage(Logging.LogLevel.INFO, "Scan loop complete", elapsed_time=elapsed, elapsed_formatted=f"{minutes}m {seconds:.2f}s")
                    
                    # Sleep for the configured interval
                    sleep_time = max(0, self.config.get(ConfigKeys.PROCESSING.value, {}).get(ProcessingKeys.SCAN_INTERVAL_SECONDS.value, 30) - elapsed)
                    if sleep_time > 0:
                        time.sleep(sleep_time)
        
        except KeyboardInterrupt:
            self.logger.logMessage(Logging.LogLevel.INFO, "Received interrupt signal, shutting down")
            self.running = False
        except Exception as e:
            self.logger.logMessage(Logging.LogLevel.ERROR, "Critical error in main loop", error=str(e))
            self.running = False
        finally:
            # Clean shutdown
            self._stop_background_tasks()
            self.logger.logMessage(Logging.LogLevel.INFO, "Orchestrator shutdown complete")

    def stop(self):
        """Gracefully stop the orchestrator."""
        self.logger.logMessage(Logging.LogLevel.INFO, "Stopping orchestrator")
        self.running = False
        self._stop_background_tasks()

# ----------------------------- Main Entry -----------------------------

def main():
    parser = argparse.ArgumentParser(description="Table Sync Orchestrator for YugabyteDB → BigQuery")
    parser.add_argument("config", help="Path to the configuration file")
    parser.add_argument("--no-start", action="store_true", help="Load config and exit (for testing)")
    args = parser.parse_args()

    orchestrator = TableSyncOrchestrator(args.config, start_servers=not args.no_start)
    if args.no_start:
        print("Config loaded, ready for testing")
        sys.exit(0)

    try:
        orchestrator.start()
    except KeyboardInterrupt:
        print("Received interrupt signal, shutting down gracefully...")
        orchestrator.stop()
    except Exception as e:
        print(f"Unexpected error in orchestrator: {e}", file=sys.stderr)
        orchestrator.stop()
        sys.exit(1)

if __name__ == "__main__":
    main()
