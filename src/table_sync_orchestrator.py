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

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import List, Optional

from flask import Flask, jsonify
from classes.table_info import TableInfo
from classes.logging import Logging
from classes.kafka_connector import KafkaConnector
from classes.bigquery_manager import BigQueryManager
from classes.config_reader import ConfigReader, ConfigKeys, ProcessingKeys, HealthCheckKeys
from classes.yugabyte_db_manager import YugabyteDBManager

# ----------------------------- Orchestrator -----------------------------

class TableSyncOrchestrator:
    def __init__(self, config_path: str, start_servers: bool = True):
        self.running = False
        self.config = ConfigReader(config_path).load_config()
        self.yugabyte_manager = YugabyteDBManager(self.config)
        self.kafka_connector = KafkaConnector(self.config)
        self.bigquery_manager = BigQueryManager(self.config)
        self.logger = Logging(self.config)
        self.yugabyte_manager.create_debezium_signal_table()
        
        if start_servers:
            if not os.getenv('DISABLE_HEALTH'):
                self._start_health_server()

    # ----------------------------- Health & Metrics Servers -----------------------------

    def _start_health_server(self):
        app = Flask(__name__)

        @app.route('/health')
        def health():
            return jsonify({'status': 'healthy', 'timestamp': datetime.utcnow().isoformat()})

        @app.route('/ready')
        def ready():
            return jsonify({'status': 'ready', 'running': self.running})

        def run_server():
            port = int((self.config.get(ConfigKeys.HEALTH_CHECK.value, {}) or {}).get(HealthCheckKeys.PORT.value, 8080))
            app.run(host='0.0.0.0', port=port, debug=False)

        threading.Thread(target=run_server, daemon=True).start()
        self.logger.logMessage(Logging.LogLevel.INFO, "Health server started", port=(self.config.get(ConfigKeys.HEALTH_CHECK.value, {}) or {}).get(HealthCheckKeys.PORT.value, 8080))
        
    # ------------------------------ Helper functions ------------------------------
    
    def getTableInfoForTable(self, table: str, tables: List[TableInfo]) -> Optional[TableInfo]:
        for t in tables:
            if t.table == table:
                return t
        return None

    # ----------------------------- Orchestrator Loop -----------------------------
    
    def _table_sync_loop(self, db):
        tables = self.yugabyte_manager._discover_tables(db)
        self.logger.logMessage(Logging.LogLevel.INFO, "Tables discovered", database=db, tables=[t.table for t in tables])

        for table_info in tables:
            # for each table in the database check if it has annotation enabled
            if table_info.annotation is not None and table_info.annotation.enabled:
                # Check to see if table has entry in debezium signal table
                if not self.yugabyte_manager.entry_exists_in_debezium_signal(table_info):
                    # Table does not have entry in debezium signal table
                    # Means this is a newly annotated table so we check to see if connectors exist as they may be in a bad state
                    
                    # but first lets check to see if there is already a teable in bigquery.
                    # this would mean that the table has been annotated and synced before
                    # so this could be a new build of the platform.
                    # what we need to do in this case is to pull the data from bigquery into the yugabyte table
                    # and then setup the connectors to catch new changes
                    self.logger.logMessage(Logging.LogLevel.INFO, "Table does not have entry in debezium signal table, checking BigQuery", table=table_info.table)
                    bigquery_exists = self.bigquery_manager.check_table_exists(table_info.bq_dataset, table_info.bq_table)
                    if bigquery_exists:
                        self.logger.logMessage(Logging.LogLevel.INFO, "BigQuery table already exists, need to backfill data into YugabyteDB", table=table_info.table, bq_table=table_info.bq_table)
                        # Here you would implement the logic to backfill data from BigQuery to YugabyteDB
                        # This is a placeholder for the actual backfill logic
                        try:
                            bigquery_data = self.bigquery_manager.fetch_bigquery_data(table_info)
                            self.logger.logMessage(Logging.LogLevel.INFO, "Fetched data from BigQuery", table=table_info.table, row_count=len(bigquery_data))
                            self.yugabyte_manager.clear_yugabyte_table(db, table_info)
                            self.logger.logMessage(Logging.LogLevel.INFO, "Cleared YugabyteDB table before backfill", database=db, table_info=table_info.table)
                            self.yugabyte_manager.insert_into_yugabyte(bigquery_data, db, table_info)
                            self.logger.logMessage(Logging.LogLevel.INFO, "Backfill from BigQuery to YugabyteDB completed", table=table_info.table)
                        except Exception as e:
                            self.logger.logMessage(Logging.LogLevel.ERROR, "Error during backfill from BigQuery", table=table_info.table, error=str(e))
                    else:
                        self.logger.logMessage(Logging.LogLevel.INFO, "BigQuery table does not exist, proceeding to set up connectors", table=table_info.table)
                    
                    self.logger.logMessage(Logging.LogLevel.INFO, "Table does not have entry in debezium signal table, checking connectors", table=table_info.table)
                    connector_statuses = self.kafka_connector.check_connector_exists(table_info)
                    if not connector_statuses['source_exists'] or not connector_statuses['sink_exists']:
                        try:
                            self.kafka_connector.setup_connectors(table_info)
                        except Exception as e:
                            self.logger.logMessage(Logging.LogLevel.ERROR, "Error setting up connectors", table=table_info.table, error=str(e))
                    else:
                        self.logger.logMessage(Logging.LogLevel.INFO, "Connectors already exist for table, resetting and rebuilding", table=table_info.table)
                        try:
                            self.kafka_connector.reset_connectors(table_info)
                            self.kafka_connector.setup_connectors(table_info)
                        except Exception as e:
                            self.logger.logMessage(Logging.LogLevel.ERROR, "Error resetting connectors", table=table_info.table, error=str(e))
                else:
                    self.logger.logMessage(Logging.LogLevel.INFO, "Table already has entry in debezium signal table, check if connectors are running", table=table_info.table)
                    connector_statuses = self.kafka_connector.check_connector_exists(table_info)
                    if not connector_statuses['source_exists'] or not connector_statuses['sink_exists']:
                        self.logger.logMessage(Logging.LogLevel.INFO, "One or more connectors do not exist, setting up connectors", table=table_info.table)
                        try:
                            self.kafka_connector.reset_connectors(table_info)
                            self.kafka_connector.setup_connectors(table_info)
                        except Exception as e:
                            self.logger.logMessage(Logging.LogLevel.ERROR, "Error setting up connectors", table=table_info.table, error=str(e))
            else:
                self.logger.logMessage(Logging.LogLevel.INFO, "Table not annotated or annotation disabled, check debezium signal table to see if entry exists", table=table_info.table)
                self.logger.logMessage(Logging.LogLevel.INFO, "Table annotation disabled or table not found, removing from signal table and tearing down connectors", table=table_info.table)
                try:
                    self.logger.logMessage(Logging.LogLevel.INFO, "Tearing down connectors and removing from signal table", table=table_info.table)
                    self.yugabyte_manager.remove_entry_from_debezium_signal(table_info.database, table_info.table)
                    self.logger.logMessage(Logging.LogLevel.INFO, "Removed entry from debezium signal table", table=table_info.table)
                    self.kafka_connector.reset_connectors(table_info)
                    self.logger.logMessage(Logging.LogLevel.INFO, "Connectors reset successfully", table=table_info.table)
                    self.bigquery_manager.delete_table(table_info)
                    self.logger.logMessage(Logging.LogLevel.INFO, "BigQuery table deleted successfully", table=table_info.table)
                except Exception as e:
                    self.logger.logMessage(Logging.LogLevel.ERROR, "Error tearing down connectors", table=table_info.table, error=str(e))
                        
        # For tables in the database check entries in the signal table for tables in database
        # Fetch all signal table entries for database and verify that annotation is still enabled for each
        for table in self.yugabyte_manager.fetch_tables_in_debezium_signal(db):
            table_info = self.getTableInfoForTable(table, tables)
            if table_info is None or table_info.annotation is None or not table_info.annotation.enabled:
                self.logger.logMessage(Logging.LogLevel.INFO, "Table annotation disabled or table not found, removing from signal table and tearing down connectors", table=table)
                try:
                    self.logger.logMessage(Logging.LogLevel.INFO, "Tearing down connectors and removing from signal table", table=table)
                    self.yugabyte_manager.remove_entry_from_debezium_signal(table_info.database, table_info.table)
                    self.logger.logMessage(Logging.LogLevel.INFO, "Removed entry from debezium signal table", table=table)
                    self.kafka_connector.reset_connectors(table_info)
                    self.logger.logMessage(Logging.LogLevel.INFO, "Connectors reset successfully", table=table)
                    self.bigquery_manager.delete_table(table_info)
                    self.logger.logMessage(Logging.LogLevel.INFO, "BigQuery table deleted successfully", table=table)
                except Exception as e:
                    self.logger.logMessage(Logging.LogLevel.ERROR, "Error tearing down connectors", table=table, error=str(e))

    # ----------------------------- Main Loop -----------------------------

    def start(self):
        self.logger.logMessage(Logging.LogLevel.INFO, "Starting orchestrator")
        self.running = True
        self.logger.logMessage(Logging.LogLevel.INFO, "Starting processing loop")
        try:
            self.logger.logMessage(Logging.LogLevel.INFO, "Starting table sync loop")
            try:
                while self.running:
                    start = time.time()                    
                    self.logger.logMessage(Logging.LogLevel.INFO, "Beginning processing")
                    try:
                        # Discover databases and create connectors as needed
                        databases = self.yugabyte_manager._discover_databases()
                        self.logger.logMessage(Logging.LogLevel.INFO, "Databases discovered", databases=databases)
                    
                        with ThreadPoolExecutor(max_workers=self.config.get(ConfigKeys.PROCESSING.value, {}).get(ProcessingKeys.MAX_SCAN_THREADS.value, 4)) as executor:
                            futures = {executor.submit(self._table_sync_loop, db): db for db in databases}

                            for future in as_completed(futures):
                                ti = futures[future]
                                try:
                                    future.result()
                                except Exception as e:
                                    self.logger.logMessage(Logging.LogLevel.ERROR, "Error in table sync loop", error=str(e))
                        
                    except Exception as e:
                        self.logger.logMessage(Logging.LogLevel.ERROR, "Error during table sync", error=str(e))
                    finally:
                        elapsed = time.time() - start
                        self.logger.logMessage(Logging.LogLevel.INFO, "Scan loop complete", elapsed_time=elapsed)
                        time.sleep(max(0, self.config.get(ConfigKeys.PROCESSING.value, {}).get(ProcessingKeys.SCAN_INTERVAL_SECONDS.value, 30) - elapsed))
            except Exception as e:
                self.logger.logMessage(Logging.LogLevel.ERROR, "Unexpected error in table sync loop", error=str(e))
            finally:
                self.logger.logMessage(Logging.LogLevel.INFO, "Table sync loop exiting")
                            
        except Exception as e:
            self.logger.logMessage(Logging.LogLevel.ERROR, "Unexpected error in orchestrator", error=str(e))
        finally:
            self.running = False
            self.logger.logMessage(Logging.LogLevel.INFO, "Orchestrator stopped")

# ----------------------------- Main Entry -----------------------------

def main():
    import argparse
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
        print("Stopping orchestrator...")
        orchestrator.running = False
    except Exception as e:
        print(f"Unexpected error in orchestrator: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()
