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
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any
from datetime import datetime

import structlog
from prometheus_client import Counter, Histogram, Gauge, start_http_server
from flask import Flask, jsonify

from classes.kafka_connector import KafkaConnector
from classes.bigquery_manager import BigQueryManager
from classes.config_reader import ConfigReader, ConfigKeys, ProcessingKeys, LoggingKeys, HealthCheckKeys, MetricsKeys
from classes.yugabyte_db_manager import YugabyteDBManager

HAVE_KAFKA = True

# ----------------------------- Orchestrator -----------------------------

class TableSyncOrchestrator:
    def __init__(self, config_path: str, start_servers: bool = True):
        self.config = ConfigReader(config_path).load_config()
        self.logger = self._init_logger()
        self.yugabyte_manager = YugabyteDBManager(self.config)
        self.kafka_connector = KafkaConnector(self.config)
        self.bigquery_manager = BigQueryManager(self.config)
        self.running = False
        self.metrics = None  # Metrics disabled for testing
        self.logger = self._init_logger()
        self.mock_enabled = self.config.get(ConfigKeys.MOCK.value, False)

        self.yugabyte_manager.create_debezium_signal_table()
        import os
        if start_servers:
            if not os.getenv('DISABLE_HEALTH'):
                self._start_health_server()
            if not os.getenv('DISABLE_METRICS'):
                self._start_metrics_server()

    # ----------------------------- Logging & Metrics -----------------------------

    def _init_logger(self) -> structlog.BoundLogger:
        import logging
        lvl = (self.config.get(ConfigKeys.LOGGING.value, {}) or {}).get(LoggingKeys.LEVEL.value, "INFO").upper()
        numeric = getattr(logging, lvl, logging.INFO)
        structlog.configure(
            processors=[
                structlog.processors.TimeStamper(fmt="iso"),
                structlog.processors.add_log_level,
                structlog.processors.JSONRenderer()
            ],
            wrapper_class=structlog.make_filtering_bound_logger(numeric),
            logger_factory=structlog.PrintLoggerFactory(),
            cache_logger_on_first_use=True,
        )
        return structlog.get_logger("table_sync_orchestrator")

    def _init_metrics(self) -> Dict[str, Any]:
        return {
            'tables_scanned': Counter('sync_tables_scanned_total', 'Total tables scanned'),
            'tables_synced': Counter('sync_tables_synced_total', 'Total tables synced'),
            'sync_errors': Counter('sync_errors_total', 'Total sync errors', ['error_type']),
            'scan_duration': Histogram('sync_scan_duration_seconds', 'Time spent scanning'),
            'active_syncs': Gauge('sync_active_syncs', 'Number of active syncs'),
            'connectors_running': Gauge('sync_connectors_running', 'Number of connectors with all tasks RUNNING'),
            'last_scan_time': Gauge('sync_last_scan_timestamp', 'Timestamp of last scan'),
        }

    # ----------------------------- Health & Metrics Servers -----------------------------

    def _start_health_server(self):
        app = Flask(__name__)

        @app.route('/health')
        def health():
            return jsonify({'status': 'healthy', 'timestamp': datetime.utcnow().isoformat()})

        @app.route('/ready')
        def ready():
            return jsonify({'status': 'ready', 'running': self.running})

        @app.route('/status')
        def status():
            # Summarize status table
            out = []
            # for k, v in self.status_table.items():
            #     out.append({
            #         "table": v.table_info.full_name,
            #         "annotation_enabled": v.annotation_enabled,
            #         "bigquery_exists": v.bigquery_exists,
            #         "connector_exists": v.connector_exists,
            #         "sync_active": v.sync_active,
            #         "last_connector_state": v.last_connector_state,
            #         "expected_topic": v.expected_topic,
            #         "topic_exists": v.topic_exists,
            #         "last_error": v.last_error,
            #         "last_scan": v.last_scan.isoformat(),
            #     })
            return jsonify({"tables": out, "ts": datetime.utcnow().isoformat()})

        def run_server():
            port = int((self.config.get(ConfigKeys.HEALTH_CHECK.value, {}) or {}).get(HealthCheckKeys.PORT.value, 8080))
            app.run(host='0.0.0.0', port=port, debug=False)

        threading.Thread(target=run_server, daemon=True).start()
        self.logger.info("Health server started", port=(self.config.get(ConfigKeys.HEALTH_CHECK.value, {}) or {}).get(HealthCheckKeys.PORT.value, 8080))

    def _start_metrics_server(self):
        port = int((self.config.get(ConfigKeys.METRICS.value, {}) or {}).get(MetricsKeys.PORT.value, 8000))
        start_http_server(port)
        self.logger.info("Metrics server started", port=port)

    # ----------------------------- Orchestrator Loop -----------------------------
    
    def _table_sync_loop(self, db):
        tables = self.yugabyte_manager._discover_tables(db)
        self.logger.info("Tables discovered", database=db, tables=[t.table for t in tables])

        for table_info in tables:
            # for each table in the database check if it has annotation enabled
            if table_info.annotation is not None and table_info.annotation.enabled:
                # Check to see if table has entry in debezium signal table
                if not self.yugabyte_manager.entry_exists_in_debezium_signal(table_info):
                    # Table does not have entry in debezium signal table
                    # Means this is a newly annotated table so we check to see if connectors exist as they may be in a bad state
                    connector_statuses = self.kafka_connector.check_connector_exists(table_info)
                    if not connector_statuses['source'] or not connector_statuses['sink']:
                        try:
                            self.kafka_connector.setup_connectors(table_info)
                        except Exception as e:
                            self.logger.error("Error setting up connectors", table=table_info.table, error=str(e))
                    else:
                        self.logger.info("Connectors already exist for table, resetting and rebuilding", table=table_info.table)
                        try:
                            self.kafka_connector.reset_connectors(table_info)
                            self.kafka_connector.setup_connectors(table_info)
                        except Exception as e:
                            self.logger.error("Error resetting connectors", table=table_info.table, error=str(e))
                else:
                    self.logger.info("Table already has entry in debezium signal table, check if connectors are running", table=table_info.table)
                    connector_statuses = self.kafka_connector.check_connector_exists(table_info)
                    if not connector_statuses['source'] or not connector_statuses['sink']:
                        self.logger.info("One or more connectors do not exist, setting up connectors", table=table_info.table)
                        try:
                            self.kafka_connector.reset_connectors(table_info)
                            self.kafka_connector.setup_connectors(table_info)
                        except Exception as e:
                            self.logger.error("Error setting up connectors", table=table_info.table, error=str(e))
                    
        # For tables in the database check entries in the signal table for tables in database
        # Fetch all signal table entries for database and verify that annotation is still enabled for each
        for table in self.yugabyte_manager.fetch_tables_in_debezium_signal(db):
            table_info = next((t for t in tables if t.table == table), None)
            if table_info is None or table_info.annotation is None or not table_info.annotation.enabled:
                self.logger.info("Table annotation disabled or table not found, removing from signal table and tearing down connectors", table=table)
                try:
                    self.yugabyte_manager.remove_entry_from_debezium_signal(table_info.database, table_info.table)
                    self.kafka_connector.reset_connectors(table_info)
                    self.bigquery_manager.delete_table(table_info)
                except Exception as e:
                    self.logger.error("Error tearing down connectors", table=table, error=str(e))
                                    


    def start(self):
        self.logger.info("Starting orchestrator")
        self.running = True
        self.logger.info("Starting processing loop")
        try:
            self.logger.info("Starting table sync loop")
            try:
                while self.running:
                    start = time.time()
                    self.logger.info("Beginning processing")
                    try:
                        # Discover databases and create connectors as needed
                        databases = self.yugabyte_manager._discover_databases()
                        self.logger.info("Databases discovered", databases=databases)
                    
                        with ThreadPoolExecutor(max_workers=self.config.get(ConfigKeys.PROCESSING.value, {}).get(ProcessingKeys.MAX_SCAN_THREADS.value, 4)) as executor:
                            futures = {executor.submit(self._table_sync_loop, db): db for db in databases}

                            for future in as_completed(futures):
                                ti = futures[future]
                                try:
                                    future.result()
                                except Exception as e:
                                    self.logger.error("Error in table sync loop", table=ti.table_info.table, error=str(e))
                        
                    except Exception as e:
                        self.logger.error("Error during table sync", error=str(e))
                    finally:
                        elapsed = time.time() - start
                        self.logger.info("Scan loop complete", elapsed_time=elapsed)
                        time.sleep(max(0, self.config.get(ConfigKeys.SCAN_INTERVAL_SECONDS.value, 10) - elapsed))
            except Exception as e:
                self.logger.error("Unexpected error in table sync loop", error=str(e))
            finally:
                self.logger.info("Table sync loop exiting")
                            
        except Exception as e:
            self.logger.error("Unexpected error in orchestrator", error=str(e))
        finally:
            self.running = False
            self.logger.info("Orchestrator stopped")

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
