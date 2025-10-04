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

import os
import sys
import signal
import threading
import time
import json
import subprocess
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime
from contextlib import contextmanager

import yaml
import psycopg2
from psycopg2.extras import RealDictCursor
from google.cloud import bigquery
import structlog
from prometheus_client import Counter, Histogram, Gauge, start_http_server
from flask import Flask, jsonify
import requests

from classes.kafka_connector import KafkaConnector
from classes.bigquery_manager import BigQueryManager
from classes.annotation_processor import TableAnnotation, AnnotationProcessor
from classes.config_reader import ConfigReader, ConfigKeys, ProcessingKeys, LoggingKeys, BigQueryKeys, HealthCheckKeys, MetricsKeys, KafkaConnectKeys
from classes.cdc_manager import CDCManager
from classes.table_info import TableInfo
from classes.sync_status import SyncStatus
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
        self.annotation_processor = AnnotationProcessor()
        self.cdc_manager = CDCManager(self.config)
        self.running = False
        self.db_connections: Dict[str, psycopg2.extensions.connection] = {}
        self.bigquery_client: Optional[bigquery.Client] = None
        self.metrics = None  # Metrics disabled for testing
        self.logger = self._init_logger()
        self.status_table: Dict[str, SyncStatus] = {}

        self._derive_project_id()
        self._init_bigquery_client()
        self._init_status_table()
        import os
        if start_servers:
            if not os.getenv('DISABLE_HEALTH'):
                self._start_health_server()
            if not os.getenv('DISABLE_METRICS'):
                self._start_metrics_server()

    # ----------------------------- Logging & Metrics -----------------------------

    def _init_logger(self) -> structlog.BoundLogger:
        import logging
        lvl = (self.config.get(ConfigKeys.LOGGING.value, {}) or {}).get(ConfigKeys.LOGGING.value, {}).get(LoggingKeys.LEVEL.value, "INFO").upper()
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

    # ----------------------------- HTTP logging helpers -----------------------------

    def _log_http_bodies_on_failure(self) -> bool:
        return bool((self.config.get(ConfigKeys.LOGGING.value, {}) or {}).get(LoggingKeys.LOG_BODIES_ON_FAILURE.value, True))

    def _redact(self, data: Any, redact_keys: Optional[set] = None) -> Any:
        DEFAULT = {
            "password", "pass", "pwd", "secret", "token", "bearer", "authorization",
            "api_key", "apikey", "sslkey", "sslpassword", "database.tls.key.password",
            "sasl.jaas.config", "sasl.password", "sasl.mechanism",
        }
        keys = set(DEFAULT)
        cfg = (self.config.get(ConfigKeys.LOGGING.value, {}) or {}).get(ConfigKeys.LOGGING_REDACT_KEYS.value, [])
        if isinstance(cfg, list):
            keys |= {str(k).lower() for k in cfg}

        def _sanitize(obj):
            if isinstance(obj, dict):
                out = {}
                for k, v in obj.items():
                    kl = str(k).lower()
                    if kl in keys or any(kl.endswith("." + rk) for rk in keys):
                        out[k] = "****"
                    else:
                        out[k] = _sanitize(v)
                return out
            elif isinstance(obj, list):
                return [_sanitize(x) for x in obj]
            else:
                return obj

        return _sanitize(data)

    def _log_http_failure(
        self,
        *,
        method: str,
        url: str,
        req_json: Optional[dict] = None,
        resp: Optional[requests.Response] = None,
        error: Optional[Exception] = None,
        note: Optional[str] = None,
    ):
        fields = {"method": method, "url": url}
        if resp is not None:
            body = resp.text or ""
            if len(body) > 8192:
                body = body[:8192] + "...(truncated)"
            fields.update({
                "status": resp.status_code,
                "response_text": body,
                "response_headers": dict(resp.headers or {}),
            })
        if error is not None:
            fields["error"] = str(error)
        if note:
            fields["note"] = note

        if self._log_http_bodies_on_failure() and req_json is not None:
            try:
                redacted = self._redact(req_json)
                pretty = json.dumps(redacted, ensure_ascii=False, indent=2)
                if len(pretty) > 8192:
                    pretty = pretty[:8192] + "...(truncated)"
                fields["request_json"] = pretty
            except Exception as e:
                fields["request_json_error"] = f"failed to serialize: {e}"

        self.logger.error("HTTP request failed", **fields)

    # ----------------------------- External Clients -----------------------------

    def _init_bigquery_client(self):
        try:
            credentials_path = self.config.get(ConfigKeys.BIGQUERY.value, {}).get(BigQueryKeys.CREDENTIALS_PATH.value, "/vault/secrets/gcp-key.json")
            project_id = self.config.get(ConfigKeys.BIGQUERY.value, {}).get(BigQueryKeys.PROJECT_ID.value, None)
            if credentials_path and os.path.exists(credentials_path) and project_id:
                os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
                self.bigquery_client = bigquery.Client(project=project_id)
                self.logger.info("BigQuery client initialized", project_id=project_id)
            else:
                self.bigquery_client = None
                self.logger.warning("BigQuery disabled (missing credentials or project_id)",
                                    credentials_path=credentials_path, project_id=project_id)
        except Exception as e:
            self.logger.error("Failed to initialize BigQuery client", error=str(e))
            self.bigquery_client = None

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
            for k, v in self.status_table.items():
                out.append({
                    "table": v.table_info.full_name,
                    "annotation_enabled": v.annotation_enabled,
                    "bigquery_exists": v.bigquery_exists,
                    "connector_exists": v.connector_exists,
                    "sync_active": v.sync_active,
                    "last_connector_state": v.last_connector_state,
                    "expected_topic": v.expected_topic,
                    "topic_exists": v.topic_exists,
                    "last_error": v.last_error,
                    "last_scan": v.last_scan.isoformat(),
                })
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

    # ----------------------------- Status -----------------------------

    def _init_status_table(self):
        self.status_table = {}
        self.logger.info("Status table initialized")

    # ----------------------------- Postgres helpers -----------------------------

    _PG_ALLOWED_KEYS = {
        'host', 'hostaddr', 'port', 'dbname', 'database', 'user', 'password',
        'connect_timeout', 'sslmode', 'sslcert', 'sslkey', 'sslrootcert', 'sslcrl',
        'options', 'application_name', 'keepalives', 'keepalives_idle',
        'keepalives_interval', 'keepalives_count',
    }

    def _pg_conn_kwargs(self, base: Dict[str, Any], database: Optional[str] = None) -> Dict[str, Any]:
        yb = dict(base or {})
        if database:
            yb['database'] = database
        if 'database' in yb:
            yb['dbname'] = yb['database']
            del yb['database']

        yb.setdefault('connect_timeout', 5)
        yb.setdefault('application_name', 'table-sync-orchestrator')
        yb.setdefault('keepalives', 1)
        yb.setdefault('keepalives_idle', 30)
        yb.setdefault('keepalives_interval', 10)
        yb.setdefault('keepalives_count', 5)

        filtered = {k: v for k, v in yb.items() if k in self._PG_ALLOWED_KEYS}
        return filtered

    def _get_system_db_connection(self):
        return self.yugabyte_manager.get_system_db_connection()

    def _get_db_connection_ctx(self, database: str):
        @contextmanager
        def get_connection():
            base_cfg = self.config.get(ConfigKeys.YUGABYTEDB.value, {}) or {}
            kwargs = self._pg_conn_kwargs(base_cfg, database=database)
            safe_log = {k: ('****' if k == 'password' else v) for k, v in kwargs.items()}
            self.logger.debug("Attempting database connection", database=database, config=safe_log)
            conn = None
            try:
                conn = psycopg2.connect(**kwargs)
                self.logger.info("Database connection established", database=database, user=kwargs.get('user'))
                yield conn
            except Exception as e:
                self.logger.error("Failed to connect to database", database=database, error=str(e))
                raise
            finally:
                if conn:
                    try:
                        conn.close()
                    except Exception:
                        pass
        return get_connection()

    # ----------------------------- Discovery -----------------------------

    def _filter_excluded_databases(self, all_databases: List[str]) -> List[str]:
        ex_cfg = self.config.get(ConfigKeys.EXCLUDED_DATABASES.value, 'postgres,template0,template1')
        excluded = [d.strip() for d in ex_cfg.split(',')] if isinstance(ex_cfg, str) else (ex_cfg or [])
        kept = [d for d in all_databases if d not in excluded]
        self.logger.debug("Database filtering applied",
                          total_databases=len(all_databases),
                          excluded_databases=excluded,
                          remaining_databases=len(kept))
        return kept

    def _discover_databases(self) -> List[str]:
        excluded = self.config.get(ConfigKeys.EXCLUDED_DATABASES.value, ['postgres', 'template0', 'template1'])
        return self.yugabyte_manager.discover_databases(excluded)

    def _discover_tables(self, database: str) -> List[TableInfo]:
        out: List[TableInfo] = []
        try:
            with self._get_db_connection_ctx(database) as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT t.table_schema,
                           t.table_name,
                           obj_description(c.oid) AS table_comment
                    FROM information_schema.tables t
                    JOIN pg_class c       ON c.relname = t.table_name
                    JOIN pg_namespace n   ON n.oid = c.relnamespace AND n.nspname = t.table_schema
                    WHERE t.table_type = 'BASE TABLE'
                      AND t.table_schema NOT IN ('information_schema','pg_catalog','pg_toast')
                    ORDER BY t.table_schema, t.table_name
                """)
                for row in cur.fetchall():
                    ann = TableAnnotation.from_comment(row['table_comment']) if row['table_comment'] else None
                    out.append(TableInfo(database=database, schema=row['table_schema'], table=row['table_name'], annotation=ann))
        except Exception as e:
            self.logger.error("Failed to discover tables", database=database, error=str(e))
        return out

    # ----------------------------- Reconciliation -----------------------------

    def _reconcile_connector(self, table_info: TableInfo, existing: SyncStatus):
        cfg = self._build_connector_config(table_info)
        name = cfg.get("name")
        self.logger.info("Reconciliation check", table=table_info.full_name, connector_name=name, existing_status=existing)

        if not self._validate_connector_config(cfg):
            self.logger.error("Connector config validation failed", config=cfg)
            return False

        if existing.connector_exists and existing.last_connector_state == "RUNNING":
            self.logger.info("Connector already exists and is RUNNING", connector_name=name)
            return True

        if existing.connector_exists:
            self.logger.info("Connector exists but is not RUNNING", connector_name=name, last_state=existing.last_connector_state)
            if self.config.get(ConfigKeys.KAFKA_CONNECT.value, {}).get(KafkaConnectKeys.RECREATE_FAILED_CONNECTORS.value, False):
                self.logger.info("Recreating failed connector", connector_name=name)
                self._kc_delete_connector(name)
                time.sleep(2)
                return self._kc_create_connector(name, cfg)

            self.logger.warning("Connector is not RUNNING; manual intervention required", connector_name=name, last_state=existing.last_connector_state)
            return False

        self.logger.info("Creating new connector", connector_name=name)
        return self._kc_create_connector(name, cfg)

    def _reconcile_table(self, table_info: TableInfo):
        self.logger.info("Starting reconciliation", table=table_info.full_name)
        status = self.status_table.get(table_info.full_name)
        if not status:
            self.logger.warning("No status found for table; skipping reconciliation", table=table_info.full_name)
            return

        if status.annotation_enabled and not table_info.annotation:
            self.logger.info("Enabling annotation for table", table=table_info.full_name)
            table_info.annotation = TableAnnotation()  # Enable with default settings

        if not status.connector_exists:
            self.logger.info("No connector found; creating one", table=table_info.full_name)
            self._reconcile_connector(table_info, status)
            return

        self.logger.info("Connector exists; checking status", table=table_info.full_name)
        conn_status = self._kc_connector_status(status.name)
        if not conn_status:
            self.logger.warning("Failed to fetch connector status; assuming absent", connector_name=status.name)
            status.connector_exists = False
            return

        tasks_running = sum(1 for t in conn_status.get("tasks", []) if t.get("state") == "RUNNING")
        if tasks_running == len(conn_status["tasks"]):
            self.logger.info("All tasks are RUNNING", connector_name=status.name)
            status.last_connector_state = "RUNNING"
            return

        self.logger.warning("Some tasks are not RUNNING", connector_name=status.name, tasks=conn_status["tasks"])
        status.last_connector_state = "PARTIALLY_RUNNING"

    def _reconcile(self):
        self.logger.info("Starting reconciliation loop")
        for table_name, status in self.status_table.items():
            try:
                table_info = status.table_info
                self._reconcile_table(table_info)
                self.logger.info("Reconciliation complete", table=table_info.full_name)
            except Exception as e:
                self.logger.error("Error during reconciliation", table=table_name, error=str(e))

    # ----------------------------- Orchestrator Loop -----------------------------
    def _build_connector_config(self, table_info: TableInfo) -> Dict[str, Any]:
        base_cfg = self.config.get(ConfigKeys.KAFKA_CONNECT.value, {}) or {}
        cfg = dict(base_cfg)  # Start with base config

        topic_prefix = (cfg.get("topic_prefix", "ybcdc") or "").strip()
        if not topic_prefix:
            topic_prefix = "ybcdc"
        server_name = f"{table_info.database}_{table_info.schema}_{table_info.table}"

        cfg.update({
            "name": f"ybcdc-{server_name}",
            "connector.class": "io.debezium.connector.yugabytedb.YugabyteDBgRPCConnector",
            "tasks.max": "1",
            "database.server.name": server_name,
            "table.include.list": f"{table_info.schema}.{table_info.table}",
            "topic.prefix": topic_prefix,
            "topic.creation.default.partitions": "1",
            "topic.creation.default.replication.factor": "3",
        })

        yb_cfg = self.config.get(ConfigKeys.YUGABYTEDB.value, {}) or {}
        if 'host' in yb_cfg:
            cfg['database.hostname'] = yb_cfg['host']
        if 'port' in yb_cfg:
            cfg['database.port'] = str(yb_cfg['port'])
        if 'user' in yb_cfg:
            cfg['database.user'] = yb_cfg['user']
        if 'password' in yb_cfg:
            cfg['database.password'] = yb_cfg['password']
        if 'sslmode' in yb_cfg:
            cfg['database.sslmode'] = yb_cfg['sslmode']

        if table_info.annotation and table_info.annotation.cdc_stream_id:
            cfg['cdc.stream.id'] = table_info.annotation.cdc_stream_id
        elif self.cdc_manager.master_addresses:
            stream_id = self.cdc_manager.ensure_cdc_stream(table_info)
            if stream_id:
                cfg['cdc.stream.id'] = stream_id

        self.logger.debug("Built connector config", table=table_info.full_name, config=self._redact(cfg))
        return cfg
    
    def _derive_project_id(self):
        try:
            bq = self.config.get(ConfigKeys.BIGQUERY.value, {}) or {}
            project_id = bq.get(ConfigKeys.BIGQUERY.value, {}).get(BigQueryKeys.PROJECT_ID.value, None)
            if project_id:
                self.logger.info("Project ID derived from config", project_id=project_id)
                return project_id

            # Attempt to infer project ID from environment
            if os.getenv("GOOGLE_CLOUD_PROJECT"):
                inferred = os.getenv("GOOGLE_CLOUD_PROJECT")
                self.logger.info("Project ID inferred from environment", project_id=inferred)
                return inferred

            self.logger.warning("No project ID configured or inferred")
        except Exception as e:
            self.logger.error("Error deriving project ID", error=str(e))

    def _table_sync_loop(self, sync_status: SyncStatus):
        name = sync_status.table_info.full_name
        self.logger.info("Starting table sync loop", table=name)
        self.status_table[name].sync_active = True
        try:
            while self.running:
                start = time.time()
                self.logger.info("Beginning scan", table=name)
                try:
                    # Scan table and detect schema changes
                    changes = self.bigquery_manager.scan_table(self.yugabyte_manager, sync_status.table_info)
                    self.logger.info("Scan complete", table=name, changes=changes)

                    if changes.get("schema_changed"):
                        self.logger.info("Schema change detected; updating BigQuery table", table=name)
                        self.bigquery_manager.update_table_schema(self.yugabyte_manager, sync_status.table_info, changes["schema"])
                        self.logger.info("BigQuery table schema updated", table=name)
                    else:
                        self.logger.info("No schema changes detected", table=name)

                    # Sync data to BigQuery
                    self.bigquery_manager.sync_table_data(self.yugabyte_manager, sync_status.table_info)
                    self.logger.info("Data sync complete", table=name)
                    self.status_table[name].last_scan = datetime.now()
                except Exception as e:
                    self.logger.error("Error during table sync", table=name, error=str(e))
                    self.status_table[name].last_error = str(e)
                finally:
                    elapsed = time.time() - start
                    self.logger.info("Scan loop complete", table=name, elapsed_time=elapsed)
                    time.sleep(max(0, self.config.get(ConfigKeys.SCAN_INTERVAL_SECONDS.value, 10) - elapsed))
        except Exception as e:
            self.logger.error("Unexpected error in table sync loop", table=name, error=str(e))
        finally:
            self.logger.info("Table sync loop exiting", table=name)
            self.status_table[name].sync_active = False

    def start(self):
        self.logger.info("Starting orchestrator")
        self.running = True
        try:
            # Discover databases and create connectors as needed
            databases = self._discover_databases()
            self.logger.info("Databases discovered", databases=databases)

            for db in databases:
                tables = self._discover_tables(db)
                self.logger.info("Tables discovered", database=db, tables=[t.table for t in tables])

                for table_info in tables:
                    # Initialize status entry
                    if table_info.full_name not in self.status_table:
                        
                        # Check for existing connector by querying Kafka
                        connector_name = f"ybcdc-{table_info.database}_{table_info.schema}_{table_info.table}"
                        connector_exists = self.kafka_connector.check_connector_exists(connector_name)
                        sync_active = False

                        if connector_exists:
                            self.logger.info("Connector already exists for table", table=table_info.full_name)
                            connector_exists = True
                            sync_active = True
                        else:
                            connector_exists = False
                            
                        if table_info.annotation is not None and table_info.annotation.enabled:
                            self.status_table[table_info.full_name] = SyncStatus(
                                table_info=table_info,
                                last_scan=None,
                                annotation_enabled=bool(table_info.annotation),
                                bigquery_exists=self.bigquery_manager.check_table_exists(table_info.schema, table_info.table),
                                connector_exists=connector_exists,
                                sync_active=sync_active
                            )

            self.logger.info("Starting table sync loops")
            with ThreadPoolExecutor(max_workers=self.config.get(ConfigKeys.PROCESSING.value, {}).get(ProcessingKeys.MAX_SCAN_THREADS.value, 4)) as executor:
                futures = {executor.submit(self._table_sync_loop, ti): ti for ti in self.status_table.values()}

                for future in as_completed(futures):
                    ti = futures[future]
                    try:
                        future.result()
                    except Exception as e:
                        self.logger.error("Error in table sync loop", table=ti.table_info.table, error=str(e))
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
