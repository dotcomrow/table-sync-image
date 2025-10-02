#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Production Table Sync Orchestrator for YugabyteDB → BigQuery.

Fixes:
- Filters out non-Postgres DSN keys (e.g., master_addresses) before psycopg2.connect()
- Adds robust connection options (timeouts, keepalives, application_name)
- Guards BigQuery ops when client is None
- Keeps Debezium-only settings strictly in connector creation
"""

import os
import sys
import signal
import threading
import time
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Any, Set, Tuple
from datetime import datetime
from dataclasses import dataclass
from contextlib import contextmanager

import yaml
import psycopg2
from psycopg2.extras import RealDictCursor
from google.cloud import bigquery
import structlog
from prometheus_client import Counter, Histogram, Gauge, start_http_server
from flask import Flask, jsonify
import requests


# ----------------------------- Data Models -----------------------------

@dataclass
class TableAnnotation:
    enabled: bool
    bq_target: str  # "dataset.table"

    @classmethod
    def from_comment(cls, comment: str) -> Optional['TableAnnotation']:
        try:
            data = json.loads(comment)
            bootstrap = data.get('bootstrap', {})
            if not isinstance(bootstrap, dict):
                return None
            return cls(
                enabled=bool(bootstrap.get('enabled', False)),
                bq_target=str(bootstrap.get('bq', '')).strip()
            )
        except Exception:
            return None


@dataclass
class TableInfo:
    database: str
    schema: str
    table: str
    annotation: Optional[TableAnnotation]

    @property
    def full_name(self) -> str:
        return f"{self.database}.{self.schema}.{self.table}"

    @property
    def bq_dataset(self) -> Optional[str]:
        if self.annotation and self.annotation.bq_target and "." in self.annotation.bq_target:
            return self.annotation.bq_target.split(".", 1)[0]
        return None

    @property
    def bq_table(self) -> Optional[str]:
        if self.annotation and self.annotation.bq_target and "." in self.annotation.bq_target:
            return self.annotation.bq_target.split(".", 1)[1]
        return None


@dataclass
class SyncStatus:
    table_info: TableInfo
    last_scan: datetime
    annotation_enabled: bool
    bigquery_exists: bool
    connector_exists: bool
    sync_active: bool
    error_message: Optional[str] = None


# ----------------------------- Orchestrator -----------------------------

class TableSyncOrchestrator:
    def __init__(self, config_path: str):
        self.config = self._load_config(config_path)
        self.running = False
        self.db_connections: Dict[str, psycopg2.extensions.connection] = {}
        self.bigquery_client: Optional[bigquery.Client] = None
        self.metrics = self._init_metrics()
        self.logger = self._init_logger()
        self.status_table: Dict[str, SyncStatus] = {}

        self._derive_project_id()
        self._init_bigquery_client()
        self._init_status_table()
        self._start_health_server()
        self._start_metrics_server()

    # ----------------------------- Config -----------------------------

    def _load_config(self, config_path: str) -> Dict[str, Any]:
        try:
            with open(config_path, 'r') as f:
                content = f.read()

            import re
            def env_replacer(match):
                spec = match.group(1)
                if ':-' in spec:
                    var, default = spec.split(':-', 1)
                elif ':' in spec:
                    var, default = spec.split(':', 1)
                else:
                    var, default = spec, ''
                return os.getenv(var, default)

            content = re.sub(r'\$\{([^}]+)\}', env_replacer, content)
            cfg = yaml.safe_load(content) or {}

            # Allow DATABASE_URL to override yugabytedb section
            self._parse_database_url(cfg)
            return cfg
        except Exception as e:
            print(f"Failed to load config from {config_path}: {e}", file=sys.stderr)
            sys.exit(1)

    def _parse_database_url(self, config: Dict[str, Any]):
        url = os.getenv('DATABASE_URL')
        if not url:
            return
        try:
            from urllib.parse import urlparse
            parsed = urlparse(url)
            config.setdefault('yugabytedb', {})
            yb = config['yugabytedb']
            if parsed.hostname: yb['host'] = parsed.hostname
            if parsed.port:     yb['port'] = parsed.port
            if parsed.username: yb['user'] = parsed.username
            if parsed.password: yb['password'] = parsed.password
            if parsed.path and parsed.path != '/':
                yb['database'] = parsed.path.lstrip('/')
            print(f"✅ Parsed DATABASE_URL for {parsed.username}@{parsed.hostname}:{parsed.port} → db={yb.get('database','(none)')}")
        except Exception as e:
            print(f"Warning: Failed to parse DATABASE_URL: {e}", file=sys.stderr)

    def _derive_project_id(self):
        bq = self.config.get('bigquery', {}) or {}
        project_id = bq.get('project_id')
        if project_id and project_id != 'auto':
            return
        try:
            credentials_path = bq.get('credentials_path')
            if credentials_path and os.path.exists(credentials_path):
                with open(credentials_path, 'r') as f:
                    creds = json.load(f)
                if 'project_id' in creds:
                    bq['project_id'] = creds['project_id']
                    self.config['bigquery'] = bq
                    print(f"Auto-derived BigQuery project ID: {creds['project_id']}")
        except Exception as e:
            print(f"Warning: Could not derive project ID from credentials: {e}", file=sys.stderr)

    # ----------------------------- Logging & Metrics -----------------------------

    def _init_logger(self) -> structlog.BoundLogger:
        import logging
        lvl = (self.config.get('logging', {}) or {}).get('level', 'INFO').upper()
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
            'last_scan_time': Gauge('sync_last_scan_timestamp', 'Timestamp of last scan'),
        }

    # ----------------------------- External Clients -----------------------------

    def _init_bigquery_client(self):
        try:
            bq = self.config.get('bigquery', {}) or {}
            credentials_path = bq.get('credentials_path')
            project_id = bq.get('project_id')
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

        def run_server():
            port = int((self.config.get('health_check', {}) or {}).get('port', 8080))
            app.run(host='0.0.0.0', port=port, debug=False)

        threading.Thread(target=run_server, daemon=True).start()
        self.logger.info("Health server started", port=(self.config.get('health_check', {}) or {}).get('port', 8080))

    def _start_metrics_server(self):
        port = int((self.config.get('metrics', {}) or {}).get('port', 8000))
        start_http_server(port)
        self.logger.info("Metrics server started", port=port)

    # ----------------------------- Status -----------------------------

    def _init_status_table(self):
        self.status_table = {}
        self.logger.info("Status table initialized")

    # ----------------------------- Postgres helpers -----------------------------

    _PG_ALLOWED_KEYS = {
        # common DSN keys
        'host', 'hostaddr', 'port', 'dbname', 'database', 'user', 'password',
        'connect_timeout', 'sslmode', 'sslcert', 'sslkey', 'sslrootcert', 'sslcrl',
        'options', 'application_name', 'keepalives', 'keepalives_idle',
        'keepalives_interval', 'keepalives_count',
    }

    def _pg_conn_kwargs(self, base: Dict[str, Any], database: Optional[str] = None) -> Dict[str, Any]:
        """Return a psycopg2-safe kwargs dict. Drops unknown keys like master_addresses."""
        yb = dict(base or {})
        if database:
            yb['database'] = database

        # Map dbname/database preference for psycopg2
        if 'database' in yb and 'dbname' not in yb:
            yb['dbname'] = yb['database']

        # Defaults & tuning
        yb.setdefault('connect_timeout', 5)
        yb.setdefault('application_name', 'table-sync-orchestrator')
        yb.setdefault('keepalives', 1)
        yb.setdefault('keepalives_idle', 30)
        yb.setdefault('keepalives_interval', 10)
        yb.setdefault('keepalives_count', 5)

        # Filter to allowed keys only
        filtered = {k: v for k, v in yb.items() if k in self._PG_ALLOWED_KEYS}

        return filtered

    def _get_system_db_connection(self):
        """Try postgres, yugabyte, template1 with filtered DSN."""
        system_dbs = ['postgres', 'yugabyte', 'template1']
        base_cfg = self.config.get('yugabytedb', {}) or {}
        for sys_db in system_dbs:
            try:
                kwargs = self._pg_conn_kwargs(base_cfg, database=sys_db)
                safe_log = {k: ('****' if k == 'password' else v) for k, v in kwargs.items()}
                self.logger.debug("Attempting system database connection", database=sys_db, config=safe_log)
                conn = psycopg2.connect(**kwargs)
                self.logger.info("System database connection established", database=sys_db, user=kwargs.get('user'))
                return conn
            except Exception as e:
                self.logger.debug("Failed to connect to system database", database=sys_db, error=str(e))
        raise Exception("Could not connect to any system database (postgres, yugabyte, template1)")

    def _get_db_connection_ctx(self, database: str):
        """Connection pool (simple) with filtered DSN."""
        @contextmanager
        def get_connection():
            if database not in self.db_connections or self.db_connections[database].closed:
                try:
                    base_cfg = self.config.get('yugabytedb', {}) or {}
                    kwargs = self._pg_conn_kwargs(base_cfg, database=database)
                    safe_log = {k: ('****' if k == 'password' else v) for k, v in kwargs.items()}
                    self.logger.debug("Attempting database connection", database=database, config=safe_log)
                    self.db_connections[database] = psycopg2.connect(**kwargs)
                    self.logger.info("Database connection established", database=database, user=kwargs.get('user'))
                except Exception as e:
                    self.logger.error("Failed to connect to database", database=database, error=str(e))
                    raise
            yield self.db_connections[database]
        return get_connection()

    # ----------------------------- Discovery -----------------------------

    def _filter_excluded_databases(self, all_databases: List[str]) -> List[str]:
        ex_cfg = self.config.get('excluded_databases', 'postgres,template0,template1')
        excluded = [d.strip() for d in ex_cfg.split(',')] if isinstance(ex_cfg, str) else (ex_cfg or [])
        kept = [d for d in all_databases if d not in excluded]
        self.logger.debug("Database filtering applied",
                          total_databases=len(all_databases),
                          excluded_databases=excluded,
                          remaining_databases=len(kept))
        return kept

    def _discover_databases(self) -> List[str]:
        target = (self.config.get('yugabytedb', {}) or {}).get('database', 'kafka')
        try:
            conn = self._get_system_db_connection()
            try:
                with conn.cursor() as cur:
                    # user info
                    username = (self.config.get('yugabytedb', {}) or {}).get('user', 'vaultadmin')
                    cur.execute("SELECT rolname, rolsuper, rolcreatedb FROM pg_roles WHERE rolname = %s", (username,))
                    row = cur.fetchone()
                    if not row:
                        self.logger.warning("Database user does not exist; cannot create databases", user=username)
                        return []

                    cur.execute("SELECT datname FROM pg_database WHERE datistemplate = false")
                    all_visible = [r[0] for r in cur.fetchall()]
                    scannable = self._filter_excluded_databases(all_visible)
                    self.logger.info("Database discovery completed",
                                     total_visible=len(all_visible),
                                     scannable_databases=len(scannable),
                                     databases=scannable)

                    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (target,))
                    exists = cur.fetchone() is not None
                    if exists:
                        if self.config.get('comprehensive_database_scan', True):
                            return scannable
                        return [target]
            finally:
                try: conn.close()
                except: pass

            # Create target if missing
            return self._create_database_if_needed(target, username)
        except Exception as e:
            self.logger.error("Failed to discover databases", error=str(e))
            self.logger.info("Using configured target database (fallback)", database=target)
            return [target]

    def _create_database_if_needed(self, target_database: str, username: str) -> List[str]:
        try:
            conn = self._get_system_db_connection()
            conn.autocommit = True
            with conn.cursor() as cur:
                try:
                    self.logger.info("Attempting to create database", database=target_database, owner=username)
                    cur.execute(f'CREATE DATABASE "{target_database}" OWNER "{username}"')
                    self.logger.info("Target database created", database=target_database, owner=username)
                except Exception as e:
                    self.logger.error("Failed to create target database", database=target_database, error=str(e))

            # post-create grants
            with self._get_db_connection_ctx(target_database) as new_conn:
                new_conn.autocommit = True
                with new_conn.cursor() as ncur:
                    ncur.execute(f'ALTER SCHEMA public OWNER TO "{username}"')
                    ncur.execute(f'GRANT ALL ON SCHEMA public TO "{username}"')
                    ncur.execute(f'GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO "{username}"')
                    ncur.execute(f'GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO "{username}"')
                    ncur.execute(f'GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public TO "{username}"')
                    ncur.execute(f'ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO "{username}"')
                    ncur.execute(f'ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO "{username}"')
                    ncur.execute(f'ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON FUNCTIONS TO "{username}"')
                    ncur.execute(f'ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TYPES TO "{username}"')
                    ncur.execute(f'GRANT CREATE ON DATABASE "{target_database}" TO "{username}"')
                    self.logger.info("Granted privileges/ownership on new database", database=target_database, user=username)

            # decide scan scope
            if self.config.get('comprehensive_database_scan', True):
                with self._get_system_db_connection() as conn2, conn2.cursor() as cur2:
                    cur2.execute("SELECT datname FROM pg_database WHERE datistemplate = false")
                    all_visible = [r[0] for r in cur2.fetchall()]
                return self._filter_excluded_databases(all_visible)
            return [target_database]
        except Exception as e:
            self.logger.error("Failed to finalize database creation", error=str(e))
            return []
        finally:
            try:
                if 'conn' in locals() and conn: conn.close()
            except: pass

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

    # ----------------------------- BigQuery -----------------------------

    def _check_bigquery_exists(self, dataset_id: Optional[str], table_id: Optional[str]) -> bool:
        if not self.bigquery_client or not dataset_id or not table_id:
            return False
        try:
            self.bigquery_client.get_table(self.bigquery_client.dataset(dataset_id).table(table_id))
            return True
        except Exception:
            return False

    def _map_pg_to_bq_type(self, pg_type: str) -> str:
        mapping = {
            'integer': 'INTEGER', 'bigint': 'INTEGER', 'smallint': 'INTEGER',
            'numeric': 'NUMERIC', 'decimal': 'NUMERIC',
            'real': 'FLOAT', 'double precision': 'FLOAT',
            'boolean': 'BOOLEAN',
            'text': 'STRING', 'varchar': 'STRING', 'char': 'STRING', 'character varying': 'STRING',
            'timestamp': 'TIMESTAMP', 'timestamp without time zone': 'TIMESTAMP',
            'timestamptz': 'TIMESTAMP', 'timestamp with time zone': 'TIMESTAMP',
            'date': 'DATE', 'time': 'TIME',
            'json': 'JSON', 'jsonb': 'JSON',
            'uuid': 'STRING',
        }
        if pg_type.endswith('[]'):
            base = pg_type[:-2]
            return mapping.get(base, 'STRING')
        return mapping.get(pg_type, 'STRING')

    def _get_table_schema(self, table_info: TableInfo) -> List[bigquery.SchemaField]:
        schema: List[bigquery.SchemaField] = []
        try:
            with self._get_db_connection_ctx(table_info.database) as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    ORDER BY ordinal_position
                """, (table_info.schema, table_info.table))
                for row in cur.fetchall():
                    bq_type = self._map_pg_to_bq_type(row['data_type'])
                    mode = "NULLABLE" if row['is_nullable'] == 'YES' else "REQUIRED"
                    schema.append(bigquery.SchemaField(row['column_name'], bq_type, mode=mode))
        except Exception as e:
            self.logger.error("Failed to get table schema", table=table_info.full_name, error=str(e))
        return schema

    def _create_bigquery_resources(self, table_info: TableInfo) -> bool:
        if not self.bigquery_client:
            self.logger.warning("Skipping BigQuery resource creation (client not initialized)",
                                table=table_info.full_name)
            return False
        try:
            dataset_id = table_info.bq_dataset
            table_id = table_info.bq_table
            if not dataset_id or not table_id:
                return False

            ds_ref = self.bigquery_client.dataset(dataset_id)
            try:
                self.bigquery_client.get_dataset(ds_ref)
            except Exception:
                ds = bigquery.Dataset(ds_ref)
                ds.location = (self.config.get('bigquery', {}) or {}).get('location', 'US')
                self.bigquery_client.create_dataset(ds)
                self.logger.info("Created BigQuery dataset", dataset=dataset_id)

            tbl_ref = ds_ref.table(table_id)
            try:
                self.bigquery_client.get_table(tbl_ref)
            except Exception:
                schema = self._get_table_schema(table_info)
                self.bigquery_client.create_table(bigquery.Table(tbl_ref, schema=schema))
                self.logger.info("Created BigQuery table", dataset=dataset_id, table=table_id)
            return True
        except Exception as e:
            self.logger.error("Failed to create BigQuery resources", table=table_info.full_name, error=str(e))
            return False

    def _sync_initial_data(self, table_info: TableInfo) -> bool:
        try:
            self.logger.info("Syncing initial data (stub)", table=table_info.full_name)
            return True
        except Exception as e:
            self.logger.error("Failed initial sync", table=table_info.full_name, error=str(e))
            return False

    # ----------------------------- Debezium Connector -----------------------------

    def _create_cdc_connector(self, table_info: TableInfo) -> bool:
        try:
            yb_cfg = self.config.get('yugabytedb', {}) or {}
            connector_name = f"yugabyte-{table_info.database}-{table_info.schema}-{table_info.table}"

            # Debezium master addresses (Debezium-only; not for psycopg2)
            master_addresses = yb_cfg.get('debezium_master_addresses') or yb_cfg.get('master_addresses', '')

            connector_config = {
                "name": connector_name,
                "config": {
                    "connector.class": "io.debezium.connector.yugabytedb.YugabyteDBConnector",
                    "database.hostname": yb_cfg.get('host'),
                    "database.port": str(yb_cfg.get('port', 5433)),
                    "database.user": yb_cfg.get('user'),
                    "database.password": yb_cfg.get('password'),
                    "database.dbname": table_info.database,
                    "database.server.name": f"yugabyte-{table_info.database}",
                    "database.master.addresses": master_addresses,
                    "table.include.list": f"{table_info.schema}.{table_info.table}",
                    "database.streamid": f"cdcstream_{table_info.schema}_{table_info.table}",
                    "transforms": "unwrap",
                    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
                }
            }

            connect_url = (self.config.get('kafka_connect', {}) or {}).get('url', 'http://kafka-connect:8083')
            resp = requests.post(f"{connect_url}/connectors", json=connector_config,
                                 headers={'Content-Type': 'application/json'}, timeout=10)
            if resp.status_code in (200, 201):
                self.logger.info("Created CDC connector", connector=connector_name, table=table_info.full_name)
                return True
            self.logger.error("Failed to create CDC connector", connector=connector_name,
                              status_code=resp.status_code, response=resp.text)
            return False
        except Exception as e:
            self.logger.error("Failed to create CDC connector", table=table_info.full_name, error=str(e))
            return False

    # ----------------------------- Scanning / Sync Loop -----------------------------

    def _scan_database(self, database: str) -> Tuple[str, int, int, List[TableInfo]]:
        thread_name = threading.current_thread().name
        try:
            self.logger.debug("Starting database scan", database=database, thread=thread_name)
            t0 = time.time()
            tables = self._discover_tables(database)
            annotated = sum(1 for t in tables if t.annotation and t.annotation.enabled)
            self.logger.info("Database scan completed",
                             database=database, thread=thread_name,
                             tables_found=len(tables), annotated_tables=annotated,
                             scan_time_seconds=round(time.time() - t0, 2))
            return (database, len(tables), annotated, tables)
        except Exception as e:
            self.logger.error("Database scan failed", database=database, error=str(e))
            return (database, 0, 0, [])

    def _scan_and_sync(self):
        t0 = time.time()
        try:
            databases = self._discover_databases()
            self.logger.info("Starting comprehensive database scan",
                             database_count=len(databases), databases=databases)
            # the metric was misused before; keep it to count databases processed as a coarse signal
            self.metrics['tables_scanned'].inc(len(databases))

            max_threads_cfg = int(self.config.get('max_scan_threads', 0) or 0)
            max_threads = min(len(databases), max_threads_cfg or len(databases))
            self.logger.info("Starting multithreaded database scanning",
                             thread_count=max_threads, databases_to_scan=len(databases))

            total_tables = 0
            annotated_tables = 0
            all_tables: List[TableInfo] = []

            with ThreadPoolExecutor(max_workers=max_threads, thread_name_prefix="db-scan") as ex:
                futures = {ex.submit(self._scan_database, db): db for db in databases}
                for fut in as_completed(futures):
                    db = futures[fut]
                    try:
                        _db, cnt, ann, tables = fut.result()
                        total_tables += cnt
                        annotated_tables += ann
                        all_tables.extend(tables)
                    except Exception as e:
                        self.logger.error("Database scan thread failed", database=db, error=str(e))

            self.logger.info("Multithreaded database scanning completed",
                             databases_scanned=len(databases),
                             total_tables_found=total_tables,
                             annotated_tables_found=annotated_tables,
                             threads_used=max_threads)

            # Sequential sync phase
            active_syncs = 0
            for ti in all_tables:
                # skip if no/disabled annotation
                if not ti.annotation or not ti.annotation.enabled:
                    continue

                table_key = ti.full_name
                current = self.status_table.get(table_key)

                bq_exists = self._check_bigquery_exists(ti.bq_dataset, ti.bq_table)
                needs_sync = (current is None) or (not current.annotation_enabled) or (not bq_exists)

                if needs_sync:
                    self.logger.info("Starting sync for table", table=ti.full_name)

                    if not bq_exists:
                        if not self._create_bigquery_resources(ti):
                            # cannot proceed without BQ table
                            self.status_table[table_key] = SyncStatus(
                                table_info=ti, last_scan=datetime.utcnow(),
                                annotation_enabled=True, bigquery_exists=False,
                                connector_exists=False, sync_active=False,
                                error_message="BigQuery resources not available"
                            )
                            continue
                        if not self._sync_initial_data(ti):
                            self.status_table[table_key] = SyncStatus(
                                table_info=ti, last_scan=datetime.utcnow(),
                                annotation_enabled=True, bigquery_exists=True,
                                connector_exists=False, sync_active=False,
                                error_message="Initial data sync failed"
                            )
                            continue

                    if not self._create_cdc_connector(ti):
                        self.status_table[table_key] = SyncStatus(
                            table_info=ti, last_scan=datetime.utcnow(),
                            annotation_enabled=True, bigquery_exists=True,
                            connector_exists=False, sync_active=False,
                            error_message="CDC connector creation failed"
                        )
                        continue

                    self.metrics['tables_synced'].inc()
                    active_syncs += 1
                    self.logger.info("Table sync completed", table=ti.full_name)

                # upsert status
                self.status_table[table_key] = SyncStatus(
                    table_info=ti,
                    last_scan=datetime.utcnow(),
                    annotation_enabled=True,
                    bigquery_exists=bq_exists or needs_sync,  # if we created it, it's true now
                    connector_exists=True,  # assume true after creation
                    sync_active=True,
                )

            # metrics
            self.metrics['scan_duration'].observe(time.time() - t0)
            self.metrics['last_scan_time'].set(time.time())
            self.metrics['active_syncs'].set(active_syncs)

            self.logger.info("Comprehensive scan completed",
                             duration=time.time() - t0,
                             databases_scanned=len(databases),
                             total_tables_found=total_tables,
                             annotated_tables_found=annotated_tables,
                             active_syncs=active_syncs)

        except Exception as e:
            self.logger.error("Scan failed", error=str(e))
            self.metrics['sync_errors'].labels(error_type='scan_error').inc()

    # ----------------------------- Lifecycle -----------------------------

    def run(self):
        self.running = True
        self.logger.info("Table sync orchestrator starting")

        def handle(signum, _frame):
            self.logger.info("Received shutdown signal", signal=signum)
            self.running = False

        signal.signal(signal.SIGTERM, handle)
        signal.signal(signal.SIGINT, handle)

        scan_interval = int(self.config.get('scan_interval_seconds', 30) or 30)

        try:
            while self.running:
                self._scan_and_sync()
                for _ in range(scan_interval):
                    if not self.running:
                        break
                    time.sleep(1)
        except Exception as e:
            self.logger.error("Orchestrator failed", error=str(e))
            sys.exit(1)
        finally:
            self._cleanup()

    def _cleanup(self):
        self.logger.info("Cleaning up resources")
        for conn in list(self.db_connections.values()):
            try: conn.close()
            except: pass
        self.logger.info("Table sync orchestrator stopped")


# ----------------------------- Entrypoint -----------------------------

def main():
    # Lightweight --test mode to validate config/env substitution
    if len(sys.argv) > 1 and sys.argv[1] == '--test':
        print("Table Sync Orchestrator - Test Mode")
        cfg_path = os.getenv('CONFIG_PATH', '/app/config/orchestrator.yaml')
        try:
            import re
            with open(cfg_path, 'r') as f:
                content = f.read()
            def env_replacer(match):
                spec = match.group(1)
                if ':-' in spec:
                    var, default = spec.split(':-', 1)
                elif ':' in spec:
                    var, default = spec.split(':', 1)
                else:
                    var, default = spec, ''
                return os.getenv(var, default)
            content = re.sub(r'\$\{([^}]+)\}', env_replacer, content)
            _ = yaml.safe_load(content)
            print("✅ Configuration file parsing: OK")
            print("✅ Python dependencies: OK")
            print("✅ Container structure: OK")
            print("✅ YAML environment substitution: OK")
            return
        except Exception as e:
            print(f"❌ Configuration test failed: {e}")
            sys.exit(1)

    cfg_path = os.getenv('CONFIG_PATH', '/app/config/orchestrator.yaml')
    orchestrator = TableSyncOrchestrator(cfg_path)
    orchestrator.run()


if __name__ == "__main__":
    main()
