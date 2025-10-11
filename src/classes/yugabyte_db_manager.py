from asyncio import subprocess
import psycopg2
from typing import Any, List
import re
import os
import json

import structlog
from psycopg2.extras import RealDictCursor
from classes.config_reader import ConfigKeys, LoggingKeys, YugabyteDBKeys
from classes.table_info import TableInfo  # <-- Add this import
from classes.table_annotation import TableAnnotation

class YugabyteDBManager:
    def __init__(self, config):
        self.config = config
        self.mock_enabled=self.config.get(ConfigKeys.YUGABYTEDB.value, {}).get(YugabyteDBKeys.MOCK.value, False)
        db_cfg = config.get(ConfigKeys.YUGABYTEDB.value, {})
        self.host = db_cfg.get('host', 'localhost')
        self.port = db_cfg.get('port', 5433)
        self.user = db_cfg.get('user', 'yugabyte')
        self.password = db_cfg.get('password', 'yugabyte')
        self.database = db_cfg.get('database', 'yugabyte')
        self.logger = self._init_logger()
        
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
        return structlog.get_logger("yugabyte_db_manager")

    def connect(self):
        if self.config.get(ConfigKeys.YUGABYTEDB.value, {}).get(YugabyteDBKeys.MOCK.value, False):
            self.logger.info("Mock connect called")
            from unittest.mock import MagicMock
            return MagicMock()
        
        """Establish a connection to the YugabyteDB database."""
        self.logger.info("Connecting to YugabyteDB", host=self.host, port=self.port, user=self.user, database=self.database)
        try:
            connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database
            )
            self.logger.info("Connection to YugabyteDB established successfully")
            return connection
        except Exception as e:
            self.logger.error("Failed to connect to YugabyteDB", error=str(e))
            raise RuntimeError(f"Failed to connect to YugabyteDB: {e}")

    def run_query(self, query: str, params: List[Any] = None):
        """Run a query on the YugabyteDB database."""
        self.logger.info("Running query on YugabyteDB", query=query, params=params)
        connection = self.connect()
        try:
            with connection.cursor() as cursor:
                cursor.execute(query, params)
                if query.strip().lower().startswith("select"):
                    result = cursor.fetchall()
                    self.logger.info("Query executed successfully", result=result)
                    return result
                connection.commit()
                self.logger.info("Query committed successfully")
        except Exception as e:
            self.logger.error("Failed to execute query", query=query, error=str(e))
            raise RuntimeError(f"Failed to execute query: {e}")
        finally:
            connection.close()
            self.logger.info("Connection to YugabyteDB closed")
            
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
        return self.discover_databases(excluded)

    def _discover_tables(self, database: str) -> List[TableInfo]:
        out: List[TableInfo] = []
        try:
            with self.connect() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
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
                    ann = TableAnnotation.from_comment(self.config, row['table_comment']) if row['table_comment'] else None
                    out.append(TableInfo(database=database, schema=row['table_schema'], table=row['table_name'], annotation=ann))
        except Exception as e:
            self.logger.error("Failed to discover tables", database=database, error=str(e))
        return out

    def create_table(self, table_name: str, schema: str):
        """Create a table in the YugabyteDB database."""
        query = f"CREATE TABLE {schema}.{table_name} (id SERIAL PRIMARY KEY);"
        self.logger.info("Creating table", table_name=table_name, schema=schema)
        self.run_query(query)
        self.logger.info("Table created successfully", table_name=table_name, schema=schema)

    def delete_table(self, table_name: str, schema: str):
        """Delete a table from the YugabyteDB database."""
        query = f"DROP TABLE IF EXISTS {schema}.{table_name};"
        self.logger.info("Deleting table", table_name=table_name, schema=schema)
        self.run_query(query)
        self.logger.info("Table deleted successfully", table_name=table_name, schema=schema)

    def create_database(self, database_name: str):
        """Create a new database in YugabyteDB."""
        query = f"CREATE DATABASE {database_name};"
        self.logger.info("Creating database", database_name=database_name)
        self.run_query(query)
        self.logger.info("Database created successfully", database_name=database_name)

    def delete_database(self, database_name: str):
        """Delete a database from YugabyteDB."""
        query = f"DROP DATABASE IF EXISTS {database_name};"
        self.logger.info("Deleting database", database_name=database_name)
        self.run_query(query)
        self.logger.info("Database deleted successfully", database_name=database_name)

    def create_schema(self, schema_name: str):
        """Create a schema in the YugabyteDB database."""
        query = f"CREATE SCHEMA {schema_name};"
        self.logger.info("Creating schema", schema_name=schema_name)
        self.run_query(query)
        self.logger.info("Schema created successfully", schema_name=schema_name)

    def delete_schema(self, schema_name: str):
        """Delete a schema from the YugabyteDB database."""
        query = f"DROP SCHEMA IF EXISTS {schema_name} CASCADE;"
        self.logger.info("Deleting schema", schema_name=schema_name)
        self.run_query(query)
        self.logger.info("Schema deleted successfully", schema_name=schema_name)

    def get_system_db_connection(self):
        """Establish a connection to a system database."""
        system_dbs = ['postgres', 'yugabyte', 'template1']
        for sys_db in system_dbs:
            try:
                self.logger.info("Connecting to system database", database=sys_db)
                conn = self.connect()
                conn.set_isolation_level(0)  # Autocommit mode
                self.logger.info("Connection to system database established", database=sys_db)
                return conn
            except Exception as e:
                self.logger.warning("Failed to connect to system database", database=sys_db, error=str(e))
                continue
        self.logger.error("Could not connect to any system database")
        raise RuntimeError("Could not connect to any system database")

    def discover_databases(self, excluded: List[str] = None) -> List[str]:
        """Discover databases in YugabyteDB."""
        excluded = excluded or ['postgres', 'template0', 'template1']
        query = "SELECT datname FROM pg_database WHERE datistemplate = false;"
        self.logger.info("Discovering databases", excluded=excluded)
        all_databases = [row[0] for row in self.run_query(query)]
        databases = [db for db in all_databases if db not in excluded]
        self.logger.info("Databases discovered", databases=databases)
        return databases

    def reconcile_table(self, table_info):
        """Reconcile a table's schema and data."""
        # Placeholder for reconciliation logic
        pass

    def get_table_schema(self, table_info: TableInfo):
        """Fetch the schema of a table from YugabyteDB."""
        schema_name = table_info.schema  # <-- Extract schema_name
        table_name = table_info.table    # <-- Extract table_name
        query = f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s;
        """
        self.logger.info("Fetching table schema", schema_name=schema_name, table_name=table_name)
        try:
            result = self.run_query(query, [schema_name, table_name])
            self.logger.info("Table schema fetched successfully", schema_name=schema_name, table_name=table_name, schema=result)
            return result
        except Exception as e:
            self.logger.error("Failed to fetch schema for table", schema_name=schema_name, table_name=table_name, error=str(e))
            raise RuntimeError(f"Failed to fetch schema for table {schema_name}.{table_name}: {e}")
        
    def create_stream(self, database_name: str) -> str:
        """Create a CDC stream for a given database using yb-admin."""
        self.logger.info("Creating CDC stream", database_name=database_name)

        master_addrs = (
            self.config.get(ConfigKeys.YUGABYTEDB.value, {}).get(YugabyteDBKeys.MASTER_ADDRESSES.value)
            or os.getenv("YB_MASTER_ADDRESSES")
        )
        if not master_addrs:
            self.logger.error("Master addresses not configured")
            raise ValueError("Master addresses not configured")

        yb_admin_bin = self.config.get(ConfigKeys.YUGABYTEDB.value, {}).get(YugabyteDBKeys.YB_ADMIN_PATH.value, "yb-admin")
        namespace = f"ysql.{database_name}"
        self.logger.debug("yb-admin binary and namespace resolved", yb_admin_bin=yb_admin_bin, namespace=namespace)

        try:
            out = subprocess.check_output(
                [yb_admin_bin, "--master_addresses", master_addrs, "create_change_data_stream", namespace],
                text=True, stderr=subprocess.STDOUT, timeout=20
            )
            self.logger.debug("yb-admin create_change_data_stream output", output=out)
            match = re.search(r"CDC Stream ID:\s*([0-9a-f]{32})", out, re.I)
            if match:
                stream_id = match.group(1)
                self.logger.info("Created CDC stream ID", stream_id=stream_id)
                return stream_id
        except subprocess.CalledProcessError as e:
            self.logger.error("Failed to create CDC stream", error=str(e))
            raise RuntimeError(f"Failed to create CDC stream: {e}")

        self.logger.error("Failed to create CDC stream: No stream ID found")
        raise RuntimeError("Failed to create CDC stream: No stream ID found")

    def insert_debezium_signal(self, table_info: TableInfo):
        """Insert a record into the public.debezium_signal table."""
        query = """
        INSERT INTO public.debezium_signal (id, type, data)
        VALUES (
          %s,
          'execute-snapshot',
          %s
        );
        """
        data = json.dumps({"data-collections": [f"{table_info.schema}.{table_info.table}"], "type": "incremental"})
        self.logger.info("Inserting record into debezium_signal table", table_name=table_info.table, data=data)
        try:
            self.run_query(query, [f'snap_{table_info.schema}_{table_info.table}', data])
            self.logger.info("Record inserted successfully", table_name=table_info.table)
        except Exception as e:
            self.logger.error("Failed to insert record into debezium_signal table", table_name=table_info.table, error=str(e))
            raise RuntimeError(f"Failed to insert record into debezium_signal table: {e}")

    def create_debezium_signal_table(self):
        """Create the debezium_signal table if it does not exist."""
        query = """
        CREATE TABLE IF NOT EXISTS public.debezium_signal (
            id   text PRIMARY KEY,
            type text NOT NULL,
            data jsonb,
            table_database text
        );
        """
        self.logger.info("Creating debezium_signal table if not exists")
        self.run_query(query)
        self.logger.info("debezium_signal table created or already exists")

    def entry_exists_in_debezium_signal(self, table_info: TableInfo) -> bool:
        """Check if an entry exists in the debezium_signal table for the given TableInfo."""
        table_id = f'snap_{table_info.schema}_{table_info.table}'
        query = """
        SELECT EXISTS (
            SELECT 1 FROM public.debezium_signal
            WHERE id = {table_id}
        );
        """
        self.logger.info("Checking if entry exists in debezium_signal table", id=table_id)
        result = self.run_query(query)
        exists = result[0][0] if result else False
        self.logger.info("Entry existence check in debezium_signal table completed", exists=exists)
        return exists
    
    def fetch_tables_in_debezium_signal(self, database: str) -> list:
        """Fetch all table entries in the public.debezium_signal table using the given database."""
        query = """
        SELECT DISTINCT data->>'data-collections' AS table_name
        FROM public.debezium_signal
        WHERE table_database = %s;
        """
        self.logger.info("Fetching table entries from debezium_signal table", database=database)
        result = self.run_query(query, [database])
        self.logger.info("Table entries fetched from debezium_signal table", count=len(result))
        return result
    
    def remove_entry_from_debezium_signal(self, database: str, table: str):
        """Remove an entry from the debezium_signal table."""
        query = """
        DELETE FROM public.debezium_signal
        WHERE table_database = %s AND data->>'data-collections' = %s;
        """
        table_identifier = f"{database}.{table}"
        self.logger.info("Removing entry from debezium_signal table", database=database, table=table)
        try:
            self.run_query(query, [database, table_identifier])
            self.logger.info("Entry removed successfully from debezium_signal table", database=database, table=table)
        except Exception as e:
            self.logger.error("Failed to remove entry from debezium_signal table", database=database, table=table, error=str(e))
            raise
