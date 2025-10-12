from asyncio import subprocess
from multiprocessing.dummy import connection
import psycopg2
from typing import Any, List
import re
import os
import json

import structlog
from psycopg2.extras import RealDictCursor, execute_batch
from classes.config_reader import ConfigKeys, LoggingKeys, YugabyteDBKeys
from classes.table_info import TableInfo
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
        return structlog.get_logger("yugabyte_db_manager")

    def connect(self, database: str = None):
        if self.config.get(ConfigKeys.YUGABYTEDB.value, {}).get(YugabyteDBKeys.MOCK.value, False):
            self.logger.info("Mock connect called")
            from unittest.mock import MagicMock
            return MagicMock()

        database_to_connect = database or self.database
        self.logger.info("Connecting to YugabyteDB", host=self.host, port=self.port, user=self.user, database=database_to_connect)
        try:
            connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=database_to_connect
            )
            with connection.cursor() as cur:
                cur.execute("SELECT current_database();")
                current_db = cur.fetchone()[0]
                self.logger.info("Connected to database", current_database=current_db)
            return connection
        except Exception as e:
            self.logger.error("Failed to connect to YugabyteDB", error=str(e))
            raise RuntimeError(f"Failed to connect to YugabyteDB: {e}")

    def run_query(self, query: str, params: List[Any] = None, database: str = None):
        """Run a query on the YugabyteDB database."""
        self.logger.info("Running query on YugabyteDB", query=query, params=params)
        connection = self.connect(database or self.database)
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

    def _discover_databases(self) -> List[str]:
        excluded = self.config.get(ConfigKeys.YUGABYTEDB.value, {}).get(YugabyteDBKeys.EXCLUDED_DATABASES.value, ['postgres', 'template0', 'template1'])
        return self.discover_databases(excluded)

    def discover_databases(self, excluded: List[str] = None) -> List[str]:
        """Discover databases in YugabyteDB."""
        excluded = excluded or ['postgres', 'template0', 'template1']
        query = "SELECT datname FROM pg_database WHERE datistemplate = false;"
        self.logger.info("Discovering databases", excluded=excluded)
        all_databases = [row[0] for row in self.run_query(query, self.database)]
        databases = [db for db in all_databases if db not in excluded]
        self.logger.info("Databases discovered", databases=databases)
        return databases
    
    def _discover_tables(self, database: str) -> List[TableInfo]:
        """Discover all tables in all schemas of the specified database."""
        out: List[TableInfo] = []
        try:
            with self.connect(database) as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
                sql_query = """
                    SELECT t.table_schema,
                           t.table_name,
                           obj_description(c.oid) AS table_comment
                    FROM information_schema.tables t
                    JOIN pg_class c       ON c.relname = t.table_name
                    JOIN pg_namespace n   ON n.oid = c.relnamespace AND n.nspname = t.table_schema
                    WHERE t.table_type = 'BASE TABLE'
                      AND t.table_schema NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
                      AND t.table_catalog = %s
                    ORDER BY t.table_schema, t.table_name
                """
                cur.execute("SHOW search_path;")
                self.logger.debug("Session search_path", search_path=cur.fetchone())
                cur.execute("SELECT current_database();")
                self.logger.debug("Current database", current_database=cur.fetchone())
                self.logger.debug("Executing SQL query", query=sql_query, params=(database,))
                cur.execute(sql_query, (database,))
                rows = cur.fetchall()
                self.logger.debug("SQL query executed successfully", row_count=len(rows), rows=rows)
                for row in rows:
                    ann = TableAnnotation.from_comment(row['table_comment']) if row['table_comment'] else None
                    out.append(TableInfo(database=database, schema=row['table_schema'], table=row['table_name'], annotation=ann))
        except Exception as e:
            self.logger.error("Failed to discover tables", database=database, error=str(e))
        return out

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
            self.run_query(query, [f'snap_{table_info.schema}_{table_info.table}', data], database=self.database)
            self.logger.info("Record inserted successfully", table_name=table_info.table)
        except Exception as e:
            self.logger.error("Failed to insert record into debezium_signal table", table_name=table_info.table, error=str(e))
            raise RuntimeError(f"Failed to insert record into debezium_signal table: {e}")

    def table_exists(self, database: str, table_name: str, schema: str) -> bool:
        query = """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = %s
            AND table_name = %s
        );
        """
        try:
            self.logger.info("Checking if table exists", table=table_name)
            result = self.run_query(query, [schema, table_name], database=database)
            return result[0][0] if result else False
        except Exception as e:
            self.logger.error("Failed to check if table exists", table=table_name, error=str(e))
            raise
    
    def create_debezium_signal_table(self):
        """Create the debezium_signal table if it does not exist."""
        if self.table_exists(self.database, 'debezium_signal', 'public'):
            self.logger.info("debezium_signal table already exists, clearing table")
            self.run_query(query="TRUNCATE TABLE public.debezium_signal;", database=self.database)
        
        query = """
        CREATE TABLE IF NOT EXISTS public.debezium_signal (
            id   text PRIMARY KEY,
            type text NOT NULL,
            data jsonb,
            table_database text
        );
        """
        self.logger.info("Creating debezium_signal table if not exists")
        self.run_query(query, self.database)
        self.logger.info("debezium_signal table created or already exists")

    def entry_exists_in_debezium_signal(self, table_info: TableInfo) -> bool:
        """Check if an entry exists in the debezium_signal table for the given TableInfo."""
        table_id = f'snap_{table_info.schema}_{table_info.table}'
        query = """
        SELECT EXISTS (
            SELECT 1 FROM public.debezium_signal
            WHERE id = %s
        );
        """
        self.logger.info("Checking if entry exists in debezium_signal table", id=table_id)
        result = self.run_query(query, [table_id], database=self.database)
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
        result = self.run_query(query, [database], database=self.database)
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
            self.run_query(query, [database, table_identifier], database=self.database)
            self.logger.info("Entry removed successfully from debezium_signal table", database=database, table=table)
        except Exception as e:
            self.logger.error("Failed to remove entry from debezium_signal table", database=database, table=table, error=str(e))
            raise
        
    def clear_yugabyte_table(self, database: str, table_info: TableInfo):
        self.logger.info("Clearing YugabyteDB table", database=database, table=table_info.table)
        try:
            with self.connect(database) as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(f"TRUNCATE TABLE {table_info.schema}.{table_info.table} CASCADE")
                conn.commit()
        finally:
            conn.close()
            self.logger.info("YugabyteDB table cleared", database=database, table=table_info.table)

    def insert_into_yugabyte(self, data, database: str, table_info: TableInfo):
        self.logger.info("Inserting data into YugabyteDB", database=database, table=table_info.table, row_count=len(data))
        try:
            with self.connect(database) as conn, conn.cursor() as cursor:
                # Assuming the table has columns matching the BigQuery table
                columns = ", ".join(data[0].keys())
                values_placeholder = ", ".join([f"%({col})s" for col in data[0].keys()])
                query = f"INSERT INTO {table_info.schema}.{table_info.table} ({columns}) VALUES ({values_placeholder})"

                # Use execute_batch for better performance with large volumes of data
                execute_batch(cursor, query, data)

                conn.commit()
                
            self.logger.info("Data inserted successfully into YugabyteDB", database=database, table=table_info.table)
        except Exception as e:
            self.logger.error("Failed to insert data into Yugabyte", error=str(e))
            raise
        finally:
            conn.close()
