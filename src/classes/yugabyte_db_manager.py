import subprocess
import psycopg2
from typing import Any, List
import re
import os
import json

from psycopg2.extras import RealDictCursor, execute_batch
from classes.config_reader import ConfigKeys, YugabyteDBKeys
from classes.table_info import TableInfo
from classes.table_annotation import TableAnnotation
from classes.logging import Logging

class YugabyteDBManager:
    def __init__(self, config, logger: Logging):
        self.config = config
        self.mock_enabled=self.config.get(ConfigKeys.YUGABYTEDB.value, {}).get(YugabyteDBKeys.MOCK.value, False)
        db_cfg = config.get(ConfigKeys.YUGABYTEDB.value, {})
        self.host = db_cfg.get('host', 'localhost')
        self.port = db_cfg.get('port', 5433)
        self.user = db_cfg.get('user', 'yugabyte')
        self.password = db_cfg.get('password', 'yugabyte')
        self.logger = logger

    def connect(self, database: str = None):
        if self.config.get(ConfigKeys.YUGABYTEDB.value, {}).get(YugabyteDBKeys.MOCK.value, False):
            self.logger.logMessage(Logging.LogLevel.DEBUG, "Mock connect called")
            from unittest.mock import MagicMock
            return MagicMock()

        database_to_connect = database
        self.logger.logMessage(Logging.LogLevel.DEBUG, "Connecting to YugabyteDB", host=self.host, port=self.port, user=self.user, database=database_to_connect)
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
                self.logger.logMessage(Logging.LogLevel.DEBUG, "Connected to database", current_database=current_db)
            return connection
        except Exception as e:
            self.logger.logMessage(Logging.LogLevel.ERROR, "Failed to connect to YugabyteDB", error=str(e))
            raise RuntimeError(f"Failed to connect to YugabyteDB: {e}")

    def run_query(self, query: str, database: str, params: List[Any] = None):
        """Run a query on the YugabyteDB database."""
        self.logger.logMessage(Logging.LogLevel.DEBUG, "Running query on YugabyteDB. query: " + query + " database: " + database + " params: " + str(params))
        connection = self.connect(database)
        try:
            with connection.cursor() as cursor:
                cursor.execute(query, params)
                if query.strip().lower().startswith("select"):
                    result = cursor.fetchall()
                    self.logger.logMessage(Logging.LogLevel.DEBUG, "Query executed successfully. result: " + str(result))
                    return result
                connection.commit()
                self.logger.logMessage(Logging.LogLevel.DEBUG, "Query committed successfully. query: " + query + " params: " + str(params))
        except Exception as e:
            self.logger.logMessage(Logging.LogLevel.ERROR, "Failed to execute query. query: " + query + " error: " + str(e))
            raise RuntimeError(f"Failed to execute query: {e}")
        finally:
            connection.close()
            self.logger.logMessage(Logging.LogLevel.DEBUG, "Connection to YugabyteDB closed")
            
    # ----------------------------- Discovery -----------------------------

    def _discover_databases(self, database: str) -> List[str]:
        excluded = self.config.get(ConfigKeys.YUGABYTEDB.value, {}).get(YugabyteDBKeys.EXCLUDED_DATABASES.value, ['postgres', 'template0', 'template1'])
        return self.discover_databases(database, excluded)

    def discover_databases(self, database: str, excluded: List[str] = None) -> List[str]:
        """Discover databases in YugabyteDB."""
        excluded = excluded or ['postgres', 'template0', 'template1']
        query = "SELECT datname FROM pg_database WHERE datistemplate = false;"
        self.logger.logMessage(Logging.LogLevel.DEBUG, "Discovering databases", excluded=excluded)
        all_databases = [row[0] for row in self.run_query(query, database, None)]
        databases = [db for db in all_databases if db not in excluded]
        self.logger.logMessage(Logging.LogLevel.DEBUG, "Databases discovered", databases=databases)
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
                self.logger.logMessage(Logging.LogLevel.DEBUG, "Session search_path", search_path=cur.fetchone())
                cur.execute("SELECT current_database();")
                self.logger.logMessage(Logging.LogLevel.DEBUG, "Current database", current_database=cur.fetchone())
                self.logger.logMessage(Logging.LogLevel.DEBUG, "Executing SQL query", query=sql_query, params=(database,))
                cur.execute(sql_query, (database,))
                rows = cur.fetchall()
                self.logger.logMessage(Logging.LogLevel.DEBUG, "SQL query executed successfully", row_count=len(rows), rows=rows)
                for row in rows:
                    ann = TableAnnotation.from_comment(row['table_comment']) if row['table_comment'] else None
                    out.append(TableInfo(database=database, schema=row['table_schema'], table=row['table_name'], annotation=ann))
        except Exception as e:
            self.logger.logMessage(Logging.LogLevel.ERROR, "Failed to discover tables", database=database, error=str(e))
        return out
        
    def delete_stream(self, stream_id: str):
        """Delete a CDC stream using yb-admin."""
        self.logger.logMessage(Logging.LogLevel.DEBUG, "Deleting CDC stream", stream_id=stream_id)

        master_addrs = (
            self.config.get(ConfigKeys.YUGABYTEDB.value, {}).get(YugabyteDBKeys.MASTER_ADDRESSES.value)
            or os.getenv("YB_MASTER_ADDRESSES")
        )
        if not master_addrs:
            self.logger.logMessage(Logging.LogLevel.ERROR, "Master addresses not configured")
            raise ValueError("Master addresses not configured")

        yb_admin_bin = self.config.get(ConfigKeys.YUGABYTEDB.value, {}).get(YugabyteDBKeys.YB_ADMIN_PATH.value, "yb-admin")
        self.logger.logMessage(Logging.LogLevel.DEBUG, "yb-admin binary resolved", yb_admin_bin=yb_admin_bin)

        try:
            out = subprocess.check_output(
                [yb_admin_bin, "--master_addresses", master_addrs, "delete_change_data_stream", stream_id],
                text=True, stderr=subprocess.STDOUT, timeout=20
            )
            self.logger.logMessage(Logging.LogLevel.DEBUG, "yb-admin delete_change_data_stream output", output=out)
            self.logger.logMessage(Logging.LogLevel.DEBUG, "Deleted CDC stream ID", stream_id=stream_id)
        except subprocess.CalledProcessError as e:
            self.logger.logMessage(Logging.LogLevel.ERROR, "Failed to delete CDC stream", error=str(e))
            raise RuntimeError(f"Failed to delete CDC stream: {e}")


    def create_stream(self, database_name: str) -> str:
        """Create a CDC stream for a given database using yb-admin."""
        self.logger.logMessage(Logging.LogLevel.DEBUG, "Creating CDC stream", database_name=database_name)

        master_addrs = (
            self.config.get(ConfigKeys.YUGABYTEDB.value, {}).get(YugabyteDBKeys.MASTER_ADDRESSES.value)
            or os.getenv("YB_MASTER_ADDRESSES")
        )
        if not master_addrs:
            self.logger.logMessage(Logging.LogLevel.ERROR, "Master addresses not configured")
            raise ValueError("Master addresses not configured")

        yb_admin_bin = self.config.get(ConfigKeys.YUGABYTEDB.value, {}).get(YugabyteDBKeys.YB_ADMIN_PATH.value, "yb-admin")
        namespace = f"ysql.{database_name}"
        self.logger.logMessage(Logging.LogLevel.DEBUG, "yb-admin binary and namespace resolved", yb_admin_bin=yb_admin_bin, namespace=namespace)

        try:
            out = subprocess.check_output(
                [yb_admin_bin, "--master_addresses", master_addrs, "create_change_data_stream", namespace],
                text=True, stderr=subprocess.STDOUT, timeout=20
            )
            self.logger.logMessage(Logging.LogLevel.DEBUG, "yb-admin create_change_data_stream output", output=out)
            match = re.search(r"CDC Stream ID:\s*([0-9a-f]{32})", out, re.I)
            if match:
                stream_id = match.group(1)
                self.logger.logMessage(Logging.LogLevel.DEBUG, "Created CDC stream ID", stream_id=stream_id)
                return stream_id
        except subprocess.CalledProcessError as e:
            self.logger.logMessage(Logging.LogLevel.ERROR, "Failed to create CDC stream", error=str(e))
            raise RuntimeError(f"Failed to create CDC stream: {e}")

        self.logger.logMessage(Logging.LogLevel.ERROR, "Failed to create CDC stream: No stream ID found")
        raise RuntimeError("Failed to create CDC stream: No stream ID found")

    def insert_debezium_signal(self, table_info: TableInfo, stream_id: str):
        """Insert a record into the public.debezium_signal table."""
        query = """
        INSERT INTO public.debezium_signal (id, type, data, table_database, stream_id)
        VALUES (
          %s,
          'execute-snapshot',
          %s,
          %s,
          %s
        );
        """
        data = json.dumps({"data-collections": [f"{table_info.schema}.{table_info.table}"], "type": "incremental"})
        self.logger.logMessage(Logging.LogLevel.DEBUG, "Inserting record into debezium_signal table", table_name=table_info.table, data=data, stream_id=stream_id, table=table_info.to_dict())
        try:
            self.run_query(query, table_info.database, [f'snap_{table_info.schema}_{table_info.table}', data, table_info.database, stream_id])
            self.logger.logMessage(Logging.LogLevel.DEBUG, "Record inserted successfully", table_name=table_info.table, table=table_info.to_dict())
        except Exception as e:
            self.logger.logMessage(Logging.LogLevel.ERROR, "Failed to insert record into debezium_signal table", table_name=table_info.table, error=str(e), table=table_info.to_dict())
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
            self.logger.logMessage(Logging.LogLevel.DEBUG, "Checking if table exists. table: " + table_name + " database: " + database + " schema: " + schema)
            result = self.run_query(query, database, [schema, table_name])
            return result[0][0] if result else False
        except Exception as e:
            self.logger.logMessage(Logging.LogLevel.ERROR, "Failed to check if table exists. error: " + str(e) + " table: " + table_name + " database: " + database + " schema: " + schema)
            raise
    
    def create_debezium_signal_table(self, database: str):
        """Create the debezium_signal table if it does not exist."""
        if self.table_exists(database, 'debezium_signal', 'public'):
            self.logger.logMessage(Logging.LogLevel.DEBUG, "debezium_signal table exists, fetching previous entries to clear streams")
            entries = self.run_query("""
                SELECT 
                    table_database, 
                    jsonb_array_elements_text(data->'data-collections') AS table_name,
                    stream_id
                FROM 
                    public.debezium_signal;
            """, database=database)
            for entry in entries:
                self.logger.logMessage(Logging.LogLevel.DEBUG, "Entry", entry=entry)
                self.logger.logMessage(Logging.LogLevel.DEBUG, "Removing CDC stream for entry", database=entry[0], table=entry[1], stream_id=entry[2])
                try:
                    self.delete_stream(entry[2])
                except Exception as e:
                    self.logger.logMessage(Logging.LogLevel.ERROR, "Failed to delete CDC stream for entry", database=entry[0], table=entry[1], error=str(e))

            self.logger.logMessage(Logging.LogLevel.DEBUG, "debezium_signal table already exists, clearing table")
            self.run_query("TRUNCATE TABLE public.debezium_signal;", database, None)
        
        query = """
        CREATE TABLE IF NOT EXISTS public.debezium_signal (
            id   text PRIMARY KEY,
            type text NOT NULL,
            data jsonb,
            table_database text,
            stream_id text
        );
        """
        self.logger.logMessage(Logging.LogLevel.DEBUG, "Creating debezium_signal table if not exists")
        self.run_query(query, database, None)
        self.logger.logMessage(Logging.LogLevel.DEBUG, "debezium_signal table created or already exists")

    def entry_exists_in_debezium_signal(self, table_info: TableInfo) -> bool:
        """Check if an entry exists in the debezium_signal table for the given TableInfo."""
        query = """
        SELECT EXISTS (
            SELECT 1 FROM public.debezium_signal
            WHERE id = '""" + f'snap_{table_info.schema}_{table_info.table}' + """'
        );
        """
        self.logger.logMessage(Logging.LogLevel.DEBUG, "Checking if entry exists in debezium_signal table. query: " + query, table=table_info.to_dict())
        result = self.run_query(query, table_info.database, None)
        exists = result[0][0] if result else False
        self.logger.logMessage(Logging.LogLevel.DEBUG, "Entry existence check in debezium_signal table completed", exists=exists, table=table_info.to_dict())
        return exists
    
    def fetch_tables_in_debezium_signal(self, database: str) -> list:
        """Fetch all table entries in the public.debezium_signal table using the given database."""
        query = """
        SELECT DISTINCT data->>'data-collections' AS table_name
        FROM public.debezium_signal
        WHERE table_database = %s;
        """
        self.logger.logMessage(Logging.LogLevel.DEBUG, "Fetching table entries from debezium_signal table", database=database)
        result = self.run_query(query, database, [database])
        self.logger.logMessage(Logging.LogLevel.DEBUG, "Table entries fetched from debezium_signal table", count=len(result))
        return result
    
    def check_stream_in_use(self, database: str, table: str) -> bool:
        try:
            query = """
            SELECT COUNT(*) 
            FROM public.debezium_signal
            WHERE table_database = %s AND data->>'data-collections' = %s;
            """
            table_identifier = f"{database}.{table}"
            self.logger.logMessage(Logging.LogLevel.DEBUG, "Checking if stream is in use for other tables", database=database, table=table)
            result = self.run_query(query, database, [database, table_identifier])
            count = result[0][0] if result else 0
            in_use = count > 1  # If more than one entry exists, the stream is in use by other tables
            self.logger.logMessage(Logging.LogLevel.DEBUG, "Stream usage check completed", in_use=in_use)
            return in_use
        except Exception as e:
            self.logger.logMessage(Logging.LogLevel.ERROR, "Error checking stream usage", database=database, table=table, error=str(e))
            raise
    
    def remove_entry_from_debezium_signal(self, database: str, table: str):
        """Remove an entry from the debezium_signal table."""
        self.logger.logMessage(Logging.LogLevel.DEBUG, "Removal requested from debezium signal, checking to see if stream is in use for other tables", database=database, table=table)
        in_use = self.check_stream_in_use(database, table)
        if in_use:
            self.logger.logMessage(Logging.LogLevel.DEBUG, "Stream is still in use by other tables, skipping removal", database=database, table=table)
            return
        query = """
        DELETE FROM public.debezium_signal
        WHERE table_database = %s AND data->>'data-collections' = %s;
        """
        table_identifier = f"{database}.{table}"
        self.logger.logMessage(Logging.LogLevel.DEBUG, "Removing entry from debezium_signal table", database=database, table=table)
        try:
            self.run_query(query, database, [database, table_identifier])
            self.logger.logMessage(Logging.LogLevel.DEBUG, "Entry removed successfully from debezium_signal table", database=database, table=table)
        except Exception as e:
            self.logger.logMessage(Logging.LogLevel.ERROR, "Failed to remove entry from debezium_signal table", database=database, table=table, error=str(e))
            raise
        
    def clear_yugabyte_table(self, database: str, table_info: TableInfo):
        self.logger.logMessage(Logging.LogLevel.DEBUG, "Clearing YugabyteDB table", database=database, table=table_info.to_dict())
        try:
            with self.connect(database) as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(f"TRUNCATE TABLE {table_info.schema}.{table_info.table} CASCADE")
                conn.commit()
        finally:
            conn.close()
            self.logger.logMessage(Logging.LogLevel.DEBUG, "YugabyteDB table cleared", database=database, table=table_info.to_dict())

    def insert_into_yugabyte(self, data, database: str, table_info: TableInfo):
        self.logger.logMessage(Logging.LogLevel.DEBUG, "Inserting data into YugabyteDB", database=database, row_count=len(data), table=table_info.to_dict())
        try:
            with self.connect(database) as conn, conn.cursor() as cursor:
                # Assuming the table has columns matching the BigQuery table
                columns = ", ".join(data[0].keys())
                values_placeholder = ", ".join([f"%({col})s" for col in data[0].keys()])
                query = f"INSERT INTO {table_info.schema}.{table_info.table} ({columns}) VALUES ({values_placeholder})"

                # Fetch column types from the database
                column_types_query = f"""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s;
                """
                cursor.execute(column_types_query, (table_info.schema, table_info.table))
                column_types = {row[0]: row[1] for row in cursor.fetchall()}
                self.logger.logMessage(Logging.LogLevel.DEBUG, "Column types fetched", column_types=column_types, table=table_info.to_dict())

                # Convert dictionary values to JSON strings only for JSON/JSONB columns
                for row in data:
                    for key, value in row.items():
                        if isinstance(value, dict):
                            if key == "id" and "id" in value:
                                # Unwrap the 'id' field
                                row[key] = value["id"]
                            elif column_types.get(key) in ("json", "jsonb"):
                                # Convert dict to JSON string
                                row[key] = json.dumps(value)
                            else:
                                self.logger.logMessage(Logging.LogLevel.WARNING, "Unexpected dict value for non-JSON column", column=key, value=value, table=table_info.to_dict())
                                row[key] = str(value)  # Fallback to string conversion

                self.logger.logMessage(Logging.LogLevel.DEBUG, "Data prepared for insertion", data=data, table=table_info.to_dict())

                # Use execute_batch for better performance with large volumes of data
                execute_batch(cursor, query, data)
                
                conn.commit()
                self.logger.logMessage(Logging.LogLevel.DEBUG, "Data inserted successfully", database=database, row_count=len(data), table=table_info.to_dict())
        except Exception as e:
            self.logger.logMessage(Logging.LogLevel.ERROR, "Failed to insert data into YugabyteDB", database=database, error=str(e), table=table_info.to_dict())
            raise RuntimeError(f"Failed to insert data into YugabyteDB: {e}")
