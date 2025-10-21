import psycopg2
from typing import Any, List
import json

from psycopg2.extras import RealDictCursor, execute_batch
from classes.config_reader import ConfigKeys, YugabyteDBKeys
from classes.table_info import TableInfo
from classes.table_annotation import TableAnnotation
from classes.logging import Logging

class YugabyteDBManager:
    debezium_signal_id_format = "snap_{schema}_{table}"
    
    def __init__(self, config, logger: Logging):
        self.config = config
        self.mock_enabled=self.config.get(ConfigKeys.YUGABYTEDB.value, {}).get(YugabyteDBKeys.MOCK.value, False)
        db_cfg = config.get(ConfigKeys.YUGABYTEDB.value, {})
        self.host = db_cfg.get('host', 'localhost')
        self.port = db_cfg.get('port', 5433)
        self.user = db_cfg.get('user', 'yugabyte')
        self.password = db_cfg.get('password', 'yugabyte')
        self.logger = logger

    def connect(self, database: str):
        if self.config.get(ConfigKeys.YUGABYTEDB.value, {}).get(YugabyteDBKeys.MOCK.value, False):
            self.logger.logMessage(Logging.LogLevel.DEBUG, "Mock connect called")
            from unittest.mock import MagicMock
            return MagicMock()

        try:
            connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=database
            )
            with connection.cursor() as cur:
                cur.execute("SELECT current_database();")
                current_db = cur.fetchone()[0]
            return connection
        except Exception as e:
            self.logger.logMessage(Logging.LogLevel.ERROR, "Failed to connect to YugabyteDB", error=str(e))
            raise RuntimeError(f"Failed to connect to YugabyteDB: {e}")

    def run_query(self, query: str, database: str, params: List[Any] = None):
        """Run a query on the YugabyteDB database."""
        self.logger.logMessage(Logging.LogLevel.INFO, "Running query on YugabyteDB", query=query, database=database, params=params)
        connection = self.connect(database)
        try:
            with connection.cursor() as cursor:
                cursor.execute(query, params)
                if query.strip().lower().startswith("select"):
                    result = cursor.fetchall()
                    self.logger.logMessage(Logging.LogLevel.INFO, "Query executed successfully", query=query, result=result)
                    return result
                connection.commit()
                self.logger.logMessage(Logging.LogLevel.DEBUG, "Query committed successfully", query=query, params=params)
        except Exception as e:
            self.logger.logMessage(Logging.LogLevel.ERROR, "Failed to execute query", query=query, error=str(e))
            raise RuntimeError(f"Failed to execute query: {e}")
        finally:
            connection.close()
            
    # ----------------------------- Discovery -----------------------------

    def _discover_databases(self, database: str = "kafka") -> List[str]:
        self.logger.logMessage(Logging.LogLevel.INFO, "Discovering databases")
        excluded = self.config.get(ConfigKeys.YUGABYTEDB.value, {}).get(YugabyteDBKeys.EXCLUDED_DATABASES.value, ['postgres', 'template0', 'template1'])
        """Discover databases in YugabyteDB."""
        query = "SELECT datname FROM pg_database WHERE datistemplate = false;"
        all_databases = [row[0] for row in self.run_query(query, database, None)]
        databases = [db for db in all_databases if db not in excluded]
        self.logger.logMessage(Logging.LogLevel.DEBUG, "Databases discovered", databases=databases)
        return databases
    
    def _has_primary_key_with_id_column(self, cursor, schema: str, table: str) -> bool:
        """Check if a table has a primary key and an 'id' column, ideally the 'id' column should be the primary key."""
        try:
            # Query to get primary key columns and check if 'id' column exists
            pk_query = """
                SELECT 
                    kcu.column_name,
                    EXISTS (
                        SELECT 1 
                        FROM information_schema.columns c 
                        WHERE c.table_schema = %s 
                        AND c.table_name = %s 
                        AND c.column_name = 'id'
                    ) as has_id_column
                FROM information_schema.table_constraints tc
                JOIN information_schema.key_column_usage kcu 
                    ON tc.constraint_name = kcu.constraint_name 
                    AND tc.table_schema = kcu.table_schema
                WHERE tc.constraint_type = 'PRIMARY KEY'
                    AND tc.table_schema = %s
                    AND tc.table_name = %s
                ORDER BY kcu.ordinal_position;
            """
            cursor.execute(pk_query, (schema, table, schema, table))
            results = cursor.fetchall()
            
            if not results:
                self.logger.logMessage(Logging.LogLevel.DEBUG, "Table has no primary key", schema=schema, table=table)
                return False
            
            # Check if table has an 'id' column
            has_id_column = results[0]['has_id_column'] if results else False
            if not has_id_column:
                self.logger.logMessage(Logging.LogLevel.DEBUG, "Table has no 'id' column", schema=schema, table=table)
                return False
            
            # Get primary key column names
            pk_columns = [row['column_name'] for row in results]
            
            # Check if 'id' is part of the primary key (preferably the only column)
            if 'id' in pk_columns:
                if len(pk_columns) == 1:
                    self.logger.logMessage(Logging.LogLevel.DEBUG, "Table has 'id' as single primary key", schema=schema, table=table)
                else:
                    self.logger.logMessage(Logging.LogLevel.DEBUG, "Table has 'id' as part of composite primary key", schema=schema, table=table, pk_columns=pk_columns)
                return True
            else:
                self.logger.logMessage(Logging.LogLevel.DEBUG, "Table has primary key but 'id' is not part of it", schema=schema, table=table, pk_columns=pk_columns)
                return False
                
        except Exception as e:
            self.logger.logMessage(Logging.LogLevel.ERROR, "Failed to check primary key for table", schema=schema, table=table, error=str(e))
            return False

    def _discover_tables(self, database: str) -> List[TableInfo]:
        """Discover all tables in all schemas of the specified database."""
        self.logger.logMessage(Logging.LogLevel.INFO, "Discovering tables in database available for sync", database=database)
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
                      AND NOT (t.table_schema = 'public' AND t.table_name = 'debezium_signal')
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
                self.logger.logMessage(Logging.LogLevel.INFO, "SQL query executed successfully", row_count=len(rows), rows=rows)
                for row in rows:
                    # Check if table has primary key with 'id' column before adding to sync candidates
                    if not self._has_primary_key_with_id_column(cur, row['table_schema'], row['table_name']):
                        self.logger.logMessage(Logging.LogLevel.INFO, "Skipping table - does not meet BigQuery sync requirements", schema=row['table_schema'], table=row['table_name'])
                        continue
                    
                    ann = TableAnnotation.from_comment(row['table_comment']) if row['table_comment'] else None
                    out.append(TableInfo(database=database, schema=row['table_schema'], table=row['table_name'], annotation=ann))
        except Exception as e:
            self.logger.logMessage(Logging.LogLevel.ERROR, "Failed to discover tables", database=database, error=str(e))
        return out

    def insert_debezium_signal(self, table_info: TableInfo, stream_id: str, signal_type: str = "execute-snapshot", auto_cleanup_after_seconds: int = None):
        """
        Insert a record into the public.debezium_signal table.
        
        Args:
            table_info: Table information
            stream_id: CDC stream ID
            signal_type: Type of signal - 'execute-snapshot', 'schema-change', 'pause', 'resume'
            auto_cleanup_after_seconds: If provided, automatically clean up this signal after N seconds
        
        Returns:
            str: The signal ID that was inserted (for manual cleanup later)
        """
        table_id = self.debezium_signal_id_format.format(schema=table_info.schema, table=table_info.table)
        
        # Add timestamp to make IDs unique for repeated operations
        import time
        timestamp = str(int(time.time()))
        if signal_type == "execute-snapshot":
            table_id = f"snap_{table_info.schema}_{table_info.table}_{timestamp}"
        
        query = """
        INSERT INTO public.debezium_signal (id, type, data)
        VALUES (
          %s,
          %s,
          %s
        );
        """
        
        if signal_type == "execute-snapshot":
            data = json.dumps({
                "data-collections": [f"{table_info.schema}.{table_info.table}"], 
                "type": "incremental"
            })
        elif signal_type == "schema-change":
            data = json.dumps({
                "database": table_info.database
            })
        else:
            data = json.dumps({})
        
        self.logger.logMessage(Logging.LogLevel.DEBUG, "Inserting record into debezium_signal table", 
                             signal_id=table_id, table_name=table_info.table, data=data, 
                             stream_id=stream_id, table=table_info.to_dict())
        try:
            self.run_query(query, table_info.database, [table_id, signal_type, data])
            self.logger.logMessage(Logging.LogLevel.DEBUG, "Signal record inserted successfully", 
                                 signal_id=table_id, table_name=table_info.table, table=table_info.to_dict())
            
            # Optional auto-cleanup (useful for temporary signals)
            if auto_cleanup_after_seconds:
                import threading
                def cleanup_signal():
                    time.sleep(auto_cleanup_after_seconds)
                    self.remove_debezium_signal_by_id(table_info.database, table_id)
                
                cleanup_thread = threading.Thread(target=cleanup_signal, daemon=True)
                cleanup_thread.start()
                self.logger.logMessage(Logging.LogLevel.DEBUG, "Auto-cleanup scheduled", 
                                     signal_id=table_id, cleanup_after_seconds=auto_cleanup_after_seconds)
            
            return table_id
            
        except Exception as e:
            self.logger.logMessage(Logging.LogLevel.ERROR, "Failed to insert record into debezium_signal table", 
                                 table_name=table_info.table, error=str(e), table=table_info.to_dict())
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
        
    def create_stream_table(self, database: str):
        if self.table_exists(database, 'database_stream', 'public'):
            self.logger.logMessage(Logging.LogLevel.DEBUG, "database_stream table already exists")
            return
        
        query = """
        CREATE TABLE IF NOT EXISTS public.database_stream (
            stream_id text PRIMARY KEY,
            created_at timestamptz DEFAULT now()
        )
        """
        self.logger.logMessage(Logging.LogLevel.DEBUG, "Creating database_stream table if not exists")
        self.run_query(query, database)
        self.logger.logMessage(Logging.LogLevel.DEBUG, "database_stream table created or already exists")
        
    def stream_exists(self, database: str) -> bool:
        select_query = """
        SELECT * FROM public.database_stream;
        """
        self.logger.logMessage(Logging.LogLevel.DEBUG, "Checking if stream exists in database_stream table")
        existing = self.run_query(select_query, database, None)
        if existing:
            self.logger.logMessage(Logging.LogLevel.DEBUG, "Stream exists in database_stream table", count=len(existing))
            return existing[0][0]
        else:
            self.logger.logMessage(Logging.LogLevel.DEBUG, "No stream exists in database_stream table")
            return None
        
    def insert_into_stream_table(self, stream_id: str, database: str):
        if self.stream_exists(database) is not None:
            self.logger.logMessage(Logging.LogLevel.DEBUG, "Stream ID already exists in database_stream table.  Review why this happened as this function should not be called if a stream already exists for this db", current_stream_id=stream_id)
            return
            
        query = """
        INSERT INTO public.database_stream (stream_id)
        VALUES (%s);
        """
        self.logger.logMessage(Logging.LogLevel.DEBUG, "Inserting stream ID into database_stream table", stream_id=stream_id)
        try:
            self.run_query(query, database, [stream_id])
            self.logger.logMessage(Logging.LogLevel.DEBUG, "Stream ID inserted successfully", stream_id=stream_id)
        except Exception as e:
            self.logger.logMessage(Logging.LogLevel.ERROR, "Failed to insert stream ID into database_stream table", stream_id=stream_id, error=str(e))
            raise RuntimeError(f"Failed to insert stream ID into database_stream table: {e}")
    
    def create_debezium_signal_table(self, database: str):
        """Create the debezium_signal and database_stream tables if they do not exist."""
        if self.table_exists(database, 'debezium_signal', 'public'):
            self.run_query("DELETE FROM public.debezium_signal WHERE id IN (SELECT id FROM public.debezium_signal);", database)
        
        query = """
        CREATE TABLE IF NOT EXISTS public.debezium_signal (
            id   text PRIMARY KEY,
            type text NOT NULL,
            data jsonb,
            created_at timestamptz DEFAULT now()
        );
        """
        self.logger.logMessage(Logging.LogLevel.DEBUG, "Creating debezium_signal table if not exists")
        self.run_query(query, database)
        self.logger.logMessage(Logging.LogLevel.INFO, "debezium_signal table created or already exists")

    def cleanup_old_debezium_signals(self, database: str, older_than_hours: int = 24):
        """
        Clean up old debezium signal entries to prevent table bloat.
        Keeps the table lean by removing processed signals older than specified hours.
        """
        query = """
        DELETE FROM public.debezium_signal 
        WHERE created_at < now() - interval '%s hours'
        """
        try:
            self.logger.logMessage(Logging.LogLevel.DEBUG, "Cleaning up old debezium signals", 
                                 older_than_hours=older_than_hours, database=database)
            self.run_query(query, database, [older_than_hours])
            self.logger.logMessage(Logging.LogLevel.INFO, "Old debezium signals cleaned up successfully", 
                                 older_than_hours=older_than_hours, database=database)
        except Exception as e:
            self.logger.logMessage(Logging.LogLevel.ERROR, "Failed to cleanup old debezium signals", 
                                 error=str(e), database=database)

    def get_active_debezium_signals(self, database: str) -> list:
        """
        Get all active debezium signals to understand what operations are pending/in-progress.
        """
        query = """
        SELECT id, type, data, created_at 
        FROM public.debezium_signal 
        ORDER BY created_at DESC
        """
        try:
            self.logger.logMessage(Logging.LogLevel.DEBUG, "Fetching active debezium signals", database=database)
            result = self.run_query(query, database)
            self.logger.logMessage(Logging.LogLevel.DEBUG, "Active debezium signals retrieved", 
                                 count=len(result), database=database)
            return result
        except Exception as e:
            self.logger.logMessage(Logging.LogLevel.ERROR, "Failed to fetch active debezium signals", 
                                 error=str(e), database=database)
            return []

    def entry_exists_in_debezium_signal(self, table_info: TableInfo) -> bool:
        """Check if an entry exists in the debezium_signal table for the given TableInfo."""
        table_id = self.debezium_signal_id_format.format(schema=table_info.schema, table=table_info.table)
        query = """
        select count(*) from public.debezium_signal where id = %s;
        """
        self.logger.logMessage(Logging.LogLevel.DEBUG, "Checking if entry exists in debezium_signal table", query=query, table_id=table_id, table=table_info.to_dict())
        result = self.run_query(query, table_info.database, [table_id])
        self.logger.logMessage(Logging.LogLevel.INFO, "Entry existence check in debezium_signal table completed", raw_result=str(result), table=table_info.to_dict())
        if result[0][0] > 0:
            self.logger.logMessage(Logging.LogLevel.DEBUG, "Entry already exists in debezium_signal table", table_id=table_id, table=table_info.to_dict())
            return True
        else:
            self.logger.logMessage(Logging.LogLevel.DEBUG, "Entry does not exist in debezium_signal table", table_id=table_id, table=table_info.to_dict())
            return False
    
    def fetch_tables_in_debezium_signal(self, database: str) -> list:
        """Fetch all table entries in the public.debezium_signal table using the given database."""
        query = """
        SELECT DISTINCT data->>'data-collections' AS table_name
        FROM public.debezium_signal
        """
        self.logger.logMessage(Logging.LogLevel.DEBUG, "Fetching table entries from debezium_signal table", database=database)
        result = self.run_query(query, database, [database])
        self.logger.logMessage(Logging.LogLevel.INFO, "Table entries fetched from debezium_signal table", count=len(result))
        return result
    
    def remove_entry_from_debezium_signal(self, database: str, table: str):
        """Remove an entry from the debezium_signal table."""
        query = """
        DELETE FROM public.debezium_signal
        WHERE data->>'data-collections' = %s;
        """
        table_identifier = f"{database}.{table}"
        self.logger.logMessage(Logging.LogLevel.DEBUG, "Removing entry from debezium_signal table", database=database, table=table)
        try:
            result = self.run_query(query, database, [table_identifier])
            self.logger.logMessage(Logging.LogLevel.DEBUG, "Entry removed successfully from debezium_signal table", result=result, database=database, table=table)
        except Exception as e:
            self.logger.logMessage(Logging.LogLevel.ERROR, "Failed to remove entry from debezium_signal table", database=database, table=table, error=str(e))
            raise
        
    def get_row_count(self, table_info: TableInfo) -> int:
        """Get the row count of a specified table."""
        query = f"SELECT COUNT(*) FROM {table_info.schema}.{table_info.table};"
        self.logger.logMessage(Logging.LogLevel.DEBUG, "Getting row count for YugabyteDB table", database=table_info.database, table=table_info.to_dict())
        try:
            result = self.run_query(query, table_info.database)
            row_count = result[0][0] if result else 0
            self.logger.logMessage(Logging.LogLevel.INFO, "Row count retrieved successfully", database=table_info.database, table=table_info.to_dict(), row_count=row_count)
            return row_count
        except Exception as e:
            self.logger.logMessage(Logging.LogLevel.ERROR, "Failed to get row count for YugabyteDB table", database=table_info.database, table=table_info.to_dict(), error=str(e))
            raise RuntimeError(f"Failed to get row count for YugabyteDB table: {e}")
        
    def clear_yugabyte_table(self, table_info: TableInfo):
        self.logger.logMessage(Logging.LogLevel.DEBUG, "Clearing YugabyteDB table", database=table_info.database, table=table_info.to_dict())
        try:
            with self.connect(table_info.database) as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
                query = f"DELETE FROM {table_info.schema}.{table_info.table} WHERE id IN (SELECT id FROM {table_info.schema}.{table_info.table});"
                cur.execute(query)
                conn.commit()
        finally:
            conn.close()
            self.logger.logMessage(Logging.LogLevel.INFO, "YugabyteDB table cleared", database=table_info.database, table=table_info.to_dict())

    def insert_into_yugabyte(self, data, table_info: TableInfo):
        if data is None or len(data) == 0:
            self.logger.logMessage(Logging.LogLevel.WARNING, "No data to insert into YugabyteDB", database=table_info.database, table=table_info.to_dict())
            return

        self.logger.logMessage(Logging.LogLevel.INFO, "Inserting data into YugabyteDB", database=table_info.database, row_count=len(data), table=table_info.to_dict())
        try:
            with self.connect(table_info.database) as conn, conn.cursor() as cursor:
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
                self.logger.logMessage(Logging.LogLevel.INFO, "Data inserted successfully", database=table_info.database, row_count=len(data), table=table_info.to_dict())
        except Exception as e:
            raise RuntimeError(f"Failed to insert data into YugabyteDB: {e}")
