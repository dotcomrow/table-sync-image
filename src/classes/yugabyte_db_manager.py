import psycopg2
from typing import Any, List

import structlog
from classes.config_reader import ConfigKeys, LoggingKeys, YugabyteDBKeys
from classes.table_info import TableInfo  # <-- Add this import

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