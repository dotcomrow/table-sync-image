import psycopg2
from typing import Any, List
from classes.config_reader import ConfigKeys
from classes.table_info import TableInfo  # <-- Add this import

class YugabyteDBManager:
    def __init__(self, config):
        db_cfg = config.get(ConfigKeys.YUGABYTEDB.value, {})
        self.host = db_cfg.get('host', 'localhost')
        self.port = db_cfg.get('port', 5433)
        self.user = db_cfg.get('user', 'yugabyte')
        self.password = db_cfg.get('password', 'yugabyte')
        self.database = db_cfg.get('database', 'yugabyte')

    def connect(self):
        """Establish a connection to the YugabyteDB database."""
        try:
            connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database
            )
            return connection
        except Exception as e:
            raise RuntimeError(f"Failed to connect to YugabyteDB: {e}")

    def run_query(self, query: str, params: List[Any] = None):
        """Run a query on the YugabyteDB database."""
        connection = self.connect()
        try:
            with connection.cursor() as cursor:
                cursor.execute(query, params)
                if query.strip().lower().startswith("select"):
                    return cursor.fetchall()
                connection.commit()
        except Exception as e:
            raise RuntimeError(f"Failed to execute query: {e}")
        finally:
            connection.close()

    def create_table(self, table_name: str, schema: str):
        """Create a table in the YugabyteDB database."""
        query = f"CREATE TABLE {schema}.{table_name} (id SERIAL PRIMARY KEY);"
        self.run_query(query)

    def delete_table(self, table_name: str, schema: str):
        """Delete a table from the YugabyteDB database."""
        query = f"DROP TABLE IF EXISTS {schema}.{table_name};"
        self.run_query(query)

    def create_database(self, database_name: str):
        """Create a new database in YugabyteDB."""
        query = f"CREATE DATABASE {database_name};"
        self.run_query(query)

    def delete_database(self, database_name: str):
        """Delete a database from YugabyteDB."""
        query = f"DROP DATABASE IF EXISTS {database_name};"
        self.run_query(query)

    def create_schema(self, schema_name: str):
        """Create a schema in the YugabyteDB database."""
        query = f"CREATE SCHEMA {schema_name};"
        self.run_query(query)

    def delete_schema(self, schema_name: str):
        """Delete a schema from the YugabyteDB database."""
        query = f"DROP SCHEMA IF EXISTS {schema_name} CASCADE;"
        self.run_query(query)

    def get_system_db_connection(self):
        """Establish a connection to a system database."""
        system_dbs = ['postgres', 'yugabyte', 'template1']
        for sys_db in system_dbs:
            try:
                conn = self.connect()
                conn.set_isolation_level(0)  # Autocommit mode
                return conn
            except Exception as e:
                continue
        raise RuntimeError("Could not connect to any system database")

    def discover_databases(self, excluded: List[str] = None) -> List[str]:
        """Discover databases in YugabyteDB."""
        excluded = excluded or ['postgres', 'template0', 'template1']
        query = "SELECT datname FROM pg_database WHERE datistemplate = false;"
        all_databases = [row[0] for row in self.run_query(query)]
        return [db for db in all_databases if db not in excluded]

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
        try:
            result = self.run_query(query, [schema_name, table_name])
            return result
        except Exception as e:
            raise RuntimeError(f"Failed to fetch schema for table {schema_name}.{table_name}: {e}")