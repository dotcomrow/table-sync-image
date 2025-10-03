from google.cloud import bigquery
import os
import subprocess
import re
from typing import List
from classes.config_reader import ConfigKeys

class BigQueryManager:
    def __init__(self, config):
        self.config = config
        self.client = bigquery.Client()

    def create_table(self, table_info):
        # Implementation migrated from table_sync_orchestrator
        dataset_id = table_info.bq_dataset
        table_id = table_info.bq_table
        if not dataset_id or not table_id:
            raise ValueError("Invalid dataset or table ID")

        dataset_ref = self.client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)

        schema = [
            bigquery.SchemaField("column_name", "STRING", mode="NULLABLE"),  # Replace with actual schema logic
        ]

        table = bigquery.Table(table_ref, schema=schema)
        self.client.create_table(table)

    def delete_table(self, dataset_id, table_id):
        # Implementation migrated from table_sync_orchestrator
        table_ref = self.client.dataset(dataset_id).table(table_id)
        self.client.delete_table(table_ref)

    def copy_initial_data(self, table_info):
        # Placeholder for initial data copy logic
        query = f"""
        INSERT INTO `{table_info.bq_dataset}.{table_info.bq_table}`
        SELECT * FROM `{table_info.source_dataset}.{table_info.source_table}`
        """
        job = self.client.query(query)
        job.result()  # Wait for the job to complete

    def check_table_exists(self, dataset_id, table_id):
        try:
            self.client.get_table(self.client.dataset(dataset_id).table(table_id))
            return True
        except Exception:
            return False

    def get_table_schema(self, table_info):
        # Logic to fetch schema from YugabyteDB
        master_addrs = (
            self.config.get(ConfigKeys.YUGABYTEDB_MASTER_ADDRESSES.value)
            or os.getenv("YB_MASTER_ADDRESSES")
        )
        if not master_addrs:
            raise ValueError("Master addresses not configured")

        yb_admin_bin = self.config.get(ConfigKeys.YUGABYTEDB_YB_ADMIN_PATH.value, "yb-admin")
        namespace = f"ysql.{table_info.database}"

        try:
            out = subprocess.check_output(
                [yb_admin_bin, "--master_addresses", master_addrs, "describe_table", namespace, table_info.table],
                text=True, stderr=subprocess.STDOUT, timeout=20
            )
            # Parse the output to extract schema details
            schema = []
            for line in out.splitlines():
                match = re.match(r"Column:\s+(\w+)\s+Type:\s+(\w+)", line)
                if match:
                    schema.append(bigquery.SchemaField(match.group(1), match.group(2).upper(), mode="NULLABLE"))
            return schema
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"Failed to fetch table schema: {e}")

    def create_database_if_needed(self, target_database: str, username: str) -> List[str]:
        try:
            # Establish a connection to the system database
            conn = self._get_system_db_connection()
            conn.autocommit = True

            # Use the existing create_table method to create the database
            table_info = type('TableInfo', (object,), {
                'bq_dataset': None,  # Placeholder for dataset
                'bq_table': target_database
            })()
            self.create_table(table_info)

            # Grant privileges and ownership on the new database
            with self._get_db_connection_ctx(target_database) as new_conn:
                new_conn.autocommit = True
                with new_conn.cursor() as ncur:
                    self._grant_privileges(ncur, target_database, username)

            # Perform a comprehensive database scan if enabled
            if self.config.get(ConfigKeys.COMPREHENSIVE_DATABASE_SCAN.value, True):
                return self._perform_comprehensive_scan()

            return [target_database]
        except Exception as e:
            self.logger.error("Failed to finalize database creation", error=str(e))
            return []
        finally:
            try:
                if 'conn' in locals() and conn:
                    conn.close()
            except Exception:
                pass

    def _create_database(self, conn, target_database: str, username: str):
        """Helper method to create a database."""
        with conn.cursor() as cur:
            try:
                self.logger.info("Attempting to create database", database=target_database, owner=username)
                cur.execute(f'CREATE DATABASE "{target_database}" OWNER "{username}"')
                self.logger.info("Target database created", database=target_database, owner=username)
            except Exception as e:
                self.logger.error("Failed to create target database", database=target_database, error=str(e))

    def _grant_privileges(self, cursor, target_database: str, username: str):
        """Helper method to grant privileges and ownership on the database."""
        cursor.execute(f'ALTER SCHEMA public OWNER TO "{username}"')
        cursor.execute(f'GRANT ALL ON SCHEMA public TO "{username}"')
        cursor.execute(f'GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO "{username}"')
        cursor.execute(f'GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO "{username}"')
        cursor.execute(f'GRANT ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA public TO "{username}"')
        cursor.execute(f'ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO "{username}"')
        cursor.execute(f'ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO "{username}"')
        cursor.execute(f'ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON FUNCTIONS TO "{username}"')
        cursor.execute(f'ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TYPES TO "{username}"')
        cursor.execute(f'GRANT CREATE ON DATABASE "{target_database}" TO "{username}"')
        self.logger.info("Granted privileges/ownership on new database", database=target_database, user=username)

    def _perform_comprehensive_scan(self) -> List[str]:
        """Helper method to perform a comprehensive database scan."""
        with self._get_system_db_connection() as conn2, conn2.cursor() as cur2:
            cur2.execute("SELECT datname FROM pg_database WHERE datistemplate = false")
            all_visible = [r[0] for r in cur2.fetchall()]
        return self._filter_excluded_databases(all_visible)

    def scan_table(self, yugabyte_manager, table_info) -> bool:
        """
        Detect if the table in BigQuery is different from the schema in YugabyteDB.

        Args:
            yugabyte_manager: An instance of YugabyteDBManager for database operations.
            table_info: An object containing database, schema, and table information.

        Returns:
            bool: True if schemas differ, False otherwise.
        """
        yugabyte_schema = yugabyte_manager.get_table_schema(table_info)

        dataset_id = table_info.schema
        table_id = table_info.table
        bigquery_table = self.client.get_table(self.client.dataset(dataset_id).table(table_id))

        bigquery_schema = {field.name: field.field_type for field in bigquery_table.schema}
        yugabyte_schema_dict = {field.name: field.field_type for field in yugabyte_schema}

        return bigquery_schema != yugabyte_schema_dict

    def update_table_schema(self, yugabyte_manager, table_info, schema_changes):
        """
        Update the schema in BigQuery to match the schema in YugabyteDB safely.

        Args:
            yugabyte_manager: An instance of YugabyteDBManager for database operations.
            table_info: An object containing database, schema, and table information.
        """
        yugabyte_schema = yugabyte_manager.get_table_schema(table_info)

        dataset_id = table_info.schema
        table_id = table_info.table
        table_ref = self.client.dataset(dataset_id).table(table_id)
        bigquery_table = self.client.get_table(table_ref)

        # Update schema safely
        bigquery_table.schema = yugabyte_schema
        self.client.update_table(bigquery_table)

    def sync_table_data(self, yugabyte_manager, table_info):
        """
        Sync data from the YugabyteDB table to the BigQuery table.

        Args:
            yugabyte_manager: An instance of YugabyteDBManager for database operations.
            table_info: An object containing database, schema, and table information.
        """
        dataset_id = table_info.schema
        table_id = table_info.table

        query = f"""
        INSERT INTO `{dataset_id}.{table_id}`
        SELECT * FROM EXTERNAL_QUERY(
            "{self.config.get(ConfigKeys.YUGABYTEDB_EXTERNAL_CONNECTION.value)}",
            "SELECT * FROM {table_info.schema}.{table_info.table}"
        )
        """
        job = self.client.query(query)
        job.result()  # Wait for the job to complete