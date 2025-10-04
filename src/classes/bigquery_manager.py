from google.cloud import bigquery
import os
import subprocess
import re
from typing import List

import structlog
from classes.config_reader import ConfigKeys, LoggingKeys

class BigQueryManager:
    def __init__(self, config):
        self.config = config
        self.logger = self._init_logger()
        self.client = None  # Initialize client as None

    def _initialize_client(self):
        if self.client is None:
            self.logger.info("Initializing BigQuery client")
            self.client = bigquery.Client()
            
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
        return structlog.get_logger("bigquery_manager")

    def create_table(self, table_info):
        self._initialize_client()
        self.logger.info("Creating table in BigQuery", table_info=table_info)
        dataset_id = table_info.bq_dataset
        table_id = table_info.bq_table
        if not dataset_id or not table_id:
            self.logger.error("Invalid dataset or table ID", dataset_id=dataset_id, table_id=table_id)
            raise ValueError("Invalid dataset or table ID")

        dataset_ref = self.client.dataset(dataset_id)
        table_ref = dataset_ref.table(table_id)

        schema = [
            bigquery.SchemaField("column_name", "STRING", mode="NULLABLE"),  # Replace with actual schema logic
        ]

        table = bigquery.Table(table_ref, schema=schema)
        self.client.create_table(table)
        self.logger.info("Table created successfully", table=table_info)

    def delete_table(self, dataset_id, table_id):
        self._initialize_client()
        self.logger.info("Deleting table in BigQuery", dataset_id=dataset_id, table_id=table_id)
        table_ref = self.client.dataset(dataset_id).table(table_id)
        self.client.delete_table(table_ref)
        self.logger.info("Table deleted successfully", dataset_id=dataset_id, table_id=table_id)

    def copy_initial_data(self, table_info):
        self._initialize_client()
        self.logger.info("Copying initial data to BigQuery table", table_info=table_info)
        query = f"""
        INSERT INTO `{table_info.bq_dataset}.{table_info.bq_table}`
        SELECT * FROM `{table_info.source_dataset}.{table_info.source_table}`
        """
        self.logger.debug("Executing query", query=query)
        job = self.client.query(query)
        job.result()  # Wait for the job to complete
        self.logger.info("Initial data copied successfully", table_info=table_info)

    def check_table_exists(self, dataset_id, table_id):
        self._initialize_client()
        self.logger.info("Checking if table exists in BigQuery", dataset_id=dataset_id, table_id=table_id)
        try:
            self.client.get_table(self.client.dataset(dataset_id).table(table_id))
            self.logger.info("Table exists", dataset_id=dataset_id, table_id=table_id)
            return True
        except Exception as e:
            self.logger.warning("Table does not exist", dataset_id=dataset_id, table_id=table_id, error=str(e))
            return False

    def get_table_schema(self, table_info):
        self._initialize_client()
        self.logger.info("Fetching table schema from YugabyteDB", table_info=table_info)
        master_addrs = (
            self.config.get(ConfigKeys.YUGABYTEDB_MASTER_ADDRESSES.value)
            or os.getenv("YB_MASTER_ADDRESSES")
        )
        if not master_addrs:
            self.logger.error("Master addresses not configured")
            raise ValueError("Master addresses not configured")

        yb_admin_bin = self.config.get(ConfigKeys.YUGABYTEDB_YB_ADMIN_PATH.value, "yb-admin")
        namespace = f"ysql.{table_info.database}"

        try:
            self.logger.debug("Running yb-admin command", command=[yb_admin_bin, "--master_addresses", master_addrs, "describe_table", namespace, table_info.table])
            out = subprocess.check_output(
                [yb_admin_bin, "--master_addresses", master_addrs, "describe_table", namespace, table_info.table],
                text=True, stderr=subprocess.STDOUT, timeout=20
            )
            self.logger.debug("yb-admin output", output=out)
            schema = []
            for line in out.splitlines():
                match = re.match(r"Column:\s+(\w+)\s+Type:\s+(\w+)", line)
                if match:
                    schema.append(bigquery.SchemaField(match.group(1), match.group(2).upper(), mode="NULLABLE"))
            self.logger.info("Schema fetched successfully", schema=schema)
            return schema
        except subprocess.CalledProcessError as e:
            self.logger.error("Failed to fetch table schema", error=str(e))
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
        self._initialize_client()
        self.logger.info("Scan table starting...", table=table_info.table)
        yugabyte_schema = yugabyte_manager.get_table_schema(table_info)

        dataset_id = table_info.schema
        table_id = table_info.table
        bigquery_table = self.client.get_table(self.client.dataset(dataset_id).table(table_id))

        bigquery_schema = {field.name: field.field_type for field in bigquery_table.schema}
        yugabyte_schema_dict = {field.name: field.field_type for field in yugabyte_schema}

        return bigquery_schema != yugabyte_schema_dict

    def update_table_schema(self, yugabyte_manager, table_info, schema_changes):
        self._initialize_client()
        yugabyte_schema = yugabyte_manager.get_table_schema(table_info)

        dataset_id = table_info.schema
        table_id = table_info.table
        table_ref = self.client.dataset(dataset_id).table(table_id)
        bigquery_table = self.client.get_table(table_ref)

        bigquery_table.schema = yugabyte_schema
        self.client.update_table(bigquery_table)

    def sync_table_data(self, yugabyte_manager, table_info):
        from concurrent.futures import ThreadPoolExecutor, as_completed

        self._initialize_client()
        dataset_id = table_info.schema
        table_id = table_info.table

        self.logger.info("Starting multithreaded data sync from YugabyteDB to BigQuery", table_info=table_info)

        # Step 1: Determine primary key ranges
        primary_key = table_info.primary_key  # Assume primary_key is provided in table_info
        yugabyte_query = f"SELECT MIN({primary_key}), MAX({primary_key}) FROM {table_info.schema}.{table_info.table}"

        try:
            with yugabyte_manager.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(yugabyte_query)
                    result = cursor.fetchone()

            if not result or result[0] is None or result[1] is None:
                self.logger.info("Table is empty, no data to sync", table_info=table_info)
                return

            min_pk, max_pk = result
            self.logger.info("Primary key range determined", min_pk=min_pk, max_pk=max_pk)

            # Step 2: Divide the range into chunks
            num_threads = 4  # Adjust based on system capabilities
            chunk_size = (max_pk - min_pk + 1) // num_threads
            ranges = [
                (start, min(start + chunk_size - 1, max_pk))
                for start in range(min_pk, max_pk + 1, chunk_size)
            ]

            self.logger.info("Primary key ranges for threads", ranges=ranges)

            # Step 3: Multithreaded data sync
            def process_chunk(pk_range):
                start_pk, end_pk = pk_range
                self.logger.debug("Processing chunk", start_pk=start_pk, end_pk=end_pk)

                query = f"""
                SELECT * FROM {table_info.schema}.{table_info.table}
                WHERE {primary_key} BETWEEN {start_pk} AND {end_pk}
                """

                with yugabyte_manager.get_connection() as conn:
                    with conn.cursor() as cursor:
                        cursor.execute(query)
                        rows = cursor.fetchall()

                        if not rows:
                            self.logger.info("No rows found for chunk", pk_range=pk_range)
                            return

                        rows_to_insert = [
                            {field.name: value for field, value in zip(cursor.description, row)}
                            for row in rows
                        ]

                        errors = self.client.insert_rows_json(
                            self.client.dataset(dataset_id).table(table_id), rows_to_insert
                        )

                        if errors:
                            self.logger.error("Failed to insert rows into BigQuery", errors=errors, pk_range=pk_range)
                            raise RuntimeError(f"Failed to insert rows for range {pk_range}: {errors}")

                        self.logger.info("Inserted chunk into BigQuery", chunk_size=len(rows), pk_range=pk_range)

            with ThreadPoolExecutor(max_workers=num_threads) as executor:
                futures = {executor.submit(process_chunk, pk_range): pk_range for pk_range in ranges}

                for future in as_completed(futures):
                    pk_range = futures[future]
                    try:
                        future.result()
                    except Exception as e:
                        self.logger.error("Error processing chunk", pk_range=pk_range, error=str(e))
                        raise

            self.logger.info("Multithreaded data sync completed successfully", table_info=table_info)

        except Exception as e:
            self.logger.error("Error during multithreaded data sync", error=str(e))
            raise

    def sync_table_data_via_gcs(self, yugabyte_manager, table_info):
        self._initialize_client()
        dataset_id = table_info.schema
        table_id = table_info.table

        self.logger.info("Starting data sync via GCS from YugabyteDB to BigQuery", table_info=table_info)

        # Step 1: Export data to a CSV file
        export_file = f"/tmp/{table_info.table}_data.csv"
        yugabyte_query = f"COPY (SELECT * FROM {table_info.schema}.{table_info.table}) TO STDOUT WITH CSV HEADER"

        try:
            with yugabyte_manager.get_connection() as conn:
                with open(export_file, 'w') as f:
                    with conn.cursor() as cursor:
                        cursor.copy_expert(yugabyte_query, f)

            self.logger.info("Data exported to file", file_path=export_file)

            # Step 2: Upload file to Google Cloud Storage
            bucket_name = self.config.get(ConfigKeys.GCS_BUCKET_NAME.value)
            gcs_path = f"{table_info.schema}/{table_info.table}_data.csv"

            from google.cloud import storage
            storage_client = storage.Client()
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(gcs_path)
            blob.upload_from_filename(export_file)

            self.logger.info("File uploaded to GCS", gcs_path=gcs_path)

            # Step 3: Load data into BigQuery
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,
                autodetect=True,
            )

            uri = f"gs://{bucket_name}/{gcs_path}"
            load_job = self.client.load_table_from_uri(
                uri, self.client.dataset(dataset_id).table(table_id), job_config=job_config
            )

            load_job.result()  # Wait for the job to complete
            self.logger.info("Data loaded into BigQuery from GCS", table_info=table_info)

        except Exception as e:
            self.logger.error("Error during GCS-based data sync", error=str(e))
            raise

        self.logger.info("Data sync via GCS completed successfully", table_info=table_info)