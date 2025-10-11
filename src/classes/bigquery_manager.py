from google.cloud import bigquery
from google.oauth2 import service_account
import structlog
from classes.config_reader import ConfigKeys, LoggingKeys, BigQueryKeys
from classes.table_info import TableInfo

class BigQueryManager:
    def __init__(self, config):
        self.config = config
        self.logger = self._init_logger()
        self.client = None  # Initialize client as None
        self.mock_enabled=self.config.get(ConfigKeys.BIGQUERY.value, {}).get(BigQueryKeys.MOCK.value, False)

    def _initialize_client(self):
        if self.client is None:
            if self.mock_enabled:
                self.logger.info("Initializing Mock BigQuery client")
                from unittest.mock import MagicMock
                self.client = MagicMock()
            else:
                self.logger.info("Initializing BigQuery client")
                credentials = service_account.Credentials.from_service_account_file("/vault/secrets/gcp-key.json")
                self.client = bigquery.Client(credentials=credentials)

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
    
    def create_dataset(self, table_info: TableInfo):
        self._initialize_client()
        dataset_id = table_info.bq_dataset
        self.logger.info("Creating dataset in BigQuery", dataset_id=dataset_id)
        dataset_ref = self.client.dataset(dataset_id)
        try:
            self.client.get_dataset(dataset_ref)
            self.logger.info("Dataset already exists", dataset_id=dataset_id)
        except Exception as e:
            self.logger.warning("Dataset does not exist, creating it", dataset_id=dataset_id, error=str(e))
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"  # Set location or make it configurable
            self.client.create_dataset(dataset)
            self.logger.info("Dataset created successfully", dataset_id=dataset_id)

    def create_table(self, table_info, schema):
        self._initialize_client()
        self.logger.info("Creating table in BigQuery", table_info=table_info)
        dataset_id = table_info.bq_dataset
        table_id = table_info.bq_table
        if not dataset_id or not table_id:
            self.logger.error("Invalid dataset or table ID", dataset_id=dataset_id, table_id=table_id)
            raise ValueError("Invalid dataset or table ID")

        # Check if dataset exists
        dataset_ref = self.client.dataset(dataset_id)
        try:
            self.logger.debug("Checking if dataset exists", dataset_id=dataset_id)
            self.client.get_dataset(dataset_ref)
            self.logger.info("Dataset exists", dataset_id=dataset_id)
        except Exception as e:
            self.logger.warning("Dataset does not exist, creating it", dataset_id=dataset_id, error=str(e))
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"  # Set location or make it configurable
            self.logger.debug("Creating dataset", dataset_id=dataset_id, location=dataset.location)
            self.client.create_dataset(dataset)
            self.logger.info("Dataset created successfully", dataset_id=dataset_id)

        # Log schema conversion details
        self.logger.debug("Starting schema conversion", input_schema=schema)
        try:
            converted_schema = [
                bigquery.SchemaField(field.name, field.field_type, mode=field.mode)
                for field in schema
            ]
            self.logger.info("Schema conversion successful", converted_schema=converted_schema)
        except Exception as e:
            self.logger.error("Schema conversion failed", error=str(e))
            raise

        # Log table creation details
        table_ref = dataset_ref.table(table_id)
        self.logger.debug("Preparing to create table", table_ref=str(table_ref), schema=converted_schema)
        table = bigquery.Table(table_ref, schema=converted_schema)
        try:
            self.logger.debug("Sending request to create table", table_ref=str(table_ref))
            response = self.client.create_table(table)
            self.logger.info("Table created successfully", table=table_info, response=str(response))
        except Exception as e:
            self.logger.error("Failed to create table", table_ref=str(table_ref), error=str(e))
            raise

    def delete_table(self, table_info: TableInfo):
        self._initialize_client()
        self.logger.info("Deleting table in BigQuery", dataset_id=table_info.bq_dataset, table_id=table_info.bq_table)
        table_ref = self.client.dataset(table_info.bq_dataset).table(table_info.bq_table)
        self.client.delete_table(table_ref)
        self.logger.info("Table deleted successfully", dataset_id=table_info.bq_dataset, table_id=table_info.bq_table)

    def check_table_exists(self, dataset_id, table_id):
        self._initialize_client()
        self.logger.info("Checking if table exists in BigQuery", dataset_id=dataset_id, table_id=table_id)
        try:
            resp = self.client.get_table(self.client.dataset(dataset_id).table(table_id))
            self.logger.info("Table exists", dataset_id=dataset_id, table_id=table_id)
            return True
        except Exception as e:
            self.logger.warning("Table does not exist", dataset_id=dataset_id, table_id=table_id, error=str(e))
            return False