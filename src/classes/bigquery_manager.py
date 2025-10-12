from google.cloud import bigquery
from google.oauth2 import service_account
from classes.config_reader import ConfigKeys, BigQueryKeys
from classes.table_info import TableInfo
from classes.logging import Logging
import logging

class BigQueryManager:
    def __init__(self, config):
        self.config = config
        self.logger = Logging(self.config)
        self.client = None  # Initialize client as None
        self.mock_enabled=self.config.get(ConfigKeys.BIGQUERY.value, {}).get(BigQueryKeys.MOCK.value, False)

    def _initialize_client(self):
        if self.client is None:
            if self.mock_enabled:
                self.logger.logMessage(logging.LogLevel.INFO, "Initializing Mock BigQuery client")
                from unittest.mock import MagicMock
                self.client = MagicMock()
            else:
                self.logger.logMessage(logging.LogLevel.INFO, "Initializing BigQuery client")
                credentials = service_account.Credentials.from_service_account_file("/vault/secrets/gcp-key.json")
                self.client = bigquery.Client(credentials=credentials)
    
    def create_dataset(self, table_info: TableInfo):
        self._initialize_client()
        dataset_id = table_info.bq_dataset
        self.logger.logMessage(logging.LogLevel.INFO, "Creating dataset in BigQuery", dataset_id=dataset_id)
        dataset_ref = self.client.dataset(dataset_id)
        try:
            self.client.get_dataset(dataset_ref)
            self.logger.logMessage(logging.LogLevel.INFO, "Dataset already exists", dataset_id=dataset_id)
        except Exception as e:
            self.logger.logMessage(logging.LogLevel.WARNING, "Dataset does not exist, creating it", dataset_id=dataset_id, error=str(e))
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = "US"  # Set location or make it configurable
            self.client.create_dataset(dataset)
            self.logger.logMessage(logging.LogLevel.INFO, "Dataset created successfully", dataset_id=dataset_id)

    def delete_table(self, table_info: TableInfo):
        self._initialize_client()
        self.logger.logMessage(logging.LogLevel.INFO, "Deleting table in BigQuery", dataset_id=table_info.bq_dataset, table_id=table_info.bq_table)
        table_ref = self.client.dataset(table_info.bq_dataset).table(table_info.bq_table)
        self.client.delete_table(table_ref)
        self.logger.logMessage(logging.LogLevel.INFO, "Table deleted successfully", dataset_id=table_info.bq_dataset, table_id=table_info.bq_table)

    def check_table_exists(self, dataset_id, table_id):
        self._initialize_client()
        self.logger.logMessage(logging.LogLevel.INFO, "Checking if table exists in BigQuery", dataset_id=dataset_id, table_id=table_id)
        try:
            resp = self.client.get_table(self.client.dataset(dataset_id).table(table_id))
            self.logger.logMessage(logging.LogLevel.INFO, "Table exists", dataset_id=dataset_id, table_id=table_id)
            return True
        except Exception as e:
            self.logger.logMessage(logging.LogLevel.WARNING, "Table does not exist", dataset_id=dataset_id, table_id=table_id, error=str(e))
            return False

    def fetch_bigquery_data(self, table_info: TableInfo):
        self.logger.logMessage(logging.LogLevel.INFO, "Fetch BigQuery data", dataset_id=table_info.bq_dataset, table_id=table_info.bq_table)
        self._initialize_client()
        query = f"SELECT * FROM `{table_info.bq_dataset}.{table_info.bq_table}`"
        self.logger.logMessage(logging.LogLevel.INFO, "Executing BigQuery", query=query)
        query_job = self.client.query(query)
        self.logger.logMessage(logging.LogLevel.INFO, "Query executed successfully", dataset_id=table_info.bq_dataset, table_id=table_info.bq_table, total_rows=query_job.result().total_rows)
        return [dict(row) for row in query_job]