import unittest
import os
import time
from unittest.mock import patch
import sys
import subprocess
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from classes.bigquery_manager import BigQueryManager
from classes.yugabyte_db_manager import YugabyteDBManager
from classes.config_reader import ConfigReader
from classes.kafka_connector import KafkaConnector
from classes.table_info import TableInfo
from classes.table_annotation import TableAnnotation

class TestKafkaConnector(unittest.TestCase):
    def setUp(self):
        config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../sample/test_config.yaml"))
        self.config = ConfigReader(config_path).load_config()
        self.yugabyte_manager = YugabyteDBManager(self.config)
        self.kafka_connector = KafkaConnector(self.config)
        self.bigquery_manager = BigQueryManager(self.config)
        
        self.table_info = TableInfo(
            database="testdb",
            schema="public",
            table="testtable",
            annotation=TableAnnotation.from_comment(self.config, '{"bootstrap":{"enabled":true, "bq": "yugabyte_backup.testtable"}}')
        )
        

    def test_create_cdc_connector(self):
        """Test the create_cdc_connector method."""
        self.kafka_connector.create_cdc_connector(self.table_info)

    def test_delete_cdc_connector(self):
        """Test the delete_cdc_connector method."""
        connector_name = "test_connector"
        self.kafka_connector.delete_cdc_connector(connector_name)

    def test_get_cdc_stream_id(self):
        """Test the get_cdc_stream_id method."""
        stream_id = self.kafka_connector.get_cdc_stream_id(self.table_info)

    def test_check_connector_exists(self):
        """Test the check_connector_exists method."""
        connector_name = "test_connector"
        exists = self.kafka_connector.check_connector_exists(connector_name)

    def test_create_source_connector(self):
        """Test the create_source_connector method."""
        self.kafka_connector.create_source_connector(
            "testdb", "public", "testtable", "test_stream_id", "localhost", 5433, "user", "password", "http://localhost:8083"
        )

    def test_create_sink_connector(self):
        """Test the create_sink_connector method."""
        self.kafka_connector.create_sink_connector(
            "testdb", "testtable", "test_topic", "test_dataset", "test_project", "test_default_dataset", "http://localhost:8083"
        )

if __name__ == "__main__":
    
    unittest.main()
