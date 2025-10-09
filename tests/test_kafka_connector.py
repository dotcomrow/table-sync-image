import unittest
import os
from unittest.mock import patch
import sys
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

    def test_create_source_connector(self):
        """Test the create_source_connector method."""
        self.kafka_connector.create_source_connector(
            "testdb", "public", self.table_info
        )

    def test_create_sink_connector(self):
        """Test the create_sink_connector method."""
        self.kafka_connector.create_sink_connector(
            "testdb", "testtable", "test_topic"
        )

if __name__ == "__main__":
    
    unittest.main()
