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
        
        self.table_info = TableInfo(
            database="testdb",
            schema="public",
            table="testtable",
            annotation=TableAnnotation.from_comment('{"bootstrap":{"enabled":true, "bq": "yugabyte_backup.testtable"}}')
        )

    def test_basic_setup(self):
        """Test BigQuery table creation."""
        src = self.kafka_connector.create_source_connector(self.table_info)
        sink = self.kafka_connector.create_sink_connector(self.table_info)
        resp = self.bigquery_manager.check_table_exists("yugabyte_backup", "testtable")
        self.assertTrue(resp)

if __name__ == "__main__":
    unittest.main()
