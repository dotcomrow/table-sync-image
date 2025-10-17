import unittest
import os
from unittest.mock import patch
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))

from services.bigquery_manager import BigQueryManager
from services.yugabyte_db_manager import YugabyteDBManager
from classes.config_reader import ConfigReader
from services.kafka_connector import KafkaConnector
from classes.table_info import TableInfo
from classes.table_annotation import TableAnnotation
from classes.logging import Logging
from classes.ybadmin_utils import YBAdminUtils

class TestKafkaConnector(unittest.TestCase):
    def setUp(self):
        config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../sample/test_config.yaml"))
        self.config = ConfigReader(config_path).load_config()
        self.logger = Logging(self.config)
        self.yugabyte_manager = YugabyteDBManager(self.config, self.logger)
        self.kafka_connector = KafkaConnector(self.config, self.logger)
        self.bigquery_manager = BigQueryManager(self.config, self.logger)
        self.yb_admin_utils = YBAdminUtils(self.config, self.logger)
        stream_id = self.yb_admin_utils.create_stream("testdb")
        self.yugabyte_manager.insert_into_stream_table(stream_id, "testdb")

        self.table_info = TableInfo(
            database="testdb",
            schema="test_schema",
            table="mcp_openapi_usage_hints",
            annotation=TableAnnotation.from_comment('{"bootstrap":{"enabled":true, "bq": "mcp_test.mcp_openapi_usage_hints"}}')
        )

    def test_basic_setup(self):
        """Test BigQuery table creation."""
        self.kafka_connector.create_source_connector(self.table_info)
        self.kafka_connector.create_sink_connector(self.table_info)
        resp = self.bigquery_manager.check_table_exists("mcp_test", "mcp_openapi_usage_hints")
        self.assertTrue(resp)

if __name__ == "__main__":
    unittest.main()
