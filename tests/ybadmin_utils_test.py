import unittest
import os

from classes.config_reader import ConfigReader
from classes.logging import Logging
from classes.table_annotation import TableAnnotation
from classes.table_info import TableInfo
from classes.ybadmin_utils import YBAdminUtils

class TestYBAdminUtils(unittest.TestCase):
    def setUp(self):
        config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../sample/test_config.yaml"))
        self.config = ConfigReader(config_path).load_config()
        self.logger = Logging(self.config)
        self.yb_admin_utils = YBAdminUtils(self.config, self.logger)
        self.result = self.yb_admin_utils.create_stream("testdb")

    # def test_delete_stream(self):
    #     # Test deleting a stream
    #     self.yb_admin_utils.delete_stream("test_stream_id")

    def test_verify_table_covered_by_stream(self):
        # Test verifying table coverage
        usage_hints = TableInfo(
            database="testdb",
            schema="test_schema",
            table="mcp_openapi_usage_hints",
            annotation=TableAnnotation.from_comment('{"bootstrap":{"enabled":true, "bq": "mcp_test.mcp_openapi_usage_hints"}}')
        )
        usage_hint_result = self.yb_admin_utils.verify_table_covered_by_stream(self.result, usage_hints)
        
        augmentations = TableInfo(
            database="testdb",
            schema="test_schema",
            table="mcp_openapi_augmentations",
            annotation=TableAnnotation.from_comment('{"bootstrap":{"enabled":true, "bq": "mcp_test.mcp_openapi_augmentations"}}')
        )
        augmentation_result = self.yb_admin_utils.verify_table_covered_by_stream(self.result, augmentations)
        
        examples = TableInfo(
            database="testdb",
            schema="test_schema",
            table="mcp_openapi_examples",
            annotation=TableAnnotation.from_comment('{"bootstrap":{"enabled":true, "bq": "mcp_test.mcp_openapi_examples"}}')
        )
        examples_result = self.yb_admin_utils.verify_table_covered_by_stream(self.result, examples)
        
        param_hints = TableInfo(
            database="testdb",
            schema="test_schema",
            table="mcp_openapi_param_hints",
            annotation=TableAnnotation.from_comment('{"bootstrap":{"enabled":true, "bq": "mcp_test.mcp_openapi_param_hints"}}')
        )
        param_hints_result = self.yb_admin_utils.verify_table_covered_by_stream(self.result, param_hints)

        self.assertTrue(usage_hint_result)
        self.assertTrue(augmentation_result)
        self.assertTrue(examples_result)
        self.assertTrue(param_hints_result)
