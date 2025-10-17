import sys
import os
import unittest
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
from classes.table_info import TableInfo
from classes.table_annotation import TableAnnotation

class TestTableInfo(unittest.TestCase):
    def setUp(self):
        config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../sample/test_config.yaml"))
        self.info = TableInfo(
            database="db",
            schema="schema",
            table="table",
            annotation=TableAnnotation.from_comment("{\"bootstrap\":{\"enabled\":true, \"bq\": \"yugabyte_backup.mcp_openapi_augmentations\"}}")
        )

    def test_example(self):
        # Add specific tests for TableInfo
        self.assertTrue(True)

    def test_bq_dataset(self):
        self.assertEqual(self.info.bq_dataset, "yugabyte_backup")

    def test_bq_table(self):
        self.assertEqual(self.info.bq_table, "mcp_openapi_augmentations")

    # def test_bq_properties_with_no_annotation(self):
    #     table_info = TableInfo(database="db", schema="schema", table="table", annotation=None)
    #     self.assertIsNone(table_info.bq_dataset)
    #     self.assertIsNone(table_info.bq_table)

if __name__ == "__main__":
    unittest.main()