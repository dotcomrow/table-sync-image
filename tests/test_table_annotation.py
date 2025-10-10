import unittest
import json
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../sample')))
from classes.table_annotation import TableAnnotation
from classes.config_reader import ConfigReader, BigQueryKeys

class TestTableAnnotation(unittest.TestCase):
    def setUp(self):
        config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../sample/test_config.yaml"))
        self.config = ConfigReader(config_path).load_config()

    def test_example(self):
        # Add specific tests for TableAnnotation
        self.assertTrue(True)

    def test_from_comment(self):
        comment = json.dumps({
            "bootstrap": {
                "enabled": True,
                "bq": "test_dataset.test_table",
                "cdc_stream_id": "stream123"
            }
        })

        annotation = TableAnnotation.from_comment(self.config, comment)

        self.assertIsNotNone(annotation)
        self.assertTrue(annotation.enabled)
        self.assertEqual(annotation.bq_dataset, "test_dataset")
        self.assertEqual(annotation.bq_table, "test_table")
        self.assertEqual(annotation.cdc_stream_id, "stream123")
        self.assertEqual(annotation.default_backup_dataset, "yugabyte_backup")

if __name__ == "__main__":
    unittest.main()