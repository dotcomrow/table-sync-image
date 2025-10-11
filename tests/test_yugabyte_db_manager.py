import sys
import os
import unittest
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../sample')))
from classes.config_reader import ConfigReader, ConfigKeys, YugabyteDBKeys
from classes.yugabyte_db_manager import YugabyteDBManager
from classes.table_info import TableInfo

class TestYugabyteDBManager(unittest.TestCase):
    def setUp(self):
        config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../sample/test_config.yaml"))
        self.config = ConfigReader(config_path).load_config()
        self.manager = YugabyteDBManager(self.config)

    def test_create_table(self):
        self.manager.create_table("test_table", "public")
        # Add assertions or mock checks here

    def test_delete_table(self):
        self.manager.delete_table("test_table", "public")
        # Add assertions or mock checks here

    def test_create_schema(self):
        self.manager.create_schema("test_schema")
        # Add assertions or mock checks here

    def test_delete_schema(self):
        self.manager.delete_schema("test_schema")
        # Add assertions or mock checks here

    def test_insert_debezium_signal(self):
        table_info = TableInfo(database="test_db", schema="public", table="test_table", annotation=None)
        self.manager.insert_debezium_signal(table_info)
        # Add assertions or mock checks here
        
    def test_discover_tables(self):
        tables = self.manager._discover_tables("test_db")
        self.assertIsInstance(tables, list)
        # Add more specific assertions based on expected tables
        
    def test_discover_databases(self):
        databases = self.manager._discover_databases()
        self.assertIsInstance(databases, list)
        # Add more specific assertions based on expected databases

if __name__ == "__main__":
    unittest.main()