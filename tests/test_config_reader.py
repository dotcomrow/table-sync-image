import sys
import os
import unittest
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
from classes.config_reader import ConfigReader

class TestConfigReader(unittest.TestCase):
    def setUp(self):
        self.reader = ConfigReader("../sample/test_config.yaml")

    def test_example(self):
        # Add specific tests for ConfigReader
        self.assertTrue(True)

    def test_is_enum_value(self):
        """Test the is_enum_value method."""
        # Valid enum value
        self.assertTrue(self.reader.is_enum_value("scan_interval_seconds"))

        # Invalid enum value
        self.assertFalse(self.reader.is_enum_value("non_existent_key"))

if __name__ == "__main__":
    unittest.main()