import sys
import os
import unittest
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../sample')))
from classes.config_reader import ConfigReader, ConfigKeys, YugabyteDBKeys

class TestConfigReader(unittest.TestCase):
    def setUp(self):
        config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../sample/test_config.yaml"))
        self.reader = ConfigReader(config_path).load_config()

    def test_example(self):
        # Add specific tests for ConfigReader
        self.assertTrue(True)

    def test_is_enum_value(self):
        """Test the is_enum_value method."""
        # Valid enum value
        self.assertTrue(ConfigReader.is_enum_value("scan_interval_seconds"))

        # Invalid enum value
        self.assertFalse(ConfigReader.is_enum_value("non_existent_key"))

        # test nested keys
        self.assertTrue(self.reader.get(ConfigKeys.YUGABYTEDB.value).get(YugabyteDBKeys.HOST.value))

if __name__ == "__main__":
    unittest.main()