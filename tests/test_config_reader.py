import unittest
from src.components.config_reader import ConfigReader

class TestConfigReader(unittest.TestCase):
    def setUp(self):
        self.reader = ConfigReader()

    def test_example(self):
        # Add specific tests for ConfigReader
        self.assertTrue(True)

if __name__ == "__main__":
    unittest.main()