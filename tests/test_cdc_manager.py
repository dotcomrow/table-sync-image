import unittest
from src.classes.cdc_manager import CDCManager

class TestCDCManager(unittest.TestCase):
    def setUp(self):
        self.manager = CDCManager(config={})

    def test_example(self):
        # Add specific tests for CDCManager
        self.assertTrue(True)

if __name__ == "__main__":
    unittest.main()