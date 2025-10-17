import unittest
from services.bigquery_manager import BigQueryManager

class TestBigQueryManager(unittest.TestCase):
    def setUp(self):
        self.manager = BigQueryManager()

    def test_example(self):
        # Add specific tests for BigQueryManager
        self.assertTrue(True)

if __name__ == "__main__":
    unittest.main()