import unittest
import os
import time
import sys
import os
import unittest
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../sample')))
from table_sync_orchestrator import TableSyncOrchestrator, TableInfo

class TestOrchestratorEndToEnd(unittest.TestCase):
    def setUp(self):
        # Use orchestrator config with real cluster endpoints
        self.config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../sample/test_config.yaml'))
        self.orchestrator = TableSyncOrchestrator(self.config_path, start_servers=False)
        # TableInfo should match the real table in the cluster
        self.table_info = TableInfo(
            database=self.orchestrator.config['yugabytedb']['database'],
            schema="public",
            table="testtable",
            annotation=None
        )

    def test_full_sync_process(self):
        # This test requires all real resources to be available in the cluster
        # Run the full sync process using real endpoints
        self.orchestrator.start()
        status = self.orchestrator.status_table.get(self.table_info.full_name)
        self.assertIsNotNone(status, "No sync status found for table")
        self.assertTrue(status.connector_exists, "Connector was not created")
        self.assertTrue(status.topic_exists, "Topic was not created")
        self.assertTrue(status.bigquery_exists, "BigQuery table was not created")
        self.assertTrue(status.sync_active, "Sync is not active (connector not running)")

if __name__ == "__main__":
    unittest.main()
