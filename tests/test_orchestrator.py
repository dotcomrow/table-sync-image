import sys
import os
import traceback
import unittest
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../src')))
from unittest.mock import MagicMock, patch
from concurrent.futures import ThreadPoolExecutor
from table_sync_orchestrator import TableSyncOrchestrator
from classes.sync_status import SyncStatus
from classes.table_info import TableInfo

class TestTableSyncOrchestrator(unittest.TestCase):

    def setUp(self):
        # Mock configuration and dependencies
        self.orchestrator = TableSyncOrchestrator(os.path.abspath(os.path.join(os.path.dirname(__file__), "../sample/test_config.yaml")))
        self.orchestrator.status_table = {}
        self.mock_logger = MagicMock()
        self.orchestrator.logger = self.mock_logger  # Assign the mock logger

        # Create mock SyncStatus objects
        self.sync_status_1 = SyncStatus(
            table_info=TableInfo(database="db1", schema="schema1", table="table1", annotation="annot1"),
            last_scan=None,
            annotation_enabled=False,
            bigquery_exists=True,
            connector_exists=True,
            sync_active=False
        )

        self.sync_status_2 = SyncStatus(
            table_info=TableInfo(database="db2", schema="schema2", table="table2", annotation="annot2"),
            last_scan=None,
            annotation_enabled=False,
            bigquery_exists=True,
            connector_exists=True,
            sync_active=False
        )

        self.orchestrator.status_table = {
            "db1.schema1.table1": self.sync_status_1,
            "db2.schema2.table2": self.sync_status_2
        }

    @patch("src.table_sync_orchestrator.ThreadPoolExecutor")
    def test_table_sync_loop(self, MockThreadPoolExecutor):
        # Mock the ThreadPoolExecutor and its behavior
        mock_executor = MockThreadPoolExecutor.return_value
        mock_future = MagicMock()
        mock_executor.submit.side_effect = [mock_future, mock_future]
        mock_future.result.side_effect = [None, Exception("Test Exception")]

        # Run the code block
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(self.orchestrator._table_sync_loop, ti): ti for ti in self.orchestrator.status_table.values()}

            for future in futures:
                ti = futures[future]
                try:
                    future.result()
                except Exception as e:
                    traceback.print_exc()
                    self.orchestrator.logger.error("Error in table sync loop", table=ti.table_info.table, error=str(e))

        # Assertions
        self.orchestrator.logger.error.assert_called_with("Error in table sync loop", table="table2", error="Test Exception")

if __name__ == "__main__":
    unittest.main()