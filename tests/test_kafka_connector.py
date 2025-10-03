import unittest
import os
import time
from unittest.mock import patch
import sys
import subprocess
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.table_sync_orchestrator import TableSyncOrchestrator, TableInfo

KAFKA_CONNECT_URL = "http://kafka-connect.kafka.svc.internal.lan:8083"
KAFKA_NAMESPACE = "kafka"

def run_kubectl(cmd):
    full_cmd = ["tsh", "kubectl"] + cmd + ["-n", KAFKA_NAMESPACE]
    result = subprocess.run(full_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    return result.stdout.strip(), result.stderr.strip(), result.returncode

KAFKA_NAMESPACE = os.getenv("KAFKA_NAMESPACE", "kafka")
KAFKA_CONNECT_URL = os.getenv("KAFKA_CONNECT_URL", "http://kafka-connect.kafka.svc.cluster.local:8083")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka.kafka.svc.cluster.local:9092")

class TestKafkaConnectorCreation(unittest.TestCase):
    def setUp(self):
        self.config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../config/orchestrator_test.yaml'))
        self.orchestrator = TableSyncOrchestrator(self.config_path, start_servers=False)
        self.table_info = TableInfo(
            database="testdb",
            schema="public",
            table="testtable",
            annotation=None
        )

    def test_connector_creation(self):
        # Remove connector if exists
        connector_name = self.orchestrator._connector_name(self.table_info)
        out, err, code = run_kubectl(["delete", "connector", connector_name, "--ignore-not-found=true"])
        time.sleep(2)
        # Create connector
        result = self.orchestrator._create_cdc_connector(self.table_info)
        self.assertTrue(result, f"Connector creation failed: {err}")
        # Check connector status
        status = self.orchestrator._connector_status(connector_name)
        self.assertIsNotNone(status, "Connector status not found")

    def test_topic_creation(self):
        topic = self.orchestrator._expected_topic_name(self.table_info)
        # Remove topic if exists
        out, err, code = run_kubectl(["exec", "kafka-0", "--", "kafka-topics.sh", "--delete", "--topic", topic, "--bootstrap-server", KAFKA_BOOTSTRAP])
        time.sleep(2)
        # Create connector (should create topic)
        self.orchestrator._create_cdc_connector(self.table_info)
        time.sleep(5)
        # Check topic existence
        exists = self.orchestrator._check_topic_exists(topic)
        self.assertTrue(exists, f"Topic {topic} was not created by connector")

if __name__ == "__main__":
    unittest.main()
