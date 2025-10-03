import requests
import re
import subprocess
import os
from classes.config_reader import ConfigKeys

class KafkaConnector:
    def __init__(self, config):
        self.config = config

    def create_cdc_connector(self, table_info):
        kc = self.config.get(ConfigKeys.KAFKA_CONNECT.value, {}).get('url')
        if not kc:
            raise ValueError("Kafka Connect URL not configured")

        connector_class = "io.debezium.connector.yugabytedb.YugabyteDBConnector"
        name = f"debezium_yb_{table_info.database}_{table_info.schema}_{table_info.table}".replace('.', '_').replace('-', '_')

        stream_id = self.get_cdc_stream_id(table_info)

        config_payload = {
            "name": name,
            "connector.class": connector_class,
            "tasks.max": "1",
            "database.streamid": stream_id,
            "database.dbname": table_info.database,
            "table.include.list": f"{table_info.schema}.{table_info.table}",
        }

        url = f"{kc}/connectors"
        response = requests.post(url, json=config_payload)
        if response.status_code not in (200, 201):
            raise RuntimeError(f"Failed to create connector: {response.text}")

    def delete_cdc_connector(self, connector_name):
        kc = self.config.get(ConfigKeys.KAFKA_CONNECT.value, {}).get('url')
        if not kc:
            raise ValueError("Kafka Connect URL not configured")

        url = f"{kc}/connectors/{connector_name}"
        response = requests.delete(url)
        if response.status_code not in (200, 204):
            raise RuntimeError(f"Failed to delete connector: {response.text}")

    def get_cdc_stream_id(self, table_info):
        master_addrs = (
            self.config.get(ConfigKeys.YUGABYTEDB.value, {}).get("master_addresses")
            or os.getenv("YB_MASTER_ADDRESSES")
        )
        if not master_addrs:
            raise ValueError("Master addresses not configured")

        yb_admin_bin = self.config.get(ConfigKeys.YUGABYTEDB.value, {}).get("yb_admin_path", "yb-admin")
        namespace = f"ysql.{table_info.database}"

        try:
            out = subprocess.check_output(
                [yb_admin_bin, "--master_addresses", master_addrs, "list_change_data_streams"],
                text=True, stderr=subprocess.STDOUT, timeout=20
            )
            match = re.search(r"CDC Stream ID:\s*([0-9a-f]{32})", out, re.I)
            if match:
                return match.group(1)
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"Failed to list CDC streams: {e}")

        try:
            out = subprocess.check_output(
                [yb_admin_bin, "--master_addresses", master_addrs, "create_change_data_stream", namespace],
                text=True, stderr=subprocess.STDOUT, timeout=20
            )
            match = re.search(r"CDC Stream ID:\s*([0-9a-f]{32})", out, re.I)
            if match:
                return match.group(1)
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"Failed to create CDC stream: {e}")

        return None

    def check_connector_exists(self, connector_name: str) -> bool:
        """
        Check if a Kafka connector exists by querying its status endpoint.

        Args:
            connector_name (str): The name of the connector to check.

        Returns:
            bool: True if the connector exists, False otherwise.
        """
        kc = self.config.get(ConfigKeys.KAFKA_CONNECT.value, {}).get('url')
        if not kc:
            raise ValueError("Kafka Connect URL not configured")

        url = f"{kc}/connectors/{connector_name}/status"
        try:
            response = requests.get(url, timeout=10)
            return response.status_code == 200
        except Exception as e:
            self.logger.error("Exception while checking connector existence", connector_name=connector_name, error=str(e))
            return False