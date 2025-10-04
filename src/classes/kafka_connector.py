import requests
import re
import subprocess
import os
from typing import Optional

import structlog
from classes.config_reader import ConfigKeys,KafkaConnectKeys, LoggingKeys, YugabyteDBKeys

class KafkaConnector:
    def __init__(self, config):
        self.config = config
        self.mock_enabled=self.config.get(ConfigKeys.KAFKA_CONNECT.value, {}).get(KafkaConnectKeys.MOCK.value, False)
        self.logger = self._init_logger()
        
    def _init_logger(self) -> structlog.BoundLogger:
        import logging
        lvl = (self.config.get(ConfigKeys.LOGGING.value, {}) or {}).get(ConfigKeys.LOGGING.value, {}).get(LoggingKeys.LEVEL.value, "INFO").upper()
        numeric = getattr(logging, lvl, logging.INFO)
        structlog.configure(
            processors=[
                structlog.processors.TimeStamper(fmt="iso"),
                structlog.processors.add_log_level,
                structlog.processors.JSONRenderer()
            ],
            wrapper_class=structlog.make_filtering_bound_logger(numeric),
            logger_factory=structlog.PrintLoggerFactory(),
            cache_logger_on_first_use=True,
        )
        return structlog.get_logger("kafka_connector")

    def create_cdc_connector(self, table_info):
        self.logger.info("Creating CDC connector", table_info=table_info)
        kc = self.config.get(ConfigKeys.KAFKA_CONNECT.value, {}).get(KafkaConnectKeys.URL.value)
        if not kc:
            self.logger.error("Kafka Connect URL not configured")
            raise ValueError("Kafka Connect URL not configured")

        connector_class = "io.debezium.connector.yugabytedb.YugabyteDBConnector"
        name = f"debezium_yb_{table_info.database}_{table_info.schema}_{table_info.table}".replace('.', '_').replace('-', '_')
        self.logger.debug("Connector name and class resolved", name=name, connector_class=connector_class)

        stream_id = self.get_cdc_stream_id(table_info)
        self.logger.debug("Stream ID resolved for connector", stream_id=stream_id)

        config_payload = {
            "name": name,
            "connector.class": connector_class,
            "tasks.max": "1",
            "database.streamid": stream_id,
            "database.dbname": table_info.database,
            "table.include.list": f"{table_info.schema}.{table_info.table}",
        }
        self.logger.debug("Connector configuration payload", config_payload=config_payload)

        url = f"{kc}/connectors"
        self.logger.debug("Kafka Connect URL resolved", url=url)
        response = requests.post(url, json=config_payload)
        self.logger.debug("Kafka Connect response", status_code=response.status_code, response_text=response.text)
        if response.status_code not in (200, 201):
            self.logger.error("Failed to create connector", response_text=response.text)
            raise RuntimeError(f"Failed to create connector: {response.text}")

        self.logger.info("CDC connector created successfully", name=name)

    def delete_cdc_connector(self, connector_name):
        self.logger.info("Deleting CDC connector", connector_name=connector_name)
        kc = self.config.get(ConfigKeys.KAFKA_CONNECT.value, {}).get(KafkaConnectKeys.URL.value)
        if not kc:
            self.logger.error("Kafka Connect URL not configured")
            raise ValueError("Kafka Connect URL not configured")

        url = f"{kc}/connectors/{connector_name}"
        self.logger.debug("Kafka Connect URL resolved for deletion", url=url)
        response = requests.delete(url)
        self.logger.debug("Kafka Connect response for deletion", status_code=response.status_code, response_text=response.text)
        if response.status_code not in (200, 204):
            self.logger.error("Failed to delete connector", response_text=response.text)
            raise RuntimeError(f"Failed to delete connector: {response.text}")

        self.logger.info("CDC connector deleted successfully", connector_name=connector_name)

    def get_cdc_stream_id(self, table_info):
        self.logger.info("Fetching CDC stream ID", table_info=table_info)
        master_addrs = (
            self.config.get(ConfigKeys.YUGABYTEDB.value, {}).get(YugabyteDBKeys.MASTER_ADDRESSES.value)
            or os.getenv("YB_MASTER_ADDRESSES")
        )
        self.logger.debug("Master addresses resolved", master_addresses=master_addrs)
        if not master_addrs:
            self.logger.error("Master addresses not configured")
            raise ValueError("Master addresses not configured")

        yb_admin_bin = self.config.get(ConfigKeys.YUGABYTEDB.value, {}).get(YugabyteDBKeys.YB_ADMIN_PATH.value, "yb-admin")
        namespace = f"ysql.{table_info.database}"
        self.logger.debug("yb-admin binary and namespace resolved", yb_admin_bin=yb_admin_bin, namespace=namespace)

        try:
            out = subprocess.check_output(
                [yb_admin_bin, "--master_addresses", master_addrs, "list_change_data_streams"],
                text=True, stderr=subprocess.STDOUT, timeout=20
            )
            self.logger.debug("yb-admin list_change_data_streams output", output=out)
            match = re.search(r"CDC Stream ID:\s*([0-9a-f]{32})", out, re.I)
            if match:
                stream_id = match.group(1)
                self.logger.info("Found CDC stream ID", stream_id=stream_id)
                return stream_id
        except subprocess.CalledProcessError as e:
            self.logger.error("Failed to list CDC streams", error=str(e))

        try:
            out = subprocess.check_output(
                [yb_admin_bin, "--master_addresses", master_addrs, "create_change_data_stream", namespace],
                text=True, stderr=subprocess.STDOUT, timeout=20
            )
            self.logger.debug("yb-admin create_change_data_stream output", output=out)
            match = re.search(r"CDC Stream ID:\s*([0-9a-f]{32})", out, re.I)
            if match:
                stream_id = match.group(1)
                self.logger.info("Created CDC stream ID", stream_id=stream_id)
                return stream_id
        except subprocess.CalledProcessError as e:
            self.logger.error("Failed to create CDC stream", error=str(e))

        self.logger.warning("No CDC stream ID found or created")
        return None

    def check_connector_exists(self, connector_name: str) -> bool:
        self.logger.info("Checking if Kafka connector exists", connector_name=connector_name)
        kc = self.config.get(ConfigKeys.KAFKA_CONNECT.value, {}).get(KafkaConnectKeys.URL.value)
        if not kc:
            self.logger.error("Kafka Connect URL not configured")
            raise ValueError("Kafka Connect URL not configured")

        url = f"{kc}/connectors/{connector_name}/status"
        self.logger.debug("Kafka Connect status URL", url=url)
        try:
            response = requests.get(url, timeout=10)
            self.logger.debug("Kafka Connect status response", status_code=response.status_code, response_text=response.text)
            exists = response.status_code == 200
            self.logger.info("Connector existence check completed", exists=exists)
            return exists
        except Exception as e:
            self.logger.error("Exception while checking connector existence", error=str(e))
            return False

    def _kc_restart_connector(self, name: str) -> bool:
        self.logger.info("Restarting Kafka connector", name=name)
        kc = self._kc_url()
        if not kc:
            self.logger.error("Kafka Connect URL not configured")
            return False
        url = f"{kc}/connectors/{name}/restart"
        self.logger.debug("Kafka Connect restart URL", url=url)
        try:
            resp = requests.post(url, timeout=10)
            self.logger.debug("Kafka Connect restart response", status_code=resp.status_code, response_text=resp.text)
            if resp.status_code == 204:
                self.logger.info("Kafka connector restarted successfully", name=name)
                return True
            self.logger.error("Failed to restart Kafka connector", status_code=resp.status_code, response_text=resp.text)
            return False
        except Exception as e:
            self.logger.error("Exception during Kafka connector restart", error=str(e))
            return False

    def _kc_connector_status(self, name: str) -> Optional[dict]:
        self.logger.info("Fetching Kafka connector status", name=name)
        kc = self._kc_url()
        if not kc:
            self.logger.error("Kafka Connect URL not configured")
            return None
        url = f"{kc}/connectors/{name}/status"
        self.logger.debug("Kafka Connect status URL", url=url)
        try:
            resp = requests.get(url, timeout=10)
            self.logger.debug("Kafka Connect status response", status_code=resp.status_code, response_text=resp.text)
            if resp.status_code == 200:
                status = resp.json()
                self.logger.info("Kafka connector status fetched successfully", status=status)
                return status
            self.logger.error("Failed to fetch Kafka connector status", status_code=resp.status_code, response_text=resp.text)
            return None
        except Exception as e:
            self.logger.error("Exception while fetching Kafka connector status", error=str(e))
            return None