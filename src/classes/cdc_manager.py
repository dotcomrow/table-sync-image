import re
import subprocess
import os
import requests
from typing import List, Optional
from enum import Enum

import structlog
from classes.config_reader import ConfigKeys, KafkaConnectKeys, LoggingKeys

class CDCManager:
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
        return structlog.get_logger("cdc_manager")

    def get_cdc_stream_id(self, table_info):
        self.logger.info("Fetching CDC stream ID", table_info=table_info)
        if table_info.annotation and table_info.annotation.cdc_stream_id:
            self.logger.debug("CDC stream ID found in table annotation", cdc_stream_id=table_info.annotation.cdc_stream_id)
            return table_info.annotation.cdc_stream_id

        yb_cfg = self.config.get(ConfigKeys.YUGABYTEDB.value, {}) or {}
        if yb_cfg.get(ConfigKeys.CDC_STREAM_ID.value):
            self.logger.debug("CDC stream ID found in YugabyteDB config", cdc_stream_id=yb_cfg[ConfigKeys.CDC_STREAM_ID.value])
            return str(yb_cfg[ConfigKeys.CDC_STREAM_ID.value])

        master_addrs = (
            yb_cfg.get(ConfigKeys.YUGABYTEDB_MASTER_ADDRESSES.value)
            or yb_cfg.get("masters")
            or yb_cfg.get(ConfigKeys.DATABASE_MASTER_ADDRESSES.value)
            or os.getenv("YB_MASTER_ADDRESSES")
        )
        self.logger.debug("Master addresses resolved", master_addresses=master_addrs)

        if "allow_yb_admin" in yb_cfg:
            allow_admin = bool(yb_cfg.get(ConfigKeys.ALLOW_YB_ADMIN.value, True))
        else:
            allow_admin = bool(master_addrs)
        self.logger.debug("Allow yb-admin status", allow_admin=allow_admin)

        if not allow_admin:
            self.logger.warning("yb-admin disabled; no CDC stream ID provided", database=table_info.database)
            return None

        if not master_addrs:
            self.logger.error("yb-admin allowed but master_addresses not configured", database=table_info.database)
            return None

        yb_admin_bin = yb_cfg.get(ConfigKeys.YB_ADMIN_PATH.value, "yb-admin")
        namespace = f"ysql.{table_info.database}"
        self.logger.debug("yb-admin binary and namespace resolved", yb_admin_bin=yb_admin_bin, namespace=namespace)

        try:
            out = subprocess.check_output(
                [yb_admin_bin, "--master_addresses", master_addrs, "list_change_data_streams"],
                text=True, stderr=subprocess.STDOUT, timeout=20
            )
            self.logger.debug("yb-admin list_change_data_streams output", output=out)
            m = re.search(r"CDC Stream ID:\s*([0-9a-f]{32})", out, re.I)
            if m:
                sid = m.group(1)
                self.logger.info("Found existing CDC stream via yb-admin", database=table_info.database, stream_id=sid)
                return sid
        except Exception as e:
            self.logger.error("yb-admin list_change_data_streams failed", error=str(e))

        try:
            out = subprocess.check_output(
                [yb_admin_bin, "--master_addresses", master_addrs, "create_change_data_stream", namespace],
                text=True, stderr=subprocess.STDOUT, timeout=20
            )
            self.logger.debug("yb-admin create_change_data_stream output", output=out)
            m = re.search(r"CDC Stream ID:\s*([0-9a-f]{32})", out, re.I)
            if m:
                sid = m.group(1)
                self.logger.info("Created CDC DB stream via yb-admin", database=table_info.database, stream_id=sid)
                return sid
            self.logger.error("yb-admin create_change_data_stream returned no stream ID", output=out)
        except Exception as e:
            self.logger.error("yb-admin create_change_data_stream failed", error=str(e))

        return None

    def create_cdc_connector(self, table_info):
        self.logger.info("Creating CDC connector", table_info=table_info)
        kc = self.config.get(ConfigKeys.KAFKA_CONNECT_URL.value)
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
        kc = self.config.get(ConfigKeys.KAFKA_CONNECT_URL.value)
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

    def _kc_url(self) -> Optional[str]:
        self.logger.info("Resolving Kafka Connect URL")
        kc_url = (self.config.get(ConfigKeys.KAFKA_CONNECT.value) or {}).get(KafkaConnectKeys.URL.value)
        self.logger.debug("Kafka Connect URL resolved", kc_url=kc_url)
        return kc_url

    def _list_connector_plugins(self) -> List[str]:
        self.logger.info("Listing Kafka connector plugins")
        kc = self._kc_url()
        if not kc:
            self.logger.warning("Kafka Connect URL not configured")
            return []
        url = f"{kc}/connector-plugins"
        self.logger.debug("Kafka Connect plugins URL", url=url)
        try:
            r = requests.get(url, timeout=10)
            self.logger.debug("Kafka Connect plugins response", status_code=r.status_code, response_text=r.text)
            if r.status_code != 200:
                self.logger.error("Failed to list connector plugins", status_code=r.status_code)
                return []
            payload = r.json() if r.text else []
            classes = [str(item.get("class")) for item in payload or [] if item.get("class")]
            self.logger.info("Connector plugins listed successfully", plugins=classes)
            return classes
        except Exception as e:
            self.logger.error("Exception while listing connector plugins", error=str(e))
            return []

    def _select_yugabyte_connector_class(self) -> Optional[str]:
        self.logger.info("Selecting Yugabyte connector class")
        plugins = self._list_connector_plugins()
        grpc_cls = "io.debezium.connector.yugabytedb.YugabyteDBgRPCConnector"
        generic_cls = "io.debezium.connector.yugabytedb.YugabyteDBConnector"
        chosen = grpc_cls if grpc_cls in plugins else (generic_cls if generic_cls in plugins else None)
        self.logger.debug("Connector class selection", grpc_cls=grpc_cls, generic_cls=generic_cls, chosen=chosen)
        return chosen

    def _validate_connector_config(self, config: dict) -> bool:
        self.logger.info("Validating connector configuration", config=config)
        required = {
            "name", "connector.class", "tasks.max", "topic.prefix",
            "database.server.name", "database.user", "database.password",
        }
        missing = [opt for opt in required if opt not in config]
        if missing:
            self.logger.error("Missing required connector config options", missing_options=missing)
            return False
        self.logger.info("Connector configuration validated successfully")
        return True

    def _kc_create_connector(self, name: str, config: dict) -> bool:
        self.logger.info("Creating Kafka connector", name=name, config=config)
        kc = self._kc_url()
        if not kc:
            self.logger.error("Kafka Connect URL not configured")
            return False
        url = f"{kc}/connectors"
        self.logger.debug("Kafka Connect URL for creation", url=url)
        try:
            resp = requests.post(url, json={"name": name, "config": config}, timeout=10)
            self.logger.debug("Kafka Connect creation response", status_code=resp.status_code, response_text=resp.text)
            if resp.status_code == 201:
                self.logger.info("Kafka connector created successfully", name=name)
                return True
            self.logger.error("Failed to create Kafka connector", status_code=resp.status_code, response_text=resp.text)
            return False
        except Exception as e:
            self.logger.error("Exception during Kafka connector creation", error=str(e))
            return False

    def _kc_delete_connector(self, name: str) -> bool:
        self.logger.info("Deleting Kafka connector", name=name)
        kc = self._kc_url()
        if not kc:
            self.logger.error("Kafka Connect URL not configured")
            return False
        url = f"{kc}/connectors/{name}"
        self.logger.debug("Kafka Connect URL for deletion", url=url)
        try:
            resp = requests.delete(url, timeout=10)
            self.logger.debug("Kafka Connect deletion response", status_code=resp.status_code, response_text=resp.text)
            if resp.status_code == 204:
                self.logger.info("Kafka connector deleted successfully", name=name)
                return True
            self.logger.error("Failed to delete Kafka connector", status_code=resp.status_code, response_text=resp.text)
            return False
        except Exception as e:
            self.logger.error("Exception during Kafka connector deletion", error=str(e))
            return False

    def _kc_restart_connector(self, name: str) -> bool:
        kc = self._kc_url()
        if not kc:
            self.logger.error("Kafka Connect URL not configured")
            return False
        url = f"{kc}/connectors/{name}/restart"
        try:
            self.logger.info("Restarting connector", name=name)
            resp = requests.post(url, timeout=10)
            if resp.status_code == 204:
                self.logger.info("Connector restarted successfully", name=name)
                return True
            self._log_http_failure(method="POST", url=url, resp=resp, note="Restart connector failed")
            return False
        except Exception as e:
            self.logger.error("Exception during connector restart", name=name, error=str(e))
            return False

    def _kc_connector_status(self, name: str) -> Optional[dict]:
        kc = self._kc_url()
        if not kc:
            self.logger.error("Kafka Connect URL not configured")
            return None
        url = f"{kc}/connectors/{name}/status"
        try:
            resp = requests.get(url, timeout=10)
            if resp.status_code == 200:
                return resp.json()
            self._log_http_failure(method="GET", url=url, resp=resp, note="Get connector status failed")
            return None
        except Exception as e:
            self.logger.error("Exception while fetching connector status", name=name, error=str(e))
            return None

    def _kc_topic_exists(self, topic_name: str) -> bool:
        kc = self._kc_url()
        if not kc:
            self.logger.error("Kafka Connect URL not configured")
            return False
        url = f"{kc}/topics/{topic_name}"
        try:
            resp = requests.get(url, timeout=10)
            return resp.status_code == 200
        except Exception as e:
            self.logger.error("Exception while checking topic existence", topic_name=topic_name, error=str(e))
            return False