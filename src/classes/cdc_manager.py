import re
import subprocess
import os
import requests
from typing import List, Optional
from enum import Enum

class ConfigKeys(Enum):
    YB_ADMIN_PATH = "yb_admin_path"
    MASTER_ADDRESSES = "master_addresses"
    CDC_STREAM_ID = "cdc_stream_id"
    ALLOW_YB_ADMIN = "allow_yb_admin"
    KAFKA_CONNECT_URL = "kafka_connect_url"
    DATABASE_MASTER_ADDRESSES = "database.master.addresses"

class CDCManager:
    def __init__(self, config):
        self.config = config

    def get_cdc_stream_id(self, table_info):
        if table_info.annotation and table_info.annotation.cdc_stream_id:
            return table_info.annotation.cdc_stream_id

        yb_cfg = self.config.get("yugabytedb", {}) or {}
        if yb_cfg.get(ConfigKeys.CDC_STREAM_ID.value):
            return str(yb_cfg[ConfigKeys.CDC_STREAM_ID.value])

        master_addrs = (
            yb_cfg.get(ConfigKeys.MASTER_ADDRESSES.value)
            or yb_cfg.get("masters")
            or yb_cfg.get(ConfigKeys.DATABASE_MASTER_ADDRESSES.value)
            or os.getenv("YB_MASTER_ADDRESSES")
        )

        if "allow_yb_admin" in yb_cfg:
            allow_admin = bool(yb_cfg.get(ConfigKeys.ALLOW_YB_ADMIN.value))
        else:
            allow_admin = bool(master_addrs)

        if not allow_admin:
            self.logger.warning("yb-admin disabled; no CDC stream id provided", database=table_info.database)
            return None

        if not master_addrs:
            self.logger.error("yb-admin allowed but master_addresses not configured (config or YB_MASTER_ADDRESSES env)")
            return None

        yb_admin_bin = yb_cfg.get(ConfigKeys.YB_ADMIN_PATH.value, "yb-admin")
        namespace = f"ysql.{table_info.database}"

        try:
            out = subprocess.check_output(
                [yb_admin_bin, "--master_addresses", master_addrs, "list_change_data_streams"],
                text=True, stderr=subprocess.STDOUT, timeout=20
            )
            m = re.search(r"CDC Stream ID:\s*([0-9a-f]{32})", out, re.I)
            if m:
                sid = m.group(1)
                self.logger.info("Found existing CDC stream via yb-admin", database=table_info.database, stream_id=sid)
                return sid
        except Exception as e:
            self.logger.debug("yb-admin list_change_data_streams failed", error=str(e))

        try:
            out = subprocess.check_output(
                [yb_admin_bin, "--master_addresses", master_addrs, "create_change_data_stream", namespace],
                text=True, stderr=subprocess.STDOUT, timeout=20
            )
            m = re.search(r"CDC Stream ID:\s*([0-9a-f]{32})", out, re.I)
            if m:
                sid = m.group(1)
                self.logger.info("Created CDC DB stream via yb-admin", database=table_info.database, stream_id=sid)
                return sid
            self.logger.error("yb-admin create_change_data_stream returned no stream id", output=out)
        except Exception as e:
            self.logger.error("yb-admin create_change_data_stream failed", error=str(e))

        return None

    def create_cdc_connector(self, table_info):
        kc = self.config.get(ConfigKeys.KAFKA_CONNECT_URL.value)
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
        kc = self.config.get(ConfigKeys.KAFKA_CONNECT_URL.value)
        if not kc:
            raise ValueError("Kafka Connect URL not configured")

        url = f"{kc}/connectors/{connector_name}"
        response = requests.delete(url)
        if response.status_code not in (200, 204):
            raise RuntimeError(f"Failed to delete connector: {response.text}")

    def _kc_url(self) -> Optional[str]:
        return (self.config.get('kafka_connect') or {}).get('url')

    def _list_connector_plugins(self) -> List[str]:
        kc = self._kc_url()
        if not kc:
            return []
        url = f"{kc}/connector-plugins"
        try:
            r = requests.get(url, timeout=10)
            if r.status_code != 200:
                self._log_http_failure(method="GET", url=url, resp=r, note="List connector plugins failed")
                return []
            payload = r.json() if r.text else []
            classes = []
            for item in payload or []:
                c = item.get("class")
                if c:
                    classes.append(str(c))
            return classes
        except Exception as e:
            self._log_http_failure(method="GET", url=url, error=e, note="List connector plugins raised exception")
            return []

    def _select_yugabyte_connector_class(self) -> Optional[str]:
        """Choose the installed Yugabyte connector class. Prefer gRPC flavor if present."""
        plugins = self._list_connector_plugins()
        grpc_cls = "io.debezium.connector.yugabytedb.YugabyteDBgRPCConnector"
        generic_cls = "io.debezium.connector.yugabytedb.YugabyteDBConnector"
        chosen = grpc_cls if grpc_cls in plugins else (generic_cls if generic_cls in plugins else None)
        return chosen

    def _validate_connector_config(self, config: dict) -> bool:
        """Validate required connector config options."""
        required = {
            "name", "connector.class", "tasks.max", "topic.prefix",
            "database.server.name", "database.user", "database.password",
        }
        missing = [opt for opt in required if opt not in config]
        if missing:
            self.logger.error("Missing required connector config options", connector_config=config, missing_options=missing)
            return False
        return True

    def _kc_create_connector(self, name: str, config: dict) -> bool:
        kc = self._kc_url()
        if not kc:
            self.logger.error("Kafka Connect URL not configured")
            return False
        url = f"{kc}/connectors"
        try:
            self.logger.info("Creating connector", name=name, config=config)
            resp = requests.post(url, json={"name": name, "config": config}, timeout=10)
            if resp.status_code == 201:
                self.logger.info("Connector created successfully", name=name)
                return True
            self._log_http_failure(method="POST", url=url, req_json={"name": name, "config": config}, resp=resp, note="Create connector failed")
            return False
        except Exception as e:
            self.logger.error("Exception during connector creation", name=name, error=str(e))
            return False

    def _kc_delete_connector(self, name: str) -> bool:
        kc = self._kc_url()
        if not kc:
            self.logger.error("Kafka Connect URL not configured")
            return False
        url = f"{kc}/connectors/{name}"
        try:
            self.logger.info("Deleting connector", name=name)
            resp = requests.delete(url, timeout=10)
            if resp.status_code == 204:
                self.logger.info("Connector deleted successfully", name=name)
                return True
            self._log_http_failure(method="DELETE", url=url, resp=resp, note="Delete connector failed")
            return False
        except Exception as e:
            self.logger.error("Exception during connector deletion", name=name, error=str(e))
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