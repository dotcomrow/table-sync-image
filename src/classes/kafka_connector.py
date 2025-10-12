import requests
import re
import subprocess
import os
import json
import time
import structlog
from classes.bigquery_manager import BigQueryManager
from classes.config_reader import ConfigKeys,KafkaConnectKeys, LoggingKeys, YugabyteDBKeys
from classes.table_info import TableInfo
from classes.yugabyte_db_manager import YugabyteDBManager

class KafkaConnector:
    def __init__(self, config):
        self.config = config
        self.mock_enabled=self.config.get(ConfigKeys.KAFKA_CONNECT.value, {}).get(KafkaConnectKeys.MOCK.value, False)
        self.logger = self._init_logger()
        self.schema_registry_url = config.get(ConfigKeys.KAFKA_CONNECT.value, {}).get(KafkaConnectKeys.SCHEMA_REGISTRY_URL.value)
        db_cfg = config.get(ConfigKeys.YUGABYTEDB.value, {})
        self.host = db_cfg.get(YugabyteDBKeys.HOST.value, 'localhost')
        self.port = db_cfg.get(YugabyteDBKeys.PORT.value, 5433)
        self.user = db_cfg.get(YugabyteDBKeys.USER.value, 'yugabyte')
        self.password = db_cfg.get(YugabyteDBKeys.PASSWORD.value, 'yugabyte')
        self.database = db_cfg.get(YugabyteDBKeys.DATABASE.value, 'yugabyte')
        self.yugabyte_manager = YugabyteDBManager(config)
        self.bigquery_manager = BigQueryManager(config)
        self.db_master_addresses = db_cfg.get(YugabyteDBKeys.MASTER_ADDRESSES.value, None)
        self.kc_url = config.get(ConfigKeys.KAFKA_CONNECT.value, {}).get(KafkaConnectKeys.URL.value)
        
    def _init_logger(self) -> structlog.BoundLogger:
        import logging
        lvl = (self.config.get(ConfigKeys.LOGGING.value, {}) or {}).get(LoggingKeys.LEVEL.value, "INFO").upper()
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
    
    def delete_sink_cdc_connector(self, table_info: TableInfo):
        self.logger.info("Deleting sink CDC connector", table=table_info.full_name)
        sink_connector_name = f"bq-sink-{table_info.database}-{table_info.table}"
        kc = self.config.get(ConfigKeys.KAFKA_CONNECT.value, {}).get(KafkaConnectKeys.URL.value)
        if not kc:
            self.logger.error("Kafka Connect URL not configured")
            raise ValueError("Kafka Connect URL not configured")

        url = f"{kc}/connectors/{sink_connector_name}"
        self.logger.debug("Kafka Connect URL resolved for deletion", url=url)
        response = requests.delete(url)
        self.logger.debug("Kafka Connect response for deletion", status_code=response.status_code, response_text=response.text)
        if response.status_code not in (200, 204):
            self.logger.error("Failed to delete connector", response_text=response.text)
            raise RuntimeError(f"Failed to delete connector: {response.text}")

        self.logger.info("CDC connector deleted successfully", connector_name=sink_connector_name)

    def delete_source_cdc_connector(self, table_info: TableInfo):
        self.logger.info("Deleting source CDC connector", table=table_info.full_name)
        source_connector_name = f"yb-source-{table_info.database}-{table_info.schema}-{table_info.table}"
        kc = self.config.get(ConfigKeys.KAFKA_CONNECT.value, {}).get(KafkaConnectKeys.URL.value)
        if not kc:
            self.logger.error("Kafka Connect URL not configured")
            raise ValueError("Kafka Connect URL not configured")

        url = f"{kc}/connectors/{source_connector_name}"
        self.logger.debug("Kafka Connect URL resolved for deletion", url=url)
        response = requests.delete(url)
        self.logger.debug("Kafka Connect response for deletion", status_code=response.status_code, response_text=response.text)
        if response.status_code not in (200, 204):
            self.logger.error("Failed to delete connector", response_text=response.text)
            raise RuntimeError(f"Failed to delete connector: {response.text}")

        self.logger.info("CDC connector deleted successfully", connector_name=source_connector_name)

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

    def check_connector_exists(self, table_info: TableInfo) -> bool:
        source_connector_name = f"yb-source-{table_info.database}-{table_info.schema}-{table_info.table}"
        sink_connector_name = f"bq-sink-{table_info.database}-{table_info.table}"

        self.logger.info("Checking if Kafka connectors exist", source_connector_name=source_connector_name, sink_connector_name=sink_connector_name)
        kc = self.config.get(ConfigKeys.KAFKA_CONNECT.value, {}).get(KafkaConnectKeys.URL.value)
        if not kc:
            self.logger.error("Kafka Connect URL not configured")
            raise ValueError("Kafka Connect URL not configured")

        source_exists = False
        url = f"{kc}/connectors/{source_connector_name}/status"
        self.logger.debug("Kafka Connect status URL", url=url)
        try:
            response = requests.get(url, timeout=10)
            self.logger.debug("Kafka Connect source status response", status_code=response.status_code, response_text=response.text)
            source_exists = response.status_code == 200
            self.logger.info("Connector source existence check completed", exists=source_exists)
        except Exception as e:
            self.logger.error("Exception while checking connector existence", error=str(e))

        sink_exists = False
        url = f"{kc}/connectors/{sink_connector_name}/status"
        self.logger.debug("Kafka Connect status URL", url=url)
        try:
            response = requests.get(url, timeout=10)
            self.logger.debug("Kafka Connect sink status response", status_code=response.status_code, response_text=response.text)
            sink_exists = response.status_code == 200
            self.logger.info("Connector sink existence check completed", exists=sink_exists)
        except Exception as e:
            self.logger.error("Exception while checking connector existence", error=str(e))

        return {"source_exists": source_exists, "sink_exists": sink_exists}
        
    def reset_connectors(self, table_info: TableInfo):
        self.logger.info("Resetting Kafka connectors for table", table=table_info.full_name)
        status = self.check_connector_exists(table_info)
        if status.get("source_exists"):
            self.logger.info("Source connector exists, deleting", table=table_info.full_name)
            self.delete_source_cdc_connector(table_info)
        
        if status.get("sink_exists"):
            self.logger.info("Sink connector exists, deleting", table=table_info.full_name)
            self.delete_sink_cdc_connector(table_info)
        
    def setup_connectors(self, table_info: TableInfo):
        self.logger.info("Setting up Kafka connectors for table", table=table_info.full_name)
        self.create_source_connector(table_info)
        self.create_sink_connector(table_info)
        
        start_time = time.time()
        timeout = 3 * 60  # 3 minutes in seconds

        while time.time() - start_time < timeout:
            resp = self.bigquery_manager.check_table_exists(table_info.annotation.bq_dataset, table_info.annotation.bq_table)
            if resp:
                break
            time.sleep(10)  # Wait for 10 seconds before retrying
        else:
            # Handle timeout case
            raise TimeoutError("The table did not exist within the 3-minute timeout.")


    def _derive_topic_and_mappings(self, table_info: TableInfo):
        """
        Builds the Debezium topic name and BigQuery dataset/table mapping.
        If table_info carries the parsed comment, use it; otherwise fall back.
        """
        # Debezium/YB topic uses: {server.name}.{schema}.{table}
        server_name = f"yb_{table_info.database}_{table_info.schema}_{table_info.table}"
        topic = f"{server_name}.{table_info.schema}.{table_info.table}"

        # Parse dataset/table hint from COMMENT e.g. {"bootstrap":{"bq":"yugabyte_backup.testtable"}}
        # If you already have parsed fields on TableInfo, replace this with those.
        dataset = getattr(table_info.annotation, "bq_dataset", None)
        table   = getattr(table_info.annotation, "bq_table", None)
        if not (dataset and table):
            # default fallback: put into a catch-all dataset and keep table name
            dataset = getattr(self, "default_bq_dataset", "raw")
            table   = table_info.table

        return topic, dataset, table, server_name


    def create_source_connector(self, table_info: TableInfo):
        stream_id = self.get_cdc_stream_id(table_info)

        # Build topic + server name consistently, so sink can subscribe correctly
        topic, _, _, server_name = self._derive_topic_and_mappings(table_info)

        source_config = {
            "connector.class": "io.debezium.connector.yugabytedb.YugabyteDBgRPCConnector",
            "tasks.max": "1",

            "database.hostname": self.host,
            "database.port": str(self.port),
            "database.master.addresses": self.db_master_addresses,
            "database.user": self.user,
            "database.password": self.password,
            "database.dbname": table_info.database,
            "database.server.name": server_name,
            "database.streamid": stream_id,

            "table.include.list": f"{table_info.schema}.{table_info.table}",
            "snapshot.mode": "initial",
            "incremental.snapshot.enabled": "true",
            "signal.data.collection": f"{table_info.schema}.debezium_signal",
            "incremental.snapshot.chunk.size": "10000",   # optional

            # Use the YB unwrap SMT (fine with Avro)
            "transforms": "unwrap",
            "transforms.unwrap.type": "io.debezium.connector.yugabytedb.transforms.YBExtractNewRecordState",
            "transforms.unwrap.delete.handling.mode": "none",
            "column.exclude.list": f"{table_info.schema}.{table_info.table}.id",

            # Topic auto-creation hints (optional)
            "topic.creation.default.replication.factor": "1",
            "topic.creation.default.partitions": "1",
            "topic.creation.default.cleanup.policy": "delete",

            # >>> IMPORTANT: Avro + Schema Registry <<<
            "key.converter": "io.confluent.connect.avro.AvroConverter",
            "value.converter": "io.confluent.connect.avro.AvroConverter",
            "key.converter.schema.registry.url": self.schema_registry_url,
            "value.converter.schema.registry.url": self.schema_registry_url,
            # Let the source register schemas automatically
            "key.converter.auto.register.schemas": "true",
            "value.converter.auto.register.schemas": "true",
        }

        self.logger.debug("Source connector configuration", source_config=source_config)
        source_connector_name = f"yb-source-{table_info.database}-{table_info.schema}-{table_info.table}"
        response = self._send_connector_request(source_connector_name, source_config)
        self.logger.info("Source connector created", response=response)
        # Insert debezium signal record
        self.yugabyte_manager.insert_debezium_signal(table_info, stream_id)


    def create_sink_connector(self, table_info: TableInfo):
        # Load the project ID from the GCP key file (unchanged)
        keyfile_path = "/vault/secrets/gcp-key.json"
        try:
            with open(keyfile_path, "r") as keyfile:
                gcp_key_data = json.load(keyfile)
                bq_project = gcp_key_data.get("project_id")
                if not bq_project:
                    raise ValueError("project_id not found in GCP key file")
        except Exception as e:
            self.logger.error("Failed to load project_id from GCP key file", error=str(e))
            raise

        # Derive topic/dataset/table consistently with the source
        topic, dataset, table_name, _server_name = self._derive_topic_and_mappings(table_info)

        sink_config = {
            "connector.class": "com.wepay.kafka.connect.bigquery.BigQuerySinkConnector",
            "tasks.max": "1",

            # Topics & explicit mappings
            "topics": topic,
            "topic2TableMap": f"{topic}:{table_name}",
            "project": bq_project,
            # Optional fallback for topics without explicit dataset mapping
            "defaultDataset": dataset,
            # Per-topic dataset (from your COMMENT "bq": "dataset.table")
            "datasets": f"{topic}:{dataset}",

            # Auth
            "keySource": "FILE",
            "keyfile": keyfile_path,

            # Table creation / schema behavior
            "autoCreateTables": "true",
            "autoUpdateSchemas": "false",
            "allowNewBigQueryFields": "false",
            "sanitizeTopics": "false",
            "sanitizeFieldNames": "false",

            # Upsert/Delete (Debezium-friendly)
            "upsertEnabled": "true",
            "deleteEnabled": "true",
            # BigQuery sink needs the name of the Kafka KEY field to match on:
            "kafkaKeyFieldName": "id",
            # Note: primaryKeyMode is not used by this sink; matching is via the Kafka key

            # >>> IMPORTANT: Avro + Schema Registry to enable autoCreateTables <<<
            "key.converter": "io.confluent.connect.avro.AvroConverter",
            "value.converter": "io.confluent.connect.avro.AvroConverter",
            "key.converter.schema.registry.url": self.schema_registry_url,
            "value.converter.schema.registry.url": self.schema_registry_url,

            # Consumer start position for new sink
            "consumer.override.auto.offset.reset": "earliest",

            # Retries / merges
            "enableRetries": "true",
            "bigQueryRetry": "6",
            "bigQueryRetryWait": "2000",
            "mergeIntervalMs": "60000",
        }

        self.bigquery_manager.create_dataset(table_info)
        sink_connector_name = f"bq-sink-{table_info.database}-{table_info.table}"
        response = self._send_connector_request(sink_connector_name, sink_config)
        self.logger.info("Sink connector created", response=response)

    def _send_connector_request(self, name: str, config: dict):
        url = f"{self.kc_url}/connectors/{name}/config"
        payload = {"name": name, "config": config}
        # The Configs API expects just {"config": ...} for PUT to /config in some distros;
        # adjust if your worker expects raw 'config' (common). If 400, try sending just 'config'.
        import requests, json
        r = requests.put(url, headers={"Content-Type": "application/json"},
                        data=json.dumps(config))  # <-- many workers want just the config map
        if r.status_code >= 400:
            self.logger.error("Connector config rejected",
                            status=r.status_code, response=r.text, sent_config=config)
            raise RuntimeError(f"Kafka Connect error {r.status_code}: {r.text}")
        return r.json()
