import requests
import re
import subprocess
import os
import json
import time
from classes.bigquery_manager import BigQueryManager
from classes.config_reader import ConfigKeys,KafkaConnectKeys, YugabyteDBKeys
from classes.table_info import TableInfo
from classes.yugabyte_db_manager import YugabyteDBManager
from classes.logging import Logging

class KafkaConnector:
    source_connector_name_format = "yb-source-{database}-{schema}-{table_name}"
    sink_connector_name_format = "bq-sink-{database}-{table_name}"
    
    def __init__(self, config, logging: Logging):
        self.config = config
        self.mock_enabled=self.config.get(ConfigKeys.KAFKA_CONNECT.value, {}).get(KafkaConnectKeys.MOCK.value, False)
        self.logger = logging
        self.schema_registry_url = config.get(ConfigKeys.KAFKA_CONNECT.value, {}).get(KafkaConnectKeys.SCHEMA_REGISTRY_URL.value)
        db_cfg = config.get(ConfigKeys.YUGABYTEDB.value, {})
        self.host = db_cfg.get(YugabyteDBKeys.HOST.value, 'localhost')
        self.port = db_cfg.get(YugabyteDBKeys.PORT.value, 5433)
        self.user = db_cfg.get(YugabyteDBKeys.USER.value, 'yugabyte')
        self.password = db_cfg.get(YugabyteDBKeys.PASSWORD.value, 'yugabyte')
        self.yugabyte_manager = YugabyteDBManager(config, logging)
        self.bigquery_manager = BigQueryManager(config, logging)
        self.db_master_addresses = db_cfg.get(YugabyteDBKeys.MASTER_ADDRESSES.value, None)
        self.kc_url = config.get(ConfigKeys.KAFKA_CONNECT.value, {}).get(KafkaConnectKeys.URL.value)
    
    def delete_sink_cdc_connector(self, table_info: TableInfo):
        self.logger.logMessage(Logging.LogLevel.DEBUG, "Deleting sink CDC connector", table=table_info.to_dict())
        sink_connector_name = self.sink_connector_name_format.format(
            database=table_info.database,
            table_name=table_info.table
        )
        kc = self.config.get(ConfigKeys.KAFKA_CONNECT.value, {}).get(KafkaConnectKeys.URL.value)
        if not kc:
            self.logger.logMessage(Logging.LogLevel.ERROR, "Kafka Connect URL not configured", table=table_info.to_dict())
            raise ValueError("Kafka Connect URL not configured")

        url = f"{kc}/connectors/{sink_connector_name}"
        self.logger.logMessage(Logging.LogLevel.DEBUG, "Kafka Connect URL resolved for deletion", url=url, table=table_info.to_dict())
        response = requests.delete(url)
        self.logger.logMessage(Logging.LogLevel.DEBUG, "Kafka Connect response for deletion", status_code=response.status_code, response_text=response.text, table=table_info.to_dict())
        if response.status_code not in (200, 204):
            self.logger.logMessage(Logging.LogLevel.ERROR, "Failed to delete connector", response_text=response.text, table=table_info.to_dict())
            raise RuntimeError(f"Failed to delete connector: {response.text}")

        self.logger.logMessage(Logging.LogLevel.DEBUG, "CDC connector deleted successfully", connector_name=sink_connector_name)

    def delete_source_cdc_connector(self, table_info: TableInfo):
        self.logger.logMessage(Logging.LogLevel.DEBUG, "Deleting source CDC connector", table=table_info.to_dict())
        source_connector_name = self.source_connector_name_format.format(
            database=table_info.database,
            schema=table_info.schema,
            table_name=table_info.table
        )
        kc = self.config.get(ConfigKeys.KAFKA_CONNECT.value, {}).get(KafkaConnectKeys.URL.value)
        if not kc:
            self.logger.logMessage(Logging.LogLevel.ERROR, "Kafka Connect URL not configured", table=table_info.to_dict())
            raise ValueError("Kafka Connect URL not configured")

        url = f"{kc}/connectors/{source_connector_name}"
        self.logger.logMessage(Logging.LogLevel.DEBUG, "Kafka Connect URL resolved for deletion", url=url, table=table_info.to_dict())
        response = requests.delete(url)
        self.logger.logMessage(Logging.LogLevel.DEBUG, "Kafka Connect response for deletion", status_code=response.status_code, response_text=response.text, table=table_info.to_dict())
        if response.status_code not in (200, 204):
            self.logger.logMessage(Logging.LogLevel.ERROR, "Failed to delete connector", response_text=response.text, table=table_info.to_dict())
            raise RuntimeError(f"Failed to delete connector: {response.text}")

        self.logger.logMessage(Logging.LogLevel.DEBUG, "CDC connector deleted successfully, removing entry from debezium signal", connector_name=source_connector_name, table=table_info.to_dict())
        self.yugabyte_manager.remove_entry_from_debezium_signal(table_info.database, table_info.table)

    def check_connector_exists(self, table_info: TableInfo) -> bool:
        source_connector_name = self.source_connector_name_format.format(
            database=table_info.database,
            schema=table_info.schema,
            table_name=table_info.table
        )
        sink_connector_name = self.sink_connector_name_format.format(
            database=table_info.database,
            table_name=table_info.table
        )

        self.logger.logMessage(Logging.LogLevel.DEBUG, "Checking if Kafka connectors exist", source_connector_name=source_connector_name, sink_connector_name=sink_connector_name, table=table_info.to_dict())
        kc = self.config.get(ConfigKeys.KAFKA_CONNECT.value, {}).get(KafkaConnectKeys.URL.value)
        if not kc:
            self.logger.logMessage(Logging.LogLevel.ERROR, "Kafka Connect URL not configured", table=table_info.to_dict())
            raise ValueError("Kafka Connect URL not configured")

        source_exists = False
        url = f"{kc}/connectors/{source_connector_name}/status"
        self.logger.logMessage(Logging.LogLevel.DEBUG, "Kafka Connect status URL", url=url, table=table_info.to_dict())
        try:
            response = requests.get(url, timeout=10)
            self.logger.logMessage(Logging.LogLevel.DEBUG, "Kafka Connect source status response", status_code=response.status_code, response_text=response.text, table=table_info.to_dict())
            source_exists = response.status_code == 200
            self.logger.logMessage(Logging.LogLevel.DEBUG, "Connector source existence check completed", exists=source_exists, table=table_info.to_dict())
        except Exception as e:
            self.logger.logMessage(Logging.LogLevel.ERROR, "Exception while checking connector existence", error=str(e), table=table_info.to_dict())

        sink_exists = False
        url = f"{kc}/connectors/{sink_connector_name}/status"
        self.logger.logMessage(Logging.LogLevel.DEBUG, "Kafka Connect status URL", url=url, table=table_info.to_dict())
        try:
            response = requests.get(url, timeout=10)
            self.logger.logMessage(Logging.LogLevel.DEBUG, "Kafka Connect sink status response", status_code=response.status_code, response_text=response.text, table=table_info.to_dict())
            sink_exists = response.status_code == 200
            self.logger.logMessage(Logging.LogLevel.DEBUG, "Connector sink existence check completed", exists=sink_exists, table=table_info.to_dict())
        except Exception as e:
            self.logger.logMessage(Logging.LogLevel.ERROR, "Exception while checking connector existence", error=str(e), table=table_info.to_dict())

        return {"source_exists": source_exists, "sink_exists": sink_exists}
        
    def reset_connectors(self, table_info: TableInfo):
        self.logger.logMessage(Logging.LogLevel.DEBUG, "Resetting Kafka connectors for table", table=table_info.to_dict())
        status = self.check_connector_exists(table_info)
        if status.get("source_exists"):
            self.logger.logMessage(Logging.LogLevel.DEBUG, "Source connector exists, deleting", table=table_info.to_dict())
            self.delete_source_cdc_connector(table_info)
        
        if status.get("sink_exists"):
            self.logger.logMessage(Logging.LogLevel.DEBUG, "Sink connector exists, deleting", table=table_info.to_dict())
            self.delete_sink_cdc_connector(table_info)
        
    def setup_connectors(self, table_info: TableInfo):
        self.logger.logMessage(Logging.LogLevel.DEBUG, "Setting up Kafka connectors for table", table=table_info.to_dict())
        self.create_source_connector(table_info)
        self.create_sink_connector(table_info)
        
        start_time = time.time()
        timeout = 3 * 60  # 3 minutes in seconds

        while time.time() - start_time < timeout:
            resp = self.check_connector_exists(table_info)
            if resp.get("source_exists") and resp.get("sink_exists"):
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
        # this method will fail if the table does not have a primary key field
        # TODO: add test to verify if connector created correctly if table has primary key
        stream_id = self.yugabyte_manager.get_cdc_stream_id(table_info)
        try:
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
                "signal.data.collection": f"public.debezium_signal",
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

            self.logger.logMessage(Logging.LogLevel.DEBUG, "Source connector configuration", source_config=source_config, table=table_info.to_dict())
            source_connector_name = self.source_connector_name_format.format(
                database=table_info.database,
                schema=table_info.schema,
                table_name=table_info.table
            )
            response = self._send_connector_request(source_connector_name, source_config)
            self.logger.logMessage(Logging.LogLevel.DEBUG, "Source connector created", response=response, table=table_info.to_dict())
            # Insert debezium signal record
            if self.yugabyte_manager.entry_exists_in_debezium_signal(table_info):
                self.yugabyte_manager.remove_entry_from_debezium_signal(table_info.database, table_info.table)
                
            self.yugabyte_manager.insert_debezium_signal(table_info, stream_id)
        except Exception as e:
            self.logger.logMessage(Logging.LogLevel.ERROR, "Failed to create source connector", error=str(e), table=table_info.to_dict())
            self.reset_connectors(table_info)
            raise


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
            self.logger.logMessage(Logging.LogLevel.ERROR, "Failed to load project_id from GCP key file", error=str(e), table=table_info.to_dict())
            raise

        # Derive topic/dataset/table consistently with the source
        try:
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
            sink_connector_name = self.sink_connector_name_format.format(
                database=table_info.database,
                table_name=table_info.table
            )
            response = self._send_connector_request(sink_connector_name, sink_config)
            self.logger.logMessage(Logging.LogLevel.DEBUG, "Sink connector created", response=response, table=table_info.to_dict())
        except Exception as e:
            self.logger.logMessage(Logging.LogLevel.ERROR, "Failed to create sink connector", error=str(e), table=table_info.to_dict())
            self.reset_connectors(table_info)
            raise

    def _send_connector_request(self, name: str, config: dict):
        url = f"{self.kc_url}/connectors/{name}/config"
        payload = {"name": name, "config": config}
        # The Configs API expects just {"config": ...} for PUT to /config in some distros;
        # adjust if your worker expects raw 'config' (common). If 400, try sending just 'config'.
        import requests, json
        r = requests.put(url, headers={"Content-Type": "application/json"},
                        data=json.dumps(config))  # <-- many workers want just the config map
        if r.status_code >= 400:
            self.logger.logMessage(Logging.LogLevel.ERROR, "Connector config rejected",
                            status=r.status_code, response=r.text, sent_config=config)
            raise RuntimeError(f"Kafka Connect error {r.status_code}: {r.text}")
        return r.json()
