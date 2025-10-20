import os
import yaml
import re
import sys
from typing import Dict, Any
from enum import Enum
from concurrent.futures import ThreadPoolExecutor

# Section names
class ConfigKeys(Enum):
    YUGABYTEDB = "yugabytedb"
    BIGQUERY = "bigquery"
    KAFKA_CONNECT = "kafka_connect"
    LOGGING = "logging"
    HEALTH_CHECK = "health_check"
    PROCESSING = "processing"
    REDIS = "redis" 

class RedisKeys(Enum):
    URL = "url"
    CACHE_KEY = "cacheKey"
    DEFAULT_TTL = "default_ttl"
    CACHE_KEYS = "cache_keys"

class YugabyteDBKeys(Enum):
    MASTER_ADDRESSES = "master_addresses"
    YB_ADMIN_PATH = "yb_admin_path"
    HOST = "host"
    PORT = "port"
    USER = "user"
    PASSWORD = "password"
    DATABASE = "database"
    MOCK = "mock"
    EXCLUDED_DATABASES = "excluded_databases"
    
class BigQueryKeys(Enum):
    CREDENTIALS_PATH = "credentials_path"
    MOCK = "mock"
    
class KafkaConnectKeys(Enum):
    URL = "url"
    SCHEMA_REGISTRY_URL = "schema_registry_url"
    MOCK = "mock"
    BOOTSTRAP = "bootstrap"
    
class LoggingKeys(Enum):
    LEVEL = "level"
    ENABLE_CLOUD_LOGGING = "enable_cloud_logging"
    
class HealthCheckKeys(Enum):
    ENABLED = "enabled"
    PORT = "port"
    
class ProcessingKeys(Enum):
    TABLE_SCANNER = "table_scanner"
    DATA_PREPARER = "database_prep"
    CACHE_CHECKER = "cache_check"
    CONNECTOR_CLEANER = "connector_cleanup"
    
class ProcessingTableScannerKeys(Enum):
    MAX_SCAN_THREADS = "max_scan_threads"
    SCAN_INTERVAL_SECONDS = "scan_interval_seconds"

class ProcessingDatabasePrepKeys(Enum):
    MAX_PREPARATION_THREADS = "max_preparation_threads"
    SCAN_INTERVAL_SECONDS = "scan_interval_seconds"
    
class ProcessingCacheCheckerKeys(Enum):
    MAX_CACHE_CHECK_THREADS = "max_cache_check_threads"
    SCAN_INTERVAL_SECONDS = "scan_interval_seconds"
    
class ProcessingConnectorCleanerKeys(Enum):
    MAX_CONNECTOR_CLEANUP_THREADS = "max_connector_cleanup_threads"
    SCAN_INTERVAL_SECONDS = "scan_interval_seconds"
    
class RedisCacheKeys(Enum):
    ROW_COUNTS = "row_counts"

class ConfigReader:
    def __init__(self, config_path):
        self.config_path = config_path

    def load_config(self) -> Dict[str, Any]:
        try:
            with open(self.config_path, 'r') as f:
                content = f.read()

            def env_replacer(match):
                spec = match.group(1)
                if ':-' in spec:
                    var, default = spec.split(':-', 1)
                elif ':' in spec:
                    var, default = spec.split(':', 1)
                else:
                    var, default = spec, ''
                return os.getenv(var, default)

            content = re.sub(r'\$\{([^}]+)\}', env_replacer, content)
            cfg = yaml.safe_load(content) or {}

            # Overwrite the get method to only accept Enums
            class ConfigDict(dict):
                def get(self, key, default=None):
                    if not ConfigReader.is_enum_value(key):
                        raise TypeError("Config keys must be instances of Enum")
                    return super().get(key, default)

            cfg = ConfigDict(cfg)

            return cfg
        except Exception as e:
            print(f"Failed to load config from {self.config_path}: {e}", file=sys.stderr)
            sys.exit(1)

    def validate_config(self, config: Dict[str, Any]):
        errors = []

        def validate_section(section_name: str, section_config: Dict[str, Any], keys_enum):
            if not isinstance(section_config, dict):
                errors.append(f"{section_name} must be a dictionary")
                return

            for key in keys_enum:
                # Skip validation for optional properties
                if key.value in ["mock", YugabyteDBKeys.EXCLUDED_DATABASES.value]:
                    continue
                if key.value not in section_config:
                    errors.append(f"{section_name}.{key.value} is required")

        # Validate yugabytedb
        yugabytedb = config.get(ConfigKeys.YUGABYTEDB.value, {})
        validate_section(ConfigKeys.YUGABYTEDB.value, yugabytedb, YugabyteDBKeys)

        # Validate bigquery
        bigquery = config.get(ConfigKeys.BIGQUERY.value, {})
        validate_section(ConfigKeys.BIGQUERY.value, bigquery, BigQueryKeys)

        # Validate kafka_connect
        kafka_connect = config.get(ConfigKeys.KAFKA_CONNECT.value, {})
        validate_section(ConfigKeys.KAFKA_CONNECT.value, kafka_connect, KafkaConnectKeys)

        # Validate logging
        logging = config.get(ConfigKeys.LOGGING.value, {})
        validate_section(ConfigKeys.LOGGING.value, logging, LoggingKeys)

        # Validate health_check
        health_check = config.get(ConfigKeys.HEALTH_CHECK.value, {})
        validate_section(ConfigKeys.HEALTH_CHECK.value, health_check, HealthCheckKeys)

        # Validate processing
        processing = config.get(ConfigKeys.PROCESSING.value, {})
        validate_section(ConfigKeys.PROCESSING.value, processing, ProcessingKeys)
        
        # Validate redis
        redis = config.get(ConfigKeys.REDIS.value, {})
        validate_section(ConfigKeys.REDIS.value, redis, RedisKeys)

        if errors:
            raise ValueError(f"Configuration validation errors: {', '.join(errors)}")

    def read_and_validate_config(self):
        config = self.load_config()
        self.validate_config(config)
        return config

    def is_enum_value(property_string: str) -> bool:
        """
        Check if a property string is a value of one of the defined Enums in a multithreaded fashion.

        Args:
            property_string (str): The property string to check.

        Returns:
            bool: True if the property string is a value of one of the Enums, False otherwise.
        """
        def check_enum(enum_class):
            return property_string in [e.value for e in enum_class]

        enum_classes = [
            ConfigKeys, YugabyteDBKeys, BigQueryKeys, KafkaConnectKeys,
            LoggingKeys, HealthCheckKeys, ProcessingKeys
        ]

        with ThreadPoolExecutor() as executor:
            results = executor.map(check_enum, enum_classes)

        return any(results)