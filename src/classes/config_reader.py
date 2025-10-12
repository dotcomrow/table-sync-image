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
    
class HealthCheckKeys(Enum):
    ENABLED = "enabled"
    PORT = "port"
    
class ProcessingKeys(Enum):
    MAX_SCAN_THREADS = "max_scan_threads"
    SCAN_INTERVAL_SECONDS = "scan_interval_seconds"

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

            # Allow DATABASE_URL to override yugabytedb section
            self.parse_database_url(cfg)
            return cfg
        except Exception as e:
            print(f"Failed to load config from {self.config_path}: {e}", file=sys.stderr)
            sys.exit(1)

    def parse_database_url(self, config: Dict[str, Any]):
        url = os.getenv('DATABASE_URL')
        if not url:
            return
        try:
            from urllib.parse import urlparse
            parsed = urlparse(url)
            config.setdefault('yugabytedb', {})
            yb = config['yugabytedb']
            if parsed.hostname: yb['host'] = parsed.hostname
            if parsed.port:     yb['port'] = parsed.port
            if parsed.username: yb['user'] = parsed.username
            if parsed.password: yb['password'] = parsed.password
            if parsed.path and parsed.path != '/':
                yb['database'] = parsed.path.lstrip('/')
            print(f"✅ Parsed DATABASE_URL for {parsed.username}@{parsed.hostname}:{parsed.port} → db={yb.get('database','(none)')}")
        except Exception as e:
            print(f"Warning: Failed to parse DATABASE_URL: {e}", file=sys.stderr)

    def validate_config(self, config: Dict[str, Any]):
        errors = []

        # Validate scan_interval_seconds
        if not isinstance(config.get(ConfigKeys.SCAN_INTERVAL_SECONDS.value), int):
            errors.append("scan_interval_seconds must be an integer")

        # Validate comprehensive_database_scan
        if not isinstance(config.get(ConfigKeys.COMPREHENSIVE_DATABASE_SCAN.value), bool):
            errors.append("comprehensive_database_scan must be a boolean")

        # Validate excluded_databases
        if not isinstance(config.get(ConfigKeys.EXCLUDED_DATABASES.value), str):
            errors.append("excluded_databases must be a comma-separated string")

        # Validate max_scan_threads
        if not isinstance(config.get(ConfigKeys.MAX_SCAN_THREADS.value), int):
            errors.append("max_scan_threads must be an integer")

        # Validate yugabytedb
        yugabytedb = config.get(ConfigKeys.YUGABYTEDB.value, {})
        if not isinstance(yugabytedb, dict):
            errors.append("yugabytedb must be a dictionary")

        # Validate bigquery
        bigquery = config.get(ConfigKeys.BIGQUERY.value, {})
        if not isinstance(bigquery, dict):
            errors.append("bigquery must be a dictionary")

        # Validate kafka_connect
        kafka_connect = config.get(ConfigKeys.KAFKA_CONNECT.value, {})
        if not isinstance(kafka_connect, dict):
            errors.append("kafka_connect must be a dictionary")

        # Validate logging
        logging = config.get(ConfigKeys.LOGGING.value, {})
        if not isinstance(logging, dict):
            errors.append("logging must be a dictionary")

        # Validate health_check
        health_check = config.get(ConfigKeys.HEALTH_CHECK.value, {})
        if not isinstance(health_check, dict):
            errors.append("health_check must be a dictionary")

        # Validate processing
        processing = config.get(ConfigKeys.PROCESSING.value, {})
        if not isinstance(processing, dict):
            errors.append("processing must be a dictionary")

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