import structlog
import enum
import logging
from uuid import uuid4
from typing import Optional, Any

from classes.config_reader import ConfigKeys, ConfigReader, LoggingKeys, BigQueryKeys, YugabyteDBKeys

try:
    from google.cloud import logging as cloud_logging
    from google.oauth2 import service_account
    CLOUD_LOGGING_AVAILABLE = True
except ImportError:
    CLOUD_LOGGING_AVAILABLE = False
    cloud_logging = None
    service_account = None

class Logging:
    class LogLevel(enum.IntEnum):
        NOTSET=0; DEBUG=10; INFO=20; WARNING=30; ERROR=40; CRITICAL=50
    
    def __init__(self, config: ConfigReader):
        self.config = config
        self.logger = self._init_logger()
    
    def _init_logger(self) -> structlog.BoundLogger:
        lvl = self.config.get(ConfigKeys.LOGGING.value).get(LoggingKeys.LEVEL.value, "INFO").upper()
        numeric = self.LogLevel[lvl] if lvl in self.LogLevel.__members__ else self.LogLevel.INFO
        
        # Set up Cloud Logging if available and enabled
        cloud_logging_client = self._setup_cloud_logging()
        
        # Configure the standard logging handlers
        standard_logger = logging.getLogger("table_sync_orchestrator")
        standard_logger.setLevel(numeric)
        
        # Clear existing handlers
        standard_logger.handlers.clear()
        
        # Add cloud logging handler if available, otherwise add console handler
        if cloud_logging_client:
            try:
                cloud_handler = cloud_logging_client.get_default_handler()
                cloud_handler.setLevel(numeric)
                standard_logger.addHandler(cloud_handler)
                print("📤 Logs will be sent to Google Cloud Logging only")
            except Exception as e:
                print(f"Warning: Failed to initialize Cloud Logging handler: {e}")
                print("Continuing with console logging only...")
                # Fallback to console handler if cloud logging fails
                console_handler = logging.StreamHandler()
                console_handler.setLevel(numeric)
                standard_logger.addHandler(console_handler)
        else:
            # Add console handler when cloud logging is not available or disabled
            console_handler = logging.StreamHandler()
            console_handler.setLevel(numeric)
            standard_logger.addHandler(console_handler)
            print("📺 Logs will be sent to console only")
        
        structlog.configure(
            processors=[
                structlog.processors.TimeStamper(fmt="iso"),
                structlog.processors.add_log_level,
                structlog.processors.JSONRenderer()
            ],
            wrapper_class=structlog.make_filtering_bound_logger(numeric),
            logger_factory=structlog.stdlib.LoggerFactory(),
            cache_logger_on_first_use=True,
            context_class=dict  # Ensure compatibility with structlog's context management
        )
        logger = structlog.get_logger("table_sync_orchestrator")
        session_id = str(uuid4())
        return logger.bind(session_id=session_id)
    
    def _setup_cloud_logging(self) -> Optional[Any]:
        """Set up Google Cloud Logging if available and configured."""
        if not CLOUD_LOGGING_AVAILABLE:
            return None
        
        try:
            # Check if cloud logging is enabled in config
            logging_config = self.config.get(ConfigKeys.LOGGING.value, {})
            if not logging_config.get(LoggingKeys.ENABLE_CLOUD_LOGGING.value, False):
                return None
            
            # Get credentials from BigQuery configuration
            bigquery_config = self.config.get(ConfigKeys.BIGQUERY.value, {})
            credentials_path = bigquery_config.get(BigQueryKeys.CREDENTIALS_PATH.value)
            
            if not credentials_path:
                print("Warning: No credentials_path found in BigQuery configuration for Cloud Logging")
                return None
            
            print(f"🔍 Attempting to load Cloud Logging credentials from: {credentials_path}")
            
            # Check if credentials file exists and is readable
            import os
            if not os.path.exists(credentials_path):
                print(f"❌ Credentials file does not exist: {credentials_path}")
                return None
                
            if not os.access(credentials_path, os.R_OK):
                print(f"❌ Credentials file is not readable: {credentials_path}")
                return None
                
            try:
                # Load service account credentials
                credentials = service_account.Credentials.from_service_account_file(credentials_path)
                print(f"✅ Loaded service account credentials from {credentials_path}")
                print(f"📧 Service account email: {credentials.service_account_email}")
            except Exception as cred_e:
                print(f"❌ Failed to load service account credentials: {cred_e}")
                return None
            
            # Extract project ID from service account credentials
            # The project_id should be available in the credentials object
            project_id = getattr(credentials, 'project_id', None)
            if not project_id:
                # Try to read project_id from the JSON file directly
                import json
                try:
                    with open(credentials_path, 'r') as f:
                        key_data = json.load(f)
                        project_id = key_data.get('project_id')
                except Exception as json_e:
                    print(f"❌ Failed to extract project_id from credentials file: {json_e}")
                    
            if not project_id:
                print("❌ Could not determine project_id from service account credentials")
                return None
                
            print(f"🏗️ Initializing Cloud Logging client for project: {project_id}")
            
            # Initialize the client with explicit credentials and project
            client = cloud_logging.Client(credentials=credentials, project=project_id)
            
            # No authentication test needed - will be validated on first write operation
            print(f"✅ Google Cloud Logging client initialized")
            print(f"� Project: {project_id}")
            print(f"� Service Account: {credentials.service_account_email}")
                
            return client
        except Exception as e:
            # Fallback to local logging if cloud logging setup fails
            print(f"Warning: Failed to set up Google Cloud Logging: {e}")
            return None

    def logMessage(self, level: LogLevel, message: str, **kwargs):
        if level == self.LogLevel.DEBUG:
            self.logger.debug(message, **kwargs)
        elif level == self.LogLevel.INFO:
            self.logger.info(message, **kwargs)
        elif level == self.LogLevel.WARNING:
            self.logger.warning(message, **kwargs)
        elif level == self.LogLevel.ERROR:
            self.logger.error(message, **kwargs)