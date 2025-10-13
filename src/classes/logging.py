import structlog
import enum
import logging
from uuid import uuid4
from typing import Optional

from classes.config_reader import ConfigKeys, ConfigReader, LoggingKeys, YugabyteDBKeys

try:
    from google.cloud import logging as cloud_logging
    CLOUD_LOGGING_AVAILABLE = True
except ImportError:
    CLOUD_LOGGING_AVAILABLE = False
    cloud_logging = None

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
        
        # Add console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(numeric)
        standard_logger.addHandler(console_handler)
        
        # Add cloud logging handler if available
        if cloud_logging_client:
            cloud_handler = cloud_logging_client.get_default_handler()
            cloud_handler.setLevel(numeric)
            standard_logger.addHandler(cloud_handler)
        
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
    
    def _setup_cloud_logging(self) -> Optional:
        """Set up Google Cloud Logging if available and configured."""
        if not CLOUD_LOGGING_AVAILABLE:
            return None
        
        try:
            # Check if cloud logging is enabled in config
            logging_config = self.config.get(ConfigKeys.LOGGING.value, {})
            if not logging_config.get(LoggingKeys.ENABLE_CLOUD_LOGGING.value, False):
                return None
            
            # Initialize the client
            client = cloud_logging.Client()
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