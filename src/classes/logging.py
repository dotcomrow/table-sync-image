import logging
import structlog
from classes.config_reader import ConfigKeys, LoggingKeys

class Logging:
    def __init__(self, config):
        self.config = config
        self.logger = self._init_logger()
    
    def _init_logger(self) -> structlog.BoundLogger:
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
        return structlog.get_logger("table_sync_orchestrator")

    def logMessage(self, level: logging.LogLevel, message: str, **kwargs):
        if level == logging.LogLevel.DEBUG:
            self.logger.debug(message, **kwargs)
        elif level == logging.LogLevel.INFO:
            self.logger.info(message, **kwargs)
        elif level == logging.LogLevel.WARNING:
            self.logger.warning(message, **kwargs)
        elif level == logging.LogLevel.ERROR:
            self.logger.error(message, **kwargs)