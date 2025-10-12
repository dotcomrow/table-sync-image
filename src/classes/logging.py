import logging
import structlog
from classes.config_reader import ConfigKeys, LoggingKeys
import enum

class Logging:
    class LogLevel(enum.IntEnum):
        NOTSET=0; DEBUG=10; INFO=20; WARNING=30; ERROR=40; CRITICAL=50
    
    def __init__(self, config):
        self.config = config
        self.logger = self._init_logger()
    
    def _init_logger(self) -> structlog.BoundLogger:
        lvl = (self.config.get(ConfigKeys.LOGGING.value, {}) or {}).get(LoggingKeys.LEVEL.value, "INFO").upper()
        numeric = getattr(logging, lvl, self.LogLevel.INFO)
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

    def logMessage(self, level: LogLevel, message: str, **kwargs):
        if level == self.LogLevel.DEBUG:
            self.logger.debug(message, **kwargs)
        elif level == self.LogLevel.INFO:
            self.logger.info(message, **kwargs)
        elif level == self.LogLevel.WARNING:
            self.logger.warning(message, **kwargs)
        elif level == self.LogLevel.ERROR:
            self.logger.error(message, **kwargs)