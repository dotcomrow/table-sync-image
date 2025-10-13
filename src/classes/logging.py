import structlog
import enum
from uuid import uuid4

from classes.config_reader import ConfigKeys, ConfigReader, LoggingKeys

class Logging:
    class LogLevel(enum.IntEnum):
        NOTSET=0; DEBUG=10; INFO=20; WARNING=30; ERROR=40; CRITICAL=50
    
    def __init__(self, config: ConfigReader):
        self.config = config
        self.logger = self._init_logger()
    
    def _init_logger(self) -> structlog.BoundLogger:
        lvl = self.config.get(ConfigKeys.LOGGING.value).get(LoggingKeys.LEVEL.value, "INFO").upper()
        print("Setting system log level to:", lvl)
        numeric = self.LogLevel[lvl] if lvl in self.LogLevel.__members__ else self.LogLevel.INFO

        structlog.configure(
            processors=[
                # make sure contextvars are merged into every log record
                structlog.contextvars.merge_contextvars,
                structlog.processors.add_log_level,
                structlog.processors.TimeStamper(fmt="iso"),
                structlog.processors.JSONRenderer(),
            ],
            # use contextvars-backed storage for bound fields
            context_class=structlog.contextvars.bound_contextvars,
            wrapper_class=structlog.make_filtering_bound_logger(numeric),
            logger_factory=structlog.PrintLoggerFactory(),
            cache_logger_on_first_use=True,
        )

        # Bind fields you want on EVERY message:
        # - session_id: tie logs for the same processing stream
        # - app: optional constant app/service name
        # You can regenerate/rebind session_id per unit of work (see below).
        structlog.contextvars.bind_contextvars(
            app="table_sync_orchestrator",
            session_id=str(uuid4()),  # or pass one in from caller
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