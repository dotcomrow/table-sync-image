from dataclasses import dataclass
from typing import Optional
import json
from classes.config_reader import ConfigKeys, ConfigReader, LoggingKeys, BigQueryKeys

@dataclass
class TableAnnotation:
    enabled: bool
    bq_target: str  # "dataset.table"
    cdc_stream_id: Optional[str] = None  # optional per-table override
    default_backup_dataset: Optional[str] = None  # optional default backup dataset

    def __init__(self, config):
        self.config = config
        self.default_backup_dataset = self.config.get(BigQueryKeys.DEFAULT_BACKUP_DATASET.value, "yugabyte_backup")
    
    @classmethod
    def from_comment(cls, config: ConfigReader, comment: str) -> Optional["TableAnnotation"]:
        try:
            data = json.loads(comment)
            bootstrap = data.get("bootstrap", {})
            if not isinstance(bootstrap, dict):
                return None
            return cls(
                config=config,
                enabled=bool(bootstrap.get("enabled", False)),
                bq_target=str(bootstrap.get("bq", "")).strip(),
                cdc_stream_id=(bootstrap.get("cdc_stream_id") or None),
            )
        except Exception:
            return None
