from dataclasses import dataclass
import sys
from typing import Optional
import json
from classes.config_reader import ConfigReader, BigQueryKeys

@dataclass
class TableAnnotation:
    enabled: bool
    bq_target: str  # "dataset.table"
    cdc_stream_id: Optional[str] = None  # optional per-table override
    default_backup_dataset: Optional[str] = None  # optional default backup dataset

    def __post_init__(self):
        # Initialize default_backup_dataset using the config
        if not self.default_backup_dataset:
            self.default_backup_dataset = self.config.get(BigQueryKeys.DEFAULT_BACKUP_DATASET.value, "yugabyte_backup")

    @classmethod
    def from_comment(cls, config: ConfigReader, comment: str) -> Optional["TableAnnotation"]:
        try:
            data = json.loads(comment)
            bootstrap = data.get("bootstrap", {})
            if not isinstance(bootstrap, dict):
                return None
            return cls(
                enabled=bool(bootstrap.get("enabled", False)),
                bq_target=str(bootstrap.get("bq", "")).strip(),
                cdc_stream_id=(bootstrap.get("cdc_stream_id") or None),
                default_backup_dataset=config.get(BigQueryKeys.DEFAULT_BACKUP_DATASET.value, "yugabyte_backup"),
            )
        except Exception as e:
            print(f"Unexpected error in annotation reader: {e}", file=sys.stderr)
            return None