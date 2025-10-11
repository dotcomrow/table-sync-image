from dataclasses import dataclass
import sys
from typing import Optional
import json
from classes.config_reader import ConfigReader, BigQueryKeys

@dataclass
class TableAnnotation:
    enabled: bool
    bq_dataset: Optional[str] = None  # extracted from bq_target
    bq_table: Optional[str] = None    # extracted from bq_target

    @classmethod
    def from_comment(cls, comment: str) -> Optional["TableAnnotation"]:
        try:
            if not comment or not comment.strip():
                return None
            data = json.loads(comment)
            bootstrap = data.get("bootstrap", {})
            if not isinstance(bootstrap, dict):
                return None
            return cls(
                enabled=bool(bootstrap.get("enabled", False)),
                bq_dataset=str(bootstrap.get("bq", "")).strip().split(".")[0],
                bq_table=str(bootstrap.get("bq", "")).strip().split(".")[1] if "." in str(bootstrap.get("bq", "")).strip() else None,
            )
        except Exception as e:
            print(f"Unexpected error in annotation reader: {e}", file=sys.stderr)
            return None