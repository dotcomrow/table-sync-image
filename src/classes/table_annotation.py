from typing import Optional
import json

class TableAnnotation:
    enabled: bool
    bq_target: str  # "dataset.table"
    cdc_stream_id: Optional[str] = None  # optional per-table override

    @classmethod
    def from_comment(cls, comment: str) -> Optional["TableAnnotation"]:
        try:
            data = json.loads(comment)
            bootstrap = data.get("bootstrap", {})
            if not isinstance(bootstrap, dict):
                return None
            return cls(
                enabled=bool(bootstrap.get("enabled", False)),
                bq_target=str(bootstrap.get("bq", "")).strip(),
                cdc_stream_id=(bootstrap.get("cdc_stream_id") or None),
            )
        except Exception:
            return None