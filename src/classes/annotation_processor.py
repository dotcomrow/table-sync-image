from dataclasses import dataclass
from typing import Optional
import json


@dataclass
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


class AnnotationProcessor:
    def __init__(self):
        pass

    def process_annotations(self, table_info):
        # Implementation migrated from table_sync_orchestrator
        comment = "dummy_comment"  # Replace with actual logic to fetch comment
        try:
            data = json.loads(comment)
            bootstrap = data.get("bootstrap", {})
            if not isinstance(bootstrap, dict):
                return None
            return {
                "enabled": bool(bootstrap.get("enabled", False)),
                "bq_target": str(bootstrap.get("bq", "")).strip(),
                "cdc_stream_id": bootstrap.get("cdc_stream_id"),
            }
        except json.JSONDecodeError:
            return None

    def search_annotations(self, database):
        # Logic to search for annotations in a database
        annotations = []
        for table in database.list_tables():
            comment = table.get_comment()  # Replace with actual logic to fetch table comments
            try:
                data = json.loads(comment)
                if "bootstrap" in data:
                    annotations.append(
                        {
                            "table": table.name,
                            "annotation": data["bootstrap"],
                        }
                    )
            except json.JSONDecodeError:
                continue
        return annotations