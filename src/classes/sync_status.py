from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from .table_info import TableInfo

@dataclass
class SyncStatus:
    table_info: TableInfo
    last_scan: datetime
    annotation_enabled: bool
    bigquery_exists: bool
    connector_exists: bool
    sync_active: bool
    last_connector_state: Optional[str] = None
    last_error: Optional[str] = None
    expected_topic: Optional[str] = None
    topic_exists: Optional[bool] = None