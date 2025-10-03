from dataclasses import dataclass
from typing import Optional
from src.classes.annotation_processor import TableAnnotation

@dataclass
class TableInfo:
    database: str
    schema: str
    table: str
    annotation: Optional[TableAnnotation]

    @property
    def full_name(self) -> str:
        return f"{self.database}.{self.schema}.{self.table}"

    @property
    def bq_dataset(self) -> Optional[str]:
        if self.annotation and self.annotation.bq_target and "." in self.annotation.bq_target:
            return self.annotation.bq_target.split(".", 1)[0]
        return None

    @property
    def bq_table(self) -> Optional[str]:
        if self.annotation and self.annotation.bq_target and "." in self.annotation.bq_target:
            return self.annotation.bq_target.split(".", 1)[1]
        return None