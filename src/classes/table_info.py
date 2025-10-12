from dataclasses import dataclass
from typing import Optional
from classes.table_annotation import TableAnnotation

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
        if self.annotation and self.annotation.bq_dataset:
            return self.annotation.bq_dataset
        return None

    @property
    def bq_table(self) -> Optional[str]:
        if self.annotation and self.annotation.bq_table:
            return self.annotation.bq_table
        return None