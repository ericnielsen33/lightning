from abc import ABC
from typing import List, Optional

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Column
from pyspark.sql.functions import current_date, col, lit, when
from lightning.data.WritableDataSource import WritableDataSource


class SCD2WritableDataSource(WritableDataSource, ABC):
    """Abstract base class for SCD2 (slowly changing dimension type 2) writable data sources.

    Subclasses should provide concrete implementations for writing/reading if needed.

    Methods to complete:
    - merge_keys: list of column names used to match records when merging
    """

    def __init__(self, merge_keys: List[str]):
        if not merge_keys or not isinstance(merge_keys, list):
            raise ValueError("merge_keys must be a non-empty list of column names")
        self.merge_keys = merge_keys

    def merge_keys(self) -> List[str]:
        pass

    def start_date_column_ref(self) -> str:
        return "start_date"

    def end_date_column_ref(self) -> str:
        return "end_date"

    def start_date_column(self) -> Column:
        return col(self.start_date_column_ref())

    def end_date_column(self) -> Column:
        return col(self.end_date_column_ref())

    def merge(self, updates: DataFrame) -> None:
        """Perform an upsert (merge) from updates into a Delta table at target_path.

        This uses the columns in self.merge_keys to build the join condition.
        If the target does not exist yet, the updates will be written out as a new Delta table.
        """

        # Build merge condition: target.key = source.key AND ...
        cond = " AND ".join([
            f"target.`{k}` = updates.`{k}`" for k in self.merge_keys
        ])

        delta_table = DeltaTable.forPath(self.session, self.uri())

        # Typical SCD2 behaviour: when matched, expire old record (set end_date);
        # and insert new records when not matched. For generic behaviour we update all
        # matched columns and insert all when not matched.
        merged = (
            delta_table.alias("target")
                .merge(updates.alias("updates"), cond)
                .whenMatchedUpdateAll(
                    set={col: col(f"updates.{col}") for col in updates.columns}
                )
                .whenNotMatchedInsertAll()
        )

        merged.execute() 
