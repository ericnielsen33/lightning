from abc import ABC, abstractmethod
from functools import reduce
from operator import and_
from typing import List, Optional

from pyspark.sql import DataFrame, Column
from pyspark.sql.types import DateType
from pyspark.sql.functions import current_date, col, lit
from lightning.data.WritableDataSource import WritableDataSource


class SCD2WritableDataSource(WritableDataSource, ABC):
    """Abstract base class for SCD2 (slowly changing dimension type 2) writable data sources.

    Subclasses must provide:
    - partition_keys: columns the table is physically partitioned by
    - merge_keys: columns compared to detect whether a record's attributes changed

    Assumes the table schema is exactly partition_keys | merge_keys | {start_date, end_date},
    and that partition_keys and merge_keys are disjoint column sets.
    """

    def __init__(self):
       super().__init__()

    @abstractmethod
    def partition_keys(self) -> List[str]:
        pass

    @abstractmethod
    def merge_keys(self) -> List[str]:
        pass

    def start_date_column_ref(self) -> str:
        return "start_date"

    def end_date_column_ref(self) -> str:
        return "end_date"

    def target_alias(self) -> str:
        return "target"

    def updates_alias(self) -> str:
        return "updates"

    def partition_columns(self, alias: Optional[str] = None) -> List[Column]:
        if alias:
            return [col(f"{alias}.{key}") for key in self.partition_keys()]
        else:
            return [col(key) for key in self.partition_keys()]

    def merge_columns(self, alias: Optional[str] = None) -> List[Column]:
        if alias:
            return [col(f"{alias}.{key}") for key in self.merge_keys()]
        else:
            return [col(key) for key in self.merge_keys()]

    def start_date_column(self, date_expr: Optional[Column] = None, alias: Optional[str] = None) -> Column:
        ref = self.start_date_column_ref()
        if date_expr is not None:
            return date_expr.cast(DateType()).alias(ref)
        if alias is not None:
            return col(f"{alias}.{ref}").alias(ref)
        return col(ref)

    def end_date_column(self, date_expr: Optional[Column] = None, alias: Optional[str] = None) -> Column:
        ref = self.end_date_column_ref()
        if date_expr is not None:
            return date_expr.cast(DateType()).alias(ref)
        if alias is not None:
            return col(f"{alias}.{ref}").alias(ref)
        return col(ref)

    def merge_condition(self) -> Column:
        conditions = [
            col(f"{self.target_alias()}.{key}") == col(f"{self.updates_alias()}.{key}")
            for key in self.merge_keys()
        ]
        return reduce(and_, conditions)

    def _partition_filter(self, partitions: dict) -> Optional[Column]:
        conditions = [col(field) == value for field, value in partitions.items()]
        return reduce(and_, conditions) if conditions else None

    def current_entities(self, **partitions: Column) -> DataFrame:
        """
        Rows in the target partitions that are not expired (end_date is null).
        """
        target = self.read().filter(self.end_date_column().isNull())
        condition = self._partition_filter(partitions)
        if condition is not None:
            target = target.filter(condition)
        return target

    def historic_entities(self, **partitions: Column) -> DataFrame:
        """
        Previously expired data for the target partitions. These values remain unchanged
        but must still be included in the merge, since a dynamic partition overwrite
        replaces every row in the affected partitions.
        """
        target = self.read().filter(self.end_date_column().isNotNull())
        condition = self._partition_filter(partitions)
        if condition is not None:
            target = target.filter(condition)
        return target.select(
            *self.partition_columns(),
            *self.merge_columns(),
            self.start_date_column(),
            self.end_date_column(),
        )

    def matched_current_entities(self, current: DataFrame, updates: DataFrame) -> DataFrame:
        """
        Entries that exist in both the current target and the updates with identical
        merge_keys. Nothing changed, so these carry their existing start_date forward
        with a null end_date.
        """
        to_merge = (
            current.alias(self.target_alias())
            .join(
                updates.alias(self.updates_alias()),
                self.merge_condition(),
                how="inner",
            )
            .select(
                *self.partition_columns(self.target_alias()),
                *self.merge_columns(self.target_alias()),
                self.start_date_column(alias=self.target_alias()),
                self.end_date_column(date_expr=lit(None)),
            )
        )
        return to_merge

    def new_current_entities(self, current: DataFrame, updates: DataFrame) -> DataFrame:
        """
        Entries that are in the updates DataFrame but not in the current target.
        """
        to_merge = (
            updates.alias(self.updates_alias())
            .join(
                current.alias(self.target_alias()),
                self.merge_condition(),
                "leftanti",
            )
            .select(
                *self.partition_columns(self.updates_alias()),
                *self.merge_columns(self.updates_alias()),
                self.start_date_column(date_expr=current_date()),
                self.end_date_column(date_expr=lit(None)),
            )
        )
        return to_merge

    def expiring_entities(self, current: DataFrame, updates: DataFrame) -> DataFrame:
        """
        Entries in the current target whose merge_keys no longer appear in the updates
        DataFrame -- i.e. a tracked attribute changed. These expire as of today.
        """
        to_merge = (
            current.alias(self.target_alias())
            .join(
                updates.alias(self.updates_alias()),
                self.merge_condition(),
                how="leftanti",
            )
            .select(
                *self.partition_columns(self.target_alias()),
                *self.merge_columns(self.target_alias()),
                self.start_date_column(alias=self.target_alias()),
                self.end_date_column(date_expr=current_date()),
            )
        )
        return to_merge

    def merge(self, updates: DataFrame, **partitions: Column) -> DataFrame:
        current = self.current_entities(**partitions)

        merged = (
            self.historic_entities(**partitions)
                .unionByName(self.matched_current_entities(current, updates))
                .unionByName(self.new_current_entities(current, updates))
                .unionByName(self.expiring_entities(current, updates))
        )

        merged.write \
            .format(self.format()) \
            .mode("overwrite") \
            .partitionBy(list(self.partition_keys())) \
            .option("partitionOverwriteMode", "dynamic") \
            .saveAsTable(self.uri())

        return merged.limit(100)
