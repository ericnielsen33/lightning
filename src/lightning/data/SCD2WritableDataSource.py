from abc import ABC
from typing import List, Optional
from collections import Set

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, Column
from pyspark.sql.types import *
from pyspark.sql.functions import current_date, col, lit
from lightning.data.WritableDataSource import WritableDataSource


class SCD2WritableDataSource(WritableDataSource, ABC):
    """Abstract base class for SCD2 (slowly changing dimension type 2) writable data sources.

    Subclasses should provide concrete implementations for writing/reading if needed.

    Methods to complete:
    - lookup_keys: list of column names used to identify unique records
    - merge_keys: list of column names used to match records when merging
    """

    def __init__(self):
       super().__init__()

    def partition_keys(self) -> Set[str]:
        pass

    def merge_keys(self) -> Set[str]:
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
        if date_expr is not None and alias is not None:
            return date_expr.cast(DateType()).alias(f"{alias}.{self.start_date_column_ref()}")
        if date_expr is not None and alias is None:
            return date_expr.cast(DateType()).alias(self.start_date_column_ref())
        if date_expr is None and alias is not None:
            return col(self.start_date_column_ref()).alias(f"{alias}.{self.start_date_column_ref()}")
        else:
            return col(self.start_date_column_ref())

    def end_date_column(self, date_expr: Optional[Column] = None, alias: Optional[str] = None) -> Column:
        if date_expr is not None and alias is not None:
            return date_expr.cast(DateType()).alias(f"{alias}.{self.end_date_column_ref()}")
        if date_expr is not None and alias is None:
            return date_expr.cast(DateType()).alias(self.end_date_column_ref())
        if date_expr is None and alias is not None:
            return col(self.end_date_column_ref()).alias(f"{alias}.{self.end_date_column_ref()}")
        else:
            return col(self.end_date_column_ref())

    def merge_condition(self) -> Column:
        condition = (
            self.merge_keys()
                .map(lambda key: col(f"{self.target_alias()}.{key}") == col(f"{self.updates_alias()}.{key}"))
                .reduce(lambda a, b: a & b)
        )   
        return condition

    def current_entities(self, **paritions: Column) -> DataFrame:
        target = (
            self.read()
            .filter(self.end_date_column() == lit(None))
            .filter(*[col(field) == value for field, value in paritions.items()])
        )
        return target

    def historic_entities(self, partitions: Column) -> DataFrame:
        """
        Processes previosly expired data from the target patitions. These values will remain unchanged.
        They must still be includeded in the merge operation using dynamic parition
        overwrite mode, while partitioning by the partion keys.
        """
        target = (
            self.read()
                .filter(self.end_date_column().isNotNull)
                .filter(*[col(field) == value for field, value in paritions.items()])
        )
        return target
    
    def matched_current_entities(self, updates: DataFrame, **paritions: Column) -> DataFrame:
        """
        Processes entires that exisit in both the target and the updates. As all of
        the values match for each merge key, each entry will be extended which is relected
        by a null end_date.
        """
        target = self.current_entities(**paritions)
        to_merge = (
            target.alias(self.target_alias())
            .join(
                updates.alias(self.updates_alias()),
                self.merge_condition(), 
                how="inner"
                )
            .select(
                *self.partition_columns(self.target_alias()), 
                *self.merge_columns(self.target_alias()),
                self.start_date_column(alias=self.target_alias()),
                self.end_date_column(date_expr=lit(None))
                )
        ) 
        return to_merge

    def new_current_entities(self, updates: DataFrame, **paritions: Column) -> DataFrame:
        """
        Processes entries that are in the updates dataframe but not in the target
        """
        target = self.current_entities(**paritions)
        to_merge = (
            updates
                .alias(self.updates_alias())
                .join(
                    target.alias(self.target_alias()),
                    self.merge_condition(),
                    "leftanti")
            .select(
                *self.partition_columns(self.updates_alias()), 
                *self.merge_columns(self.updates_alias()),
                self.start_date_column(date_expr=current_date()),
                self.end_date_column(date_expr=lit(None))
                )
        )
        return to_merge

    def expiring_entitites(self, updates: DataFrame, **partitions: Column) -> DataFrame:
        """
            Proceses values in the target, that have new dimension values to populate
            from the updates DataFrame. These values will expire and the end_date_column
            being set to the current date.
        """
        target = self.current_entities(**partitions)
        to_merge = (
            target.alias(self.target_alias())
            .join(
                updates.alias(self.updates_alias()),
                self.merge_condition(), 
                how="leftanti"
                )
            .select(
                *self.partition_columns(self.target_alias()), 
                *self.merge_columns(self.target_alias()),
                self.start_date_column(alias=self.target_alias()),
                self.end_date_column(date_expr=current_date())
                )
        ) 
        return to_merge
        
    def merge(self, updates: DataFrame, **partitions: Column) -> DataFrame:
        merged = (
            self.historic_entities(**partitions)
                .union(self.matched_current_entities(updates, **partitions))
                .union(self.new_current_entities(updates, **partitions))
                .union(self.expiring_entitites(updates, **partitions))
                
        )

        merged.write\
            .format(self.format())\
            .mode("overwrite")\
            .partitionBy(list(self.partition_keys))\
            .option("partitionOverwriteMode", "dynamic")\
            .saveAsTable(self.uri())
        
        return merged.limit(100)

