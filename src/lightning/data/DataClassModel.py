"""
Mixin ABC for @dataclass-decorated record types providing collection-level
Spark/Delta bulk operations, driven entirely by the dataclass's own field
type hints (no hand-written DDL):

  - spark_schema(): derive a pyspark StructType from the dataclass fields.
  - to_dataframe(records): array of instances -> Spark DataFrame.
  - create_table() / insert(records) / delete_table(): manage the backing
    Unity Catalog Delta table (catalog.schema.table).

All capabilities are exposed as classmethods, not instance methods:
dataclass auto-generates __init__ for the concrete subclass, which would
silently discard any ABC __init__ attempting to set self.session (as
ReadOnlyDataSource/WritableDataSource do); and every operation here acts
on a collection of instances, not one instance's own state, so a
throwaway instance is never needed to invoke them.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import typing
from abc import ABC, abstractmethod
from typing import Any, Union

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DataType,
    DateType,
    DoubleType,
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


_PRIMITIVE_TYPE_MAP: dict[type, DataType] = {
    str: StringType(),
    int: IntegerType(),
    float: DoubleType(),
    bool: BooleanType(),
    dt.date: DateType(),
    dt.datetime: TimestampType(),
}
# Exact dict-key lookup (not isinstance/issubclass) is deliberate: dt.datetime
# is a subclass of dt.date, so an issubclass-based mapping would risk
# mis-routing datetime -> DateType.


def _is_optional(py_type: Any) -> tuple[bool, Any]:
    if typing.get_origin(py_type) is Union:
        args = typing.get_args(py_type)
        non_none = [a for a in args if a is not type(None)]
        if len(non_none) == 1 and type(None) in args:
            return True, non_none[0]
    return False, py_type


def _resolve_field_type(py_type: Any, field_name: str) -> tuple[DataType, bool]:
    nullable, inner_type = _is_optional(py_type)
    origin = typing.get_origin(inner_type)

    if origin is list:
        args = typing.get_args(inner_type)
        if len(args) != 1:
            raise TypeError(
                f"Unsupported type '{py_type}' for field '{field_name}': "
                f"list/List must be parameterized, e.g. list[str]."
            )
        element_type, element_nullable = _resolve_field_type(args[0], field_name)
        return ArrayType(element_type, containsNull=element_nullable), nullable

    if origin is dict:
        args = typing.get_args(inner_type)
        if len(args) != 2:
            raise TypeError(
                f"Unsupported type '{py_type}' for field '{field_name}': "
                f"dict/Dict must be parameterized, e.g. dict[str, int]."
            )
        key_type, _ = _resolve_field_type(args[0], field_name)
        value_type, value_nullable = _resolve_field_type(args[1], field_name)
        return MapType(key_type, value_type, valueContainsNull=value_nullable), nullable

    if dataclasses.is_dataclass(inner_type):
        return _dataclass_to_struct_type(inner_type), nullable

    if inner_type in _PRIMITIVE_TYPE_MAP:
        return _PRIMITIVE_TYPE_MAP[inner_type], nullable

    raise TypeError(
        f"Unsupported type '{py_type}' for field '{field_name}': cannot derive "
        f"a Spark schema. Supported: {', '.join(t.__name__ for t in _PRIMITIVE_TYPE_MAP)}, "
        f"Optional[...], list[...], dict[...], and nested @dataclass types."
    )


def _dataclass_to_struct_type(dc_type: type) -> StructType:
    if not dataclasses.is_dataclass(dc_type):
        raise TypeError(f"{dc_type!r} is not a dataclass; cannot derive a Spark schema.")

    # get_type_hints (not raw dataclasses.fields(...).type) is required so
    # string/forward-ref annotations resolve correctly, e.g. under
    # `from __future__ import annotations` in the subclass's own module.
    hints = typing.get_type_hints(dc_type)
    fields = []
    for f in dataclasses.fields(dc_type):
        field_type = hints.get(f.name, f.type)
        data_type, nullable = _resolve_field_type(field_type, f.name)
        fields.append(StructField(f.name, data_type, nullable=nullable))
    return StructType(fields)


class DataClassModel(ABC):
    """See module docstring. Mix into a @dataclass; do not instantiate to
    use its capabilities - call classmethods directly on the class."""

    @classmethod
    @abstractmethod
    def catalog(cls) -> str:
        """Unity Catalog catalog name that owns this table."""
        raise NotImplementedError(f"{cls.__name__} must implement catalog()")

    @classmethod
    @abstractmethod
    def schema_name(cls) -> str:
        """
        Unity Catalog schema (database) name that owns this table.

        Named schema_name(), not schema(): schema() is reserved elsewhere in
        this codebase (ReadOnlyDataSource) for this exact catalog-schema
        string, but on DataClassModel that name must stay free of ambiguity
        with the derived-StructType concept (spark_schema()). Do not rename
        this back to schema() for "consistency" with ReadOnlyDataSource -
        that would reintroduce the ambiguity this rename avoids.
        """
        raise NotImplementedError(f"{cls.__name__} must implement schema_name()")

    @classmethod
    @abstractmethod
    def table(cls) -> str:
        """Table name (unqualified)."""
        raise NotImplementedError(f"{cls.__name__} must implement table()")

    @classmethod
    def uri(cls) -> str:
        """Fully-qualified catalog.schema.table URI (mirrors ReadOnlyDataSource.uri())."""
        return f"{cls.catalog()}.{cls.schema_name()}.{cls.table()}"

    @classmethod
    def format(cls) -> str:
        """Storage format (mirrors WritableDataSource.format())."""
        return "delta"

    @classmethod
    def get_session(cls) -> SparkSession:
        """Mirrors SessionProvider.get_session(), as a classmethod."""
        return SparkSession.builder.getOrCreate()

    @classmethod
    def spark_schema(cls) -> StructType:
        """Derives this dataclass's pyspark StructType from its own fields/type hints."""
        return _dataclass_to_struct_type(cls)

    @classmethod
    def to_dataframe(cls, records: list) -> DataFrame:
        """Converts an array of dataclass instances into a Spark DataFrame,
        using the schema derived by spark_schema()."""
        rows = [dataclasses.asdict(r) for r in records]
        return cls.get_session().createDataFrame(rows, schema=cls.spark_schema())

    @classmethod
    def table_exists(cls) -> bool:
        """True if the Delta table already exists at uri()."""
        return DeltaTable.isDeltaTable(cls.get_session(), cls.uri())

    @classmethod
    def create_table(cls) -> None:
        """
        Creates the Delta table at uri() with the schema derived from this
        dataclass, if it does not already exist. Idempotent.
        """
        if not cls.table_exists():
            empty_df = cls.get_session().createDataFrame([], schema=cls.spark_schema())
            empty_df.write.format(cls.format()).saveAsTable(cls.uri())

    @classmethod
    def insert(cls, records: list, mode: str = "append") -> None:
        """
        Ensures the table exists (create_table() is idempotent) and writes
        `records` into it.
        """
        cls.create_table()
        df = cls.to_dataframe(records)
        df.write.format(cls.format()).mode(mode).saveAsTable(cls.uri())

    @classmethod
    def delete_table(cls) -> None:
        """DROP TABLE IF EXISTS - a full table drop, not a row-level delete."""
        cls.get_session().sql(f"DROP TABLE IF EXISTS {cls.uri()}")
