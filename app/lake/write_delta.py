import polars as pl
from config.object_storage import object_storage_options
from enum import Enum


class WriteMode(Enum):
    """
    Enumeration for the different write modes for a Delta table.

    Attributes:
        APPEND: Add new data to the existing table.
        OVERWRITE: Replace the entire table with the new data.
        IGNORE: If the table already exists, do nothing.
        ERROR: If the table already exists, raise an error.
    """
    APPEND = "append"
    OVERWRITE = "overwrite"
    IGNORE = "ignore"
    ERROR = "error"


class SchemaMode(Enum):
    """
    Enumeration for how to handle schema differences when writing to a Delta table.

    Attributes:
        OVERWRITE: Overwrite the existing schema with the new DataFrame's schema.
        MERGE: Merge the new schema with the existing schema.
    """
    OVERWRITE = "overwrite"
    MERGE = "merge"


def write_delta(
    df: pl.DataFrame,
    table: str,
    write_mode: WriteMode = WriteMode.APPEND,
    schema_mode: SchemaMode | None = None,
) -> None:
    """
    Writes a Polars DataFrame to a Delta Lake table.

    Args:
        df: The Polars DataFrame to write.
        table: The destination Delta table.
        mode: The write mode to use. Defaults to WriteMode.APPEND.
        schema_mode: The schema evolution mode to use. Defaults to None.
        delta_write_options: Additional options to pass to the Delta writer.
    """
    delta_write_options = {}

    if schema_mode:
        delta_write_options["schema_mode"] = schema_mode.value

    return df.write_delta(
        target=object_storage_options["DELTA_LAKE_URL"] + table,
        mode=write_mode.value,
        storage_options=object_storage_options,
        delta_write_options=delta_write_options,
    )
