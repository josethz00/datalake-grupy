import polars as pl
from config.object_storage import object_storage_options


def scan_delta(table: str) -> pl.LazyFrame:
    return pl.scan_delta(
        source=object_storage_options['DELTA_LAKE_URL'] + table,
        storage_options=object_storage_options
    )
