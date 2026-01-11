"""
vroom-csv: High-performance CSV parser with SIMD acceleration.

This package provides Python bindings for the libvroom CSV parser, featuring:
- SIMD-accelerated parsing using Google Highway
- Multi-threaded parsing for large files
- Automatic dialect detection
- Arrow PyCapsule interface for zero-copy interoperability with PyArrow, Polars, DuckDB

Basic Usage
-----------
>>> import vroom_csv
>>> table = vroom_csv.read_csv("data.csv")
>>> print(f"Loaded {table.num_rows} rows, {table.num_columns} columns")

Dialect Detection
-----------------
>>> dialect = vroom_csv.detect_dialect("data.csv")
>>> print(f"Delimiter: {dialect.delimiter!r}, Has header: {dialect.has_header}")

Arrow Interoperability
----------------------
>>> import pyarrow as pa
>>> arrow_table = pa.table(vroom_csv.read_csv("data.csv"))

>>> import polars as pl
>>> df = pl.from_arrow(vroom_csv.read_csv("data.csv"))
"""

from vroom_csv._core import (
    BatchedReader,
    Dialect,
    RecordBatch,
    Table,
    detect_dialect,
    read_csv,
    read_csv_batched,
    VroomError,
    ParseError,
    IOError,
    __version__,
    LIBVROOM_VERSION,
)

__all__ = [
    "BatchedReader",
    "Dialect",
    "RecordBatch",
    "Table",
    "detect_dialect",
    "read_csv",
    "read_csv_batched",
    "VroomError",
    "ParseError",
    "IOError",
    "__version__",
    "LIBVROOM_VERSION",
]
