# vroom-csv

High-performance CSV parser with SIMD acceleration for Python.

## Features

- **SIMD-accelerated parsing** using Google Highway for portable vectorization
- **Multi-threaded parsing** for large files
- **Automatic dialect detection** (delimiter, quoting, line endings)
- **Arrow PyCapsule interface** for zero-copy interoperability with PyArrow, Polars, DuckDB

## Installation

```bash
pip install ./python
```

Or for development:

```bash
pip install -e ./python[dev]
```

## Quick Start

```python
import vroom_csv

# Read a CSV file
table = vroom_csv.read_csv("data.csv")

print(f"Loaded {table.num_rows} rows, {table.num_columns} columns")
print(f"Columns: {table.column_names}")

# Access data
names = table.column("name")
first_row = table.row(0)
```

## Arrow Interoperability

vroom-csv implements the Arrow PyCapsule interface for zero-copy data exchange:

### PyArrow

```python
import pyarrow as pa
import vroom_csv

table = vroom_csv.read_csv("data.csv")
arrow_table = pa.table(table)  # Zero-copy conversion

# Now use PyArrow's features
arrow_table.to_pandas()
```

### Polars

```python
import polars as pl
import vroom_csv

table = vroom_csv.read_csv("data.csv")
df = pl.from_arrow(table)  # Zero-copy conversion

# Now use Polars' features
df.filter(pl.col("age") > 30)
```

### DuckDB

```python
import duckdb
import vroom_csv

table = vroom_csv.read_csv("data.csv")
result = duckdb.query("SELECT * FROM table WHERE age > 30")
```

## API Reference

### `read_csv(path, delimiter=None, has_header=True, num_threads=1)`

Read a CSV file and return a Table object.

**Parameters:**
- `path` (str): Path to the CSV file
- `delimiter` (str, optional): Field delimiter character. Auto-detected if not specified
- `has_header` (bool): Whether the first row contains column headers (default: True)
- `num_threads` (int): Number of threads for parsing (default: 1)

**Returns:** `Table` object

### Table

The `Table` class represents parsed CSV data.

**Properties:**
- `num_rows`: Number of data rows (excluding header)
- `num_columns`: Number of columns
- `column_names`: List of column names

**Methods:**
- `column(index_or_name)`: Get column data as list of strings
- `row(index)`: Get row data as list of strings
- `has_errors()`: Check if any parse errors occurred
- `errors()`: Get list of parse error messages

**Arrow PyCapsule Methods:**
- `__arrow_c_schema__()`: Export schema via Arrow C Data Interface
- `__arrow_c_stream__()`: Export data via Arrow C Stream Interface

## License

MIT License - see LICENSE file in the repository root.
