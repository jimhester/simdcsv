# vroom-csv Examples

Interactive Jupyter notebooks demonstrating vroom-csv usage.

## Notebooks

| Notebook | Description |
|----------|-------------|
| [getting_started.ipynb](getting_started.ipynb) | Basic usage: reading CSV files, accessing data, dialect detection |
| [arrow_interop.ipynb](arrow_interop.ipynb) | Arrow integration: PyArrow, Polars, DuckDB interoperability |

## Running the Examples

```bash
# Install Jupyter and dependencies
pip install vroom-csv[arrow] jupyter polars duckdb

# Start Jupyter
jupyter notebook
```

## Requirements

- Python 3.9+
- vroom-csv
- pyarrow (for Arrow examples)
- polars (for Polars examples)
- duckdb (for DuckDB examples)
