"""Tests for Arrow PyCapsule interface."""

import tempfile

import pytest


@pytest.fixture
def simple_csv():
    """Create a simple CSV file for testing."""
    content = "name,age,city\nAlice,30,New York\nBob,25,Los Angeles\nCharlie,35,Chicago\n"
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(content)
        return f.name


class TestArrowCapsule:
    """Tests for Arrow PyCapsule interface methods."""

    def test_arrow_c_schema_method_exists(self, simple_csv):
        """Test that __arrow_c_schema__ method exists."""
        import vroom_csv

        table = vroom_csv.read_csv(simple_csv)
        assert hasattr(table, "__arrow_c_schema__")

    def test_arrow_c_stream_method_exists(self, simple_csv):
        """Test that __arrow_c_stream__ method exists."""
        import vroom_csv

        table = vroom_csv.read_csv(simple_csv)
        assert hasattr(table, "__arrow_c_stream__")

    def test_arrow_c_schema_returns_capsule(self, simple_csv):
        """Test that __arrow_c_schema__ returns a PyCapsule."""
        import vroom_csv

        table = vroom_csv.read_csv(simple_csv)
        capsule = table.__arrow_c_schema__()

        # Check it's a PyCapsule (limited checking available in pure Python)
        assert capsule is not None

    def test_arrow_c_stream_returns_capsule(self, simple_csv):
        """Test that __arrow_c_stream__ returns a PyCapsule."""
        import vroom_csv

        table = vroom_csv.read_csv(simple_csv)
        capsule = table.__arrow_c_stream__()

        assert capsule is not None


# Try to import pyarrow, skip tests if not available
try:
    import pyarrow as pa
    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False


@pytest.mark.skipif(not HAS_PYARROW, reason="pyarrow not installed")
class TestPyArrowInterop:
    """Tests for PyArrow interoperability."""

    def test_convert_to_pyarrow_table(self, simple_csv):
        """Test converting to PyArrow Table."""
        import pyarrow as pa

        import vroom_csv

        table = vroom_csv.read_csv(simple_csv)
        arrow_table = pa.table(table)

        assert arrow_table.num_rows == 3
        assert arrow_table.num_columns == 3
        assert arrow_table.column_names == ["name", "age", "city"]

    def test_pyarrow_column_types(self, simple_csv):
        """Test that columns are string type in PyArrow."""
        import pyarrow as pa

        import vroom_csv

        table = vroom_csv.read_csv(simple_csv)
        arrow_table = pa.table(table)

        # All columns should be string type (initial implementation)
        for col in arrow_table.columns:
            assert pa.types.is_string(col.type) or pa.types.is_large_string(col.type)

    def test_pyarrow_data_values(self, simple_csv):
        """Test that data values are correctly transferred."""
        import pyarrow as pa

        import vroom_csv

        table = vroom_csv.read_csv(simple_csv)
        arrow_table = pa.table(table)

        names = arrow_table.column("name").to_pylist()
        assert names == ["Alice", "Bob", "Charlie"]

        ages = arrow_table.column("age").to_pylist()
        assert ages == ["30", "25", "35"]

        cities = arrow_table.column("city").to_pylist()
        assert cities == ["New York", "Los Angeles", "Chicago"]


# Try to import polars, skip tests if not available
try:
    import polars as pl
    HAS_POLARS = True
except ImportError:
    HAS_POLARS = False


@pytest.mark.skipif(not HAS_POLARS, reason="polars not installed")
class TestPolarsInterop:
    """Tests for Polars interoperability."""

    def test_convert_to_polars_dataframe(self, simple_csv):
        """Test converting to Polars DataFrame."""
        import polars as pl

        import vroom_csv

        table = vroom_csv.read_csv(simple_csv)
        df = pl.from_arrow(table)

        assert df.shape == (3, 3)
        assert df.columns == ["name", "age", "city"]

    def test_polars_data_values(self, simple_csv):
        """Test that data values are correctly transferred to Polars."""
        import polars as pl

        import vroom_csv

        table = vroom_csv.read_csv(simple_csv)
        df = pl.from_arrow(table)

        names = df["name"].to_list()
        assert names == ["Alice", "Bob", "Charlie"]

        ages = df["age"].to_list()
        assert ages == ["30", "25", "35"]
