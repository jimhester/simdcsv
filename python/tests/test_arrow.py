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

    def test_pyarrow_column_types_with_type_inference(self, simple_csv):
        """Test that columns have inferred types in PyArrow."""
        import pyarrow as pa

        import vroom_csv

        table = vroom_csv.read_csv(simple_csv)
        arrow_table = pa.table(table)

        # name and city should be string (contain text)
        assert pa.types.is_string(arrow_table.schema.field("name").type)
        assert pa.types.is_string(arrow_table.schema.field("city").type)
        # age should be int64 (numeric)
        assert pa.types.is_int64(arrow_table.schema.field("age").type)

    def test_pyarrow_column_types_without_type_inference(self, simple_csv):
        """Test that all columns are strings when type inference is disabled."""
        import pyarrow as pa

        import vroom_csv

        table = vroom_csv.read_csv(simple_csv, infer_types=False)
        arrow_table = pa.table(table)

        # All columns should be string type when type inference is disabled
        for col in arrow_table.columns:
            assert pa.types.is_string(col.type) or pa.types.is_large_string(col.type)

    def test_pyarrow_data_values_with_type_inference(self, simple_csv):
        """Test that data values are correctly transferred with types."""
        import pyarrow as pa

        import vroom_csv

        table = vroom_csv.read_csv(simple_csv)
        arrow_table = pa.table(table)

        names = arrow_table.column("name").to_pylist()
        assert names == ["Alice", "Bob", "Charlie"]

        # age is now int64, not string
        ages = arrow_table.column("age").to_pylist()
        assert ages == [30, 25, 35]

        cities = arrow_table.column("city").to_pylist()
        assert cities == ["New York", "Los Angeles", "Chicago"]

    def test_pyarrow_data_values_without_type_inference(self, simple_csv):
        """Test that data values are strings when type inference is disabled."""
        import pyarrow as pa

        import vroom_csv

        table = vroom_csv.read_csv(simple_csv, infer_types=False)
        arrow_table = pa.table(table)

        names = arrow_table.column("name").to_pylist()
        assert names == ["Alice", "Bob", "Charlie"]

        # age is string when type inference is disabled
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

    def test_polars_data_values_with_type_inference(self, simple_csv):
        """Test that data values are correctly transferred to Polars with types."""
        import polars as pl

        import vroom_csv

        table = vroom_csv.read_csv(simple_csv)
        df = pl.from_arrow(table)

        names = df["name"].to_list()
        assert names == ["Alice", "Bob", "Charlie"]

        # age is int64 with type inference
        ages = df["age"].to_list()
        assert ages == [30, 25, 35]

    def test_polars_data_values_without_type_inference(self, simple_csv):
        """Test that data values are strings in Polars when type inference is disabled."""
        import polars as pl

        import vroom_csv

        table = vroom_csv.read_csv(simple_csv, infer_types=False)
        df = pl.from_arrow(table)

        names = df["name"].to_list()
        assert names == ["Alice", "Bob", "Charlie"]

        # age is string when type inference is disabled
        ages = df["age"].to_list()
        assert ages == ["30", "25", "35"]


# =============================================================================
# Type Inference Tests
# =============================================================================


@pytest.fixture
def typed_csv():
    """Create a CSV file with various types for testing."""
    content = (
        "id,value,price,flag,name\n"
        "1,100,10.5,true,Alice\n"
        "2,200,20.75,false,Bob\n"
        "3,300,30.0,true,Charlie\n"
    )
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(content)
        return f.name


@pytest.fixture
def double_csv():
    """Create a CSV file with double values."""
    content = "value\n1.5\n2.7\n3.14\n"
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(content)
        return f.name


@pytest.fixture
def boolean_csv():
    """Create a CSV file with boolean values."""
    content = "flag\ntrue\nfalse\nTrue\nFalse\n"
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(content)
        return f.name


@pytest.fixture
def null_csv():
    """Create a CSV file with null values."""
    content = "id,value\n1,100\n2,NA\n3,\n4,NULL\n"
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(content)
        return f.name


@pytest.fixture
def mixed_int_double_csv():
    """Create a CSV file with mixed int and double values."""
    content = "value\n1\n2.5\n3\n"
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(content)
        return f.name


@pytest.fixture
def mixed_type_csv():
    """Create a CSV file with mixed numeric and string values."""
    content = "value\n1\nhello\n3\n"
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(content)
        return f.name


@pytest.mark.skipif(not HAS_PYARROW, reason="pyarrow not installed")
class TestTypeInference:
    """Tests for automatic type inference."""

    def test_infer_int64_column(self, typed_csv):
        """Test that integer columns are correctly inferred as int64."""
        import pyarrow as pa

        import vroom_csv

        table = vroom_csv.read_csv(typed_csv)
        arrow_table = pa.table(table)

        assert pa.types.is_int64(arrow_table.schema.field("id").type)
        assert pa.types.is_int64(arrow_table.schema.field("value").type)

        ids = arrow_table.column("id").to_pylist()
        assert ids == [1, 2, 3]

        values = arrow_table.column("value").to_pylist()
        assert values == [100, 200, 300]

    def test_infer_double_column(self, typed_csv):
        """Test that float columns are correctly inferred as float64."""
        import pyarrow as pa

        import vroom_csv

        table = vroom_csv.read_csv(typed_csv)
        arrow_table = pa.table(table)

        assert pa.types.is_float64(arrow_table.schema.field("price").type)

        prices = arrow_table.column("price").to_pylist()
        assert prices == [10.5, 20.75, 30.0]

    def test_infer_boolean_column(self, typed_csv):
        """Test that boolean columns are correctly inferred."""
        import pyarrow as pa

        import vroom_csv

        table = vroom_csv.read_csv(typed_csv)
        arrow_table = pa.table(table)

        assert pa.types.is_boolean(arrow_table.schema.field("flag").type)

        flags = arrow_table.column("flag").to_pylist()
        assert flags == [True, False, True]

    def test_infer_string_column(self, typed_csv):
        """Test that string columns remain as strings."""
        import pyarrow as pa

        import vroom_csv

        table = vroom_csv.read_csv(typed_csv)
        arrow_table = pa.table(table)

        assert pa.types.is_string(arrow_table.schema.field("name").type)

        names = arrow_table.column("name").to_pylist()
        assert names == ["Alice", "Bob", "Charlie"]

    def test_pure_double_column(self, double_csv):
        """Test inference of a pure double column."""
        import pyarrow as pa

        import vroom_csv

        table = vroom_csv.read_csv(double_csv)
        arrow_table = pa.table(table)

        assert pa.types.is_float64(arrow_table.schema.field("value").type)

        values = arrow_table.column("value").to_pylist()
        assert values == [1.5, 2.7, 3.14]

    def test_pure_boolean_column(self, boolean_csv):
        """Test inference of a pure boolean column."""
        import pyarrow as pa

        import vroom_csv

        table = vroom_csv.read_csv(boolean_csv)
        arrow_table = pa.table(table)

        assert pa.types.is_boolean(arrow_table.schema.field("flag").type)

        flags = arrow_table.column("flag").to_pylist()
        assert flags == [True, False, True, False]

    def test_null_values_with_int(self, null_csv):
        """Test that null values are properly handled with int columns."""
        import pyarrow as pa

        import vroom_csv

        table = vroom_csv.read_csv(null_csv)
        arrow_table = pa.table(table)

        # value column should be int64 with nulls
        assert pa.types.is_int64(arrow_table.schema.field("value").type)

        values = arrow_table.column("value").to_pylist()
        assert values == [100, None, None, None]

    def test_mixed_int_double_promotes_to_double(self, mixed_int_double_csv):
        """Test that mixed int/double columns promote to double."""
        import pyarrow as pa

        import vroom_csv

        table = vroom_csv.read_csv(mixed_int_double_csv)
        arrow_table = pa.table(table)

        # Should be promoted to float64
        assert pa.types.is_float64(arrow_table.schema.field("value").type)

        values = arrow_table.column("value").to_pylist()
        assert values == [1.0, 2.5, 3.0]

    def test_mixed_numeric_string_becomes_string(self, mixed_type_csv):
        """Test that mixed numeric and string columns become string."""
        import pyarrow as pa

        import vroom_csv

        table = vroom_csv.read_csv(mixed_type_csv)
        arrow_table = pa.table(table)

        # Should fall back to string
        assert pa.types.is_string(arrow_table.schema.field("value").type)

        values = arrow_table.column("value").to_pylist()
        assert values == ["1", "hello", "3"]

    def test_type_inference_rows_parameter(self, typed_csv):
        """Test that type_inference_rows parameter works."""
        import pyarrow as pa

        import vroom_csv

        # With type_inference_rows=0 (all rows), should still work
        table = vroom_csv.read_csv(typed_csv, type_inference_rows=0)
        arrow_table = pa.table(table)

        assert pa.types.is_int64(arrow_table.schema.field("id").type)
        assert pa.types.is_float64(arrow_table.schema.field("price").type)
        assert pa.types.is_boolean(arrow_table.schema.field("flag").type)

    def test_disable_type_inference(self, typed_csv):
        """Test that infer_types=False keeps all columns as strings."""
        import pyarrow as pa

        import vroom_csv

        table = vroom_csv.read_csv(typed_csv, infer_types=False)
        arrow_table = pa.table(table)

        # All columns should be strings
        for field in arrow_table.schema:
            assert pa.types.is_string(field.type), f"Expected string type for {field.name}"

        # Data should be strings
        ids = arrow_table.column("id").to_pylist()
        assert ids == ["1", "2", "3"]

        flags = arrow_table.column("flag").to_pylist()
        assert flags == ["true", "false", "true"]
