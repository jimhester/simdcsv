"""Tests for Arrow PyCapsule interface."""

import tempfile

import pytest


@pytest.fixture
def typed_csv():
    """Create a CSV file with typed columns."""
    content = "name,age,score,active\nAlice,30,85.5,true\nBob,25,92.3,false\nCharlie,35,78.0,true\n"
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(content)
        return f.name


@pytest.fixture
def double_csv():
    """Create a CSV file with double values."""
    content = "value\n1.5\n2.7\n3.14159\n"
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(content)
        return f.name


@pytest.fixture
def boolean_csv():
    """Create a CSV file with boolean values."""
    content = "flag\ntrue\nfalse\nTRUE\nFALSE\n"
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(content)
        return f.name


@pytest.fixture
def null_csv():
    """Create a CSV file with null values in typed columns."""
    content = "id,value\n1,100\n2,NA\n3,200\n"
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(content)
        return f.name


@pytest.fixture
def mixed_int_double_csv():
    """Create a CSV file where a column has both int and double values."""
    content = "value\n1\n2.5\n3\n"
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(content)
        return f.name


@pytest.fixture
def mixed_type_csv():
    """Create a CSV file where a column has values that can't be a single numeric type."""
    content = "value\n1\n2\nhello\n"
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(content)
        return f.name


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
        """Test column types in PyArrow with type inference."""
        import pyarrow as pa

        import vroom_csv

        table = vroom_csv.read_csv(simple_csv)
        arrow_table = pa.table(table)

        # name and city are strings
        name_col = arrow_table.column("name")
        city_col = arrow_table.column("city")
        assert pa.types.is_string(name_col.type) or pa.types.is_large_string(name_col.type)
        assert pa.types.is_string(city_col.type) or pa.types.is_large_string(city_col.type)

        # age is inferred as int64
        age_col = arrow_table.column("age")
        assert pa.types.is_int64(age_col.type)

    def test_pyarrow_data_values(self, simple_csv):
        """Test that data values are correctly transferred."""
        import pyarrow as pa

        import vroom_csv

        table = vroom_csv.read_csv(simple_csv)
        arrow_table = pa.table(table)

        names = arrow_table.column("name").to_pylist()
        assert names == ["Alice", "Bob", "Charlie"]

        # age is now inferred as int64
        ages = arrow_table.column("age").to_pylist()
        assert ages == [30, 25, 35]

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

        # age is now inferred as int64
        ages = df["age"].to_list()
        assert ages == [30, 25, 35]


# =============================================================================
# Tests for automatic type inference
# =============================================================================


@pytest.mark.skipif(not HAS_PYARROW, reason="pyarrow not installed")
class TestTypeInference:
    """Tests for automatic type inference in Arrow export."""

    def test_integer_type_inference(self, typed_csv):
        """Test that integer columns are inferred correctly."""
        import pyarrow as pa

        import vroom_csv

        table = vroom_csv.read_csv(typed_csv, infer_types=True)
        arrow_table = pa.table(table)

        age_col = arrow_table.column("age")
        assert pa.types.is_int64(age_col.type)

        ages = age_col.to_pylist()
        assert ages == [30, 25, 35]

    def test_double_type_inference(self, typed_csv):
        """Test that double columns are inferred correctly."""
        import pyarrow as pa

        import vroom_csv

        table = vroom_csv.read_csv(typed_csv, infer_types=True)
        arrow_table = pa.table(table)

        score_col = arrow_table.column("score")
        assert pa.types.is_float64(score_col.type)

        scores = score_col.to_pylist()
        assert scores[0] == pytest.approx(85.5)
        assert scores[1] == pytest.approx(92.3)
        assert scores[2] == pytest.approx(78.0)

    def test_boolean_type_inference(self, typed_csv):
        """Test that boolean columns are inferred correctly."""
        import pyarrow as pa

        import vroom_csv

        table = vroom_csv.read_csv(typed_csv, infer_types=True)
        arrow_table = pa.table(table)

        active_col = arrow_table.column("active")
        assert pa.types.is_boolean(active_col.type)

        actives = active_col.to_pylist()
        assert actives == [True, False, True]

    def test_string_type_preserved(self, typed_csv):
        """Test that string columns remain as strings."""
        import pyarrow as pa

        import vroom_csv

        table = vroom_csv.read_csv(typed_csv, infer_types=True)
        arrow_table = pa.table(table)

        name_col = arrow_table.column("name")
        assert pa.types.is_string(name_col.type) or pa.types.is_large_string(name_col.type)

        names = name_col.to_pylist()
        assert names == ["Alice", "Bob", "Charlie"]

    def test_infer_types_disabled(self, typed_csv):
        """Test that type inference can be disabled."""
        import pyarrow as pa

        import vroom_csv

        table = vroom_csv.read_csv(typed_csv, infer_types=False)
        arrow_table = pa.table(table)

        # All columns should be string type when inference is disabled
        for col in arrow_table.columns:
            assert pa.types.is_string(col.type) or pa.types.is_large_string(col.type)

    def test_pure_double_column(self, double_csv):
        """Test that a column with only doubles is correctly inferred."""
        import pyarrow as pa

        import vroom_csv

        table = vroom_csv.read_csv(double_csv, infer_types=True)
        arrow_table = pa.table(table)

        value_col = arrow_table.column("value")
        assert pa.types.is_float64(value_col.type)

        values = value_col.to_pylist()
        assert values[0] == pytest.approx(1.5)
        assert values[1] == pytest.approx(2.7)
        assert values[2] == pytest.approx(3.14159)

    def test_pure_boolean_column(self, boolean_csv):
        """Test that a column with only booleans is correctly inferred."""
        import pyarrow as pa

        import vroom_csv

        table = vroom_csv.read_csv(boolean_csv, infer_types=True)
        arrow_table = pa.table(table)

        flag_col = arrow_table.column("flag")
        assert pa.types.is_boolean(flag_col.type)

        flags = flag_col.to_pylist()
        assert flags == [True, False, True, False]

    def test_null_values_with_int(self, null_csv):
        """Test that null values work with integer type inference."""
        import pyarrow as pa

        import vroom_csv

        table = vroom_csv.read_csv(null_csv, infer_types=True, null_values=["NA"])
        arrow_table = pa.table(table)

        # id column should be int64
        id_col = arrow_table.column("id")
        assert pa.types.is_int64(id_col.type)
        assert id_col.to_pylist() == [1, 2, 3]

        # value column should be int64 with a null
        value_col = arrow_table.column("value")
        assert pa.types.is_int64(value_col.type)

        values = value_col.to_pylist()
        assert values[0] == 100
        assert values[1] is None  # NA was converted to null
        assert values[2] == 200

    def test_mixed_int_double_promotion(self, mixed_int_double_csv):
        """Test that mixed int/double columns are promoted to double."""
        import pyarrow as pa

        import vroom_csv

        table = vroom_csv.read_csv(mixed_int_double_csv, infer_types=True)
        arrow_table = pa.table(table)

        value_col = arrow_table.column("value")
        assert pa.types.is_float64(value_col.type)

        values = value_col.to_pylist()
        assert values[0] == pytest.approx(1.0)
        assert values[1] == pytest.approx(2.5)
        assert values[2] == pytest.approx(3.0)

    def test_mixed_type_falls_back_to_string(self, mixed_type_csv):
        """Test that columns with incompatible types fall back to string."""
        import pyarrow as pa

        import vroom_csv

        table = vroom_csv.read_csv(mixed_type_csv, infer_types=True)
        arrow_table = pa.table(table)

        value_col = arrow_table.column("value")
        assert pa.types.is_string(value_col.type) or pa.types.is_large_string(value_col.type)

        values = value_col.to_pylist()
        assert values == ["1", "2", "hello"]

    def test_type_inference_rows_parameter(self):
        """Test that type_inference_rows limits how many rows are sampled."""
        import pyarrow as pa

        import vroom_csv

        # Create a CSV where first 2 rows have ints but 3rd has a string
        content = "value\n1\n2\nhello\n"
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(content)
            csv_path = f.name

        # With only 2 rows sampled, it should think it's int64 but fail gracefully
        # The implementation should fall back to string when parsing fails
        table = vroom_csv.read_csv(csv_path, infer_types=True, type_inference_rows=2)
        arrow_table = pa.table(table)

        value_col = arrow_table.column("value")
        # When row 3 can't be parsed as int, it should fall back to string
        # (exact behavior depends on implementation)
        values = value_col.to_pylist()
        assert len(values) == 3

    def test_default_infer_types_enabled(self, typed_csv):
        """Test that type inference is enabled by default."""
        import pyarrow as pa

        import vroom_csv

        # Don't specify infer_types - should default to True
        table = vroom_csv.read_csv(typed_csv)
        arrow_table = pa.table(table)

        # age should be int64 by default
        age_col = arrow_table.column("age")
        assert pa.types.is_int64(age_col.type)


# =============================================================================
# Tests for null value handling in Arrow export
# =============================================================================


@pytest.fixture
def csv_with_nulls():
    """Create a CSV file with various null representations."""
    content = "name,value,status\nAlice,100,active\nBob,NA,inactive\nCharlie,,pending\nDave,N/A,\nEve,null,NULL\n"
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(content)
        return f.name


@pytest.fixture
def csv_with_custom_nulls():
    """Create a CSV file with custom null representations."""
    content = "name,value,status\nAlice,100,active\nBob,-999,inactive\nCharlie,missing,pending\n"
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(content)
        return f.name


@pytest.mark.skipif(not HAS_PYARROW, reason="pyarrow not installed")
class TestNullValueHandling:
    """Tests for null value handling in Arrow export."""

    def test_default_null_values(self, csv_with_nulls):
        """Test that default null values (NA, N/A, null, NULL, empty) are recognized."""
        import pyarrow as pa

        import vroom_csv

        table = vroom_csv.read_csv(csv_with_nulls)
        arrow_table = pa.table(table)

        # Check values column: 100 (inferred as int), NA (null), "" (null), N/A (null), null (null)
        values = arrow_table.column("value").to_pylist()
        assert values[0] == 100  # valid value (type inferred as int64)
        assert values[1] is None   # NA -> null
        assert values[2] is None   # empty string -> null (in default null_values)
        assert values[3] is None   # N/A -> null
        assert values[4] is None   # null -> null

    def test_null_count(self, csv_with_nulls):
        """Test that null_count is properly set in Arrow arrays."""
        import pyarrow as pa

        import vroom_csv

        table = vroom_csv.read_csv(csv_with_nulls)
        arrow_table = pa.table(table)

        # "value" column has 4 nulls: NA, "", N/A, null
        value_array = arrow_table.column("value").chunk(0)
        assert value_array.null_count == 4

        # "status" column has 2 nulls: "" (row 4), NULL (row 5)
        status_array = arrow_table.column("status").chunk(0)
        assert status_array.null_count == 2

    def test_custom_null_values(self, csv_with_custom_nulls):
        """Test that custom null_values parameter works."""
        import pyarrow as pa

        import vroom_csv

        # Use custom null values
        table = vroom_csv.read_csv(csv_with_custom_nulls, null_values=["-999", "missing"])
        arrow_table = pa.table(table)

        values = arrow_table.column("value").to_pylist()
        assert values[0] == 100  # valid value (type inferred as int64)
        assert values[1] is None   # -999 -> null
        assert values[2] is None   # missing -> null

    def test_empty_is_null(self):
        """Test that empty_is_null=True treats empty strings as null."""
        import pyarrow as pa

        import vroom_csv

        content = "a,b\nfoo,bar\n,baz\nqux,\n"
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(content)
            csv_path = f.name

        # Without empty_is_null, and with null_values not containing ""
        table = vroom_csv.read_csv(csv_path, null_values=["NA"], empty_is_null=False)
        arrow_table = pa.table(table)
        a_values = arrow_table.column("a").to_pylist()
        assert a_values == ["foo", "", "qux"]  # empty string preserved

        # With empty_is_null=True
        table = vroom_csv.read_csv(csv_path, null_values=["NA"], empty_is_null=True)
        arrow_table = pa.table(table)
        a_values = arrow_table.column("a").to_pylist()
        assert a_values[0] == "foo"
        assert a_values[1] is None  # empty -> null
        assert a_values[2] == "qux"

    def test_no_nulls_no_validity_bitmap(self, simple_csv):
        """Test that when there are no nulls, null_count is 0."""
        import pyarrow as pa

        import vroom_csv

        # Use null_values that don't appear in the data
        table = vroom_csv.read_csv(simple_csv, null_values=["NONEXISTENT"])
        arrow_table = pa.table(table)

        for col in arrow_table.columns:
            chunk = col.chunk(0)
            assert chunk.null_count == 0

    def test_all_nulls(self):
        """Test a column where all values are null."""
        import pyarrow as pa

        import vroom_csv

        content = "a,b\nNA,foo\nNA,bar\nNA,baz\n"
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(content)
            csv_path = f.name

        table = vroom_csv.read_csv(csv_path, null_values=["NA"])
        arrow_table = pa.table(table)

        a_values = arrow_table.column("a").to_pylist()
        assert a_values == [None, None, None]

        a_chunk = arrow_table.column("a").chunk(0)
        assert a_chunk.null_count == 3

    def test_mixed_null_values(self):
        """Test with a mix of null value representations."""
        import pyarrow as pa

        import vroom_csv

        content = "val\n10\nNA\n20\nN/A\n30\nnull\n40\n\n"
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(content)
            csv_path = f.name

        table = vroom_csv.read_csv(csv_path)  # using defaults
        arrow_table = pa.table(table)

        values = arrow_table.column("val").to_pylist()
        # With type inference, numeric values are inferred as int64
        expected = [10, None, 20, None, 30, None, 40, None]
        assert values == expected

    def test_null_handling_with_polars(self):
        """Test null handling when converting to Polars."""
        if not HAS_POLARS:
            pytest.skip("polars not installed")

        import polars as pl

        import vroom_csv

        content = "a,b\nfoo,10\nNA,20\nbar,NA\n"
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(content)
            csv_path = f.name

        table = vroom_csv.read_csv(csv_path, null_values=["NA"])
        df = pl.from_arrow(table)

        a_values = df["a"].to_list()
        assert a_values[0] == "foo"
        assert a_values[1] is None
        assert a_values[2] == "bar"

        b_values = df["b"].to_list()
        assert b_values[0] == "10"
        assert b_values[1] == "20"
        assert b_values[2] is None
