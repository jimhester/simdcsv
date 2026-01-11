"""Tests for automatic type inference in Arrow export."""

import tempfile

import pytest

# Try to import pyarrow, skip tests if not available
try:
    import pyarrow as pa

    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False


@pytest.fixture
def integer_csv():
    """Create a CSV file with integer data."""
    content = "id,count,value\n1,100,42\n2,200,84\n3,300,126\n"
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(content)
        return f.name


@pytest.fixture
def double_csv():
    """Create a CSV file with double/float data."""
    content = "x,y,z\n1.5,2.5,3.5\n4.0,5.0,6.0\n7.25,8.75,9.125\n"
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(content)
        return f.name


@pytest.fixture
def boolean_csv():
    """Create a CSV file with boolean data."""
    content = "active,verified,subscribed\ntrue,false,yes\nTrue,False,no\nTRUE,FALSE,YES\n"
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(content)
        return f.name


@pytest.fixture
def mixed_csv():
    """Create a CSV file with mixed types."""
    content = "name,age,score,active\nAlice,30,95.5,true\nBob,25,87.0,false\nCharlie,35,92.75,true\n"
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(content)
        return f.name


@pytest.fixture
def null_csv():
    """Create a CSV file with null values."""
    content = "id,value,status\n1,100,active\n2,NA,pending\n3,,inactive\n4,NULL,\n"
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(content)
        return f.name


@pytest.fixture
def promotion_csv():
    """Create a CSV file that tests type promotion.

    Note: 0 and 1 are treated as integers during type inference (not booleans)
    to avoid ambiguity. So:
    - col_int_double: 1, 2.5, 3 -> INT + DOUBLE = DOUBLE
    - col_bool_int: true, 5, 10 -> BOOL + INT = INT
    - col_bool_double: true, 1.5, false -> BOOL + DOUBLE = DOUBLE
    """
    content = "col_int_double,col_bool_int,col_bool_double\n1,true,true\n2.5,5,1.5\n3,10,false\n"
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(content)
        return f.name


class TestTypeInferenceBasic:
    """Basic tests for type inference functionality."""

    def test_integer_inference(self, integer_csv):
        """Test that integer columns are correctly inferred."""
        import vroom_csv

        table = vroom_csv.read_csv(integer_csv)
        # The table should be usable (columns should be inferred)
        assert table.num_rows == 3
        assert table.num_columns == 3

    def test_double_inference(self, double_csv):
        """Test that double columns are correctly inferred."""
        import vroom_csv

        table = vroom_csv.read_csv(double_csv)
        assert table.num_rows == 3
        assert table.num_columns == 3

    def test_boolean_inference(self, boolean_csv):
        """Test that boolean columns are correctly inferred."""
        import vroom_csv

        table = vroom_csv.read_csv(boolean_csv)
        assert table.num_rows == 3
        assert table.num_columns == 3

    def test_infer_types_disabled(self, integer_csv):
        """Test that inference can be disabled."""
        import vroom_csv

        table = vroom_csv.read_csv(integer_csv, infer_types=False)
        assert table.num_rows == 3
        assert table.num_columns == 3


@pytest.mark.skipif(not HAS_PYARROW, reason="pyarrow not installed")
class TestTypeInferenceArrow:
    """Tests for type inference with PyArrow conversion."""

    def test_integer_arrow_type(self, integer_csv):
        """Test that integer columns have int64 Arrow type."""
        import pyarrow as pa

        import vroom_csv

        table = vroom_csv.read_csv(integer_csv)
        arrow_table = pa.table(table)

        # All columns should be int64
        for col in arrow_table.columns:
            assert pa.types.is_int64(col.type), f"Expected int64, got {col.type}"

        # Check values
        assert arrow_table.column("id").to_pylist() == [1, 2, 3]
        assert arrow_table.column("count").to_pylist() == [100, 200, 300]
        assert arrow_table.column("value").to_pylist() == [42, 84, 126]

    def test_double_arrow_type(self, double_csv):
        """Test that double columns have float64 Arrow type."""
        import pyarrow as pa

        import vroom_csv

        table = vroom_csv.read_csv(double_csv)
        arrow_table = pa.table(table)

        # All columns should be float64
        for col in arrow_table.columns:
            assert pa.types.is_float64(col.type), f"Expected float64, got {col.type}"

        # Check values
        assert arrow_table.column("x").to_pylist() == [1.5, 4.0, 7.25]
        assert arrow_table.column("y").to_pylist() == [2.5, 5.0, 8.75]

    def test_boolean_arrow_type(self, boolean_csv):
        """Test that boolean columns have bool Arrow type."""
        import pyarrow as pa

        import vroom_csv

        table = vroom_csv.read_csv(boolean_csv)
        arrow_table = pa.table(table)

        # All columns should be boolean
        for col in arrow_table.columns:
            assert pa.types.is_boolean(col.type), f"Expected boolean, got {col.type}"

        # Check values
        assert arrow_table.column("active").to_pylist() == [True, True, True]
        assert arrow_table.column("verified").to_pylist() == [False, False, False]

    def test_string_preserved(self, mixed_csv):
        """Test that string columns remain strings."""
        import pyarrow as pa

        import vroom_csv

        table = vroom_csv.read_csv(mixed_csv)
        arrow_table = pa.table(table)

        # name should be string
        assert pa.types.is_string(arrow_table.column("name").type)
        assert arrow_table.column("name").to_pylist() == ["Alice", "Bob", "Charlie"]

        # age should be int64
        assert pa.types.is_int64(arrow_table.column("age").type)
        assert arrow_table.column("age").to_pylist() == [30, 25, 35]

        # score should be float64
        assert pa.types.is_float64(arrow_table.column("score").type)

        # active should be boolean
        assert pa.types.is_boolean(arrow_table.column("active").type)

    def test_infer_types_disabled_arrow(self, integer_csv):
        """Test that disabling inference produces string columns."""
        import pyarrow as pa

        import vroom_csv

        table = vroom_csv.read_csv(integer_csv, infer_types=False)
        arrow_table = pa.table(table)

        # All columns should be strings when inference is disabled
        for col in arrow_table.columns:
            assert pa.types.is_string(col.type), f"Expected string, got {col.type}"

        # Values should be string representations
        assert arrow_table.column("id").to_pylist() == ["1", "2", "3"]

    def test_null_handling(self, null_csv):
        """Test that null values are handled correctly."""
        import pyarrow as pa

        import vroom_csv

        table = vroom_csv.read_csv(null_csv)
        arrow_table = pa.table(table)

        # id should be int64 (no nulls in that column)
        assert pa.types.is_int64(arrow_table.column("id").type)

        # value column should handle nulls
        value_col = arrow_table.column("value")
        assert value_col.null_count >= 2  # NA, empty, and NULL are null values

    def test_type_promotion_int_double(self, promotion_csv):
        """Test that INT64 + DOUBLE promotes to DOUBLE."""
        import pyarrow as pa

        import vroom_csv

        table = vroom_csv.read_csv(promotion_csv)
        arrow_table = pa.table(table)

        # col_int_double has 1, 2.5, 3 -> should be DOUBLE
        col = arrow_table.column("col_int_double")
        assert pa.types.is_float64(col.type), f"Expected float64, got {col.type}"

    def test_type_promotion_bool_int(self, promotion_csv):
        """Test that BOOLEAN + INT64 promotes to INT64."""
        import pyarrow as pa

        import vroom_csv

        table = vroom_csv.read_csv(promotion_csv)
        arrow_table = pa.table(table)

        # col_bool_int has true, 5, 10 -> should be INT64
        # (0 and 1 are treated as integers during inference to avoid ambiguity)
        col = arrow_table.column("col_bool_int")
        assert pa.types.is_int64(col.type), f"Expected int64, got {col.type}"

    def test_type_promotion_bool_double(self, promotion_csv):
        """Test that BOOLEAN + DOUBLE promotes to DOUBLE."""
        import pyarrow as pa

        import vroom_csv

        table = vroom_csv.read_csv(promotion_csv)
        arrow_table = pa.table(table)

        # col_bool_double has true, 1.5, false -> should be DOUBLE
        col = arrow_table.column("col_bool_double")
        assert pa.types.is_float64(col.type), f"Expected float64, got {col.type}"

    def test_type_inference_rows_parameter(self, integer_csv):
        """Test that type_inference_rows parameter works."""
        import pyarrow as pa

        import vroom_csv

        # Using just 1 row should still work
        table = vroom_csv.read_csv(integer_csv, type_inference_rows=1)
        arrow_table = pa.table(table)

        # Should still infer int64
        assert pa.types.is_int64(arrow_table.column("id").type)

    def test_dtype_explicit(self, mixed_csv):
        """Test explicit dtype specification."""
        import pyarrow as pa

        import vroom_csv

        # Force age to be string, let others be inferred
        table = vroom_csv.read_csv(mixed_csv, dtype={"age": "string"})
        arrow_table = pa.table(table)

        # age should be string (explicitly set)
        assert pa.types.is_string(arrow_table.column("age").type)

        # score should still be float64 (inferred)
        assert pa.types.is_float64(arrow_table.column("score").type)

    def test_dtype_with_inference_disabled(self, mixed_csv):
        """Test dtype with inference disabled for other columns."""
        import pyarrow as pa

        import vroom_csv

        # Force age to int64, disable inference for others
        table = vroom_csv.read_csv(mixed_csv, dtype={"age": "int64"}, infer_types=False)
        arrow_table = pa.table(table)

        # age should be int64 (explicitly set)
        assert pa.types.is_int64(arrow_table.column("age").type)

        # others should be string (no inference)
        assert pa.types.is_string(arrow_table.column("name").type)
        assert pa.types.is_string(arrow_table.column("score").type)
        assert pa.types.is_string(arrow_table.column("active").type)

    def test_default_behavior(self, mixed_csv):
        """Test that default behavior includes type inference."""
        import pyarrow as pa

        import vroom_csv

        # Default should infer types
        table = vroom_csv.read_csv(mixed_csv)
        arrow_table = pa.table(table)

        # Should have typed columns by default
        assert pa.types.is_string(arrow_table.column("name").type)
        assert pa.types.is_int64(arrow_table.column("age").type)
        assert pa.types.is_float64(arrow_table.column("score").type)
        assert pa.types.is_boolean(arrow_table.column("active").type)
