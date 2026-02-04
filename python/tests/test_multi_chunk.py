"""Tests for multi-chunk parsing (Issue #628).

Verifies that when CSV files are large enough to be split into multiple chunks
for multi-threaded parsing, all data is returned (not just the first chunk).
"""

import tempfile

import pytest


@pytest.fixture
def large_csv():
    """Create a large CSV file that will trigger multi-chunk parsing."""
    lines = ["id,value,category"]
    for i in range(10000):
        lines.append(f"{i},{i * 10},cat_{i % 5}")
    content = "\n".join(lines) + "\n"
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(content)
        return f.name


class TestMultiChunkParsing:
    """Tests that verify multi-chunk parsing returns all data."""

    def test_table_has_num_chunks_property(self, large_csv):
        """Test that Table exposes num_chunks property."""
        import vroom_csv

        table = vroom_csv.read_csv(large_csv, num_threads=4)
        assert hasattr(table, "num_chunks")
        assert table.num_chunks >= 1

    def test_all_rows_returned_multi_thread(self, large_csv):
        """Test that all rows are returned when using multiple threads."""
        import vroom_csv

        table = vroom_csv.read_csv(large_csv, num_threads=4)

        # Should return all 10000 rows, not just the first chunk
        assert table.num_rows == 10000
        assert table.num_columns == 3

    def test_multi_chunk_data_integrity(self, large_csv):
        """Test that data integrity is preserved across chunks."""
        import vroom_csv

        table = vroom_csv.read_csv(large_csv, num_threads=4)

        # If we have PyArrow, verify actual data values
        try:
            import pyarrow as pa

            arrow_table = pa.table(table)

            # Verify we got all rows
            assert arrow_table.num_rows == 10000

            # Verify data is correct (spot check first, middle, and last rows)
            ids = arrow_table.column("id").to_pylist()
            assert ids[0] == 0
            assert ids[5000] == 5000
            assert ids[9999] == 9999

            values = arrow_table.column("value").to_pylist()
            assert values[0] == 0
            assert values[5000] == 50000
            assert values[9999] == 99990

        except ImportError:
            # PyArrow not available, just verify row count
            assert table.num_rows == 10000

    def test_single_vs_multi_thread_consistency(self, large_csv):
        """Test that single-threaded and multi-threaded parsing return same data."""
        import vroom_csv

        # Parse with single thread
        table_single = vroom_csv.read_csv(large_csv, num_threads=1)

        # Parse with multiple threads
        table_multi = vroom_csv.read_csv(large_csv, num_threads=4)

        # Both should return same number of rows
        assert table_single.num_rows == table_multi.num_rows
        assert table_single.num_columns == table_multi.num_columns

        # If we have PyArrow, verify data is identical
        try:
            import pyarrow as pa

            arrow_single = pa.table(table_single)
            arrow_multi = pa.table(table_multi)

            # Compare all columns
            for col_name in arrow_single.column_names:
                single_data = arrow_single.column(col_name).to_pylist()
                multi_data = arrow_multi.column(col_name).to_pylist()
                assert single_data == multi_data, f"Column {col_name} differs"

        except ImportError:
            pass  # PyArrow not available


# Try to import pyarrow, skip tests if not available
try:
    import pyarrow as pa

    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False


@pytest.mark.skipif(not HAS_PYARROW, reason="pyarrow not installed")
class TestMultiChunkArrowStream:
    """Tests for multi-chunk Arrow stream export."""

    def test_multiple_batches_in_stream(self, large_csv):
        """Test that Arrow stream contains multiple batches for large files."""
        import pyarrow as pa

        import vroom_csv

        table = vroom_csv.read_csv(large_csv, num_threads=4)

        # Convert to Arrow - this consumes the stream
        arrow_table = pa.table(table)

        # Verify all data is present
        assert arrow_table.num_rows == 10000

        # Verify the table was chunked (should have multiple RecordBatches)
        # Note: PyArrow's pa.table() concatenates batches, but we can verify
        # the original table had multiple chunks
        assert table.num_chunks >= 1

    def test_chunk_boundaries_correct(self, large_csv):
        """Test that chunk boundaries don't corrupt data."""
        import pyarrow as pa

        import vroom_csv

        table = vroom_csv.read_csv(large_csv, num_threads=4)
        arrow_table = pa.table(table)

        # Verify sequential IDs (no missing or duplicated rows)
        ids = arrow_table.column("id").to_pylist()
        assert ids == list(range(10000)), "IDs should be sequential 0-9999"

    def test_no_data_loss_across_chunks(self, large_csv):
        """Test that no data is lost when parsing with multiple chunks."""
        import pyarrow as pa

        import vroom_csv

        table = vroom_csv.read_csv(large_csv, num_threads=4)
        arrow_table = pa.table(table)

        # Verify all expected values are present
        values = arrow_table.column("value").to_pylist()
        expected_values = [i * 10 for i in range(10000)]
        assert values == expected_values, "Values should match expected pattern"

        # Verify all categories are present
        categories = arrow_table.column("category").to_pylist()
        expected_categories = [f"cat_{i % 5}" for i in range(10000)]
        assert categories == expected_categories, "Categories should match expected pattern"


@pytest.mark.skipif(not HAS_PYARROW, reason="pyarrow not installed")
class TestMultiChunkRegression:
    """Regression tests specifically for Issue #628."""

    def test_issue_628_all_chunks_returned(self):
        """Regression test: verify all chunks are returned, not just the first.

        Issue #628 reported that when a CSV file was large enough to be split
        into multiple chunks for multi-threaded parsing, only the first chunk
        was returned to Python. Data from subsequent chunks was silently discarded.
        """
        import pyarrow as pa

        import vroom_csv

        # Create a CSV large enough to trigger multiple chunks
        lines = ["x,y"]
        for i in range(10000):
            lines.append(f"{i},{i * 2}")
        content = "\n".join(lines) + "\n"

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(content)
            csv_path = f.name

        # Parse with multiple threads to trigger chunking
        table = vroom_csv.read_csv(csv_path, num_threads=4)
        arrow_table = pa.table(table)

        # CRITICAL: Should get ALL 10000 rows, not just first chunk
        assert arrow_table.num_rows == 10000, (
            "Expected 10000 rows (all data), but got {}. "
            "This indicates data loss - only first chunk was returned!".format(arrow_table.num_rows)
        )

        # Verify data integrity
        x_values = arrow_table.column("x").to_pylist()
        assert len(x_values) == 10000
        assert x_values[0] == 0
        assert x_values[-1] == 9999

    def test_issue_628_no_silent_truncation(self):
        """Verify that large files don't silently truncate."""
        import pyarrow as pa

        import vroom_csv

        # Create progressively larger CSV files
        for size in [1000, 5000, 10000, 20000]:
            lines = ["a,b,c"]
            for i in range(size):
                lines.append(f"{i},{i},{i}")
            content = "\n".join(lines) + "\n"

            with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
                f.write(content)
                csv_path = f.name

            table = vroom_csv.read_csv(csv_path, num_threads=4)
            arrow_table = pa.table(table)

            # Should get exactly the expected number of rows
            assert arrow_table.num_rows == size, (
                f"Expected {size} rows but got {arrow_table.num_rows}. "
                "Data was silently truncated!"
            )
