"""Tests for the current stub API after libvroom2 migration.

These tests verify only the functionality that's actually implemented
in the stub bindings.
"""

import tempfile
import os

import pytest


@pytest.fixture
def simple_csv():
    """Create a simple CSV file for testing."""
    content = "name,age,city\nAlice,30,New York\nBob,25,Los Angeles\nCharlie,35,Chicago\n"
    f = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
    f.write(content)
    f.close()
    yield f.name
    os.unlink(f.name)


@pytest.fixture
def tsv_file():
    """Create a TSV file for testing."""
    content = "name\tage\tcity\nAlice\t30\tNew York\nBob\t25\tLos Angeles\nCharlie\t35\tChicago\n"
    f = tempfile.NamedTemporaryFile(mode="w", suffix=".tsv", delete=False)
    f.write(content)
    f.close()
    yield f.name
    os.unlink(f.name)


@pytest.fixture
def no_header_csv():
    """Create a CSV file without header."""
    content = "Alice,30,New York\nBob,25,Los Angeles\nCharlie,35,Chicago\n"
    f = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
    f.write(content)
    f.close()
    yield f.name
    os.unlink(f.name)


class TestReadCsv:
    """Tests for read_csv function."""

    def test_read_simple_csv(self, simple_csv):
        """Test reading a simple CSV file."""
        import vroom_csv

        table = vroom_csv.read_csv(simple_csv)

        assert table.num_rows == 3
        assert table.num_columns == 3
        assert table.column_names == ["name", "age", "city"]

    def test_read_with_separator(self, tsv_file):
        """Test reading with explicit separator parameter."""
        import vroom_csv

        table = vroom_csv.read_csv(tsv_file, separator="\t")

        assert table.num_rows == 3
        assert table.num_columns == 3
        assert table.column_names == ["name", "age", "city"]

    def test_read_without_header(self, no_header_csv):
        """Test reading a file without header."""
        import vroom_csv

        table = vroom_csv.read_csv(no_header_csv, has_header=False)

        assert table.num_rows == 3
        assert table.num_columns == 3

    def test_read_nonexistent_file(self):
        """Test error handling for non-existent file."""
        import vroom_csv

        with pytest.raises(RuntimeError):
            vroom_csv.read_csv("/nonexistent/path/to/file.csv")


class TestToParquet:
    """Tests for to_parquet function."""

    def test_basic_conversion(self, simple_csv):
        """Test basic CSV to Parquet conversion."""
        import vroom_csv

        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            output_path = f.name

        try:
            vroom_csv.to_parquet(simple_csv, output_path)
            assert os.path.exists(output_path)
            assert os.path.getsize(output_path) > 0
        finally:
            if os.path.exists(output_path):
                os.unlink(output_path)

    def test_compression_zstd(self, simple_csv):
        """Test conversion with ZSTD compression."""
        import vroom_csv

        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            output_path = f.name

        try:
            vroom_csv.to_parquet(simple_csv, output_path, compression="zstd")
            assert os.path.exists(output_path)
        finally:
            if os.path.exists(output_path):
                os.unlink(output_path)

    def test_compression_snappy(self, simple_csv):
        """Test conversion with Snappy compression."""
        import vroom_csv

        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            output_path = f.name

        try:
            vroom_csv.to_parquet(simple_csv, output_path, compression="snappy")
            assert os.path.exists(output_path)
        finally:
            if os.path.exists(output_path):
                os.unlink(output_path)

    def test_compression_none(self, simple_csv):
        """Test conversion with no compression."""
        import vroom_csv

        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as f:
            output_path = f.name

        try:
            vroom_csv.to_parquet(simple_csv, output_path, compression="none")
            assert os.path.exists(output_path)
        finally:
            if os.path.exists(output_path):
                os.unlink(output_path)

    def test_nonexistent_input(self):
        """Test error for non-existent input file."""
        import vroom_csv

        with pytest.raises(RuntimeError):
            vroom_csv.to_parquet("/nonexistent/file.csv", "/tmp/output.parquet")


class TestExceptions:
    """Tests for exception types."""

    def test_vroom_error_exists(self):
        """Test VroomError exception exists."""
        import vroom_csv

        assert hasattr(vroom_csv, "VroomError")

    def test_parse_error_exists(self):
        """Test ParseError exception exists."""
        import vroom_csv

        assert hasattr(vroom_csv, "ParseError")

    def test_io_error_exists(self):
        """Test IOError exception exists."""
        import vroom_csv

        assert hasattr(vroom_csv, "IOError")


class TestVersion:
    """Tests for version information."""

    def test_version_exists(self):
        """Test that version is exposed."""
        import vroom_csv

        assert hasattr(vroom_csv, "__version__")
        assert isinstance(vroom_csv.__version__, str)
