"""Basic tests for vroom-csv Python bindings."""

import tempfile

import pytest


@pytest.fixture
def simple_csv():
    """Create a simple CSV file for testing."""
    content = "name,age,city\nAlice,30,New York\nBob,25,Los Angeles\nCharlie,35,Chicago\n"
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(content)
        return f.name


@pytest.fixture
def tsv_file():
    """Create a TSV file for testing."""
    content = "name\tage\tcity\nAlice\t30\tNew York\nBob\t25\tLos Angeles\nCharlie\t35\tChicago\n"
    with tempfile.NamedTemporaryFile(mode="w", suffix=".tsv", delete=False) as f:
        f.write(content)
        return f.name


@pytest.fixture
def no_header_csv():
    """Create a CSV file without header."""
    content = "Alice,30,New York\nBob,25,Los Angeles\nCharlie,35,Chicago\n"
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(content)
        return f.name


class TestReadCsv:
    """Tests for read_csv function."""

    def test_read_simple_csv(self, simple_csv):
        """Test reading a simple CSV file."""
        import vroom_csv

        table = vroom_csv.read_csv(simple_csv)

        assert table.num_rows == 3
        assert table.num_columns == 3
        assert table.column_names == ["name", "age", "city"]

    def test_read_with_explicit_delimiter(self, tsv_file):
        """Test reading with explicit tab delimiter."""
        import vroom_csv

        table = vroom_csv.read_csv(tsv_file, delimiter="\t")

        assert table.num_rows == 3
        assert table.num_columns == 3
        assert table.column_names == ["name", "age", "city"]

    def test_read_without_header(self, no_header_csv):
        """Test reading a file without header."""
        import vroom_csv

        table = vroom_csv.read_csv(no_header_csv, has_header=False)

        assert table.num_rows == 3
        assert table.num_columns == 3
        # Should have auto-generated column names
        assert table.column_names == ["column_0", "column_1", "column_2"]

    def test_read_nonexistent_file(self):
        """Test error handling for non-existent file."""
        import vroom_csv

        with pytest.raises((ValueError, vroom_csv.VroomError)):
            vroom_csv.read_csv("/nonexistent/path/to/file.csv")

    def test_invalid_delimiter(self, simple_csv):
        """Test error for invalid delimiter."""
        import vroom_csv

        with pytest.raises(ValueError, match="single character"):
            vroom_csv.read_csv(simple_csv, delimiter=",,")

    def test_multi_threaded_parsing(self, simple_csv):
        """Test multi-threaded parsing works."""
        import vroom_csv

        table = vroom_csv.read_csv(simple_csv, num_threads=4)

        assert table.num_rows == 3
        assert table.num_columns == 3


class TestTable:
    """Tests for Table class."""

    def test_column_by_index(self, simple_csv):
        """Test getting column by index."""
        import vroom_csv

        table = vroom_csv.read_csv(simple_csv)

        names = table.column(0)
        assert names == ["Alice", "Bob", "Charlie"]

        ages = table.column(1)
        assert ages == ["30", "25", "35"]

    def test_column_by_name(self, simple_csv):
        """Test getting column by name."""
        import vroom_csv

        table = vroom_csv.read_csv(simple_csv)

        names = table.column("name")
        assert names == ["Alice", "Bob", "Charlie"]

        cities = table.column("city")
        assert cities == ["New York", "Los Angeles", "Chicago"]

    def test_column_not_found(self, simple_csv):
        """Test error for non-existent column."""
        import vroom_csv

        table = vroom_csv.read_csv(simple_csv)

        with pytest.raises(KeyError, match="nonexistent"):
            table.column("nonexistent")

    def test_column_index_out_of_range(self, simple_csv):
        """Test error for out-of-range column index."""
        import vroom_csv

        table = vroom_csv.read_csv(simple_csv)

        with pytest.raises(IndexError):
            table.column(100)

    def test_row(self, simple_csv):
        """Test getting row by index."""
        import vroom_csv

        table = vroom_csv.read_csv(simple_csv)

        row0 = table.row(0)
        assert row0 == ["Alice", "30", "New York"]

        row2 = table.row(2)
        assert row2 == ["Charlie", "35", "Chicago"]

    def test_row_index_out_of_range(self, simple_csv):
        """Test error for out-of-range row index."""
        import vroom_csv

        table = vroom_csv.read_csv(simple_csv)

        with pytest.raises(IndexError):
            table.row(100)

    def test_len(self, simple_csv):
        """Test __len__ returns num_rows."""
        import vroom_csv

        table = vroom_csv.read_csv(simple_csv)
        assert len(table) == 3

    def test_repr(self, simple_csv):
        """Test string representation."""
        import vroom_csv

        table = vroom_csv.read_csv(simple_csv)
        assert "3 rows" in repr(table)
        assert "3 columns" in repr(table)


class TestVersion:
    """Tests for version information."""

    def test_version_exists(self):
        """Test that version is exposed."""
        import vroom_csv

        assert hasattr(vroom_csv, "__version__")
        assert isinstance(vroom_csv.__version__, str)

    def test_libvroom_version_exists(self):
        """Test that libvroom version is exposed."""
        import vroom_csv

        assert hasattr(vroom_csv, "LIBVROOM_VERSION")
        assert isinstance(vroom_csv.LIBVROOM_VERSION, str)


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
