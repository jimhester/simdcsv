"""Tests for read_csv_rows() streaming row-by-row iteration."""

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


@pytest.fixture
def larger_csv():
    """Create a larger CSV file for skip/limit testing."""
    lines = ["id,value"]
    for i in range(10):
        lines.append(f"{i},{i * 10}")
    content = "\n".join(lines) + "\n"
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(content)
        return f.name


@pytest.fixture
def typed_csv():
    """Create a CSV with different types."""
    content = "name,age,score,active\nAlice,30,95.5,true\nBob,25,87.3,false\nCharlie,35,92.1,true\n"
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(content)
        return f.name


@pytest.fixture
def quoted_csv():
    """Create a CSV with quoted fields."""
    content = 'name,description,value\n"Alice","Has a ""nickname""",100\n"Bob","Simple",200\n'
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(content)
        return f.name


class TestReadCsvRowsBasic:
    """Basic tests for read_csv_rows function."""

    def test_basic_iteration(self, simple_csv):
        """Test basic row-by-row iteration."""
        import vroom_csv

        rows = list(vroom_csv.read_csv_rows(simple_csv))

        assert len(rows) == 3
        assert rows[0] == {"name": "Alice", "age": "30", "city": "New York"}
        assert rows[1] == {"name": "Bob", "age": "25", "city": "Los Angeles"}
        assert rows[2] == {"name": "Charlie", "age": "35", "city": "Chicago"}

    def test_iterator_protocol(self, simple_csv):
        """Test that the iterator protocol works correctly."""
        import vroom_csv

        iterator = vroom_csv.read_csv_rows(simple_csv)

        # Should be iterable
        assert iter(iterator) is iterator

        # Should yield rows one at a time
        row1 = next(iterator)
        assert row1["name"] == "Alice"

        row2 = next(iterator)
        assert row2["name"] == "Bob"

        row3 = next(iterator)
        assert row3["name"] == "Charlie"

        # Should raise StopIteration when exhausted
        with pytest.raises(StopIteration):
            next(iterator)

    def test_for_loop_iteration(self, simple_csv):
        """Test using for loop to iterate."""
        import vroom_csv

        names = []
        for row in vroom_csv.read_csv_rows(simple_csv):
            names.append(row["name"])

        assert names == ["Alice", "Bob", "Charlie"]

    def test_column_names_property(self, simple_csv):
        """Test column_names property on iterator."""
        import vroom_csv

        iterator = vroom_csv.read_csv_rows(simple_csv)

        # After reading header, column names should be available
        assert iterator.column_names == ["name", "age", "city"]


class TestReadCsvRowsDelimiter:
    """Tests for delimiter option."""

    def test_explicit_delimiter(self, tsv_file):
        """Test reading with explicit tab delimiter."""
        import vroom_csv

        rows = list(vroom_csv.read_csv_rows(tsv_file, delimiter="\t"))

        assert len(rows) == 3
        assert rows[0]["name"] == "Alice"
        assert rows[0]["age"] == "30"

    def test_invalid_delimiter(self, simple_csv):
        """Test error for invalid delimiter."""
        import vroom_csv

        with pytest.raises(ValueError, match="single character"):
            list(vroom_csv.read_csv_rows(simple_csv, delimiter=",,"))


class TestReadCsvRowsNoHeader:
    """Tests for files without headers."""

    def test_no_header(self, no_header_csv):
        """Test reading file without header."""
        import vroom_csv

        rows = list(vroom_csv.read_csv_rows(no_header_csv, has_header=False))

        assert len(rows) == 3
        assert rows[0] == {"column_0": "Alice", "column_1": "30", "column_2": "New York"}


class TestReadCsvRowsSkipRows:
    """Tests for skip_rows option."""

    def test_skip_rows_basic(self, larger_csv):
        """Test skipping first N rows."""
        import vroom_csv

        rows = list(vroom_csv.read_csv_rows(larger_csv, skip_rows=3))

        assert len(rows) == 7
        assert rows[0] == {"id": "3", "value": "30"}

    def test_skip_rows_exceeds_total(self, larger_csv):
        """Test skip_rows larger than total rows returns empty iterator."""
        import vroom_csv

        rows = list(vroom_csv.read_csv_rows(larger_csv, skip_rows=100))

        assert len(rows) == 0


class TestReadCsvRowsNRows:
    """Tests for n_rows option."""

    def test_n_rows_basic(self, larger_csv):
        """Test reading only first N rows."""
        import vroom_csv

        rows = list(vroom_csv.read_csv_rows(larger_csv, n_rows=3))

        assert len(rows) == 3
        assert rows[0] == {"id": "0", "value": "0"}
        assert rows[2] == {"id": "2", "value": "20"}

    def test_n_rows_exceeds_total(self, simple_csv):
        """Test n_rows larger than total rows reads all rows."""
        import vroom_csv

        rows = list(vroom_csv.read_csv_rows(simple_csv, n_rows=100))

        assert len(rows) == 3


class TestReadCsvRowsSkipAndNRows:
    """Tests for combining skip_rows and n_rows."""

    def test_skip_and_n_rows_combined(self, larger_csv):
        """Test using skip_rows and n_rows together."""
        import vroom_csv

        rows = list(vroom_csv.read_csv_rows(larger_csv, skip_rows=2, n_rows=4))

        assert len(rows) == 4
        assert rows[0] == {"id": "2", "value": "20"}
        assert rows[3] == {"id": "5", "value": "50"}


class TestReadCsvRowsUsecols:
    """Tests for usecols option."""

    def test_usecols_by_name(self, simple_csv):
        """Test selecting specific columns by name."""
        import vroom_csv

        rows = list(vroom_csv.read_csv_rows(simple_csv, usecols=["name", "city"]))

        assert len(rows) == 3
        assert rows[0] == {"name": "Alice", "city": "New York"}
        assert "age" not in rows[0]

    def test_usecols_by_index(self, simple_csv):
        """Test selecting specific columns by index."""
        import vroom_csv

        rows = list(vroom_csv.read_csv_rows(simple_csv, usecols=[0, 2]))

        assert len(rows) == 3
        assert rows[0] == {"name": "Alice", "city": "New York"}

    def test_usecols_mixed(self, simple_csv):
        """Test selecting columns by mixed name and index."""
        import vroom_csv

        rows = list(vroom_csv.read_csv_rows(simple_csv, usecols=["name", 1]))

        assert len(rows) == 3
        assert rows[0] == {"name": "Alice", "age": "30"}

    def test_usecols_invalid_name(self, simple_csv):
        """Test error for invalid column name in usecols."""
        import vroom_csv

        with pytest.raises(KeyError):
            list(vroom_csv.read_csv_rows(simple_csv, usecols=["nonexistent"]))

    def test_usecols_invalid_index(self, simple_csv):
        """Test error for out-of-range column index in usecols."""
        import vroom_csv

        with pytest.raises(IndexError):
            list(vroom_csv.read_csv_rows(simple_csv, usecols=[100]))


class TestReadCsvRowsDtype:
    """Tests for dtype option."""

    def test_dtype_int(self, typed_csv):
        """Test integer type conversion."""
        import vroom_csv

        rows = list(vroom_csv.read_csv_rows(typed_csv, dtype={"age": "int64"}))

        assert rows[0]["age"] == 30
        assert rows[1]["age"] == 25
        assert isinstance(rows[0]["age"], int)

    def test_dtype_float(self, typed_csv):
        """Test float type conversion."""
        import vroom_csv

        rows = list(vroom_csv.read_csv_rows(typed_csv, dtype={"score": "float64"}))

        assert rows[0]["score"] == pytest.approx(95.5)
        assert rows[1]["score"] == pytest.approx(87.3)
        assert isinstance(rows[0]["score"], float)

    def test_dtype_bool(self, typed_csv):
        """Test boolean type conversion."""
        import vroom_csv

        rows = list(vroom_csv.read_csv_rows(typed_csv, dtype={"active": "bool"}))

        assert rows[0]["active"] is True
        assert rows[1]["active"] is False
        assert isinstance(rows[0]["active"], bool)

    def test_dtype_multiple(self, typed_csv):
        """Test multiple dtype conversions."""
        import vroom_csv

        rows = list(
            vroom_csv.read_csv_rows(
                typed_csv, dtype={"age": "int64", "score": "float64", "active": "bool"}
            )
        )

        assert rows[0]["age"] == 30
        assert rows[0]["score"] == pytest.approx(95.5)
        assert rows[0]["active"] is True

    def test_dtype_invalid_value_returns_none(self):
        """Test that invalid values return None when dtype is specified."""
        content = "name,age\nAlice,30\nBob,invalid\n"
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write(content)
            path = f.name

        import vroom_csv

        rows = list(vroom_csv.read_csv_rows(path, dtype={"age": "int64"}))

        assert rows[0]["age"] == 30
        assert rows[1]["age"] is None


class TestReadCsvRowsQuotedFields:
    """Tests for handling quoted fields."""

    def test_quoted_fields(self, quoted_csv):
        """Test reading quoted fields."""
        import vroom_csv

        rows = list(vroom_csv.read_csv_rows(quoted_csv))

        assert len(rows) == 2
        assert rows[0]["name"] == "Alice"
        assert rows[0]["description"] == 'Has a "nickname"'
        assert rows[0]["value"] == "100"


class TestReadCsvRowsErrorHandling:
    """Tests for error handling."""

    def test_nonexistent_file(self):
        """Test error handling for non-existent file."""
        import vroom_csv

        with pytest.raises((ValueError, vroom_csv.VroomError)):
            list(vroom_csv.read_csv_rows("/nonexistent/path/to/file.csv"))


class TestReadCsvRowsMemoryEfficiency:
    """Tests demonstrating memory-efficient usage patterns."""

    def test_early_termination(self, larger_csv):
        """Test that iteration can be stopped early without reading entire file."""
        import vroom_csv

        iterator = vroom_csv.read_csv_rows(larger_csv)
        first_row = next(iterator)

        assert first_row["id"] == "0"
        # Iterator can be abandoned without reading rest of file

    def test_filtering_pattern(self, simple_csv):
        """Test common filtering pattern."""
        import vroom_csv

        # Filter rows where age > 25
        matching = [
            row
            for row in vroom_csv.read_csv_rows(simple_csv, dtype={"age": "int64"})
            if row["age"] and row["age"] > 25
        ]

        assert len(matching) == 2
        assert matching[0]["name"] == "Alice"
        assert matching[1]["name"] == "Charlie"

    def test_generator_chaining(self, simple_csv):
        """Test chaining with other generators."""
        import vroom_csv

        # Chain transformations
        names_upper = (row["name"].upper() for row in vroom_csv.read_csv_rows(simple_csv))

        result = list(names_upper)
        assert result == ["ALICE", "BOB", "CHARLIE"]
