"""Type stubs for vroom_csv._core module."""

from collections.abc import Callable
from typing import Any, Sequence, overload

__version__: str
LIBVROOM_VERSION: str

class VroomError(RuntimeError):
    """Base exception for vroom-csv errors."""

    ...

class ParseError(VroomError):
    """Exception raised when CSV parsing fails."""

    ...

class IOError(VroomError):  # noqa: A001
    """Exception raised for I/O errors."""

    ...

class Dialect:
    """CSV dialect configuration and detection result.

    A Dialect describes the format of a CSV file: field delimiter, quote character,
    escape handling, etc. Obtain a Dialect by calling detect_dialect() on a file.

    Attributes
    ----------
    delimiter : str
        Field separator character (e.g., ',' for CSV, '\\t' for TSV).
    quote_char : str
        Quote character for escaping fields (typically '"').
    escape_char : str
        Escape character (typically '"' or '\\').
    double_quote : bool
        Whether quotes are escaped by doubling ("").
    line_ending : str
        Detected line ending style ('\\n', '\\r\\n', '\\r', 'mixed', or 'unknown').
    has_header : bool
        Whether the first row appears to be a header.
    confidence : float
        Detection confidence from 0.0 to 1.0.
    """

    @property
    def delimiter(self) -> str:
        """Field delimiter character."""
        ...

    @property
    def quote_char(self) -> str:
        """Quote character."""
        ...

    @property
    def escape_char(self) -> str:
        """Escape character."""
        ...

    @property
    def double_quote(self) -> bool:
        """Whether quotes are escaped by doubling."""
        ...

    @property
    def line_ending(self) -> str:
        """Detected line ending style."""
        ...

    @property
    def has_header(self) -> bool:
        """Whether first row appears to be header."""
        ...

    @property
    def confidence(self) -> float:
        """Detection confidence (0.0-1.0)."""
        ...

class Table:
    """A parsed CSV table with Arrow PyCapsule interface support.

    This class provides access to parsed CSV data and implements the Arrow
    PyCapsule interface for zero-copy interoperability with PyArrow, Polars,
    DuckDB, and other Arrow-compatible libraries.
    """

    @property
    def num_rows(self) -> int:
        """Number of data rows."""
        ...

    @property
    def num_columns(self) -> int:
        """Number of columns."""
        ...

    @property
    def column_names(self) -> list[str]:
        """List of column names."""
        ...

    @overload
    def column(self, index: int) -> list[str]:
        """Get column by index as list of strings."""
        ...

    @overload
    def column(self, name: str) -> list[str]:
        """Get column by name as list of strings."""
        ...

    def column(self, index_or_name: int | str) -> list[str]:
        """Get column by index or name as list of strings."""
        ...

    def row(self, index: int) -> list[str]:
        """Get row by index as list of strings."""
        ...

    def has_errors(self) -> bool:
        """Check if any parse errors occurred."""
        ...

    def error_summary(self) -> str:
        """Get summary of parse errors."""
        ...

    def errors(self) -> list[str]:
        """Get list of all parse error messages."""
        ...

    def __len__(self) -> int: ...
    def __repr__(self) -> str: ...
    def __arrow_c_schema__(self) -> Any:
        """Export table schema via Arrow C Data Interface."""
        ...

    def __arrow_c_stream__(self, requested_schema: Any = None) -> Any:
        """Export table data via Arrow C Stream Interface."""
        ...

def detect_dialect(path: str) -> Dialect:
    """Detect the CSV dialect of a file.

    Analyzes the file content to determine the field delimiter, quote character,
    and other format settings.

    Parameters
    ----------
    path : str
        Path to the CSV file to analyze.

    Returns
    -------
    Dialect
        A Dialect object describing the detected CSV format.

    Raises
    ------
    ValueError
        If the file cannot be read or dialect cannot be determined.
    """
    ...

def read_csv(
    path: str,
    delimiter: str | None = None,
    quote_char: str | None = None,
    has_header: bool = True,
    encoding: str | None = None,
    skip_rows: int = 0,
    n_rows: int | None = None,
    usecols: Sequence[str | int] | None = None,
    null_values: Sequence[str] | None = None,
    empty_is_null: bool = True,
    dtype: dict[str, str] | None = None,
    num_threads: int = 1,
    progress: Callable[[int, int], None] | None = None,
) -> Table:
    """Read a CSV file and return a Table object.

    Parameters
    ----------
    path : str
        Path to the CSV file to read.
    delimiter : str, optional
        Field delimiter character. If not specified, the delimiter is
        auto-detected from the file content.
    quote_char : str, optional
        Quote character for escaping fields. Default is '"'.
    has_header : bool, default True
        Whether the first row contains column headers.
    encoding : str, optional
        File encoding. If not specified, encoding is auto-detected.
        Currently accepted but not fully implemented.
    skip_rows : int, default 0
        Number of rows to skip at the start of the file.
    n_rows : int, optional
        Maximum number of rows to read. If not specified, reads all rows.
    usecols : sequence of str or int, optional
        List of column names or indices to read. If not specified, reads
        all columns.
    null_values : sequence of str, optional
        List of strings to treat as null/NA values.
    empty_is_null : bool, default True
        Whether to treat empty strings as null values.
    dtype : dict, optional
        Dictionary mapping column names to data types.
    num_threads : int, default 1
        Number of threads to use for parsing.
    progress : callable, optional
        A callback function for progress reporting during parsing.
        The callback receives two arguments: (bytes_read: int, total_bytes: int).
        It is called periodically during parsing at chunk boundaries (typically
        every 1-4MB). Use this to display progress bars or update UIs.

    Returns
    -------
    Table
        A Table object containing the parsed CSV data.

    Raises
    ------
    ValueError
        If the file cannot be read or parsed.
    ParseError
        If there are fatal parse errors in the CSV.
    IndexError
        If a column index in usecols is out of range.
    KeyError
        If a column name in usecols is not found.
    """
    ...
