/**
 * @file bindings.cpp
 * @brief Python bindings for libvroom using pybind11.
 *
 * This module provides Python access to the libvroom high-performance CSV parser.
 * It implements the Arrow PyCapsule interface for zero-copy interoperability with
 * PyArrow, Polars, and DuckDB.
 *
 * Migrated from libvroom2 architecture.
 */

#include "libvroom.h"

#include <memory>
#include <optional>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <stdexcept>
#include <string>
#include <vector>

namespace py = pybind11;

// =============================================================================
// Arrow C Data Interface structures (for PyCapsule protocol)
// See: https://arrow.apache.org/docs/format/CDataInterface.html
// =============================================================================

struct ArrowSchema {
  const char* format;
  const char* name;
  const char* metadata;
  int64_t flags;
  int64_t n_children;
  ArrowSchema** children;
  ArrowSchema* dictionary;
  void (*release)(ArrowSchema*);
  void* private_data;
};

struct ArrowArray {
  int64_t length;
  int64_t null_count;
  int64_t offset;
  int64_t n_buffers;
  int64_t n_children;
  const void** buffers;
  ArrowArray** children;
  ArrowArray* dictionary;
  void (*release)(ArrowArray*);
  void* private_data;
};

struct ArrowArrayStream {
  int (*get_schema)(ArrowArrayStream*, ArrowSchema* out);
  int (*get_next)(ArrowArrayStream*, ArrowArray* out);
  const char* (*get_last_error)(ArrowArrayStream*);
  void (*release)(ArrowArrayStream*);
  void* private_data;
};

// =============================================================================
// Custom Python exceptions
// =============================================================================

static PyObject* VroomError = nullptr;
static PyObject* ParseError = nullptr;
static PyObject* IOError_custom = nullptr;

// =============================================================================
// Table class - holds parsed CSV data
// =============================================================================

class Table : public std::enable_shared_from_this<Table> {
public:
  Table(std::vector<vroom::ColumnSchema> schema,
        std::vector<std::unique_ptr<vroom::ArrowColumnBuilder>> columns, size_t num_rows)
      : schema_(std::move(schema)), columns_(std::move(columns)), num_rows_(num_rows) {}

  // Make non-copyable (unique_ptr members)
  Table(const Table&) = delete;
  Table& operator=(const Table&) = delete;
  Table(Table&&) = default;
  Table& operator=(Table&&) = default;

  size_t num_rows() const { return num_rows_; }
  size_t num_columns() const { return columns_.size(); }

  const std::vector<vroom::ColumnSchema>& schema() const { return schema_; }

  std::vector<std::string> column_names() const {
    std::vector<std::string> names;
    names.reserve(schema_.size());
    for (const auto& col : schema_) {
      names.push_back(col.name);
    }
    return names;
  }

  // Arrow PyCapsule protocol - __arrow_c_stream__
  // TODO: Implement for zero-copy export

private:
  std::vector<vroom::ColumnSchema> schema_;
  std::vector<std::unique_ptr<vroom::ArrowColumnBuilder>> columns_;
  size_t num_rows_;
};

// =============================================================================
// read_csv function - main entry point
// =============================================================================

std::shared_ptr<Table> read_csv(const std::string& path,
                                std::optional<char> separator = std::nullopt,
                                std::optional<char> quote = std::nullopt, bool has_header = true,
                                std::optional<size_t> num_threads = std::nullopt) {
  // Set up options
  vroom::CsvOptions csv_opts;
  if (separator)
    csv_opts.separator = *separator;
  if (quote)
    csv_opts.quote = *quote;
  csv_opts.has_header = has_header;
  if (num_threads)
    csv_opts.num_threads = *num_threads;

  // Create reader and open file
  vroom::CsvReader reader(csv_opts);
  auto open_result = reader.open(path);
  if (!open_result.ok) {
    throw std::runtime_error(open_result.error);
  }

  // Read all data
  auto read_result = reader.read_all();
  if (!read_result.ok) {
    throw std::runtime_error(read_result.error);
  }

  // Flatten chunks into single column builders
  auto& chunks = read_result.value.chunks;
  size_t total_rows = read_result.value.total_rows;
  auto schema = reader.schema();

  // For simplicity, if there's only one chunk, use it directly
  if (chunks.size() == 1) {
    return std::make_shared<Table>(schema, std::move(chunks[0]), total_rows);
  }

  // Multiple chunks - need to merge them
  // TODO: Implement chunked table or merge logic
  if (chunks.empty()) {
    return std::make_shared<Table>(schema,
                                   std::vector<std::unique_ptr<vroom::ArrowColumnBuilder>>{}, 0);
  }

  // For now, just return the first chunk
  // TODO: Proper merge implementation
  return std::make_shared<Table>(schema, std::move(chunks[0]), chunks[0].empty() ? 0 : total_rows);
}

// =============================================================================
// to_parquet function - CSV to Parquet conversion
// =============================================================================

void to_parquet(const std::string& input_path, const std::string& output_path,
                std::optional<std::string> compression = std::nullopt,
                std::optional<size_t> row_group_size = std::nullopt,
                std::optional<size_t> num_threads = std::nullopt) {
  vroom::VroomOptions opts;
  opts.input_path = input_path;
  opts.output_path = output_path;

  // Set compression
  if (compression) {
    if (*compression == "zstd") {
      opts.parquet.compression = vroom::Compression::ZSTD;
    } else if (*compression == "snappy") {
      opts.parquet.compression = vroom::Compression::SNAPPY;
    } else if (*compression == "lz4") {
      opts.parquet.compression = vroom::Compression::LZ4;
    } else if (*compression == "gzip") {
      opts.parquet.compression = vroom::Compression::GZIP;
    } else if (*compression == "none") {
      opts.parquet.compression = vroom::Compression::NONE;
    } else {
      throw std::runtime_error("Unknown compression: " + *compression);
    }
  }

  if (row_group_size) {
    opts.parquet.row_group_size = *row_group_size;
  }

  if (num_threads) {
    opts.threads.num_threads = *num_threads;
  }

  auto result = vroom::convert_csv_to_parquet(opts);
  if (!result.ok()) {
    throw std::runtime_error(result.error);
  }
}

// =============================================================================
// Python module definition
// =============================================================================

PYBIND11_MODULE(_core, m) {
  m.doc() = R"doc(
        vroom_csv._core - High-performance CSV parser with Arrow interop

        This module provides the core C++ implementation of the vroom CSV parser.
        For the high-level Python API, use vroom_csv directly.
    )doc";

  // Register custom exceptions
  VroomError = PyErr_NewException("vroom_csv.VroomError", PyExc_RuntimeError, nullptr);
  ParseError = PyErr_NewException("vroom_csv.ParseError", VroomError, nullptr);
  IOError_custom = PyErr_NewException("vroom_csv.IOError", VroomError, nullptr);

  m.attr("VroomError") = py::handle(VroomError);
  m.attr("ParseError") = py::handle(ParseError);
  m.attr("IOError") = py::handle(IOError_custom);

  // Table class (using shared_ptr holder for move-only type)
  py::class_<Table, std::shared_ptr<Table>>(m, "Table", R"doc(
        A table of data read from a CSV file.

        This class implements the Arrow PyCapsule interface (__arrow_c_stream__)
        for zero-copy interoperability with PyArrow, Polars, and DuckDB.
    )doc")
      .def_property_readonly("num_rows", &Table::num_rows, "Number of rows in the table")
      .def_property_readonly("num_columns", &Table::num_columns, "Number of columns in the table")
      .def_property_readonly("column_names", &Table::column_names, "List of column names");

  // read_csv function
  m.def("read_csv", &read_csv, py::arg("path"), py::arg("separator") = py::none(),
        py::arg("quote") = py::none(), py::arg("has_header") = true,
        py::arg("num_threads") = py::none(),
        R"doc(
        Read a CSV file into a Table.

        Parameters
        ----------
        path : str
            Path to the CSV file to read.
        separator : str, optional
            Field separator character. Default is ','.
        quote : str, optional
            Quote character. Default is '"'.
        has_header : bool, optional
            Whether the file has a header row. Default is True.
        num_threads : int, optional
            Number of threads to use. Default is auto-detect.

        Returns
        -------
        Table
            A Table object containing the parsed data.

        Examples
        --------
        >>> import vroom_csv
        >>> table = vroom_csv.read_csv("data.csv")
        >>> print(table.num_rows, table.num_columns)
    )doc");

  // to_parquet function
  m.def("to_parquet", &to_parquet, py::arg("input_path"), py::arg("output_path"),
        py::arg("compression") = py::none(), py::arg("row_group_size") = py::none(),
        py::arg("num_threads") = py::none(),
        R"doc(
        Convert a CSV file to Parquet format.

        Parameters
        ----------
        input_path : str
            Path to the input CSV file.
        output_path : str
            Path to the output Parquet file.
        compression : str, optional
            Compression codec: "zstd", "snappy", "lz4", "gzip", or "none".
            Default is "zstd".
        row_group_size : int, optional
            Number of rows per row group. Default is 1,000,000.
        num_threads : int, optional
            Number of threads to use. Default is auto-detect.

        Examples
        --------
        >>> import vroom_csv
        >>> vroom_csv.to_parquet("data.csv", "data.parquet")
    )doc");

  // Version info
  m.attr("__version__") = "2.0.0";
}
