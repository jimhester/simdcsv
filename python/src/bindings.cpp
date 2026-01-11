/**
 * @file bindings.cpp
 * @brief Python bindings for libvroom using pybind11.
 *
 * This module provides Python access to the libvroom high-performance CSV parser.
 * It implements the Arrow PyCapsule interface for zero-copy interoperability with
 * PyArrow, Polars, and DuckDB.
 */

#include "libvroom.h"

#include "dialect.h"
#include "error.h"

#include <memory>
#include <optional>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <sstream>
#include <string>
#include <vector>

namespace py = pybind11;

// =============================================================================
// Arrow C Data Interface structures (for PyCapsule protocol)
// See: https://arrow.apache.org/docs/format/CDataInterface.html
// =============================================================================

// Forward declarations
struct ArrowSchema;
struct ArrowArray;
struct ArrowArrayStream;

// Arrow schema structure
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

// Arrow array structure
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

// Arrow array stream structure (for streaming export)
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
// Helper: Convert libvroom errors to Python exceptions
// =============================================================================

void translate_libvroom_exception(const libvroom::ParseException& e) {
  std::ostringstream ss;
  ss << e.what();
  if (!e.errors().empty()) {
    ss << "\n\nErrors:\n";
    for (const auto& err : e.errors()) {
      ss << "  " << err.to_string() << "\n";
    }
  }
  PyErr_SetString(ParseError, ss.str().c_str());
}

// =============================================================================
// Internal data structures for Arrow export
// =============================================================================

// Holds parsed CSV data and manages memory for Arrow export
struct TableData {
  libvroom::FileBuffer buffer;
  libvroom::Parser::Result result;
  std::vector<std::string> column_names;
  std::vector<std::vector<std::string>> columns_data; // Materialized column data
  bool columns_materialized = false;

  // Materialize all columns as strings for Arrow export
  void materialize_columns() {
    if (columns_materialized)
      return;

    size_t n_cols = result.num_columns();
    size_t n_rows = result.num_rows();
    columns_data.resize(n_cols);

    for (size_t col = 0; col < n_cols; ++col) {
      columns_data[col].reserve(n_rows);
      for (size_t row = 0; row < n_rows; ++row) {
        auto r = result.row(row);
        columns_data[col].push_back(r.get_string(col));
      }
    }
    columns_materialized = true;
  }
};

// Schema release callback
static void release_schema(ArrowSchema* schema) {
  if (schema->release == nullptr)
    return;

  // Free format string (we allocated it)
  if (schema->format) {
    delete[] schema->format;
  }

  // Free name string
  if (schema->name) {
    delete[] schema->name;
  }

  // Release children
  if (schema->children) {
    for (int64_t i = 0; i < schema->n_children; ++i) {
      if (schema->children[i] && schema->children[i]->release) {
        schema->children[i]->release(schema->children[i]);
        delete schema->children[i];
      }
    }
    delete[] schema->children;
  }

  schema->release = nullptr;
}

// Array release callback
static void release_array(ArrowArray* array) {
  if (array->release == nullptr)
    return;

  // Free buffers - private_data points to our buffer holder
  if (array->private_data) {
    auto* data = static_cast<std::vector<char>*>(array->private_data);
    delete data;
  }

  if (array->buffers) {
    delete[] array->buffers;
  }

  // Release children
  if (array->children) {
    for (int64_t i = 0; i < array->n_children; ++i) {
      if (array->children[i] && array->children[i]->release) {
        array->children[i]->release(array->children[i]);
        delete array->children[i];
      }
    }
    delete[] array->children;
  }

  array->release = nullptr;
}

// Stream private data
struct StreamPrivateData {
  std::shared_ptr<TableData> table_data;
  bool schema_exported = false;
  bool data_exported = false;
  std::string last_error;
};

// Stream release callback
static void release_stream(ArrowArrayStream* stream) {
  if (stream->release == nullptr)
    return;
  if (stream->private_data) {
    auto* data = static_cast<StreamPrivateData*>(stream->private_data);
    delete data;
  }
  stream->release = nullptr;
}

// Build schema for a string column
static void build_string_column_schema(ArrowSchema* schema, const char* name) {
  schema->format = new char[2];
  std::strcpy(const_cast<char*>(schema->format), "u"); // utf8 string
  schema->name = new char[std::strlen(name) + 1];
  std::strcpy(const_cast<char*>(schema->name), name);
  schema->metadata = nullptr;
  schema->flags = 2; // ARROW_FLAG_NULLABLE
  schema->n_children = 0;
  schema->children = nullptr;
  schema->dictionary = nullptr;
  schema->release = release_schema;
  schema->private_data = nullptr;
}

// Build schema for struct (table)
static void build_struct_schema(ArrowSchema* schema, const std::vector<std::string>& column_names) {
  // Struct format
  schema->format = new char[3];
  std::strcpy(const_cast<char*>(schema->format), "+s"); // struct
  schema->name = nullptr;
  schema->metadata = nullptr;
  schema->flags = 0;
  schema->n_children = static_cast<int64_t>(column_names.size());
  schema->children = new ArrowSchema*[column_names.size()];
  schema->dictionary = nullptr;
  schema->release = release_schema;
  schema->private_data = nullptr;

  for (size_t i = 0; i < column_names.size(); ++i) {
    schema->children[i] = new ArrowSchema();
    build_string_column_schema(schema->children[i], column_names[i].c_str());
  }
}

// Build Arrow array for a string column
static void build_string_column_array(ArrowArray* array, const std::vector<std::string>& data) {
  // Calculate total data size
  size_t total_size = 0;
  for (const auto& s : data) {
    total_size += s.size();
  }

  // Allocate buffer holder (owns the data)
  auto* buffer_holder = new std::vector<char>();

  // Build offsets buffer (int32 offsets for utf8 format)
  std::vector<int32_t> offsets;
  offsets.reserve(data.size() + 1);
  int32_t offset = 0;
  offsets.push_back(offset);
  for (const auto& s : data) {
    offset += static_cast<int32_t>(s.size());
    offsets.push_back(offset);
  }

  // Build data buffer
  buffer_holder->reserve(offsets.size() * sizeof(int32_t) + total_size);

  // Copy offsets
  const char* offsets_ptr = reinterpret_cast<const char*>(offsets.data());
  buffer_holder->insert(buffer_holder->end(), offsets_ptr,
                        offsets_ptr + offsets.size() * sizeof(int32_t));

  // Copy string data
  size_t data_start = buffer_holder->size();
  for (const auto& s : data) {
    buffer_holder->insert(buffer_holder->end(), s.begin(), s.end());
  }

  // Set up array
  array->length = static_cast<int64_t>(data.size());
  array->null_count = 0;
  array->offset = 0;
  array->n_buffers = 3; // validity (null), offsets, data
  array->n_children = 0;
  array->buffers = new const void*[3];
  array->buffers[0] = nullptr;                            // validity bitmap (all valid)
  array->buffers[1] = buffer_holder->data();              // offsets
  array->buffers[2] = buffer_holder->data() + data_start; // data
  array->children = nullptr;
  array->dictionary = nullptr;
  array->release = release_array;
  array->private_data = buffer_holder;
}

// Build Arrow array for struct (table)
static void build_struct_array(ArrowArray* array, std::shared_ptr<TableData> table_data) {
  table_data->materialize_columns();

  size_t n_cols = table_data->columns_data.size();
  size_t n_rows = n_cols > 0 ? table_data->columns_data[0].size() : 0;

  array->length = static_cast<int64_t>(n_rows);
  array->null_count = 0;
  array->offset = 0;
  array->n_buffers = 1; // Just validity bitmap for struct
  array->n_children = static_cast<int64_t>(n_cols);
  array->buffers = new const void*[1];
  array->buffers[0] = nullptr; // validity bitmap (all valid)
  array->children = new ArrowArray*[n_cols];
  array->dictionary = nullptr;
  array->release = release_array;
  array->private_data = nullptr;

  for (size_t i = 0; i < n_cols; ++i) {
    array->children[i] = new ArrowArray();
    build_string_column_array(array->children[i], table_data->columns_data[i]);
  }
}

// Stream callbacks
static int stream_get_schema(ArrowArrayStream* stream, ArrowSchema* out) {
  auto* priv = static_cast<StreamPrivateData*>(stream->private_data);
  if (!priv || !priv->table_data) {
    return -1;
  }

  build_struct_schema(out, priv->table_data->column_names);
  priv->schema_exported = true;
  return 0;
}

static int stream_get_next(ArrowArrayStream* stream, ArrowArray* out) {
  auto* priv = static_cast<StreamPrivateData*>(stream->private_data);
  if (!priv || !priv->table_data) {
    return -1;
  }

  if (priv->data_exported) {
    // No more batches - signal end of stream
    out->release = nullptr;
    return 0;
  }

  build_struct_array(out, priv->table_data);
  priv->data_exported = true;
  return 0;
}

static const char* stream_get_last_error(ArrowArrayStream* stream) {
  auto* priv = static_cast<StreamPrivateData*>(stream->private_data);
  if (!priv) {
    return "Invalid stream";
  }
  return priv->last_error.empty() ? nullptr : priv->last_error.c_str();
}

// =============================================================================
// Table class - main Python interface
// =============================================================================

class Table {
public:
  Table(std::shared_ptr<TableData> data) : data_(std::move(data)) {}

  // Number of rows (excluding header)
  size_t num_rows() const { return data_->result.num_rows(); }

  // Number of columns
  size_t num_columns() const { return data_->result.num_columns(); }

  // Column names
  std::vector<std::string> column_names() const { return data_->column_names; }

  // Get a single column as list of strings
  std::vector<std::string> column(size_t index) const {
    if (index >= data_->result.num_columns()) {
      throw py::index_error("Column index out of range");
    }
    return data_->result.column_string(index);
  }

  // Get a column by name
  std::vector<std::string> column_by_name(const std::string& name) const {
    auto idx = data_->result.column_index(name);
    if (!idx) {
      throw py::key_error("Column not found: " + name);
    }
    return data_->result.column_string(*idx);
  }

  // Get a single row as list of strings
  std::vector<std::string> row(size_t index) const {
    if (index >= data_->result.num_rows()) {
      throw py::index_error("Row index out of range");
    }
    auto r = data_->result.row(index);
    std::vector<std::string> result;
    result.reserve(data_->result.num_columns());
    for (size_t col = 0; col < data_->result.num_columns(); ++col) {
      result.push_back(r.get_string(col));
    }
    return result;
  }

  // Arrow PyCapsule interface: __arrow_c_schema__
  py::object arrow_c_schema() const {
    auto* schema = new ArrowSchema();
    build_struct_schema(schema, data_->column_names);

    // Create PyCapsule with destructor
    return py::capsule(schema, "arrow_schema", [](void* ptr) {
      auto* s = static_cast<ArrowSchema*>(ptr);
      if (s->release) {
        s->release(s);
      }
      delete s;
    });
  }

  // Arrow PyCapsule interface: __arrow_c_stream__
  py::object arrow_c_stream(py::object requested_schema = py::none()) const {
    // Note: requested_schema is currently ignored - we always export strings
    (void)requested_schema;

    auto* stream = new ArrowArrayStream();
    auto* priv = new StreamPrivateData();
    priv->table_data = data_;
    priv->schema_exported = false;
    priv->data_exported = false;

    stream->get_schema = stream_get_schema;
    stream->get_next = stream_get_next;
    stream->get_last_error = stream_get_last_error;
    stream->release = release_stream;
    stream->private_data = priv;

    return py::capsule(stream, "arrow_array_stream", [](void* ptr) {
      auto* s = static_cast<ArrowArrayStream*>(ptr);
      if (s->release) {
        s->release(s);
      }
      delete s;
    });
  }

  // String representation
  std::string repr() const {
    std::ostringstream ss;
    ss << "Table(" << num_rows() << " rows, " << num_columns() << " columns)";
    return ss.str();
  }

  // Check for parse errors
  bool has_errors() const { return data_->result.has_errors(); }

  // Get error summary
  std::string error_summary() const { return data_->result.error_summary(); }

  // Get all errors
  std::vector<std::string> errors() const {
    std::vector<std::string> result;
    for (const auto& err : data_->result.errors()) {
      result.push_back(err.to_string());
    }
    return result;
  }

private:
  std::shared_ptr<TableData> data_;
};

// =============================================================================
// read_csv function
// =============================================================================

Table read_csv(const std::string& path, std::optional<std::string> delimiter = std::nullopt,
               bool has_header = true, size_t num_threads = 1) {
  auto data = std::make_shared<TableData>();

  // Load file
  try {
    data->buffer = libvroom::load_file(path);
  } catch (const std::runtime_error& e) {
    throw py::value_error(std::string("Failed to load file: ") + e.what());
  }

  if (!data->buffer.valid()) {
    throw py::value_error("Failed to load file: " + path);
  }

  // Set up parser options
  libvroom::ParseOptions options;
  if (delimiter) {
    libvroom::Dialect dialect;
    if (delimiter->length() != 1) {
      throw py::value_error("Delimiter must be a single character");
    }
    dialect.delimiter = (*delimiter)[0];
    options.dialect = dialect;
  }

  // Parse
  libvroom::Parser parser(num_threads);
  data->result = parser.parse(data->buffer.data(), data->buffer.size(), options);

  if (!data->result.success()) {
    std::ostringstream ss;
    ss << "Failed to parse CSV file";
    if (data->result.has_errors()) {
      ss << ": " << data->result.error_summary();
    }
    throw py::value_error(ss.str());
  }

  // Configure header handling
  data->result.set_has_header(has_header);

  // Get column names
  if (has_header) {
    data->column_names = data->result.header();
  } else {
    // Generate column names
    size_t n_cols = data->result.num_columns();
    data->column_names.reserve(n_cols);
    for (size_t i = 0; i < n_cols; ++i) {
      data->column_names.push_back("column_" + std::to_string(i));
    }
  }

  return Table(std::move(data));
}

// =============================================================================
// Module definition
// =============================================================================

PYBIND11_MODULE(_core, m) {
  m.doc() = "High-performance CSV parser with SIMD acceleration";

  // Create custom exceptions
  VroomError = PyErr_NewException("vroom_csv.VroomError", PyExc_RuntimeError, nullptr);
  ParseError = PyErr_NewException("vroom_csv.ParseError", VroomError, nullptr);
  IOError_custom = PyErr_NewException("vroom_csv.IOError", VroomError, nullptr);

  // Only translate libvroom-specific exceptions.
  // Let pybind11's built-in exceptions (index_error, key_error, value_error) pass through.
  py::register_exception_translator([](std::exception_ptr p) {
    try {
      if (p)
        std::rethrow_exception(p);
    } catch (const libvroom::ParseException& e) {
      translate_libvroom_exception(e);
    }
    // Do NOT catch std::runtime_error - it would intercept pybind11 exceptions
  });

  m.attr("VroomError") = py::handle(VroomError);
  m.attr("ParseError") = py::handle(ParseError);
  m.attr("IOError") = py::handle(IOError_custom);

  // Table class
  py::class_<Table>(m, "Table", R"doc(
A parsed CSV table with Arrow PyCapsule interface support.

This class provides access to parsed CSV data and implements the Arrow
PyCapsule interface for zero-copy interoperability with PyArrow, Polars,
DuckDB, and other Arrow-compatible libraries.

Examples
--------
>>> import vroom_csv
>>> table = vroom_csv.read_csv("data.csv")
>>> print(table.num_rows(), table.num_columns())

# Convert to PyArrow
>>> import pyarrow as pa
>>> arrow_table = pa.table(table)

# Convert to Polars
>>> import polars as pl
>>> df = pl.from_arrow(table)
)doc")
      .def_property_readonly("num_rows", &Table::num_rows, "Number of data rows")
      .def_property_readonly("num_columns", &Table::num_columns, "Number of columns")
      .def_property_readonly("column_names", &Table::column_names, "List of column names")
      .def("column", &Table::column, py::arg("index"), "Get column by index as list of strings")
      .def("column", &Table::column_by_name, py::arg("name"),
           "Get column by name as list of strings")
      .def("row", &Table::row, py::arg("index"), "Get row by index as list of strings")
      .def("__repr__", &Table::repr)
      .def("__len__", &Table::num_rows)
      // Arrow PyCapsule interface
      .def("__arrow_c_schema__", &Table::arrow_c_schema,
           "Export table schema via Arrow C Data Interface")
      .def("__arrow_c_stream__", &Table::arrow_c_stream, py::arg("requested_schema") = py::none(),
           "Export table data via Arrow C Stream Interface")
      // Error handling
      .def("has_errors", &Table::has_errors, "Check if any parse errors occurred")
      .def("error_summary", &Table::error_summary, "Get summary of parse errors")
      .def("errors", &Table::errors, "Get list of all parse error messages");

  // read_csv function
  m.def("read_csv", &read_csv, py::arg("path"), py::arg("delimiter") = py::none(),
        py::arg("has_header") = true, py::arg("num_threads") = 1,
        R"doc(
Read a CSV file and return a Table object.

Parameters
----------
path : str
    Path to the CSV file to read.
delimiter : str, optional
    Field delimiter character. If not specified, the delimiter is
    auto-detected from the file content.
has_header : bool, default True
    Whether the first row contains column headers.
num_threads : int, default 1
    Number of threads to use for parsing.

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

Examples
--------
>>> import vroom_csv
>>> table = vroom_csv.read_csv("data.csv")
>>> print(f"Loaded {table.num_rows} rows")

>>> # With explicit delimiter
>>> table = vroom_csv.read_csv("data.tsv", delimiter="\t")

>>> # Multi-threaded parsing
>>> table = vroom_csv.read_csv("large.csv", num_threads=4)

>>> # Convert to PyArrow
>>> import pyarrow as pa
>>> arrow_table = pa.table(table)

>>> # Convert to Polars
>>> import polars as pl
>>> df = pl.from_arrow(table)
)doc");

  // Version info
  m.attr("__version__") = "0.1.0";
  m.attr("LIBVROOM_VERSION") = LIBVROOM_VERSION_STRING;
}
