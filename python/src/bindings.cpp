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
#include "libvroom/arrow_c_data.h"
#include "libvroom/arrow_export.h"

#include <memory>
#include <optional>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <stdexcept>
#include <string>
#include <vector>

namespace py = pybind11;

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
  Table(std::vector<libvroom::ColumnSchema> schema,
        std::vector<std::unique_ptr<libvroom::ArrowColumnBuilder>> columns, size_t num_rows)
      : schema_(std::move(schema)), columns_(std::move(columns)), num_rows_(num_rows) {}

  // Make non-copyable (unique_ptr members)
  Table(const Table&) = delete;
  Table& operator=(const Table&) = delete;
  Table(Table&&) = default;
  Table& operator=(Table&&) = default;

  size_t num_rows() const { return num_rows_; }
  size_t num_columns() const { return columns_.size(); }

  const std::vector<libvroom::ColumnSchema>& schema() const { return schema_; }

  std::vector<std::string> column_names() const {
    std::vector<std::string> names;
    names.reserve(schema_.size());
    for (const auto& col : schema_) {
      names.push_back(col.name);
    }
    return names;
  }

  // Access to column builders for export
  const std::vector<std::unique_ptr<libvroom::ArrowColumnBuilder>>& columns() const {
    return columns_;
  }

private:
  std::vector<libvroom::ColumnSchema> schema_;
  std::vector<std::unique_ptr<libvroom::ArrowColumnBuilder>> columns_;
  size_t num_rows_;
};

// =============================================================================
// Arrow stream export for Table
// =============================================================================

// Private data for the stream - keeps Table alive and tracks state
struct TableStreamPrivate {
  std::shared_ptr<Table> table;
  bool batch_returned = false;
  std::string last_error;
};

// Private data for struct schema - owns child schemas
struct StructSchemaPrivate {
  std::string name_storage;
  std::vector<std::unique_ptr<libvroom::ArrowSchema>> child_schemas;
  std::vector<libvroom::ArrowSchema*> child_schema_ptrs;
};

// Release callback for struct schema
void release_struct_schema(libvroom::ArrowSchema* schema) {
  if (schema->release == nullptr)
    return;

  // Release all child schemas first
  if (schema->children) {
    for (int64_t i = 0; i < schema->n_children; ++i) {
      if (schema->children[i] && schema->children[i]->release) {
        schema->children[i]->release(schema->children[i]);
      }
    }
  }

  // Delete our private data (which owns the child ArrowSchema objects)
  if (schema->private_data) {
    delete static_cast<StructSchemaPrivate*>(schema->private_data);
  }

  schema->release = nullptr;
}

// Stream get_schema callback
int table_stream_get_schema(libvroom::ArrowArrayStream* stream, libvroom::ArrowSchema* out) {
  auto* priv = static_cast<TableStreamPrivate*>(stream->private_data);
  auto& table = priv->table;
  const auto& table_schema = table->schema();

  // Create private data to own the child schemas
  auto* schema_priv = new StructSchemaPrivate();
  schema_priv->name_storage = "";

  // Create struct schema with children for each column
  for (size_t i = 0; i < table->num_columns(); ++i) {
    auto child = std::make_unique<libvroom::ArrowSchema>();
    table->columns()[i]->export_schema(child.get(), table_schema[i].name);
    schema_priv->child_schema_ptrs.push_back(child.get());
    schema_priv->child_schemas.push_back(std::move(child));
  }

  // Set up struct schema
  out->format = libvroom::arrow_format::STRUCT;
  out->name = schema_priv->name_storage.c_str();
  out->metadata = nullptr;
  out->flags = 0;
  out->n_children = static_cast<int64_t>(table->num_columns());
  out->children = schema_priv->child_schema_ptrs.data();
  out->dictionary = nullptr;
  out->release = release_struct_schema;
  out->private_data = schema_priv;

  return 0;
}

// Private data for struct array - owns child arrays and keeps table alive
struct StructArrayPrivate {
  std::shared_ptr<Table> table; // Keep table alive while array is in use
  std::vector<std::unique_ptr<libvroom::ArrowArray>> child_arrays;
  std::vector<libvroom::ArrowArray*> child_array_ptrs;
  // Note: child_privates are owned by the ArrowArray's release callback, not stored here
  std::vector<const void*> struct_buffers;
};

// Release callback for struct array
void release_struct_array(libvroom::ArrowArray* array) {
  if (array->release == nullptr)
    return;

  // Release all child arrays first
  if (array->children) {
    for (int64_t i = 0; i < array->n_children; ++i) {
      if (array->children[i] && array->children[i]->release) {
        array->children[i]->release(array->children[i]);
      }
    }
  }

  // Delete our private data (which owns the child ArrowArray objects and the table)
  if (array->private_data) {
    delete static_cast<StructArrayPrivate*>(array->private_data);
  }

  array->release = nullptr;
}

// Stream get_next callback
int table_stream_get_next(libvroom::ArrowArrayStream* stream, libvroom::ArrowArray* out) {
  auto* stream_priv = static_cast<TableStreamPrivate*>(stream->private_data);

  if (stream_priv->batch_returned) {
    // No more batches - signal end of stream
    libvroom::init_empty_array(out);
    return 0;
  }

  auto& table = stream_priv->table;

  // Create private data to own the child arrays and keep table alive
  auto* array_priv = new StructArrayPrivate();
  array_priv->table = table; // Keep table alive

  // Create child arrays for each column
  for (size_t i = 0; i < table->num_columns(); ++i) {
    // ArrowColumnPrivate is owned by the ArrowArray's release callback
    auto* child_priv = new libvroom::ArrowColumnPrivate();
    auto child = std::make_unique<libvroom::ArrowArray>();
    table->columns()[i]->export_to_arrow(child.get(), child_priv);

    array_priv->child_array_ptrs.push_back(child.get());
    array_priv->child_arrays.push_back(std::move(child));
  }

  // Set up struct array
  array_priv->struct_buffers = {nullptr}; // Struct has no buffers itself

  out->length = static_cast<int64_t>(table->num_rows());
  out->null_count = 0;
  out->offset = 0;
  out->n_buffers = 1;
  out->n_children = static_cast<int64_t>(table->num_columns());
  out->buffers = array_priv->struct_buffers.data();
  out->children = array_priv->child_array_ptrs.data();
  out->dictionary = nullptr;
  out->release = release_struct_array;
  out->private_data = array_priv;

  stream_priv->batch_returned = true;
  return 0;
}

// Stream get_last_error callback
const char* table_stream_get_last_error(libvroom::ArrowArrayStream* stream) {
  auto* priv = static_cast<TableStreamPrivate*>(stream->private_data);
  return priv->last_error.empty() ? nullptr : priv->last_error.c_str();
}

// Stream release callback
void table_stream_release(libvroom::ArrowArrayStream* stream) {
  if (stream->release == nullptr)
    return;

  auto* priv = static_cast<TableStreamPrivate*>(stream->private_data);
  delete priv;

  stream->release = nullptr;
}

// =============================================================================
// read_csv function - main entry point
// =============================================================================

// Result structure for read_csv when returning errors
struct ReadResult {
  std::shared_ptr<Table> table;
  std::vector<libvroom::ParseError> errors;
  bool has_warnings;
  bool has_fatal;
};

std::shared_ptr<Table> read_csv(const std::string& path,
                                std::optional<char> separator = std::nullopt,
                                std::optional<char> quote = std::nullopt, bool has_header = true,
                                std::optional<size_t> num_threads = std::nullopt,
                                std::optional<std::string> error_mode = std::nullopt,
                                std::optional<size_t> max_errors = std::nullopt) {
  // Set up options
  libvroom::CsvOptions csv_opts;
  if (separator)
    csv_opts.separator = *separator;
  if (quote)
    csv_opts.quote = *quote;
  csv_opts.has_header = has_header;
  if (num_threads)
    csv_opts.num_threads = *num_threads;

  // Set error handling options
  if (error_mode) {
    if (*error_mode == "disabled") {
      csv_opts.error_mode = libvroom::ErrorMode::DISABLED;
    } else if (*error_mode == "fail_fast" || *error_mode == "strict") {
      csv_opts.error_mode = libvroom::ErrorMode::FAIL_FAST;
    } else if (*error_mode == "permissive") {
      csv_opts.error_mode = libvroom::ErrorMode::PERMISSIVE;
    } else if (*error_mode == "best_effort") {
      csv_opts.error_mode = libvroom::ErrorMode::BEST_EFFORT;
    } else {
      throw std::runtime_error("Unknown error_mode: " + *error_mode +
                               " (use 'disabled', 'fail_fast', 'permissive', or 'best_effort')");
    }
  }
  if (max_errors) {
    csv_opts.max_errors = *max_errors;
    // Enable error collection if max_errors is specified
    if (csv_opts.error_mode == libvroom::ErrorMode::DISABLED) {
      csv_opts.error_mode = libvroom::ErrorMode::PERMISSIVE;
    }
  }

  // Create reader and open file
  libvroom::CsvReader reader(csv_opts);
  auto open_result = reader.open(path);
  if (!open_result.ok) {
    // Include any collected errors in the exception message
    std::string error_msg = open_result.error;
    if (reader.has_errors()) {
      error_msg += "\n\nParse errors:\n";
      for (const auto& err : reader.errors()) {
        error_msg += "  " + err.to_string() + "\n";
      }
    }
    throw std::runtime_error(error_msg);
  }

  // Read all data
  auto read_result = reader.read_all();
  if (!read_result.ok) {
    // Include any collected errors in the exception message
    std::string error_msg = read_result.error;
    if (reader.has_errors()) {
      error_msg += "\n\nParse errors:\n";
      for (const auto& err : reader.errors()) {
        error_msg += "  " + err.to_string() + "\n";
      }
    }
    throw std::runtime_error(error_msg);
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
                                   std::vector<std::unique_ptr<libvroom::ArrowColumnBuilder>>{}, 0);
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
                std::optional<size_t> num_threads = std::nullopt,
                std::optional<std::string> error_mode = std::nullopt,
                std::optional<size_t> max_errors = std::nullopt) {
  libvroom::VroomOptions opts;
  opts.input_path = input_path;
  opts.output_path = output_path;

  // Set compression
  if (compression) {
    if (*compression == "zstd") {
      opts.parquet.compression = libvroom::Compression::ZSTD;
    } else if (*compression == "snappy") {
      opts.parquet.compression = libvroom::Compression::SNAPPY;
    } else if (*compression == "lz4") {
      opts.parquet.compression = libvroom::Compression::LZ4;
    } else if (*compression == "gzip") {
      opts.parquet.compression = libvroom::Compression::GZIP;
    } else if (*compression == "none") {
      opts.parquet.compression = libvroom::Compression::NONE;
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

  // Set error handling options
  if (error_mode) {
    if (*error_mode == "disabled") {
      opts.csv.error_mode = libvroom::ErrorMode::DISABLED;
    } else if (*error_mode == "fail_fast" || *error_mode == "strict") {
      opts.csv.error_mode = libvroom::ErrorMode::FAIL_FAST;
    } else if (*error_mode == "permissive") {
      opts.csv.error_mode = libvroom::ErrorMode::PERMISSIVE;
    } else if (*error_mode == "best_effort") {
      opts.csv.error_mode = libvroom::ErrorMode::BEST_EFFORT;
    } else {
      throw std::runtime_error("Unknown error_mode: " + *error_mode +
                               " (use 'disabled', 'fail_fast', 'permissive', or 'best_effort')");
    }
  }
  if (max_errors) {
    opts.csv.max_errors = *max_errors;
    // Enable error collection if max_errors is specified
    if (opts.csv.error_mode == libvroom::ErrorMode::DISABLED) {
      opts.csv.error_mode = libvroom::ErrorMode::PERMISSIVE;
    }
  }

  auto result = libvroom::convert_csv_to_parquet(opts);
  if (!result.ok()) {
    // Include any collected errors in the exception message
    std::string error_msg = result.error;
    if (result.has_errors()) {
      error_msg += "\n\nParse errors:\n";
      for (const auto& err : result.parse_errors) {
        error_msg += "  " + err.to_string() + "\n";
      }
    }
    throw std::runtime_error(error_msg);
  }

  // Warn about any errors collected even on success (permissive mode)
  // TODO: Consider returning warnings to Python instead of just logging
}

// =============================================================================
// to_arrow_ipc function - CSV to Arrow IPC conversion
// =============================================================================

void to_arrow_ipc(const std::string& input_path, const std::string& output_path,
                  std::optional<size_t> batch_size = std::nullopt,
                  std::optional<size_t> num_threads = std::nullopt) {
  libvroom::CsvOptions csv_opts;
  if (num_threads) {
    csv_opts.num_threads = *num_threads;
  }

  libvroom::ArrowIpcOptions ipc_opts;
  if (batch_size) {
    ipc_opts.batch_size = *batch_size;
  }

  auto result = libvroom::convert_csv_to_arrow_ipc(input_path, output_path, csv_opts, ipc_opts);
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
      .def_property_readonly("column_names", &Table::column_names, "List of column names")
      .def(
          "__arrow_c_stream__",
          [](std::shared_ptr<Table> self, py::object requested_schema) {
            // Create stream
            auto* stream = new libvroom::ArrowArrayStream();
            auto* priv = new TableStreamPrivate();
            priv->table = self;

            stream->get_schema = table_stream_get_schema;
            stream->get_next = table_stream_get_next;
            stream->get_last_error = table_stream_get_last_error;
            stream->release = table_stream_release;
            stream->private_data = priv;

            // Wrap in PyCapsule with required name
            return py::capsule(stream, "arrow_array_stream", [](void* ptr) {
              auto* s = static_cast<libvroom::ArrowArrayStream*>(ptr);
              if (s->release)
                s->release(s);
              delete s;
            });
          },
          py::arg("requested_schema") = py::none(),
          "Export table as Arrow stream via PyCapsule (zero-copy)")
      .def(
          "__arrow_c_schema__",
          [](std::shared_ptr<Table> self) {
            auto* schema = new libvroom::ArrowSchema();
            auto* priv = new StructSchemaPrivate();
            const auto& table_schema = self->schema();

            // Create child schemas
            for (size_t i = 0; i < self->num_columns(); ++i) {
              auto child = std::make_unique<libvroom::ArrowSchema>();
              self->columns()[i]->export_schema(child.get(), table_schema[i].name);
              priv->child_schema_ptrs.push_back(child.get());
              priv->child_schemas.push_back(std::move(child));
            }

            priv->name_storage = "";
            schema->format = libvroom::arrow_format::STRUCT;
            schema->name = priv->name_storage.c_str();
            schema->metadata = nullptr;
            schema->flags = 0;
            schema->n_children = static_cast<int64_t>(self->num_columns());
            schema->children = priv->child_schema_ptrs.data();
            schema->dictionary = nullptr;
            schema->release = release_struct_schema;
            schema->private_data = priv;

            return py::capsule(schema, "arrow_schema", [](void* ptr) {
              auto* s = static_cast<libvroom::ArrowSchema*>(ptr);
              if (s->release)
                s->release(s);
              delete s;
            });
          },
          "Export table schema as Arrow schema via PyCapsule");

  // read_csv function
  m.def("read_csv", &read_csv, py::arg("path"), py::arg("separator") = py::none(),
        py::arg("quote") = py::none(), py::arg("has_header") = true,
        py::arg("num_threads") = py::none(), py::arg("error_mode") = py::none(),
        py::arg("max_errors") = py::none(),
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
        error_mode : str, optional
            Error handling mode:
            - "disabled" (default): No error collection, maximum performance
            - "fail_fast" or "strict": Stop on first error
            - "permissive": Collect all errors, stop on fatal
            - "best_effort": Ignore errors, parse what's possible
        max_errors : int, optional
            Maximum number of errors to collect. Default is 10000.
            Setting this automatically enables "permissive" mode if error_mode is not set.

        Returns
        -------
        Table
            A Table object containing the parsed data.

        Raises
        ------
        RuntimeError
            If parsing fails. In permissive mode, collected errors are included
            in the exception message.

        Examples
        --------
        >>> import vroom_csv
        >>> table = vroom_csv.read_csv("data.csv")
        >>> print(table.num_rows, table.num_columns)

        # With error handling
        >>> table = vroom_csv.read_csv("data.csv", error_mode="permissive")
    )doc");

  // to_parquet function
  m.def("to_parquet", &to_parquet, py::arg("input_path"), py::arg("output_path"),
        py::arg("compression") = py::none(), py::arg("row_group_size") = py::none(),
        py::arg("num_threads") = py::none(), py::arg("error_mode") = py::none(),
        py::arg("max_errors") = py::none(),
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
        error_mode : str, optional
            Error handling mode:
            - "disabled" (default): No error collection, maximum performance
            - "fail_fast" or "strict": Stop on first error
            - "permissive": Collect all errors, stop on fatal
            - "best_effort": Ignore errors, parse what's possible
        max_errors : int, optional
            Maximum number of errors to collect. Default is 10000.
            Setting this automatically enables "permissive" mode if error_mode is not set.

        Raises
        ------
        RuntimeError
            If parsing fails. In permissive mode, collected errors are included
            in the exception message.

        Examples
        --------
        >>> import vroom_csv
        >>> vroom_csv.to_parquet("data.csv", "data.parquet")

        # With error handling
        >>> vroom_csv.to_parquet("data.csv", "data.parquet", error_mode="strict")
    )doc");

  // to_arrow_ipc function
  m.def("to_arrow_ipc", &to_arrow_ipc, py::arg("input_path"), py::arg("output_path"),
        py::arg("batch_size") = py::none(), py::arg("num_threads") = py::none(),
        R"doc(
        Convert a CSV file to Arrow IPC format.

        NOTE: This function is not yet implemented. It will raise an error
        explaining that Arrow IPC output requires FlatBuffers integration.
        Use to_parquet() for columnar output instead.

        Parameters
        ----------
        input_path : str
            Path to the input CSV file.
        output_path : str
            Path to the output Arrow IPC file (.arrow or .feather).
        batch_size : int, optional
            Number of rows per record batch. Default is 65536.
        num_threads : int, optional
            Number of threads to use. Default is auto-detect.

        Raises
        ------
        RuntimeError
            Always raised - Arrow IPC output is not yet implemented.
    )doc");

  // Version info
  m.attr("__version__") = "2.0.0";
}
