/**
 * @file bindings.cpp
 * @brief Python bindings for libvroom using pybind11.
 *
 * This module provides Python access to the libvroom high-performance CSV parser.
 * It implements the Arrow PyCapsule interface for zero-copy interoperability with
 * PyArrow, Polars, and DuckDB.
 */

#include "libvroom.h"
#include "libvroom_types.h"

#include "dialect.h"
#include "error.h"
#include "extraction_config.h"
#include "streaming.h"
#include "value_extraction.h"

#include <algorithm>
#include <climits>
#include <cmath>
#include <cstdint>
#include <functional>
#include <iterator>
#include <memory>
#include <optional>
#include <pybind11/functional.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
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
// Column type enum for dtype support
// =============================================================================

enum class ColumnType { STRING, INT64, FLOAT64, BOOL };

// Map Python dtype strings to ColumnType enum
static std::optional<ColumnType> parse_dtype_string(const std::string& dtype) {
  // Support various common dtype string formats
  if (dtype == "str" || dtype == "string" || dtype == "object" || dtype == "U" || dtype == "<U" ||
      dtype == "utf8") {
    return ColumnType::STRING;
  }
  if (dtype == "int" || dtype == "int64" || dtype == "Int64" || dtype == "i8" || dtype == "<i8") {
    return ColumnType::INT64;
  }
  if (dtype == "float" || dtype == "float64" || dtype == "Float64" || dtype == "f8" ||
      dtype == "<f8" || dtype == "double") {
    return ColumnType::FLOAT64;
  }
  if (dtype == "bool" || dtype == "boolean") {
    return ColumnType::BOOL;
  }
  return std::nullopt;
}

// Get Arrow format string for a column type
static const char* column_type_to_arrow_format(ColumnType type) {
  switch (type) {
  case ColumnType::STRING:
    return "u"; // utf8 string
  case ColumnType::INT64:
    return "l"; // int64
  case ColumnType::FLOAT64:
    return "g"; // float64
  case ColumnType::BOOL:
    return "b"; // bool
  }
  return "u"; // default to string
}

// Convert libvroom::FieldType to ColumnType
static ColumnType field_type_to_column_type(libvroom::FieldType type) {
  switch (type) {
  case libvroom::FieldType::BOOLEAN:
    return ColumnType::BOOL;
  case libvroom::FieldType::INTEGER:
    return ColumnType::INT64;
  case libvroom::FieldType::FLOAT:
    return ColumnType::FLOAT64;
  case libvroom::FieldType::DATE:
    // DATE not yet supported in Arrow export, fall back to string
    return ColumnType::STRING;
  case libvroom::FieldType::STRING:
  case libvroom::FieldType::EMPTY:
  default:
    return ColumnType::STRING;
  }
}

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

// Null value configuration for Arrow export
struct NullValueConfig {
  std::vector<std::string> null_values = {"", "NA", "N/A", "null", "NULL", "None", "NaN"};
  bool empty_is_null = false;

  // Check if a value should be treated as null
  bool is_null_value(const std::string& value) const {
    // Check empty_is_null first
    if (empty_is_null && value.empty()) {
      return true;
    }
    // Check against null_values list
    for (const auto& null_str : null_values) {
      if (value == null_str) {
        return true;
      }
    }
    return false;
  }
};

// Holds parsed CSV data and manages memory for Arrow export
struct TableData {
  libvroom::FileBuffer buffer;
  libvroom::Parser::Result result;
  std::vector<std::string> column_names;
  std::vector<size_t> selected_columns;               // Indices of selected columns (empty = all)
  std::vector<std::vector<std::string>> columns_data; // Materialized column data
  std::vector<ColumnType> column_types;               // Type for each column
  bool columns_materialized = false;
  size_t skip_rows_ = 0;         // Number of data rows to skip
  std::optional<size_t> n_rows_; // Maximum rows to return (nullopt = all)
  NullValueConfig null_config;   // Null value configuration for Arrow export

  // Get effective number of columns (considering selection)
  size_t effective_num_columns() const {
    return selected_columns.empty() ? result.num_columns() : selected_columns.size();
  }

  // Map logical column index to underlying column index
  size_t map_column_index(size_t logical_idx) const {
    if (selected_columns.empty()) {
      return logical_idx;
    }
    return selected_columns[logical_idx];
  }

  // Get the type for a column (STRING by default if not specified)
  ColumnType get_column_type(size_t col) const {
    if (col < column_types.size()) {
      return column_types[col];
    }
    return ColumnType::STRING;
  }

  // Get the effective number of rows after applying skip_rows and n_rows
  size_t effective_num_rows() const {
    size_t total = result.num_rows();
    if (skip_rows_ >= total) {
      return 0;
    }
    size_t available = total - skip_rows_;
    if (n_rows_ && *n_rows_ < available) {
      return *n_rows_;
    }
    return available;
  }

  // Convert a filtered row index to the underlying result row index
  size_t translate_row_index(size_t filtered_index) const { return filtered_index + skip_rows_; }

  // Materialize all columns as strings for Arrow export
  void materialize_columns() {
    if (columns_materialized)
      return;

    size_t n_cols = effective_num_columns();
    size_t n_rows = effective_num_rows();
    columns_data.resize(n_cols);

    for (size_t col = 0; col < n_cols; ++col) {
      size_t underlying_col = map_column_index(col);
      columns_data[col].reserve(n_rows);
      for (size_t row = 0; row < n_rows; ++row) {
        auto r = result.row(translate_row_index(row));
        columns_data[col].push_back(r.get_string(underlying_col));
      }
    }
    columns_materialized = true;
  }
};

// =============================================================================
// Dialect Python class - exposes CSV dialect detection results
// =============================================================================

class Dialect {
public:
  Dialect() = default;

  Dialect(char delim, char quote, char escape, bool double_quote, const std::string& line_ending,
          bool has_hdr, double conf)
      : delimiter_(std::string(1, delim)), quote_char_(std::string(1, quote)),
        escape_char_(std::string(1, escape)), double_quote_(double_quote),
        line_ending_(line_ending), has_header_(has_hdr), confidence_(conf) {}

  // Construct from libvroom DetectionResult
  explicit Dialect(const libvroom::DetectionResult& result)
      : delimiter_(std::string(1, result.dialect.delimiter)),
        quote_char_(std::string(1, result.dialect.quote_char)),
        escape_char_(std::string(1, result.dialect.escape_char)),
        double_quote_(result.dialect.double_quote), has_header_(result.has_header),
        confidence_(result.confidence) {
    // Convert line ending enum to string
    switch (result.dialect.line_ending) {
    case libvroom::Dialect::LineEnding::LF:
      line_ending_ = "\\n";
      break;
    case libvroom::Dialect::LineEnding::CRLF:
      line_ending_ = "\\r\\n";
      break;
    case libvroom::Dialect::LineEnding::CR:
      line_ending_ = "\\r";
      break;
    case libvroom::Dialect::LineEnding::MIXED:
      line_ending_ = "mixed";
      break;
    default:
      line_ending_ = "unknown";
    }
  }

  std::string delimiter() const { return delimiter_; }
  std::string quote_char() const { return quote_char_; }
  std::string escape_char() const { return escape_char_; }
  bool double_quote() const { return double_quote_; }
  std::string line_ending() const { return line_ending_; }
  bool has_header() const { return has_header_; }
  double confidence() const { return confidence_; }

  std::string repr() const {
    std::ostringstream ss;
    ss << "Dialect(delimiter=" << py::repr(py::str(delimiter_)).cast<std::string>()
       << ", quote_char=" << py::repr(py::str(quote_char_)).cast<std::string>()
       << ", has_header=" << (has_header_ ? "True" : "False") << ", confidence=" << confidence_
       << ")";
    return ss.str();
  }

private:
  std::string delimiter_ = ",";
  std::string quote_char_ = "\"";
  std::string escape_char_ = "\"";
  bool double_quote_ = true;
  std::string line_ending_ = "unknown";
  bool has_header_ = true;
  double confidence_ = 0.0;
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

// Build schema for a column with specified type
static void build_column_schema(ArrowSchema* schema, const char* name, ColumnType type) {
  const char* format = column_type_to_arrow_format(type);
  size_t format_len = std::strlen(format);
  schema->format = new char[format_len + 1];
  std::strcpy(const_cast<char*>(schema->format), format);
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

// Build schema for a string column (convenience wrapper)
static void build_string_column_schema(ArrowSchema* schema, const char* name) {
  build_column_schema(schema, name, ColumnType::STRING);
}

// Build schema for struct (table) with column types
static void build_struct_schema(ArrowSchema* schema, const std::vector<std::string>& column_names,
                                const std::vector<ColumnType>& column_types) {
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
    ColumnType type = (i < column_types.size()) ? column_types[i] : ColumnType::STRING;
    build_column_schema(schema->children[i], column_names[i].c_str(), type);
  }
}

// Helper to calculate the number of bytes needed for a validity bitmap
static size_t validity_bitmap_bytes(size_t num_elements) {
  return (num_elements + 7) / 8;
}

// Build Arrow array for a string column with null value handling
static void build_string_column_array(ArrowArray* array, const std::vector<std::string>& data,
                                      const NullValueConfig& null_config) {
  // First pass: identify null values and calculate total data size
  std::vector<bool> is_null(data.size());
  size_t total_size = 0;
  int64_t null_count = 0;

  for (size_t i = 0; i < data.size(); ++i) {
    if (null_config.is_null_value(data[i])) {
      is_null[i] = true;
      null_count++;
    } else {
      is_null[i] = false;
      total_size += data[i].size();
    }
  }

  // Arrow utf8 format uses int32 offsets, so total size must fit in int32_t
  // INT32_MAX is 2,147,483,647 (~2GB)
  constexpr size_t MAX_UTF8_SIZE = static_cast<size_t>(INT32_MAX);
  if (total_size > MAX_UTF8_SIZE) {
    throw std::overflow_error("Column data exceeds 2GB limit for Arrow utf8 format. "
                              "Total size: " +
                              std::to_string(total_size) + " bytes.");
  }

  // Allocate buffer holder (owns the data)
  auto* buffer_holder = new std::vector<char>();

  // Calculate space needed for validity bitmap
  size_t validity_size = validity_bitmap_bytes(data.size());

  // Build validity bitmap (if there are any nulls)
  // Arrow validity bitmaps: 1 = valid, 0 = null
  // Bits are packed LSB-first within each byte
  std::vector<uint8_t> validity_bitmap;
  if (null_count > 0) {
    validity_bitmap.resize(validity_size, 0xFF); // Start with all valid
    for (size_t i = 0; i < data.size(); ++i) {
      if (is_null[i]) {
        // Clear the bit for null values
        validity_bitmap[i / 8] &= ~(1 << (i % 8));
      }
    }
  }

  // Build offsets buffer (int32 offsets for utf8 format)
  std::vector<int32_t> offsets;
  offsets.reserve(data.size() + 1);
  int32_t offset = 0;
  offsets.push_back(offset);
  for (size_t i = 0; i < data.size(); ++i) {
    // For null values, we still need to advance the offset by 0 (no data stored)
    if (!is_null[i]) {
      offset += static_cast<int32_t>(data[i].size());
    }
    offsets.push_back(offset);
  }

  // Build data buffer: validity bitmap (if needed) + offsets + string data
  size_t offsets_size = offsets.size() * sizeof(int32_t);
  buffer_holder->reserve(validity_size + offsets_size + total_size);

  // Copy validity bitmap (if there are nulls)
  if (null_count > 0) {
    const char* validity_ptr = reinterpret_cast<const char*>(validity_bitmap.data());
    buffer_holder->insert(buffer_holder->end(), validity_ptr, validity_ptr + validity_size);
  }

  // Copy offsets
  size_t offsets_start = buffer_holder->size();
  const char* offsets_ptr = reinterpret_cast<const char*>(offsets.data());
  buffer_holder->insert(buffer_holder->end(), offsets_ptr, offsets_ptr + offsets_size);

  // Copy string data (only for non-null values)
  size_t data_start = buffer_holder->size();
  for (size_t i = 0; i < data.size(); ++i) {
    if (!is_null[i]) {
      buffer_holder->insert(buffer_holder->end(), data[i].begin(), data[i].end());
    }
  }

  // Set up array
  array->length = static_cast<int64_t>(data.size());
  array->null_count = null_count;
  array->offset = 0;
  array->n_buffers = 3; // validity, offsets, data
  array->n_children = 0;
  array->buffers = new const void*[3];
  // Validity bitmap: nullptr means all valid, otherwise points to bitmap
  array->buffers[0] = (null_count > 0) ? buffer_holder->data() : nullptr;
  array->buffers[1] = buffer_holder->data() + offsets_start; // offsets
  array->buffers[2] = buffer_holder->data() + data_start;    // data
  array->children = nullptr;
  array->dictionary = nullptr;
  array->release = release_array;
  array->private_data = buffer_holder;
}

// Build Arrow array for an int64 column with type conversion
static void build_int64_column_array(ArrowArray* array, const std::vector<std::string>& data,
                                     const NullValueConfig& null_config) {
  size_t n_rows = data.size();

  // Allocate buffer for int64 values
  auto* buffer_holder = new std::vector<char>();
  buffer_holder->resize(n_rows * sizeof(int64_t));
  int64_t* values = reinterpret_cast<int64_t*>(buffer_holder->data());

  // Allocate validity bitmap (1 bit per value, rounded up to bytes)
  size_t validity_bytes = (n_rows + 7) / 8;
  auto* validity = new std::vector<uint8_t>(validity_bytes, 0xFF); // All valid initially

  int64_t null_count = 0;
  libvroom::ExtractionConfig config;

  for (size_t i = 0; i < n_rows; ++i) {
    const std::string& s = data[i];
    // Check for null values first
    if (null_config.is_null_value(s)) {
      (*validity)[i / 8] &= ~(1 << (i % 8));
      values[i] = 0;
      ++null_count;
      continue;
    }
    auto result = libvroom::parse_integer<int64_t>(s.data(), s.size(), config);
    if (result.ok()) {
      values[i] = result.get();
    } else {
      // Set null in validity bitmap (clear the bit)
      (*validity)[i / 8] &= ~(1 << (i % 8));
      values[i] = 0; // Arrow convention for null values
      ++null_count;
    }
  }

  // Set up array
  array->length = static_cast<int64_t>(n_rows);
  array->null_count = null_count;
  array->offset = 0;
  array->n_buffers = 2; // validity, data
  array->n_children = 0;
  array->buffers = new const void*[2];
  array->buffers[0] = null_count > 0 ? validity->data() : nullptr;
  array->buffers[1] = buffer_holder->data();
  array->children = nullptr;
  array->dictionary = nullptr;
  array->release = release_array;

  // Store both buffers in private_data for cleanup
  // We'll use a pair to hold both vectors
  struct BufferPair {
    std::vector<char>* values;
    std::vector<uint8_t>* validity;
  };
  auto* pair = new BufferPair{buffer_holder, validity};
  array->private_data = pair;

  // Custom release for int64 with validity buffer
  array->release = [](ArrowArray* arr) {
    if (arr->release == nullptr)
      return;
    if (arr->private_data) {
      auto* p = static_cast<BufferPair*>(arr->private_data);
      delete p->values;
      delete p->validity;
      delete p;
    }
    if (arr->buffers) {
      delete[] arr->buffers;
    }
    arr->release = nullptr;
  };
}

// Build Arrow array for a float64 column with type conversion
static void build_float64_column_array(ArrowArray* array, const std::vector<std::string>& data,
                                       const NullValueConfig& null_config) {
  size_t n_rows = data.size();

  // Allocate buffer for float64 values
  auto* buffer_holder = new std::vector<char>();
  buffer_holder->resize(n_rows * sizeof(double));
  double* values = reinterpret_cast<double*>(buffer_holder->data());

  // Allocate validity bitmap
  size_t validity_bytes = (n_rows + 7) / 8;
  auto* validity = new std::vector<uint8_t>(validity_bytes, 0xFF);

  int64_t null_count = 0;
  libvroom::ExtractionConfig config;

  for (size_t i = 0; i < n_rows; ++i) {
    const std::string& s = data[i];
    // Check for null values first
    if (null_config.is_null_value(s)) {
      (*validity)[i / 8] &= ~(1 << (i % 8));
      values[i] = 0.0;
      ++null_count;
      continue;
    }
    auto result = libvroom::parse_double(s.data(), s.size(), config);
    if (result.ok()) {
      values[i] = result.get();
    } else {
      (*validity)[i / 8] &= ~(1 << (i % 8));
      values[i] = 0.0;
      ++null_count;
    }
  }

  // Set up array
  array->length = static_cast<int64_t>(n_rows);
  array->null_count = null_count;
  array->offset = 0;
  array->n_buffers = 2;
  array->n_children = 0;
  array->buffers = new const void*[2];
  array->buffers[0] = null_count > 0 ? validity->data() : nullptr;
  array->buffers[1] = buffer_holder->data();
  array->children = nullptr;
  array->dictionary = nullptr;

  struct BufferPair {
    std::vector<char>* values;
    std::vector<uint8_t>* validity;
  };
  auto* pair = new BufferPair{buffer_holder, validity};
  array->private_data = pair;

  array->release = [](ArrowArray* arr) {
    if (arr->release == nullptr)
      return;
    if (arr->private_data) {
      auto* p = static_cast<BufferPair*>(arr->private_data);
      delete p->values;
      delete p->validity;
      delete p;
    }
    if (arr->buffers) {
      delete[] arr->buffers;
    }
    arr->release = nullptr;
  };
}

// Build Arrow array for a boolean column with type conversion
static void build_bool_column_array(ArrowArray* array, const std::vector<std::string>& data,
                                    const NullValueConfig& null_config) {
  size_t n_rows = data.size();

  // Allocate buffer for boolean values (1 bit per value)
  size_t data_bytes = (n_rows + 7) / 8;
  auto* data_buffer = new std::vector<uint8_t>(data_bytes, 0);

  // Allocate validity bitmap
  size_t validity_bytes = (n_rows + 7) / 8;
  auto* validity = new std::vector<uint8_t>(validity_bytes, 0xFF);

  int64_t null_count = 0;
  libvroom::ExtractionConfig config;

  for (size_t i = 0; i < n_rows; ++i) {
    const std::string& s = data[i];
    // Check for null values first
    if (null_config.is_null_value(s)) {
      (*validity)[i / 8] &= ~(1 << (i % 8));
      ++null_count;
      continue;
    }
    auto result = libvroom::parse_bool(s.data(), s.size(), config);
    if (result.ok()) {
      if (result.get()) {
        (*data_buffer)[i / 8] |= (1 << (i % 8));
      }
      // false is already 0 (default)
    } else {
      (*validity)[i / 8] &= ~(1 << (i % 8));
      ++null_count;
    }
  }

  // Set up array
  array->length = static_cast<int64_t>(n_rows);
  array->null_count = null_count;
  array->offset = 0;
  array->n_buffers = 2;
  array->n_children = 0;
  array->buffers = new const void*[2];
  array->buffers[0] = null_count > 0 ? validity->data() : nullptr;
  array->buffers[1] = data_buffer->data();
  array->children = nullptr;
  array->dictionary = nullptr;

  struct BufferPair {
    std::vector<uint8_t>* values;
    std::vector<uint8_t>* validity;
  };
  auto* pair = new BufferPair{data_buffer, validity};
  array->private_data = pair;

  array->release = [](ArrowArray* arr) {
    if (arr->release == nullptr)
      return;
    if (arr->private_data) {
      auto* p = static_cast<BufferPair*>(arr->private_data);
      delete p->values;
      delete p->validity;
      delete p;
    }
    if (arr->buffers) {
      delete[] arr->buffers;
    }
    arr->release = nullptr;
  };
}

// Build Arrow array for a column with specified type
static void build_column_array(ArrowArray* array, const std::vector<std::string>& data,
                               ColumnType type, const NullValueConfig& null_config) {
  switch (type) {
  case ColumnType::INT64:
    build_int64_column_array(array, data, null_config);
    break;
  case ColumnType::FLOAT64:
    build_float64_column_array(array, data, null_config);
    break;
  case ColumnType::BOOL:
    build_bool_column_array(array, data, null_config);
    break;
  case ColumnType::STRING:
  default:
    build_string_column_array(array, data, null_config);
    break;
  }
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
    ColumnType type = table_data->get_column_type(i);
    build_column_array(array->children[i], table_data->columns_data[i], type,
                       table_data->null_config);
  }
}

// Stream callbacks
static int stream_get_schema(ArrowArrayStream* stream, ArrowSchema* out) {
  auto* priv = static_cast<StreamPrivateData*>(stream->private_data);
  if (!priv || !priv->table_data) {
    return -1;
  }

  build_struct_schema(out, priv->table_data->column_names, priv->table_data->column_types);
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

  // Number of rows (excluding header, after applying skip_rows/n_rows)
  size_t num_rows() const { return data_->effective_num_rows(); }

  // Number of columns (respects column selection)
  size_t num_columns() const { return data_->effective_num_columns(); }

  // Column names
  std::vector<std::string> column_names() const { return data_->column_names; }

  // Get a single column as list of strings (respects skip_rows/n_rows and column selection)
  std::vector<std::string> column(size_t index) const {
    if (index >= data_->effective_num_columns()) {
      throw py::index_error("Column index out of range");
    }
    // Extract only the filtered rows with column mapping
    size_t underlying_idx = data_->map_column_index(index);
    std::vector<std::string> result;
    size_t n = data_->effective_num_rows();
    result.reserve(n);
    for (size_t i = 0; i < n; ++i) {
      auto r = data_->result.row(data_->translate_row_index(i));
      result.push_back(r.get_string(underlying_idx));
    }
    return result;
  }

  // Get a column by name (respects skip_rows/n_rows and column selection)
  std::vector<std::string> column_by_name(const std::string& name) const {
    // Find the name in our selected column names
    auto it = std::find(data_->column_names.begin(), data_->column_names.end(), name);
    if (it == data_->column_names.end()) {
      throw py::key_error("Column not found: " + name);
    }
    size_t logical_idx = static_cast<size_t>(std::distance(data_->column_names.begin(), it));
    return column(logical_idx);
  }

  // Get a single row as list of strings (respects skip_rows/n_rows and column selection)
  std::vector<std::string> row(size_t index) const {
    if (index >= data_->effective_num_rows()) {
      throw py::index_error("Row index out of range");
    }
    auto r = data_->result.row(data_->translate_row_index(index));
    std::vector<std::string> result;
    size_t n_cols = data_->effective_num_columns();
    result.reserve(n_cols);
    for (size_t col = 0; col < n_cols; ++col) {
      size_t underlying_col = data_->map_column_index(col);
      result.push_back(r.get_string(underlying_col));
    }
    return result;
  }

  // Arrow PyCapsule interface: __arrow_c_schema__
  py::object arrow_c_schema() const {
    auto* schema = new ArrowSchema();
    build_struct_schema(schema, data_->column_names, data_->column_types);

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
    // Note: requested_schema is currently ignored - we use column_types from parsing
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
// BatchedReader class - iterator for memory-efficient batch processing
// =============================================================================

// Holds data for a single batch
struct BatchData {
  std::vector<std::string> column_names;
  std::vector<std::vector<std::string>> columns_data;
  std::vector<ColumnType> column_types;
  NullValueConfig null_config;

  size_t num_rows() const { return columns_data.empty() ? 0 : columns_data[0].size(); }

  size_t num_columns() const { return column_names.size(); }

  ColumnType get_column_type(size_t col) const {
    if (col < column_types.size()) {
      return column_types[col];
    }
    return ColumnType::STRING;
  }
};

// Build schema for batch (struct with column types)
static void build_batch_schema(ArrowSchema* schema, const std::vector<std::string>& column_names,
                               const std::vector<ColumnType>& column_types) {
  // Reuse the existing struct schema builder
  build_struct_schema(schema, column_names, column_types);
}

// Build Arrow array for batch
static void build_batch_array(ArrowArray* array, std::shared_ptr<BatchData> batch_data) {
  size_t n_cols = batch_data->columns_data.size();
  size_t n_rows = batch_data->num_rows();

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
    ColumnType type = batch_data->get_column_type(i);
    build_column_array(array->children[i], batch_data->columns_data[i], type,
                       batch_data->null_config);
  }
}

// Stream private data for batch
struct BatchStreamPrivateData {
  std::shared_ptr<BatchData> batch_data;
  bool schema_exported = false;
  bool data_exported = false;
  std::string last_error;
};

// Batch stream callbacks
static int batch_stream_get_schema(ArrowArrayStream* stream, ArrowSchema* out) {
  auto* priv = static_cast<BatchStreamPrivateData*>(stream->private_data);
  if (!priv || !priv->batch_data) {
    return -1;
  }

  build_batch_schema(out, priv->batch_data->column_names, priv->batch_data->column_types);
  priv->schema_exported = true;
  return 0;
}

static int batch_stream_get_next(ArrowArrayStream* stream, ArrowArray* out) {
  auto* priv = static_cast<BatchStreamPrivateData*>(stream->private_data);
  if (!priv || !priv->batch_data) {
    return -1;
  }

  if (priv->data_exported) {
    // No more batches - signal end of stream
    out->release = nullptr;
    return 0;
  }

  build_batch_array(out, priv->batch_data);
  priv->data_exported = true;
  return 0;
}

static const char* batch_stream_get_last_error(ArrowArrayStream* stream) {
  auto* priv = static_cast<BatchStreamPrivateData*>(stream->private_data);
  if (!priv) {
    return "Invalid stream";
  }
  return priv->last_error.empty() ? nullptr : priv->last_error.c_str();
}

static void release_batch_stream(ArrowArrayStream* stream) {
  if (stream->release == nullptr)
    return;
  if (stream->private_data) {
    auto* data = static_cast<BatchStreamPrivateData*>(stream->private_data);
    delete data;
  }
  stream->release = nullptr;
}

// RecordBatch class - represents a single batch from the iterator
class RecordBatch {
public:
  RecordBatch(std::shared_ptr<BatchData> data) : data_(std::move(data)) {}

  // Number of rows in this batch
  size_t num_rows() const { return data_->num_rows(); }

  // Number of columns
  size_t num_columns() const { return data_->num_columns(); }

  // Column names
  std::vector<std::string> column_names() const { return data_->column_names; }

  // Get a single column as list of strings
  std::vector<std::string> column(size_t index) const {
    if (index >= data_->columns_data.size()) {
      throw py::index_error("Column index out of range");
    }
    return data_->columns_data[index];
  }

  // Get a column by name
  std::vector<std::string> column_by_name(const std::string& name) const {
    auto it = std::find(data_->column_names.begin(), data_->column_names.end(), name);
    if (it == data_->column_names.end()) {
      throw py::key_error("Column not found: " + name);
    }
    size_t idx = static_cast<size_t>(std::distance(data_->column_names.begin(), it));
    return column(idx);
  }

  // Get a single row as list of strings
  std::vector<std::string> row(size_t index) const {
    if (index >= data_->num_rows()) {
      throw py::index_error("Row index out of range");
    }
    std::vector<std::string> result;
    result.reserve(data_->columns_data.size());
    for (const auto& col : data_->columns_data) {
      result.push_back(col[index]);
    }
    return result;
  }

  // Arrow PyCapsule interface: __arrow_c_schema__
  py::object arrow_c_schema() const {
    auto* schema = new ArrowSchema();
    build_batch_schema(schema, data_->column_names, data_->column_types);

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
    (void)requested_schema;

    auto* stream = new ArrowArrayStream();
    auto* priv = new BatchStreamPrivateData();
    priv->batch_data = data_;
    priv->schema_exported = false;
    priv->data_exported = false;

    stream->get_schema = batch_stream_get_schema;
    stream->get_next = batch_stream_get_next;
    stream->get_last_error = batch_stream_get_last_error;
    stream->release = release_batch_stream;
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
    ss << "RecordBatch(" << num_rows() << " rows, " << num_columns() << " columns)";
    return ss.str();
  }

private:
  std::shared_ptr<BatchData> data_;
};

// BatchedReader class - Python iterator over CSV batches
class BatchedReader {
public:
  BatchedReader(const std::string& path, size_t batch_size,
                std::optional<std::string> delimiter = std::nullopt,
                std::optional<std::string> quote_char = std::nullopt, bool has_header = true,
                std::optional<std::vector<std::string>> null_values = std::nullopt,
                bool empty_is_null = true,
                std::optional<std::unordered_map<std::string, std::string>> dtype = std::nullopt)
      : path_(path), batch_size_(batch_size), has_header_(has_header), exhausted_(false) {
    // Configure null value handling
    if (null_values) {
      null_config_.null_values = *null_values;
    }
    null_config_.empty_is_null = empty_is_null;

    // Store dtype overrides for later
    if (dtype) {
      dtype_overrides_ = *dtype;
    }

    // Configure stream options
    libvroom::StreamConfig config;
    config.parse_header = has_header;

    if (delimiter) {
      if (delimiter->length() != 1) {
        throw py::value_error("Delimiter must be a single character");
      }
      config.dialect.delimiter = (*delimiter)[0];
    }

    if (quote_char) {
      if (quote_char->length() != 1) {
        throw py::value_error("quote_char must be a single character");
      }
      config.dialect.quote_char = (*quote_char)[0];
    }

    // Create the stream reader
    try {
      reader_ = std::make_unique<libvroom::StreamReader>(path, config);
    } catch (const std::runtime_error& e) {
      throw py::value_error(std::string("Failed to open file: ") + e.what());
    }

    // Note: column names will be populated on first batch read
    // The header is parsed lazily when the first row is read
  }

  // Return self for iterator protocol
  BatchedReader& iter() { return *this; }

  // Get next batch - returns RecordBatch or raises StopIteration
  RecordBatch next() {
    if (exhausted_) {
      throw py::stop_iteration();
    }

    auto batch_data = std::make_shared<BatchData>();
    batch_data->null_config = null_config_;

    // Read rows into the batch
    size_t rows_read = 0;
    bool first_row_of_batch = true;
    size_t n_cols = 0;

    while (rows_read < batch_size_ && reader_->next_row()) {
      const auto& row = reader_->row();

      // On first row of first batch, get column names from header or generate them
      if (first_row_of_batch && column_names_.empty()) {
        // Try to get column names from the reader's header (if parse_header was enabled)
        const auto& header = reader_->header();
        if (!header.empty()) {
          column_names_ = header;
        } else {
          // No header - generate column names from the first row's field count
          size_t field_count = row.field_count();
          column_names_.reserve(field_count);
          for (size_t i = 0; i < field_count; ++i) {
            column_names_.push_back("column_" + std::to_string(i));
          }
        }
      }

      // On first row of any batch, initialize column vectors
      if (first_row_of_batch) {
        n_cols = column_names_.size();
        batch_data->columns_data.resize(n_cols);
        for (auto& col : batch_data->columns_data) {
          col.reserve(batch_size_);
        }
        first_row_of_batch = false;
      }

      // Extract field values
      for (size_t i = 0; i < n_cols && i < row.field_count(); ++i) {
        batch_data->columns_data[i].push_back(row[i].unescaped());
      }
      // Handle rows with fewer fields than expected
      for (size_t i = row.field_count(); i < n_cols; ++i) {
        batch_data->columns_data[i].push_back("");
      }

      ++rows_read;
    }

    // Check if we're done (no rows read means iterator is exhausted)
    if (rows_read == 0) {
      exhausted_ = true;
      throw py::stop_iteration();
    }

    // Note: We don't check reader_->eof() here because the streaming reader
    // might report eof while still having buffered rows. The only reliable
    // way to know we're done is when next_row() returns false, which happens
    // in the loop above and causes rows_read == 0 on the next call.

    // Set column names
    batch_data->column_names = column_names_;

    // Perform type inference on the batch data
    size_t num_cols = batch_data->columns_data.size();
    batch_data->column_types.resize(num_cols, ColumnType::STRING);

    if (rows_read > 0 && num_cols > 0) {
      libvroom::ColumnTypeInference inference(num_cols);

      // Sample rows for type inference (use all rows in small batches)
      size_t rows_to_sample = std::min(rows_read, static_cast<size_t>(1000));
      for (size_t row = 0; row < rows_to_sample; ++row) {
        for (size_t col = 0; col < num_cols; ++col) {
          const std::string& value = batch_data->columns_data[col][row];
          inference.add_field(col, reinterpret_cast<const uint8_t*>(value.data()), value.size());
        }
      }

      // Get inferred types
      std::vector<libvroom::FieldType> inferred = inference.infer_types();
      for (size_t col = 0; col < num_cols; ++col) {
        batch_data->column_types[col] = field_type_to_column_type(inferred[col]);
      }
    }

    // Apply dtype overrides
    for (const auto& [col_name, type_str] : dtype_overrides_) {
      auto it =
          std::find(batch_data->column_names.begin(), batch_data->column_names.end(), col_name);
      if (it == batch_data->column_names.end()) {
        throw py::value_error("Column not found for dtype: " + col_name);
      }
      size_t col_idx = std::distance(batch_data->column_names.begin(), it);

      auto col_type = parse_dtype_string(type_str);
      if (!col_type) {
        throw py::value_error("Unknown dtype '" + type_str + "' for column '" + col_name + "'");
      }
      batch_data->column_types[col_idx] = *col_type;
    }

    return RecordBatch(std::move(batch_data));
  }

  // Get column names (available after first batch or from header)
  std::vector<std::string> column_names() const { return column_names_; }

  // Get batch size
  size_t batch_size() const { return batch_size_; }

  // Get file path
  std::string path() const { return path_; }

  // Check if iterator is exhausted
  bool is_exhausted() const { return exhausted_; }

  // String representation
  std::string repr() const {
    std::ostringstream ss;
    ss << "BatchedReader(path=" << py::repr(py::str(path_)).cast<std::string>()
       << ", batch_size=" << batch_size_ << ")";
    return ss.str();
  }

private:
  std::string path_;
  size_t batch_size_;
  bool has_header_;
  bool exhausted_;
  std::unique_ptr<libvroom::StreamReader> reader_;
  std::vector<std::string> column_names_;
  NullValueConfig null_config_;
  std::unordered_map<std::string, std::string> dtype_overrides_;
};

// Factory function for read_csv_batched
BatchedReader
read_csv_batched(const std::string& path, size_t batch_size = 10000,
                 std::optional<std::string> delimiter = std::nullopt,
                 std::optional<std::string> quote_char = std::nullopt, bool has_header = true,
                 std::optional<std::vector<std::string>> null_values = std::nullopt,
                 bool empty_is_null = true,
                 std::optional<std::unordered_map<std::string, std::string>> dtype = std::nullopt) {
  return BatchedReader(path, batch_size, delimiter, quote_char, has_header, null_values,
                       empty_is_null, dtype);
}

// =============================================================================
// detect_dialect function
// =============================================================================

Dialect detect_dialect(const std::string& path) {
  // Load file
  libvroom::FileBuffer buffer;
  try {
    buffer = libvroom::load_file(path);
  } catch (const std::runtime_error& e) {
    throw py::value_error(std::string("Failed to load file: ") + e.what());
  }

  if (!buffer.valid()) {
    throw py::value_error("Failed to load file: " + path);
  }

  // Detect dialect
  auto result = libvroom::detect_dialect(buffer.data(), buffer.size());

  if (!result.success()) {
    throw py::value_error("Failed to detect CSV dialect");
  }

  return Dialect(result);
}

// =============================================================================
// read_csv function with full options
// =============================================================================

// Type alias for Python progress callback: (bytes_read: int, total_bytes: int) -> None
using PyProgressCallback = std::function<void(size_t, size_t)>;

Table read_csv(const std::string& path, std::optional<std::string> delimiter = std::nullopt,
               std::optional<std::string> quote_char = std::nullopt, bool has_header = true,
               std::optional<std::string> encoding = std::nullopt, size_t skip_rows = 0,
               std::optional<size_t> n_rows = std::nullopt,
               std::optional<std::vector<py::object>> usecols = std::nullopt,
               std::optional<std::vector<std::string>> null_values = std::nullopt,
               bool empty_is_null = true,
               std::optional<std::unordered_map<std::string, std::string>> dtype = std::nullopt,
               size_t num_threads = 1, std::optional<PyProgressCallback> progress = std::nullopt) {
  auto data = std::make_shared<TableData>();

  // Configure null value handling
  if (null_values) {
    data->null_config.null_values = *null_values;
  }
  data->null_config.empty_is_null = empty_is_null;

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
  libvroom::Dialect dialect;

  if (delimiter) {
    if (delimiter->length() != 1) {
      throw py::value_error("Delimiter must be a single character");
    }
    dialect.delimiter = (*delimiter)[0];
    options.dialect = dialect;
  }

  if (quote_char) {
    if (quote_char->length() != 1) {
      throw py::value_error("quote_char must be a single character");
    }
    dialect.quote_char = (*quote_char)[0];
    options.dialect = dialect;
  }

  // If any dialect option was specified, ensure we use explicit dialect
  if (delimiter || quote_char) {
    options.dialect = dialect;
  }

  // Set up progress callback if provided
  // The Python callback has signature (bytes_read: int, total_bytes: int) -> None
  // The C++ callback expects (bytes_processed: size_t, total_bytes: size_t) -> bool
  // We wrap the Python callback to handle:
  // 1. GIL acquisition for thread safety
  // 2. Python exception handling
  // 3. Return value (Python returns None, C++ expects bool for cancellation)
  if (progress) {
    options.progress_callback = [py_callback = *progress](size_t bytes_processed,
                                                          size_t total_bytes) -> bool {
      // Acquire GIL before calling Python code
      py::gil_scoped_acquire acquire;
      try {
        py_callback(bytes_processed, total_bytes);
        return true; // Continue parsing
      } catch (py::error_already_set& e) {
        // Re-throw Python exceptions to be handled by pybind11
        throw;
      } catch (...) {
        // For any other exception, abort parsing
        return false;
      }
    };
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
  std::vector<std::string> all_column_names;
  if (has_header) {
    all_column_names = data->result.header();
  } else {
    size_t n_cols = data->result.num_columns();
    all_column_names.reserve(n_cols);
    for (size_t i = 0; i < n_cols; ++i) {
      all_column_names.push_back("column_" + std::to_string(i));
    }
  }

  // Handle column selection (usecols)
  if (usecols) {
    for (const auto& col : *usecols) {
      if (py::isinstance<py::int_>(col)) {
        // Column by index
        size_t idx = col.cast<size_t>();
        if (idx >= all_column_names.size()) {
          throw py::index_error("Column index " + std::to_string(idx) + " out of range");
        }
        data->selected_columns.push_back(idx);
      } else if (py::isinstance<py::str>(col)) {
        // Column by name
        std::string name = col.cast<std::string>();
        auto it = std::find(all_column_names.begin(), all_column_names.end(), name);
        if (it == all_column_names.end()) {
          throw py::key_error("Column not found: " + name);
        }
        data->selected_columns.push_back(
            static_cast<size_t>(std::distance(all_column_names.begin(), it)));
      } else {
        throw py::type_error("usecols elements must be int or str");
      }
    }

    // Filter column names based on selection
    data->column_names.reserve(data->selected_columns.size());
    for (size_t idx : data->selected_columns) {
      data->column_names.push_back(all_column_names[idx]);
    }
  } else {
    data->column_names = std::move(all_column_names);
  }

  // Store row filtering parameters
  data->skip_rows_ = skip_rows;
  data->n_rows_ = n_rows;

  // Note: encoding is accepted but not fully implemented in this phase.
  // It is stored for future use or passed through where possible.
  (void)encoding; // Encoding is handled automatically by libvroom

  // Automatic type inference: detect column types from data
  // Use effective_num_rows() to respect skip_rows and n_rows parameters
  size_t n_cols = data->column_names.size();
  data->column_types.resize(n_cols, ColumnType::STRING);

  // Run type inference on sampled rows (from the filtered dataset)
  constexpr size_t TYPE_INFERENCE_ROWS = 1000;
  size_t effective_rows = data->effective_num_rows();
  size_t n_rows_to_sample = std::min(effective_rows, TYPE_INFERENCE_ROWS);

  if (n_rows_to_sample > 0) {
    libvroom::ColumnTypeInference inference(n_cols);

    for (size_t row = 0; row < n_rows_to_sample; ++row) {
      // Use translate_row_index to account for skip_rows
      auto r = data->result.row(data->translate_row_index(row));
      for (size_t col = 0; col < n_cols; ++col) {
        // Map to underlying column if using usecols
        size_t underlying_col = data->selected_columns.empty() ? col : data->selected_columns[col];
        std::string value = r.get_string(underlying_col);
        inference.add_field(col, reinterpret_cast<const uint8_t*>(value.data()), value.size());
      }
    }

    // Get inferred types and convert to ColumnType
    std::vector<libvroom::FieldType> inferred = inference.infer_types();
    for (size_t col = 0; col < n_cols; ++col) {
      data->column_types[col] = field_type_to_column_type(inferred[col]);
    }
  }

  // Apply explicit dtype overrides (these take precedence over inferred types)
  if (dtype) {
    for (const auto& [col_name, type_str] : *dtype) {
      // Find column index by name
      auto it = std::find(data->column_names.begin(), data->column_names.end(), col_name);
      if (it == data->column_names.end()) {
        throw py::value_error("Column not found for dtype: " + col_name);
      }
      size_t col_idx = std::distance(data->column_names.begin(), it);

      // Parse the type string
      auto col_type = parse_dtype_string(type_str);
      if (!col_type) {
        throw py::value_error("Unknown dtype '" + type_str + "' for column '" + col_name +
                              "'. Supported types: str, string, object, int, int64, float, "
                              "float64, double, bool, boolean");
      }
      data->column_types[col_idx] = *col_type;
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

  // Dialect class
  py::class_<Dialect>(m, "Dialect", R"doc(
CSV dialect configuration and detection result.

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

Examples
--------
>>> import vroom_csv
>>> dialect = vroom_csv.detect_dialect("data.csv")
>>> print(f"Delimiter: {dialect.delimiter!r}")
>>> print(f"Has header: {dialect.has_header}")
>>> print(f"Confidence: {dialect.confidence:.0%}")
)doc")
      .def_property_readonly("delimiter", &Dialect::delimiter, "Field delimiter character")
      .def_property_readonly("quote_char", &Dialect::quote_char, "Quote character")
      .def_property_readonly("escape_char", &Dialect::escape_char, "Escape character")
      .def_property_readonly("double_quote", &Dialect::double_quote,
                             "Whether quotes are escaped by doubling")
      .def_property_readonly("line_ending", &Dialect::line_ending, "Detected line ending style")
      .def_property_readonly("has_header", &Dialect::has_header,
                             "Whether first row appears to be header")
      .def_property_readonly("confidence", &Dialect::confidence, "Detection confidence (0.0-1.0)")
      .def("__repr__", &Dialect::repr);

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
>>> print(table.num_rows, table.num_columns)

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

  // detect_dialect function
  m.def("detect_dialect", &detect_dialect, py::arg("path"),
        R"doc(
Detect the CSV dialect of a file.

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

Examples
--------
>>> import vroom_csv
>>> dialect = vroom_csv.detect_dialect("data.csv")
>>> print(f"Delimiter: {dialect.delimiter!r}")
>>> print(f"Quote char: {dialect.quote_char!r}")
>>> print(f"Has header: {dialect.has_header}")
>>> print(f"Confidence: {dialect.confidence:.0%}")

# Use detected dialect with read_csv
>>> table = vroom_csv.read_csv("data.csv", delimiter=dialect.delimiter)
)doc");

  // read_csv function with full options
  m.def("read_csv", &read_csv, py::arg("path"), py::arg("delimiter") = py::none(),
        py::arg("quote_char") = py::none(), py::arg("has_header") = true,
        py::arg("encoding") = py::none(), py::arg("skip_rows") = 0, py::arg("n_rows") = py::none(),
        py::arg("usecols") = py::none(), py::arg("null_values") = py::none(),
        py::arg("empty_is_null") = true, py::arg("dtype") = py::none(), py::arg("num_threads") = 1,
        py::arg("progress") = py::none(),
        R"doc(
Read a CSV file and return a Table object.

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
    Number of data rows to skip after the header (if has_header=True)
    or from the beginning of the file (if has_header=False).
n_rows : int, optional
    Maximum number of data rows to read. If not specified, all rows
    are read.
usecols : list of str or int, optional
    List of column names or indices to read. If not specified, reads
    all columns.
null_values : list[str], optional
    List of strings to interpret as null/missing values during Arrow export.
    If not specified, defaults to ["", "NA", "N/A", "null", "NULL", "None", "NaN"].
    When converting to Arrow format (via PyArrow, Polars, etc.), values matching
    this list will be represented as null in the resulting Arrow array.
empty_is_null : bool, default True
    If True, empty strings are treated as null values during Arrow export,
    in addition to any values in null_values.
dtype : dict[str, str], optional
    Dictionary mapping column names to data types for Arrow export.
    By default, column types are automatically inferred from the data
    (integers, floats, booleans, strings). Use this parameter to override
    inferred types for specific columns.
    Supported types: 'str', 'string', 'object' (string), 'int', 'int64'
    (64-bit integer), 'float', 'float64', 'double' (64-bit float),
    'bool', 'boolean' (boolean).
    Values that cannot be converted to the specified type become null.
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
    If the file cannot be read or parsed, or if an unknown dtype is specified.
ParseError
    If there are fatal parse errors in the CSV.
IndexError
    If a column index in usecols is out of range.
KeyError
    If a column name in usecols is not found.

Examples
--------
>>> import vroom_csv
>>> table = vroom_csv.read_csv("data.csv")
>>> print(f"Loaded {table.num_rows} rows")

>>> # With explicit delimiter
>>> table = vroom_csv.read_csv("data.tsv", delimiter="\\t")

>>> # Read specific columns
>>> table = vroom_csv.read_csv("data.csv", usecols=["id", "name", "value"])

>>> # With null value handling for Arrow export
>>> table = vroom_csv.read_csv("data.csv", null_values=["NA", "N/A", "-"])
>>> import pyarrow as pa
>>> arrow_table = pa.table(table)  # NA, N/A, and - will be null

>>> # Skip first 10 rows and read only 100 rows
>>> table = vroom_csv.read_csv("data.csv", skip_rows=10, n_rows=100)

>>> # Multi-threaded parsing
>>> table = vroom_csv.read_csv("large.csv", num_threads=4)

>>> # Types are automatically inferred (integers, floats, bools detected)
>>> table = vroom_csv.read_csv("data.csv")
>>> import pyarrow as pa
>>> arrow_table = pa.table(table)  # columns have inferred types

>>> # Override inferred types with explicit dtype
>>> table = vroom_csv.read_csv("data.csv", dtype={"zip_code": "string", "age": "int64"})

>>> # Treat empty strings as null (default behavior)
>>> table = vroom_csv.read_csv("data.csv", empty_is_null=True)

>>> # With progress callback
>>> def show_progress(bytes_read, total_bytes):
...     pct = bytes_read / total_bytes * 100 if total_bytes > 0 else 0
...     print(f"\r{pct:.1f}%", end="", flush=True)
>>> table = vroom_csv.read_csv("huge.csv", progress=show_progress)
>>> print()  # newline after progress

>>> # Convert to Polars
>>> import polars as pl
>>> df = pl.from_arrow(table)
)doc");

  // RecordBatch class (for batched reading)
  py::class_<RecordBatch>(m, "RecordBatch", R"doc(
A single batch of rows from a batched CSV read operation.

This class represents a single batch returned by BatchedReader iteration.
It implements the Arrow PyCapsule interface for zero-copy interoperability
with PyArrow, Polars, DuckDB, and other Arrow-compatible libraries.

Examples
--------
>>> import vroom_csv
>>> for batch in vroom_csv.read_csv_batched("data.csv", batch_size=1000):
...     print(f"Batch has {batch.num_rows} rows")
...     # Convert to Polars for processing
...     import polars as pl
...     df = pl.from_arrow(batch)
...     # Process df...
)doc")
      .def_property_readonly("num_rows", &RecordBatch::num_rows, "Number of rows in this batch")
      .def_property_readonly("num_columns", &RecordBatch::num_columns, "Number of columns")
      .def_property_readonly("column_names", &RecordBatch::column_names, "List of column names")
      .def("column", &RecordBatch::column, py::arg("index"),
           "Get column by index as list of strings")
      .def("column", &RecordBatch::column_by_name, py::arg("name"),
           "Get column by name as list of strings")
      .def("row", &RecordBatch::row, py::arg("index"), "Get row by index as list of strings")
      .def("__repr__", &RecordBatch::repr)
      .def("__len__", &RecordBatch::num_rows)
      // Arrow PyCapsule interface
      .def("__arrow_c_schema__", &RecordBatch::arrow_c_schema,
           "Export batch schema via Arrow C Data Interface")
      .def("__arrow_c_stream__", &RecordBatch::arrow_c_stream,
           py::arg("requested_schema") = py::none(),
           "Export batch data via Arrow C Stream Interface");

  // BatchedReader class
  py::class_<BatchedReader>(m, "BatchedReader", R"doc(
Iterator for memory-efficient batch processing of large CSV files.

This class provides an iterator that reads CSV files in batches, keeping only
one batch in memory at a time. Each batch is a RecordBatch object that
implements the Arrow PyCapsule interface.

Use read_csv_batched() to create a BatchedReader.

Attributes
----------
path : str
    Path to the CSV file being read.
batch_size : int
    Number of rows per batch.
column_names : list[str]
    Column names from the CSV header (or generated names if no header).

Examples
--------
>>> import vroom_csv
>>> import polars as pl
>>>
>>> # Process large file in batches
>>> for batch in vroom_csv.read_csv_batched("large.csv", batch_size=10000):
...     df = pl.from_arrow(batch)
...     # Process each batch without loading entire file
...     process(df)
>>>
>>> # Early termination is safe
>>> reader = vroom_csv.read_csv_batched("large.csv")
>>> for batch in reader:
...     if should_stop(batch):
...         break  # Resources cleaned up automatically
)doc")
      .def_property_readonly("path", &BatchedReader::path, "Path to the CSV file")
      .def_property_readonly("batch_size", &BatchedReader::batch_size, "Number of rows per batch")
      .def_property_readonly("column_names", &BatchedReader::column_names, "List of column names")
      .def("__repr__", &BatchedReader::repr)
      .def("__iter__", &BatchedReader::iter, py::return_value_policy::reference)
      .def("__next__", &BatchedReader::next);

  // read_csv_batched function
  m.def("read_csv_batched", &read_csv_batched, py::arg("path"), py::arg("batch_size") = 10000,
        py::arg("delimiter") = py::none(), py::arg("quote_char") = py::none(),
        py::arg("has_header") = true, py::arg("null_values") = py::none(),
        py::arg("empty_is_null") = true, py::arg("dtype") = py::none(),
        R"doc(
Read a CSV file in batches for memory-efficient processing.

This function returns an iterator that yields RecordBatch objects,
each containing batch_size rows (except possibly the last batch).
Only one batch is kept in memory at a time, making this suitable
for processing files larger than available memory.

Parameters
----------
path : str
    Path to the CSV file to read.
batch_size : int, default 10000
    Number of rows per batch.
delimiter : str, optional
    Field delimiter character. If not specified, defaults to comma (',').
quote_char : str, optional
    Quote character for escaping fields. Default is '"'.
has_header : bool, default True
    Whether the first row contains column headers.
null_values : list[str], optional
    List of strings to interpret as null/missing values during Arrow export.
    If not specified, defaults to ["", "NA", "N/A", "null", "NULL", "None", "NaN"].
empty_is_null : bool, default True
    If True, empty strings are treated as null values during Arrow export.
dtype : dict[str, str], optional
    Dictionary mapping column names to data types for Arrow export.
    By default, column types are automatically inferred from the data.
    Supported types: 'str', 'string', 'int', 'int64', 'float', 'float64',
    'bool', 'boolean'.

Returns
-------
BatchedReader
    An iterator yielding RecordBatch objects.

Raises
------
ValueError
    If the file cannot be opened, delimiter/quote_char is not a single
    character, or an unknown dtype is specified.

Examples
--------
>>> import vroom_csv
>>> import polars as pl
>>>
>>> # Basic usage - process file in batches
>>> for batch in vroom_csv.read_csv_batched("large.csv"):
...     df = pl.from_arrow(batch)
...     print(f"Processing {df.shape[0]} rows")
>>>
>>> # Custom batch size
>>> for batch in vroom_csv.read_csv_batched("large.csv", batch_size=50000):
...     process(batch)
>>>
>>> # With explicit delimiter (TSV)
>>> for batch in vroom_csv.read_csv_batched("data.tsv", delimiter="\\t"):
...     process(batch)
>>>
>>> # Aggregate results across batches
>>> total_sum = 0
>>> for batch in vroom_csv.read_csv_batched("data.csv", dtype={"value": "int64"}):
...     import pyarrow as pa
...     arrow_table = pa.table(batch)
...     total_sum += sum(v for v in arrow_table.column("value").to_pylist() if v is not None)
>>>
>>> # File without header
>>> for batch in vroom_csv.read_csv_batched("no_header.csv", has_header=False):
...     print(batch.column_names)  # ['column_0', 'column_1', ...]
)doc");

  // Version info
  m.attr("__version__") = "0.1.0";
  m.attr("LIBVROOM_VERSION") = LIBVROOM_VERSION_STRING;
}
