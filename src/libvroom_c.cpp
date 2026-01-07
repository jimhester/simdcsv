/**
 * @file libvroom_c.cpp
 * @brief C API wrapper implementation for the libvroom library.
 */

#include "libvroom_c.h"

#include "libvroom.h"

#include "common_defs.h"
#include "dialect.h"
#include "encoding.h"
#include "error.h"
#include "io_util.h"
#include "mem_util.h"

#include <cstring>
#include <new>
#include <string>
#include <thread>
#include <vector>

// Internal structures wrapping C++ objects
struct libvroom_parser {
  libvroom::Parser parser;

  libvroom_parser(size_t num_threads = 1) : parser(num_threads) {}
};

struct libvroom_index {
  libvroom::ParseIndex idx;
  size_t num_threads;

  // Default constructor - index will be populated by Parser::parse()
  libvroom_index(size_t threads) : idx(), num_threads(threads) {}

  // Note: No explicit destructor needed - libvroom::ParseIndex has its own destructor
  // that handles cleanup of n_indexes and indexes arrays
};

struct libvroom_buffer {
  std::vector<uint8_t> data;
  size_t original_length; // Length of the original data (without padding)

  libvroom_buffer(const uint8_t* ptr, size_t len) : original_length(len) {
    // Allocate space for data + SIMD padding to allow safe 64-byte reads
    data.resize(len + LIBVROOM_PADDING);
    std::memcpy(data.data(), ptr, len);
    // Zero the padding bytes to avoid undefined behavior in SIMD comparisons
    std::memset(data.data() + len, 0, LIBVROOM_PADDING);
  }
  libvroom_buffer() : original_length(0) {}
};

struct libvroom_dialect {
  libvroom::Dialect dialect;

  libvroom_dialect() = default;
  libvroom_dialect(const libvroom::Dialect& d) : dialect(d) {}
};

struct libvroom_error_collector {
  libvroom::ErrorCollector collector;

  libvroom_error_collector(libvroom::ErrorMode mode, size_t max_errors)
      : collector(mode, max_errors) {}
};

struct libvroom_detection_result {
  libvroom::DetectionResult result;
  std::string warning_str;

  libvroom_detection_result(const libvroom::DetectionResult& r) : result(r) {
    if (!r.warning.empty()) {
      warning_str = r.warning;
    }
  }
};

struct libvroom_load_result {
  LoadResult cpp_result;

  libvroom_load_result(LoadResult&& r) : cpp_result(std::move(r)) {}
};

// Helper functions to convert between C and C++ types
static libvroom::ErrorMode to_cpp_mode(libvroom_error_mode_t mode) {
  switch (mode) {
  case LIBVROOM_MODE_STRICT:
    return libvroom::ErrorMode::STRICT;
  case LIBVROOM_MODE_PERMISSIVE:
    return libvroom::ErrorMode::PERMISSIVE;
  case LIBVROOM_MODE_BEST_EFFORT:
    return libvroom::ErrorMode::BEST_EFFORT;
  default:
    return libvroom::ErrorMode::STRICT;
  }
}

static libvroom_error_mode_t to_c_mode(libvroom::ErrorMode mode) {
  switch (mode) {
  case libvroom::ErrorMode::STRICT:
    return LIBVROOM_MODE_STRICT;
  case libvroom::ErrorMode::PERMISSIVE:
    return LIBVROOM_MODE_PERMISSIVE;
  case libvroom::ErrorMode::BEST_EFFORT:
    return LIBVROOM_MODE_BEST_EFFORT;
  default:
    return LIBVROOM_MODE_STRICT;
  }
}

static libvroom_error_t to_c_error(libvroom::ErrorCode code) {
  switch (code) {
  case libvroom::ErrorCode::NONE:
    return LIBVROOM_OK;
  case libvroom::ErrorCode::UNCLOSED_QUOTE:
    return LIBVROOM_ERROR_UNCLOSED_QUOTE;
  case libvroom::ErrorCode::INVALID_QUOTE_ESCAPE:
    return LIBVROOM_ERROR_INVALID_QUOTE_ESCAPE;
  case libvroom::ErrorCode::QUOTE_IN_UNQUOTED_FIELD:
    return LIBVROOM_ERROR_QUOTE_IN_UNQUOTED;
  case libvroom::ErrorCode::INCONSISTENT_FIELD_COUNT:
    return LIBVROOM_ERROR_INCONSISTENT_FIELDS;
  case libvroom::ErrorCode::FIELD_TOO_LARGE:
    return LIBVROOM_ERROR_FIELD_TOO_LARGE;
  case libvroom::ErrorCode::MIXED_LINE_ENDINGS:
    return LIBVROOM_ERROR_MIXED_LINE_ENDINGS;
  case libvroom::ErrorCode::INVALID_UTF8:
    return LIBVROOM_ERROR_INVALID_UTF8;
  case libvroom::ErrorCode::NULL_BYTE:
    return LIBVROOM_ERROR_NULL_BYTE;
  case libvroom::ErrorCode::EMPTY_HEADER:
    return LIBVROOM_ERROR_EMPTY_HEADER;
  case libvroom::ErrorCode::DUPLICATE_COLUMN_NAMES:
    return LIBVROOM_ERROR_DUPLICATE_COLUMNS;
  case libvroom::ErrorCode::AMBIGUOUS_SEPARATOR:
    return LIBVROOM_ERROR_AMBIGUOUS_SEPARATOR;
  case libvroom::ErrorCode::FILE_TOO_LARGE:
    return LIBVROOM_ERROR_FILE_TOO_LARGE;
  case libvroom::ErrorCode::IO_ERROR:
    return LIBVROOM_ERROR_IO;
  case libvroom::ErrorCode::INTERNAL_ERROR:
    return LIBVROOM_ERROR_INTERNAL;
  default:
    return LIBVROOM_ERROR_INTERNAL;
  }
}

static libvroom_severity_t to_c_severity(libvroom::ErrorSeverity severity) {
  switch (severity) {
  case libvroom::ErrorSeverity::WARNING:
    return LIBVROOM_SEVERITY_WARNING;
  case libvroom::ErrorSeverity::ERROR:
    return LIBVROOM_SEVERITY_ERROR;
  case libvroom::ErrorSeverity::FATAL:
    return LIBVROOM_SEVERITY_FATAL;
  default:
    return LIBVROOM_SEVERITY_ERROR;
  }
}

// Version
const char* libvroom_version(void) {
  static const char version[] = "0.1.0";
  return version;
}

// Error strings
const char* libvroom_error_string(libvroom_error_t error) {
  switch (error) {
  case LIBVROOM_OK:
    return "No error";
  case LIBVROOM_ERROR_UNCLOSED_QUOTE:
    return "Unclosed quote";
  case LIBVROOM_ERROR_INVALID_QUOTE_ESCAPE:
    return "Invalid quote escape";
  case LIBVROOM_ERROR_QUOTE_IN_UNQUOTED:
    return "Quote in unquoted field";
  case LIBVROOM_ERROR_INCONSISTENT_FIELDS:
    return "Inconsistent field count";
  case LIBVROOM_ERROR_FIELD_TOO_LARGE:
    return "Field too large";
  case LIBVROOM_ERROR_MIXED_LINE_ENDINGS:
    return "Mixed line endings";
  case LIBVROOM_ERROR_INVALID_UTF8:
    return "Invalid UTF-8";
  case LIBVROOM_ERROR_NULL_BYTE:
    return "Null byte in data";
  case LIBVROOM_ERROR_EMPTY_HEADER:
    return "Empty header";
  case LIBVROOM_ERROR_DUPLICATE_COLUMNS:
    return "Duplicate columns";
  case LIBVROOM_ERROR_AMBIGUOUS_SEPARATOR:
    return "Ambiguous separator";
  case LIBVROOM_ERROR_FILE_TOO_LARGE:
    return "File too large";
  case LIBVROOM_ERROR_IO:
    return "I/O error";
  case LIBVROOM_ERROR_INTERNAL:
    return "Internal error";
  case LIBVROOM_ERROR_NULL_POINTER:
    return "Null pointer";
  case LIBVROOM_ERROR_INVALID_ARGUMENT:
    return "Invalid argument";
  case LIBVROOM_ERROR_OUT_OF_MEMORY:
    return "Out of memory";
  case LIBVROOM_ERROR_INVALID_HANDLE:
    return "Invalid handle";
  default:
    return "Unknown error";
  }
}

// Buffer Management
libvroom_buffer_t* libvroom_buffer_load_file(const char* filename) {
  if (!filename)
    return nullptr;

  try {
    auto [ptr, size] = read_file(filename, LIBVROOM_PADDING);
    if (size == 0)
      return nullptr;

    auto* buffer = new (std::nothrow) libvroom_buffer();
    if (!buffer) {
      return nullptr;
    }

    // Store the original length (data size without padding)
    buffer->original_length = size;
    // Copy from buffer into vector
    buffer->data.assign(ptr.get(), ptr.get() + size);
    // Memory automatically freed when ptr goes out of scope
    return buffer;
  } catch (...) {
    return nullptr;
  }
}

libvroom_buffer_t* libvroom_buffer_create(const uint8_t* data, size_t length) {
  if (!data || length == 0)
    return nullptr;

  try {
    return new (std::nothrow) libvroom_buffer(data, length);
  } catch (...) {
    return nullptr;
  }
}

const uint8_t* libvroom_buffer_data(const libvroom_buffer_t* buffer) {
  if (!buffer)
    return nullptr;
  return buffer->data.data();
}

size_t libvroom_buffer_length(const libvroom_buffer_t* buffer) {
  if (!buffer)
    return 0;
  return buffer->original_length; // Return original length, not padded size
}

void libvroom_buffer_destroy(libvroom_buffer_t* buffer) {
  delete buffer;
}

// Dialect Configuration
libvroom_dialect_t* libvroom_dialect_create(char delimiter, char quote_char, char escape_char,
                                            bool double_quote) {
  try {
    auto* d = new (std::nothrow) libvroom_dialect();
    if (d) {
      d->dialect.delimiter = delimiter;
      d->dialect.quote_char = quote_char;
      d->dialect.escape_char = escape_char;
      d->dialect.double_quote = double_quote;
    }
    return d;
  } catch (...) {
    return nullptr;
  }
}

char libvroom_dialect_delimiter(const libvroom_dialect_t* dialect) {
  if (!dialect)
    return '\0';
  return dialect->dialect.delimiter;
}

char libvroom_dialect_quote_char(const libvroom_dialect_t* dialect) {
  if (!dialect)
    return '\0';
  return dialect->dialect.quote_char;
}

char libvroom_dialect_escape_char(const libvroom_dialect_t* dialect) {
  if (!dialect)
    return '\0';
  return dialect->dialect.escape_char;
}

bool libvroom_dialect_double_quote(const libvroom_dialect_t* dialect) {
  if (!dialect)
    return false;
  return dialect->dialect.double_quote;
}

void libvroom_dialect_destroy(libvroom_dialect_t* dialect) {
  delete dialect;
}

// Error Collector
libvroom_error_collector_t* libvroom_error_collector_create(libvroom_error_mode_t mode,
                                                            size_t max_errors) {
  try {
    return new (std::nothrow) libvroom_error_collector(to_cpp_mode(mode), max_errors);
  } catch (...) {
    return nullptr;
  }
}

libvroom_error_mode_t libvroom_error_collector_mode(const libvroom_error_collector_t* collector) {
  if (!collector)
    return LIBVROOM_MODE_STRICT;
  return to_c_mode(collector->collector.mode());
}

bool libvroom_error_collector_has_errors(const libvroom_error_collector_t* collector) {
  if (!collector)
    return false;
  return collector->collector.has_errors();
}

bool libvroom_error_collector_has_fatal(const libvroom_error_collector_t* collector) {
  if (!collector)
    return false;
  return collector->collector.has_fatal_errors();
}

size_t libvroom_error_collector_count(const libvroom_error_collector_t* collector) {
  if (!collector)
    return 0;
  return collector->collector.errors().size();
}

libvroom_error_t libvroom_error_collector_get(const libvroom_error_collector_t* collector,
                                              size_t index, libvroom_parse_error_t* error) {
  if (!collector)
    return LIBVROOM_ERROR_NULL_POINTER;
  if (!error)
    return LIBVROOM_ERROR_NULL_POINTER;

  const auto& errors = collector->collector.errors();
  if (index >= errors.size())
    return LIBVROOM_ERROR_INVALID_ARGUMENT;

  const auto& e = errors[index];
  error->code = to_c_error(e.code);
  error->severity = to_c_severity(e.severity);
  error->line = e.line;
  error->column = e.column;
  error->byte_offset = e.byte_offset;
  error->message = e.message.c_str();
  error->context = e.context.c_str();

  return LIBVROOM_OK;
}

void libvroom_error_collector_clear(libvroom_error_collector_t* collector) {
  if (!collector)
    return;
  collector->collector.clear();
}

void libvroom_error_collector_destroy(libvroom_error_collector_t* collector) {
  delete collector;
}

// Index Structure
libvroom_index_t* libvroom_index_create(size_t buffer_length, size_t num_threads) {
  // Note: buffer_length is now ignored since Parser allocates the index internally
  (void)buffer_length; // Suppress unused parameter warning
  if (num_threads == 0)
    return nullptr;

  try {
    return new (std::nothrow) libvroom_index(num_threads);
  } catch (...) {
    return nullptr;
  }
}

size_t libvroom_index_num_threads(const libvroom_index_t* index) {
  if (!index)
    return 0;
  return index->num_threads;
}

size_t libvroom_index_columns(const libvroom_index_t* index) {
  if (!index)
    return 0;
  return index->idx.columns;
}

uint64_t libvroom_index_count(const libvroom_index_t* index, size_t thread_id) {
  if (!index)
    return 0;
  if (thread_id >= index->num_threads)
    return 0;
  if (!index->idx.n_indexes)
    return 0;
  return index->idx.n_indexes[thread_id];
}

uint64_t libvroom_index_total_count(const libvroom_index_t* index) {
  if (!index)
    return 0;
  if (!index->idx.n_indexes)
    return 0;
  uint64_t total = 0;
  for (size_t i = 0; i < index->num_threads; ++i) {
    total += index->idx.n_indexes[i];
  }
  return total;
}

const uint64_t* libvroom_index_positions(const libvroom_index_t* index) {
  if (!index)
    return nullptr;
  return index->idx.indexes;
}

void libvroom_index_destroy(libvroom_index_t* index) {
  delete index;
}

// Parser
libvroom_parser_t* libvroom_parser_create(void) {
  try {
    return new (std::nothrow) libvroom_parser(1);
  } catch (...) {
    return nullptr;
  }
}

libvroom_error_t libvroom_parse(libvroom_parser_t* parser, const libvroom_buffer_t* buffer,
                                libvroom_index_t* index, libvroom_error_collector_t* errors,
                                const libvroom_dialect_t* dialect) {
  if (!parser || !buffer || !index)
    return LIBVROOM_ERROR_NULL_POINTER;

  try {
    libvroom::Dialect d = dialect ? dialect->dialect : libvroom::Dialect::csv();

    // Configure parser with the number of threads from the index
    parser->parser.set_num_threads(index->num_threads);

    // Build parse options
    libvroom::ParseOptions options;
    options.dialect = d;
    if (errors) {
      options.errors = &errors->collector;
    }

    // Parse using the unified Parser API
    // Use original_length (not padded data.size()) for correct parsing
    auto result = parser->parser.parse(buffer->data.data(), buffer->original_length, options);

    // Move the index from the result
    index->idx = std::move(result.idx);

    if (errors && errors->collector.has_fatal_errors()) {
      const auto& errs = errors->collector.errors();
      for (const auto& e : errs) {
        if (e.severity == libvroom::ErrorSeverity::FATAL) {
          return to_c_error(e.code);
        }
      }
    }

    return result.success() ? LIBVROOM_OK : LIBVROOM_ERROR_INTERNAL;
  } catch (...) {
    return LIBVROOM_ERROR_INTERNAL;
  }
}

void libvroom_parser_destroy(libvroom_parser_t* parser) {
  delete parser;
}

// Dialect Detection
libvroom_detection_result_t* libvroom_detect_dialect(const libvroom_buffer_t* buffer) {
  if (!buffer)
    return nullptr;

  try {
    libvroom::DialectDetector detector;
    auto result = detector.detect(buffer->data.data(), buffer->original_length);
    return new (std::nothrow) libvroom_detection_result(result);
  } catch (...) {
    return nullptr;
  }
}

bool libvroom_detection_result_success(const libvroom_detection_result_t* result) {
  if (!result)
    return false;
  return result->result.success();
}

double libvroom_detection_result_confidence(const libvroom_detection_result_t* result) {
  if (!result)
    return 0.0;
  return result->result.confidence;
}

libvroom_dialect_t* libvroom_detection_result_dialect(const libvroom_detection_result_t* result) {
  if (!result)
    return nullptr;

  try {
    auto* d = new (std::nothrow) libvroom_dialect(result->result.dialect);
    return d;
  } catch (...) {
    return nullptr;
  }
}

size_t libvroom_detection_result_columns(const libvroom_detection_result_t* result) {
  if (!result)
    return 0;
  return result->result.detected_columns;
}

size_t libvroom_detection_result_rows_analyzed(const libvroom_detection_result_t* result) {
  if (!result)
    return 0;
  return result->result.rows_analyzed;
}

bool libvroom_detection_result_has_header(const libvroom_detection_result_t* result) {
  if (!result)
    return false;
  return result->result.has_header;
}

const char* libvroom_detection_result_warning(const libvroom_detection_result_t* result) {
  if (!result)
    return nullptr;
  if (result->warning_str.empty())
    return nullptr;
  return result->warning_str.c_str();
}

void libvroom_detection_result_destroy(libvroom_detection_result_t* result) {
  delete result;
}

libvroom_error_t libvroom_parse_auto(libvroom_parser_t* parser, const libvroom_buffer_t* buffer,
                                     libvroom_index_t* index, libvroom_error_collector_t* errors,
                                     libvroom_detection_result_t** detected) {
  if (!parser || !buffer || !index)
    return LIBVROOM_ERROR_NULL_POINTER;

  try {
    // Configure parser with the number of threads from the index
    parser->parser.set_num_threads(index->num_threads);

    // Build parse options for auto-detection (dialect = nullopt)
    libvroom::ParseOptions options;
    // Leave dialect as nullopt for auto-detection
    if (errors) {
      options.errors = &errors->collector;
    }

    // Parse using the unified Parser API with auto-detection
    // Use original_length (not padded data.size()) for correct parsing
    auto result = parser->parser.parse(buffer->data.data(), buffer->original_length, options);

    // Store detection result if requested
    if (detected) {
      *detected = new (std::nothrow) libvroom_detection_result(result.detection);
    }

    // Check if detection succeeded
    if (!result.detection.success()) {
      return LIBVROOM_ERROR_AMBIGUOUS_SEPARATOR;
    }

    // Move the index from the result
    index->idx = std::move(result.idx);

    return result.success() ? LIBVROOM_OK : LIBVROOM_ERROR_INTERNAL;
  } catch (...) {
    return LIBVROOM_ERROR_INTERNAL;
  }
}

// Utility Functions
size_t libvroom_recommended_threads(void) {
  return std::thread::hardware_concurrency();
}

size_t libvroom_simd_padding(void) {
  return LIBVROOM_PADDING;
}

// Encoding helper functions
static libvroom_encoding_t to_c_encoding(libvroom::Encoding enc) {
  switch (enc) {
  case libvroom::Encoding::UTF8:
    return LIBVROOM_ENCODING_UTF8;
  case libvroom::Encoding::UTF8_BOM:
    return LIBVROOM_ENCODING_UTF8_BOM;
  case libvroom::Encoding::UTF16_LE:
    return LIBVROOM_ENCODING_UTF16_LE;
  case libvroom::Encoding::UTF16_BE:
    return LIBVROOM_ENCODING_UTF16_BE;
  case libvroom::Encoding::UTF32_LE:
    return LIBVROOM_ENCODING_UTF32_LE;
  case libvroom::Encoding::UTF32_BE:
    return LIBVROOM_ENCODING_UTF32_BE;
  case libvroom::Encoding::LATIN1:
    return LIBVROOM_ENCODING_LATIN1;
  case libvroom::Encoding::UNKNOWN:
  default:
    return LIBVROOM_ENCODING_UNKNOWN;
  }
}

// Encoding Detection and Transcoding
const char* libvroom_encoding_string(libvroom_encoding_t encoding) {
  switch (encoding) {
  case LIBVROOM_ENCODING_UTF8:
    return "UTF-8";
  case LIBVROOM_ENCODING_UTF8_BOM:
    return "UTF-8 (BOM)";
  case LIBVROOM_ENCODING_UTF16_LE:
    return "UTF-16LE";
  case LIBVROOM_ENCODING_UTF16_BE:
    return "UTF-16BE";
  case LIBVROOM_ENCODING_UTF32_LE:
    return "UTF-32LE";
  case LIBVROOM_ENCODING_UTF32_BE:
    return "UTF-32BE";
  case LIBVROOM_ENCODING_LATIN1:
    return "Latin-1";
  case LIBVROOM_ENCODING_UNKNOWN:
  default:
    return "Unknown";
  }
}

libvroom_error_t libvroom_detect_encoding(const uint8_t* data, size_t length,
                                          libvroom_encoding_result_t* result) {
  if (!result)
    return LIBVROOM_ERROR_NULL_POINTER;

  // detect_encoding handles null data gracefully
  auto cpp_result = libvroom::detect_encoding(data, length);

  result->encoding = to_c_encoding(cpp_result.encoding);
  result->bom_length = cpp_result.bom_length;
  result->confidence = cpp_result.confidence;
  result->needs_transcoding = cpp_result.needs_transcoding;

  return LIBVROOM_OK;
}

libvroom_load_result_t* libvroom_load_file_with_encoding(const char* filename) {
  if (!filename)
    return nullptr;

  try {
    auto result = read_file_with_encoding(filename, LIBVROOM_PADDING);
    if (!result.valid()) {
      return nullptr;
    }

    return new (std::nothrow) libvroom_load_result(std::move(result));
  } catch (...) {
    return nullptr;
  }
}

const uint8_t* libvroom_load_result_data(const libvroom_load_result_t* result) {
  if (!result)
    return nullptr;
  return result->cpp_result.data();
}

size_t libvroom_load_result_length(const libvroom_load_result_t* result) {
  if (!result)
    return 0;
  return result->cpp_result.size;
}

libvroom_encoding_t libvroom_load_result_encoding(const libvroom_load_result_t* result) {
  if (!result)
    return LIBVROOM_ENCODING_UNKNOWN;
  return to_c_encoding(result->cpp_result.encoding.encoding);
}

size_t libvroom_load_result_bom_length(const libvroom_load_result_t* result) {
  if (!result)
    return 0;
  return result->cpp_result.encoding.bom_length;
}

double libvroom_load_result_confidence(const libvroom_load_result_t* result) {
  if (!result)
    return 0.0;
  return result->cpp_result.encoding.confidence;
}

bool libvroom_load_result_was_transcoded(const libvroom_load_result_t* result) {
  if (!result)
    return false;
  // Data was transformed if either:
  // 1. needs_transcoding is true (UTF-16/UTF-32 -> UTF-8)
  // 2. BOM was present and stripped (includes UTF-8 BOM)
  return result->cpp_result.encoding.needs_transcoding ||
         result->cpp_result.encoding.bom_length > 0;
}

libvroom_buffer_t* libvroom_load_result_to_buffer(const libvroom_load_result_t* result) {
  if (!result || !result->cpp_result.valid())
    return nullptr;

  try {
    return new (std::nothrow) libvroom_buffer(result->cpp_result.data(), result->cpp_result.size);
  } catch (...) {
    return nullptr;
  }
}

void libvroom_load_result_destroy(libvroom_load_result_t* result) {
  delete result;
}
