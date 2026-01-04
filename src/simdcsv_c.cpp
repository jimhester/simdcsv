/**
 * @file simdcsv_c.cpp
 * @brief C API wrapper implementation for the simdcsv library.
 */

#include "simdcsv_c.h"
#include "two_pass.h"
#include "error.h"
#include "dialect.h"
#include "io_util.h"
#include "mem_util.h"
#include "common_defs.h"

#include <cstring>
#include <new>
#include <string>
#include <vector>
#include <thread>

// Internal structures wrapping C++ objects
struct simdcsv_parser {
    simdcsv::two_pass parser;
};

struct simdcsv_index {
    simdcsv::index idx;
    size_t num_threads;

    simdcsv_index(size_t buffer_length, size_t threads)
        : idx(), num_threads(threads) {
        // Initialize will be called during parsing
    }
};

struct simdcsv_buffer {
    std::vector<uint8_t> data;

    simdcsv_buffer(const uint8_t* ptr, size_t len) : data(ptr, ptr + len) {}
    simdcsv_buffer() = default;
};

struct simdcsv_dialect {
    simdcsv::Dialect dialect;

    simdcsv_dialect() = default;
    simdcsv_dialect(const simdcsv::Dialect& d) : dialect(d) {}
};

struct simdcsv_error_collector {
    simdcsv::ErrorCollector collector;

    simdcsv_error_collector(simdcsv::ErrorMode mode, size_t max_errors)
        : collector(mode, max_errors) {}
};

struct simdcsv_detection_result {
    simdcsv::DetectionResult result;
    std::string warning_str;

    simdcsv_detection_result(const simdcsv::DetectionResult& r) : result(r) {
        if (r.warning) {
            warning_str = *r.warning;
        }
    }
};

// Helper functions to convert between C and C++ types
static simdcsv::ErrorMode to_cpp_mode(simdcsv_error_mode_t mode) {
    switch (mode) {
        case SIMDCSV_MODE_STRICT: return simdcsv::ErrorMode::STRICT;
        case SIMDCSV_MODE_PERMISSIVE: return simdcsv::ErrorMode::PERMISSIVE;
        case SIMDCSV_MODE_BEST_EFFORT: return simdcsv::ErrorMode::BEST_EFFORT;
        default: return simdcsv::ErrorMode::STRICT;
    }
}

static simdcsv_error_mode_t to_c_mode(simdcsv::ErrorMode mode) {
    switch (mode) {
        case simdcsv::ErrorMode::STRICT: return SIMDCSV_MODE_STRICT;
        case simdcsv::ErrorMode::PERMISSIVE: return SIMDCSV_MODE_PERMISSIVE;
        case simdcsv::ErrorMode::BEST_EFFORT: return SIMDCSV_MODE_BEST_EFFORT;
        default: return SIMDCSV_MODE_STRICT;
    }
}

static simdcsv_error_t to_c_error(simdcsv::ErrorCode code) {
    switch (code) {
        case simdcsv::ErrorCode::OK: return SIMDCSV_OK;
        case simdcsv::ErrorCode::UNCLOSED_QUOTE: return SIMDCSV_ERROR_UNCLOSED_QUOTE;
        case simdcsv::ErrorCode::INVALID_QUOTE_ESCAPE: return SIMDCSV_ERROR_INVALID_QUOTE_ESCAPE;
        case simdcsv::ErrorCode::QUOTE_IN_UNQUOTED: return SIMDCSV_ERROR_QUOTE_IN_UNQUOTED;
        case simdcsv::ErrorCode::INCONSISTENT_FIELDS: return SIMDCSV_ERROR_INCONSISTENT_FIELDS;
        case simdcsv::ErrorCode::FIELD_TOO_LARGE: return SIMDCSV_ERROR_FIELD_TOO_LARGE;
        case simdcsv::ErrorCode::MIXED_LINE_ENDINGS: return SIMDCSV_ERROR_MIXED_LINE_ENDINGS;
        case simdcsv::ErrorCode::INVALID_LINE_ENDING: return SIMDCSV_ERROR_INVALID_LINE_ENDING;
        case simdcsv::ErrorCode::INVALID_UTF8: return SIMDCSV_ERROR_INVALID_UTF8;
        case simdcsv::ErrorCode::NULL_BYTE: return SIMDCSV_ERROR_NULL_BYTE;
        case simdcsv::ErrorCode::EMPTY_HEADER: return SIMDCSV_ERROR_EMPTY_HEADER;
        case simdcsv::ErrorCode::DUPLICATE_COLUMNS: return SIMDCSV_ERROR_DUPLICATE_COLUMNS;
        case simdcsv::ErrorCode::AMBIGUOUS_SEPARATOR: return SIMDCSV_ERROR_AMBIGUOUS_SEPARATOR;
        case simdcsv::ErrorCode::FILE_TOO_LARGE: return SIMDCSV_ERROR_FILE_TOO_LARGE;
        case simdcsv::ErrorCode::IO_ERROR: return SIMDCSV_ERROR_IO;
        case simdcsv::ErrorCode::INTERNAL_ERROR: return SIMDCSV_ERROR_INTERNAL;
        default: return SIMDCSV_ERROR_INTERNAL;
    }
}

static simdcsv_severity_t to_c_severity(simdcsv::ErrorSeverity severity) {
    switch (severity) {
        case simdcsv::ErrorSeverity::WARNING: return SIMDCSV_SEVERITY_WARNING;
        case simdcsv::ErrorSeverity::ERROR: return SIMDCSV_SEVERITY_ERROR;
        case simdcsv::ErrorSeverity::FATAL: return SIMDCSV_SEVERITY_FATAL;
        default: return SIMDCSV_SEVERITY_ERROR;
    }
}

// Version
const char* simdcsv_version(void) {
    static const char version[] = "0.1.0";
    return version;
}

// Error strings
const char* simdcsv_error_string(simdcsv_error_t error) {
    switch (error) {
        case SIMDCSV_OK: return "No error";
        case SIMDCSV_ERROR_UNCLOSED_QUOTE: return "Unclosed quote";
        case SIMDCSV_ERROR_INVALID_QUOTE_ESCAPE: return "Invalid quote escape";
        case SIMDCSV_ERROR_QUOTE_IN_UNQUOTED: return "Quote in unquoted field";
        case SIMDCSV_ERROR_INCONSISTENT_FIELDS: return "Inconsistent field count";
        case SIMDCSV_ERROR_FIELD_TOO_LARGE: return "Field too large";
        case SIMDCSV_ERROR_MIXED_LINE_ENDINGS: return "Mixed line endings";
        case SIMDCSV_ERROR_INVALID_LINE_ENDING: return "Invalid line ending";
        case SIMDCSV_ERROR_INVALID_UTF8: return "Invalid UTF-8";
        case SIMDCSV_ERROR_NULL_BYTE: return "Null byte in data";
        case SIMDCSV_ERROR_EMPTY_HEADER: return "Empty header";
        case SIMDCSV_ERROR_DUPLICATE_COLUMNS: return "Duplicate columns";
        case SIMDCSV_ERROR_AMBIGUOUS_SEPARATOR: return "Ambiguous separator";
        case SIMDCSV_ERROR_FILE_TOO_LARGE: return "File too large";
        case SIMDCSV_ERROR_IO: return "I/O error";
        case SIMDCSV_ERROR_INTERNAL: return "Internal error";
        case SIMDCSV_ERROR_NULL_POINTER: return "Null pointer";
        case SIMDCSV_ERROR_INVALID_ARGUMENT: return "Invalid argument";
        case SIMDCSV_ERROR_OUT_OF_MEMORY: return "Out of memory";
        case SIMDCSV_ERROR_INVALID_HANDLE: return "Invalid handle";
        default: return "Unknown error";
    }
}

// Buffer Management
simdcsv_buffer_t* simdcsv_buffer_load_file(const char* filename) {
    if (!filename) return nullptr;

    try {
        auto corpus = get_corpus(filename, SIMDCSV_PADDING);
        if (corpus.empty()) return nullptr;

        auto* buffer = new (std::nothrow) simdcsv_buffer();
        if (!buffer) return nullptr;

        buffer->data = std::move(corpus);
        return buffer;
    } catch (...) {
        return nullptr;
    }
}

simdcsv_buffer_t* simdcsv_buffer_create(const uint8_t* data, size_t length) {
    if (!data || length == 0) return nullptr;

    try {
        return new (std::nothrow) simdcsv_buffer(data, length);
    } catch (...) {
        return nullptr;
    }
}

const uint8_t* simdcsv_buffer_data(const simdcsv_buffer_t* buffer) {
    if (!buffer) return nullptr;
    return buffer->data.data();
}

size_t simdcsv_buffer_length(const simdcsv_buffer_t* buffer) {
    if (!buffer) return 0;
    return buffer->data.size();
}

void simdcsv_buffer_destroy(simdcsv_buffer_t* buffer) {
    delete buffer;
}

// Dialect Configuration
simdcsv_dialect_t* simdcsv_dialect_csv(void) {
    try {
        auto* d = new (std::nothrow) simdcsv_dialect();
        if (d) d->dialect = simdcsv::Dialect::csv();
        return d;
    } catch (...) {
        return nullptr;
    }
}

simdcsv_dialect_t* simdcsv_dialect_tsv(void) {
    try {
        auto* d = new (std::nothrow) simdcsv_dialect();
        if (d) d->dialect = simdcsv::Dialect::tsv();
        return d;
    } catch (...) {
        return nullptr;
    }
}

simdcsv_dialect_t* simdcsv_dialect_semicolon(void) {
    try {
        auto* d = new (std::nothrow) simdcsv_dialect();
        if (d) {
            d->dialect.delimiter = ';';
            d->dialect.quote_char = '"';
            d->dialect.escape_char = '"';
            d->dialect.double_quote = true;
        }
        return d;
    } catch (...) {
        return nullptr;
    }
}

simdcsv_dialect_t* simdcsv_dialect_pipe(void) {
    try {
        auto* d = new (std::nothrow) simdcsv_dialect();
        if (d) {
            d->dialect.delimiter = '|';
            d->dialect.quote_char = '"';
            d->dialect.escape_char = '"';
            d->dialect.double_quote = true;
        }
        return d;
    } catch (...) {
        return nullptr;
    }
}

simdcsv_dialect_t* simdcsv_dialect_create(char delimiter, char quote_char,
                                           char escape_char, bool double_quote) {
    try {
        auto* d = new (std::nothrow) simdcsv_dialect();
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

char simdcsv_dialect_delimiter(const simdcsv_dialect_t* dialect) {
    if (!dialect) return '\0';
    return dialect->dialect.delimiter;
}

char simdcsv_dialect_quote_char(const simdcsv_dialect_t* dialect) {
    if (!dialect) return '\0';
    return dialect->dialect.quote_char;
}

char simdcsv_dialect_escape_char(const simdcsv_dialect_t* dialect) {
    if (!dialect) return '\0';
    return dialect->dialect.escape_char;
}

bool simdcsv_dialect_double_quote(const simdcsv_dialect_t* dialect) {
    if (!dialect) return false;
    return dialect->dialect.double_quote;
}

simdcsv_error_t simdcsv_dialect_validate(const simdcsv_dialect_t* dialect) {
    if (!dialect) return SIMDCSV_ERROR_NULL_POINTER;

    // Basic validation
    if (dialect->dialect.delimiter == '\0') return SIMDCSV_ERROR_INVALID_ARGUMENT;
    if (dialect->dialect.delimiter == dialect->dialect.quote_char) return SIMDCSV_ERROR_INVALID_ARGUMENT;

    return SIMDCSV_OK;
}

size_t simdcsv_dialect_to_string(const simdcsv_dialect_t* dialect, char* buffer, size_t buffer_size) {
    if (!dialect) return 0;

    std::string str = dialect->dialect.to_string();
    if (buffer && buffer_size > 0) {
        size_t copy_len = std::min(str.size(), buffer_size - 1);
        std::memcpy(buffer, str.c_str(), copy_len);
        buffer[copy_len] = '\0';
    }
    return str.size();
}

void simdcsv_dialect_destroy(simdcsv_dialect_t* dialect) {
    delete dialect;
}

// Error Collector
simdcsv_error_collector_t* simdcsv_error_collector_create(simdcsv_error_mode_t mode, size_t max_errors) {
    try {
        return new (std::nothrow) simdcsv_error_collector(to_cpp_mode(mode), max_errors);
    } catch (...) {
        return nullptr;
    }
}

simdcsv_error_mode_t simdcsv_error_collector_mode(const simdcsv_error_collector_t* collector) {
    if (!collector) return SIMDCSV_MODE_STRICT;
    return to_c_mode(collector->collector.mode());
}

void simdcsv_error_collector_set_mode(simdcsv_error_collector_t* collector, simdcsv_error_mode_t mode) {
    if (!collector) return;
    collector->collector.set_mode(to_cpp_mode(mode));
}

bool simdcsv_error_collector_has_errors(const simdcsv_error_collector_t* collector) {
    if (!collector) return false;
    return collector->collector.has_errors();
}

bool simdcsv_error_collector_has_fatal(const simdcsv_error_collector_t* collector) {
    if (!collector) return false;
    return collector->collector.has_fatal();
}

size_t simdcsv_error_collector_count(const simdcsv_error_collector_t* collector) {
    if (!collector) return 0;
    return collector->collector.errors().size();
}

simdcsv_error_t simdcsv_error_collector_get(const simdcsv_error_collector_t* collector,
                                             size_t index, simdcsv_parse_error_t* error) {
    if (!collector) return SIMDCSV_ERROR_NULL_POINTER;
    if (!error) return SIMDCSV_ERROR_NULL_POINTER;

    const auto& errors = collector->collector.errors();
    if (index >= errors.size()) return SIMDCSV_ERROR_INVALID_ARGUMENT;

    const auto& e = errors[index];
    error->code = to_c_error(e.code);
    error->severity = to_c_severity(e.severity);
    error->line = e.line;
    error->column = e.column;
    error->byte_offset = e.byte_offset;
    error->message = e.message.c_str();
    error->context = e.context.c_str();

    return SIMDCSV_OK;
}

void simdcsv_error_collector_clear(simdcsv_error_collector_t* collector) {
    if (!collector) return;
    collector->collector.clear();
}

void simdcsv_error_collector_destroy(simdcsv_error_collector_t* collector) {
    delete collector;
}

// Index Structure
simdcsv_index_t* simdcsv_index_create(size_t buffer_length, size_t num_threads) {
    if (buffer_length == 0 || num_threads == 0) return nullptr;

    try {
        return new (std::nothrow) simdcsv_index(buffer_length, num_threads);
    } catch (...) {
        return nullptr;
    }
}

size_t simdcsv_index_num_threads(const simdcsv_index_t* index) {
    if (!index) return 0;
    return index->num_threads;
}

size_t simdcsv_index_columns(const simdcsv_index_t* index) {
    if (!index) return 0;
    return index->idx.n_cols;
}

uint64_t simdcsv_index_count(const simdcsv_index_t* index, size_t thread_id) {
    if (!index) return 0;
    if (thread_id >= index->num_threads) return 0;
    return index->idx.cnt[thread_id];
}

uint64_t simdcsv_index_total_count(const simdcsv_index_t* index) {
    if (!index) return 0;
    uint64_t total = 0;
    for (size_t i = 0; i < index->num_threads; ++i) {
        total += index->idx.cnt[i];
    }
    return total;
}

const uint64_t* simdcsv_index_positions(const simdcsv_index_t* index) {
    if (!index) return nullptr;
    return index->idx.pos.data();
}

void simdcsv_index_destroy(simdcsv_index_t* index) {
    delete index;
}

// Parser
simdcsv_parser_t* simdcsv_parser_create(void) {
    try {
        return new (std::nothrow) simdcsv_parser();
    } catch (...) {
        return nullptr;
    }
}

simdcsv_error_t simdcsv_parse(simdcsv_parser_t* parser, const simdcsv_buffer_t* buffer,
                               simdcsv_index_t* index, const simdcsv_dialect_t* dialect) {
    if (!parser || !buffer || !index) return SIMDCSV_ERROR_NULL_POINTER;

    try {
        simdcsv::Dialect d = dialect ? dialect->dialect : simdcsv::Dialect::csv();
        index->idx = parser->parser.init(buffer->data.size(), index->num_threads);
        parser->parser.parse(buffer->data.data(), index->idx, buffer->data.size(), d);
        return SIMDCSV_OK;
    } catch (...) {
        return SIMDCSV_ERROR_INTERNAL;
    }
}

simdcsv_error_t simdcsv_parse_with_errors(simdcsv_parser_t* parser, const simdcsv_buffer_t* buffer,
                                           simdcsv_index_t* index, simdcsv_error_collector_t* errors,
                                           const simdcsv_dialect_t* dialect) {
    if (!parser || !buffer || !index) return SIMDCSV_ERROR_NULL_POINTER;

    try {
        simdcsv::Dialect d = dialect ? dialect->dialect : simdcsv::Dialect::csv();
        index->idx = parser->parser.init(buffer->data.size(), index->num_threads);

        if (errors) {
            parser->parser.parse_with_errors(buffer->data.data(), index->idx,
                                             buffer->data.size(), errors->collector, d);
        } else {
            parser->parser.parse(buffer->data.data(), index->idx, buffer->data.size(), d);
        }

        if (errors && errors->collector.has_fatal()) {
            const auto& errs = errors->collector.errors();
            for (const auto& e : errs) {
                if (e.severity == simdcsv::ErrorSeverity::FATAL) {
                    return to_c_error(e.code);
                }
            }
        }

        return SIMDCSV_OK;
    } catch (...) {
        return SIMDCSV_ERROR_INTERNAL;
    }
}

simdcsv_error_t simdcsv_parse_two_pass_with_errors(simdcsv_parser_t* parser, const simdcsv_buffer_t* buffer,
                                                    simdcsv_index_t* index, simdcsv_error_collector_t* errors,
                                                    const simdcsv_dialect_t* dialect) {
    if (!parser || !buffer || !index) return SIMDCSV_ERROR_NULL_POINTER;

    try {
        simdcsv::Dialect d = dialect ? dialect->dialect : simdcsv::Dialect::csv();
        index->idx = parser->parser.init(buffer->data.size(), index->num_threads);

        if (errors) {
            parser->parser.parse_two_pass_with_errors(buffer->data.data(), index->idx,
                                                      buffer->data.size(), errors->collector, d);
        } else {
            parser->parser.parse_two_pass(buffer->data.data(), index->idx, buffer->data.size(), d);
        }

        if (errors && errors->collector.has_fatal()) {
            const auto& errs = errors->collector.errors();
            for (const auto& e : errs) {
                if (e.severity == simdcsv::ErrorSeverity::FATAL) {
                    return to_c_error(e.code);
                }
            }
        }

        return SIMDCSV_OK;
    } catch (...) {
        return SIMDCSV_ERROR_INTERNAL;
    }
}

void simdcsv_parser_destroy(simdcsv_parser_t* parser) {
    delete parser;
}

// Dialect Detection
simdcsv_detection_result_t* simdcsv_detect_dialect(const simdcsv_buffer_t* buffer) {
    if (!buffer) return nullptr;

    try {
        simdcsv::DialectDetector detector;
        auto result = detector.detect(buffer->data.data(), buffer->data.size());
        return new (std::nothrow) simdcsv_detection_result(result);
    } catch (...) {
        return nullptr;
    }
}

bool simdcsv_detection_result_success(const simdcsv_detection_result_t* result) {
    if (!result) return false;
    return result->result.success;
}

double simdcsv_detection_result_confidence(const simdcsv_detection_result_t* result) {
    if (!result) return 0.0;
    return result->result.confidence;
}

simdcsv_dialect_t* simdcsv_detection_result_dialect(const simdcsv_detection_result_t* result) {
    if (!result) return nullptr;

    try {
        auto* d = new (std::nothrow) simdcsv_dialect(result->result.dialect);
        return d;
    } catch (...) {
        return nullptr;
    }
}

size_t simdcsv_detection_result_columns(const simdcsv_detection_result_t* result) {
    if (!result) return 0;
    return result->result.detected_columns;
}

size_t simdcsv_detection_result_rows_analyzed(const simdcsv_detection_result_t* result) {
    if (!result) return 0;
    return result->result.rows_analyzed;
}

bool simdcsv_detection_result_has_header(const simdcsv_detection_result_t* result) {
    if (!result) return false;
    return result->result.has_header;
}

const char* simdcsv_detection_result_warning(const simdcsv_detection_result_t* result) {
    if (!result) return nullptr;
    if (result->warning_str.empty()) return nullptr;
    return result->warning_str.c_str();
}

void simdcsv_detection_result_destroy(simdcsv_detection_result_t* result) {
    delete result;
}

simdcsv_error_t simdcsv_parse_auto(simdcsv_parser_t* parser, const simdcsv_buffer_t* buffer,
                                    simdcsv_index_t* index, simdcsv_error_collector_t* errors,
                                    simdcsv_detection_result_t** detected) {
    if (!parser || !buffer || !index) return SIMDCSV_ERROR_NULL_POINTER;

    try {
        // First detect the dialect
        simdcsv::DialectDetector detector;
        auto result = detector.detect(buffer->data.data(), buffer->data.size());

        if (detected) {
            *detected = new (std::nothrow) simdcsv_detection_result(result);
        }

        if (!result.success) {
            return SIMDCSV_ERROR_AMBIGUOUS_SEPARATOR;
        }

        // Parse with detected dialect
        index->idx = parser->parser.init(buffer->data.size(), index->num_threads);

        if (errors) {
            parser->parser.parse_with_errors(buffer->data.data(), index->idx,
                                             buffer->data.size(), errors->collector, result.dialect);
        } else {
            parser->parser.parse(buffer->data.data(), index->idx, buffer->data.size(), result.dialect);
        }

        return SIMDCSV_OK;
    } catch (...) {
        return SIMDCSV_ERROR_INTERNAL;
    }
}

// Utility Functions
size_t simdcsv_recommended_threads(void) {
    return std::thread::hardware_concurrency();
}

size_t simdcsv_simd_padding(void) {
    return SIMDCSV_PADDING;
}
