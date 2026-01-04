/**
 * @file simdcsv_c.cpp
 * @brief Implementation of the C API wrapper for simdcsv.
 */

#include "simdcsv_c.h"
#include "two_pass.h"
#include "error.h"
#include "dialect.h"
#include "io_util.h"
#include "mem_util.h"

#include <cstring>
#include <new>

// Version string
static const char* SIMDCSV_VERSION_STRING = "0.1.0";

/* ============================================================================
 * Internal wrapper structures
 * ========================================================================== */

struct simdcsv_parser {
    simdcsv::two_pass parser;
};

struct simdcsv_index {
    simdcsv::index idx;

    simdcsv_index() = default;
    simdcsv_index(simdcsv::index&& i) : idx(std::move(i)) {}
};

struct simdcsv_errors {
    simdcsv::ErrorCollector collector;

    explicit simdcsv_errors(simdcsv::ErrorMode mode) : collector(mode) {}
};

/* ============================================================================
 * Internal conversion helpers
 * ========================================================================== */

static simdcsv::ErrorMode to_cpp_error_mode(simdcsv_error_mode_t mode) {
    switch (mode) {
        case SIMDCSV_ERROR_MODE_STRICT:
            return simdcsv::ErrorMode::STRICT;
        case SIMDCSV_ERROR_MODE_PERMISSIVE:
            return simdcsv::ErrorMode::PERMISSIVE;
        case SIMDCSV_ERROR_MODE_BEST_EFFORT:
            return simdcsv::ErrorMode::BEST_EFFORT;
        default:
            return simdcsv::ErrorMode::STRICT;
    }
}

static simdcsv_error_mode_t to_c_error_mode(simdcsv::ErrorMode mode) {
    switch (mode) {
        case simdcsv::ErrorMode::STRICT:
            return SIMDCSV_ERROR_MODE_STRICT;
        case simdcsv::ErrorMode::PERMISSIVE:
            return SIMDCSV_ERROR_MODE_PERMISSIVE;
        case simdcsv::ErrorMode::BEST_EFFORT:
            return SIMDCSV_ERROR_MODE_BEST_EFFORT;
        default:
            return SIMDCSV_ERROR_MODE_STRICT;
    }
}

static simdcsv_error_code_t to_c_error_code(simdcsv::ErrorCode code) {
    switch (code) {
        case simdcsv::ErrorCode::NONE:
            return SIMDCSV_ERROR_NONE;
        case simdcsv::ErrorCode::UNCLOSED_QUOTE:
            return SIMDCSV_ERROR_UNCLOSED_QUOTE;
        case simdcsv::ErrorCode::INVALID_QUOTE_ESCAPE:
            return SIMDCSV_ERROR_INVALID_QUOTE_ESCAPE;
        case simdcsv::ErrorCode::QUOTE_IN_UNQUOTED_FIELD:
            return SIMDCSV_ERROR_QUOTE_IN_UNQUOTED;
        case simdcsv::ErrorCode::INCONSISTENT_FIELD_COUNT:
            return SIMDCSV_ERROR_INCONSISTENT_FIELDS;
        case simdcsv::ErrorCode::FIELD_TOO_LARGE:
            return SIMDCSV_ERROR_FIELD_TOO_LARGE;
        case simdcsv::ErrorCode::MIXED_LINE_ENDINGS:
            return SIMDCSV_ERROR_MIXED_LINE_ENDINGS;
        case simdcsv::ErrorCode::INVALID_LINE_ENDING:
            return SIMDCSV_ERROR_INVALID_LINE_ENDING;
        case simdcsv::ErrorCode::INVALID_UTF8:
            return SIMDCSV_ERROR_INVALID_UTF8;
        case simdcsv::ErrorCode::NULL_BYTE:
            return SIMDCSV_ERROR_NULL_BYTE;
        case simdcsv::ErrorCode::EMPTY_HEADER:
            return SIMDCSV_ERROR_EMPTY_HEADER;
        case simdcsv::ErrorCode::DUPLICATE_COLUMN_NAMES:
            return SIMDCSV_ERROR_DUPLICATE_COLUMNS;
        case simdcsv::ErrorCode::AMBIGUOUS_SEPARATOR:
            return SIMDCSV_ERROR_AMBIGUOUS_SEPARATOR;
        case simdcsv::ErrorCode::FILE_TOO_LARGE:
            return SIMDCSV_ERROR_FILE_TOO_LARGE;
        case simdcsv::ErrorCode::IO_ERROR:
            return SIMDCSV_ERROR_IO_ERROR;
        case simdcsv::ErrorCode::INTERNAL_ERROR:
            return SIMDCSV_ERROR_INTERNAL;
        default:
            return SIMDCSV_ERROR_INTERNAL;
    }
}

static simdcsv_error_severity_t to_c_severity(simdcsv::ErrorSeverity severity) {
    switch (severity) {
        case simdcsv::ErrorSeverity::WARNING:
            return SIMDCSV_SEVERITY_WARNING;
        case simdcsv::ErrorSeverity::ERROR:
            return SIMDCSV_SEVERITY_ERROR;
        case simdcsv::ErrorSeverity::FATAL:
            return SIMDCSV_SEVERITY_FATAL;
        default:
            return SIMDCSV_SEVERITY_ERROR;
    }
}

static simdcsv_line_ending_t to_c_line_ending(simdcsv::Dialect::LineEnding le) {
    switch (le) {
        case simdcsv::Dialect::LineEnding::LF:
            return SIMDCSV_LINE_ENDING_LF;
        case simdcsv::Dialect::LineEnding::CRLF:
            return SIMDCSV_LINE_ENDING_CRLF;
        case simdcsv::Dialect::LineEnding::CR:
            return SIMDCSV_LINE_ENDING_CR;
        case simdcsv::Dialect::LineEnding::MIXED:
            return SIMDCSV_LINE_ENDING_MIXED;
        case simdcsv::Dialect::LineEnding::UNKNOWN:
        default:
            return SIMDCSV_LINE_ENDING_UNKNOWN;
    }
}

static simdcsv::Dialect::LineEnding to_cpp_line_ending(simdcsv_line_ending_t le) {
    switch (le) {
        case SIMDCSV_LINE_ENDING_LF:
            return simdcsv::Dialect::LineEnding::LF;
        case SIMDCSV_LINE_ENDING_CRLF:
            return simdcsv::Dialect::LineEnding::CRLF;
        case SIMDCSV_LINE_ENDING_CR:
            return simdcsv::Dialect::LineEnding::CR;
        case SIMDCSV_LINE_ENDING_MIXED:
            return simdcsv::Dialect::LineEnding::MIXED;
        case SIMDCSV_LINE_ENDING_UNKNOWN:
        default:
            return simdcsv::Dialect::LineEnding::UNKNOWN;
    }
}

static simdcsv::Dialect to_cpp_dialect(const simdcsv_dialect_t* dialect) {
    if (!dialect) {
        return simdcsv::Dialect::csv();
    }
    simdcsv::Dialect d;
    d.delimiter = dialect->delimiter;
    d.quote_char = dialect->quote_char;
    d.escape_char = dialect->escape_char;
    d.double_quote = dialect->double_quote != 0;
    d.line_ending = to_cpp_line_ending(dialect->line_ending);
    return d;
}

static simdcsv_dialect_t to_c_dialect(const simdcsv::Dialect& dialect) {
    simdcsv_dialect_t d;
    d.delimiter = dialect.delimiter;
    d.quote_char = dialect.quote_char;
    d.escape_char = dialect.escape_char;
    d.double_quote = dialect.double_quote ? 1 : 0;
    d.line_ending = to_c_line_ending(dialect.line_ending);
    return d;
}

/* ============================================================================
 * Version Information
 * ========================================================================== */

extern "C" {

const char* simdcsv_version(void) {
    return SIMDCSV_VERSION_STRING;
}

/* ============================================================================
 * Memory Management Functions
 * ========================================================================== */

uint8_t* simdcsv_alloc_buffer(size_t length, size_t padding) {
    return allocate_padded_buffer(length, padding);
}

void simdcsv_free_buffer(void* buffer) {
    aligned_free(buffer);
}

void simdcsv_free_string(char* str) {
    delete[] str;
}

int simdcsv_load_file(const char* filename, size_t padding,
                      uint8_t** out_buffer, size_t* out_length) {
    if (!filename || !out_buffer || !out_length) {
        return -1;
    }

    try {
        auto corpus = get_corpus(std::string(filename), padding);
        *out_buffer = const_cast<uint8_t*>(corpus.data());
        *out_length = corpus.size();
        return 0;
    } catch (...) {
        *out_buffer = nullptr;
        *out_length = 0;
        return -1;
    }
}

/* ============================================================================
 * Parser Lifecycle Functions
 * ========================================================================== */

simdcsv_parser_t* simdcsv_parser_create(void) {
    try {
        return new simdcsv_parser();
    } catch (...) {
        return nullptr;
    }
}

void simdcsv_parser_destroy(simdcsv_parser_t* parser) {
    delete parser;
}

/* ============================================================================
 * Index (Parsing Result) Lifecycle Functions
 * ========================================================================== */

simdcsv_index_t* simdcsv_index_create(simdcsv_parser_t* parser,
                                      size_t buffer_length, uint8_t n_threads) {
    if (!parser) {
        return nullptr;
    }

    try {
        auto* idx_wrapper = new simdcsv_index();
        idx_wrapper->idx = parser->parser.init(buffer_length, n_threads);
        return idx_wrapper;
    } catch (...) {
        return nullptr;
    }
}

void simdcsv_index_destroy(simdcsv_index_t* index) {
    delete index;
}

uint64_t simdcsv_index_columns(const simdcsv_index_t* index) {
    if (!index) {
        return 0;
    }
    return index->idx.columns;
}

uint8_t simdcsv_index_n_threads(const simdcsv_index_t* index) {
    if (!index) {
        return 0;
    }
    return index->idx.n_threads;
}

uint64_t simdcsv_index_count(const simdcsv_index_t* index, uint8_t thread_id) {
    if (!index || thread_id >= index->idx.n_threads) {
        return 0;
    }
    return index->idx.n_indexes[thread_id];
}

const uint64_t* simdcsv_index_positions(const simdcsv_index_t* index) {
    if (!index) {
        return nullptr;
    }
    return index->idx.indexes;
}

uint64_t simdcsv_index_total_count(const simdcsv_index_t* index) {
    if (!index) {
        return 0;
    }
    uint64_t total = 0;
    for (uint8_t i = 0; i < index->idx.n_threads; ++i) {
        total += index->idx.n_indexes[i];
    }
    return total;
}

int simdcsv_index_write(const simdcsv_index_t* index, const char* filename) {
    if (!index || !filename) {
        return -1;
    }

    try {
        // Cast away const since write() is not const-qualified but doesn't modify
        const_cast<simdcsv::index&>(index->idx).write(std::string(filename));
        return 0;
    } catch (...) {
        return -1;
    }
}

simdcsv_index_t* simdcsv_index_read(const char* filename) {
    if (!filename) {
        return nullptr;
    }

    try {
        // First read the header to get n_threads and calculate sizes
        std::FILE* fp = std::fopen(filename, "rb");
        if (!fp) {
            return nullptr;
        }

        uint64_t columns;
        uint8_t n_threads;
        if (std::fread(&columns, sizeof(uint64_t), 1, fp) != 1 ||
            std::fread(&n_threads, sizeof(uint8_t), 1, fp) != 1) {
            std::fclose(fp);
            return nullptr;
        }
        std::fclose(fp);

        // Create wrapper and allocate index arrays
        auto* idx_wrapper = new simdcsv_index();
        idx_wrapper->idx.columns = columns;
        idx_wrapper->idx.n_threads = n_threads;
        idx_wrapper->idx.n_indexes = new uint64_t[n_threads];

        // Read the full file to get total size
        fp = std::fopen(filename, "rb");
        if (!fp) {
            delete idx_wrapper;
            return nullptr;
        }

        // Skip header we already read
        std::fseek(fp, sizeof(uint64_t) + sizeof(uint8_t), SEEK_SET);

        // Read n_indexes array
        if (std::fread(idx_wrapper->idx.n_indexes, sizeof(uint64_t), n_threads, fp) != n_threads) {
            std::fclose(fp);
            delete idx_wrapper;
            return nullptr;
        }

        // Calculate total indexes
        uint64_t total_size = 0;
        for (uint8_t i = 0; i < n_threads; ++i) {
            total_size += idx_wrapper->idx.n_indexes[i];
        }

        // Allocate and read indexes
        idx_wrapper->idx.indexes = new uint64_t[total_size];
        if (std::fread(idx_wrapper->idx.indexes, sizeof(uint64_t), total_size, fp) != total_size) {
            std::fclose(fp);
            delete idx_wrapper;
            return nullptr;
        }

        std::fclose(fp);
        return idx_wrapper;
    } catch (...) {
        return nullptr;
    }
}

/* ============================================================================
 * Error Collector Lifecycle Functions
 * ========================================================================== */

simdcsv_errors_t* simdcsv_errors_create(simdcsv_error_mode_t mode) {
    try {
        return new simdcsv_errors(to_cpp_error_mode(mode));
    } catch (...) {
        return nullptr;
    }
}

void simdcsv_errors_destroy(simdcsv_errors_t* errors) {
    delete errors;
}

void simdcsv_errors_clear(simdcsv_errors_t* errors) {
    if (errors) {
        errors->collector.clear();
    }
}

void simdcsv_errors_set_mode(simdcsv_errors_t* errors, simdcsv_error_mode_t mode) {
    if (errors) {
        errors->collector.set_mode(to_cpp_error_mode(mode));
    }
}

simdcsv_error_mode_t simdcsv_errors_get_mode(const simdcsv_errors_t* errors) {
    if (!errors) {
        return SIMDCSV_ERROR_MODE_STRICT;
    }
    return to_c_error_mode(errors->collector.mode());
}

int simdcsv_errors_has_errors(const simdcsv_errors_t* errors) {
    if (!errors) {
        return 0;
    }
    return errors->collector.has_errors() ? 1 : 0;
}

int simdcsv_errors_has_fatal(const simdcsv_errors_t* errors) {
    if (!errors) {
        return 0;
    }
    return errors->collector.has_fatal_errors() ? 1 : 0;
}

size_t simdcsv_errors_count(const simdcsv_errors_t* errors) {
    if (!errors) {
        return 0;
    }
    return errors->collector.error_count();
}

int simdcsv_errors_get(const simdcsv_errors_t* errors, size_t index,
                       simdcsv_parse_error_t* out_error) {
    if (!errors || !out_error) {
        return -1;
    }

    const auto& errs = errors->collector.errors();
    if (index >= errs.size()) {
        return -1;
    }

    const auto& err = errs[index];
    out_error->code = to_c_error_code(err.code);
    out_error->severity = to_c_severity(err.severity);
    out_error->line = err.line;
    out_error->column = err.column;
    out_error->byte_offset = err.byte_offset;
    out_error->message = err.message.c_str();
    out_error->context = err.context.c_str();

    return 0;
}

char* simdcsv_errors_summary(const simdcsv_errors_t* errors) {
    if (!errors) {
        return nullptr;
    }

    try {
        std::string summary = errors->collector.summary();
        char* result = new char[summary.size() + 1];
        std::strcpy(result, summary.c_str());
        return result;
    } catch (...) {
        return nullptr;
    }
}

/* ============================================================================
 * Parsing Functions
 * ========================================================================== */

int simdcsv_parse(simdcsv_parser_t* parser, const uint8_t* buffer,
                  simdcsv_index_t* index, size_t length,
                  const simdcsv_dialect_t* dialect) {
    if (!parser || !buffer || !index) {
        return -1;
    }

    try {
        simdcsv::Dialect cpp_dialect = to_cpp_dialect(dialect);
        bool success = parser->parser.parse(buffer, index->idx, length, cpp_dialect);
        return success ? 0 : -1;
    } catch (...) {
        return -1;
    }
}

int simdcsv_parse_with_errors(simdcsv_parser_t* parser, const uint8_t* buffer,
                              simdcsv_index_t* index, size_t length,
                              simdcsv_errors_t* errors,
                              const simdcsv_dialect_t* dialect) {
    if (!parser || !buffer || !index || !errors) {
        return -1;
    }

    try {
        simdcsv::Dialect cpp_dialect = to_cpp_dialect(dialect);
        bool success = parser->parser.parse_with_errors(
            buffer, index->idx, length, errors->collector, cpp_dialect);
        return success ? 0 : -1;
    } catch (...) {
        return -1;
    }
}

int simdcsv_parse_mt(simdcsv_parser_t* parser, const uint8_t* buffer,
                     simdcsv_index_t* index, size_t length,
                     simdcsv_errors_t* errors,
                     const simdcsv_dialect_t* dialect) {
    if (!parser || !buffer || !index || !errors) {
        return -1;
    }

    try {
        simdcsv::Dialect cpp_dialect = to_cpp_dialect(dialect);
        bool success = parser->parser.parse_two_pass_with_errors(
            buffer, index->idx, length, errors->collector, cpp_dialect);
        return success ? 0 : -1;
    } catch (...) {
        return -1;
    }
}

int simdcsv_parse_auto(simdcsv_parser_t* parser, const uint8_t* buffer,
                       simdcsv_index_t* index, size_t length,
                       simdcsv_errors_t* errors,
                       simdcsv_detection_result_t* out_detected) {
    if (!parser || !buffer || !index || !errors) {
        return -1;
    }

    try {
        simdcsv::DetectionResult detected;
        bool success = parser->parser.parse_auto(
            buffer, index->idx, length, errors->collector,
            out_detected ? &detected : nullptr);

        if (out_detected && success) {
            out_detected->dialect = to_c_dialect(detected.dialect);
            out_detected->confidence = detected.confidence;
            out_detected->has_header = detected.has_header ? 1 : 0;
            out_detected->detected_columns = detected.detected_columns;
            out_detected->rows_analyzed = detected.rows_analyzed;
            // Warning is borrowed from internal static - not thread safe but simple
            // For full thread safety, would need to copy the string
            out_detected->warning = detected.warning.empty() ? nullptr : detected.warning.c_str();
        }

        return success ? 0 : -1;
    } catch (...) {
        return -1;
    }
}

/* ============================================================================
 * Dialect Detection Functions
 * ========================================================================== */

int simdcsv_detect_dialect(const uint8_t* buffer, size_t length,
                           simdcsv_detection_result_t* out_result) {
    if (!buffer || !out_result) {
        return -1;
    }

    try {
        simdcsv::DialectDetector detector;
        simdcsv::DetectionResult result = detector.detect(buffer, length);

        out_result->dialect = to_c_dialect(result.dialect);
        out_result->confidence = result.confidence;
        out_result->has_header = result.has_header ? 1 : 0;
        out_result->detected_columns = result.detected_columns;
        out_result->rows_analyzed = result.rows_analyzed;
        out_result->warning = result.warning.empty() ? nullptr : result.warning.c_str();

        return 0;
    } catch (...) {
        return -1;
    }
}

int simdcsv_detect_dialect_file(const char* filename,
                                simdcsv_detection_result_t* out_result) {
    if (!filename || !out_result) {
        return -1;
    }

    try {
        simdcsv::DialectDetector detector;
        simdcsv::DetectionResult result = detector.detect_file(std::string(filename));

        out_result->dialect = to_c_dialect(result.dialect);
        out_result->confidence = result.confidence;
        out_result->has_header = result.has_header ? 1 : 0;
        out_result->detected_columns = result.detected_columns;
        out_result->rows_analyzed = result.rows_analyzed;
        out_result->warning = result.warning.empty() ? nullptr : result.warning.c_str();

        return 0;
    } catch (...) {
        return -1;
    }
}

/* ============================================================================
 * Dialect Helper Functions
 * ========================================================================== */

simdcsv_dialect_t simdcsv_dialect_csv(void) {
    return to_c_dialect(simdcsv::Dialect::csv());
}

simdcsv_dialect_t simdcsv_dialect_tsv(void) {
    return to_c_dialect(simdcsv::Dialect::tsv());
}

simdcsv_dialect_t simdcsv_dialect_semicolon(void) {
    return to_c_dialect(simdcsv::Dialect::semicolon());
}

simdcsv_dialect_t simdcsv_dialect_pipe(void) {
    return to_c_dialect(simdcsv::Dialect::pipe());
}

int simdcsv_dialect_is_valid(const simdcsv_dialect_t* dialect) {
    if (!dialect) {
        return 0;
    }
    simdcsv::Dialect cpp_dialect = to_cpp_dialect(dialect);
    return cpp_dialect.is_valid() ? 1 : 0;
}

} // extern "C"
