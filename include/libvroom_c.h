/**
 * @file libvroom_c.h
 * @brief C API wrapper for the libvroom high-performance CSV parser.
 */

#ifndef LIBVROOM_C_H
#define LIBVROOM_C_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

#define LIBVROOM_VERSION_MAJOR 0
#define LIBVROOM_VERSION_MINOR 1
#define LIBVROOM_VERSION_PATCH 0

const char* libvroom_version(void);

typedef enum libvroom_error {
    LIBVROOM_OK = 0,
    LIBVROOM_ERROR_UNCLOSED_QUOTE = 1,
    LIBVROOM_ERROR_INVALID_QUOTE_ESCAPE = 2,
    LIBVROOM_ERROR_QUOTE_IN_UNQUOTED = 3,
    LIBVROOM_ERROR_INCONSISTENT_FIELDS = 4,
    LIBVROOM_ERROR_FIELD_TOO_LARGE = 5,
    LIBVROOM_ERROR_MIXED_LINE_ENDINGS = 6,
    LIBVROOM_ERROR_INVALID_LINE_ENDING = 7,
    LIBVROOM_ERROR_INVALID_UTF8 = 8,
    LIBVROOM_ERROR_NULL_BYTE = 9,
    LIBVROOM_ERROR_EMPTY_HEADER = 10,
    LIBVROOM_ERROR_DUPLICATE_COLUMNS = 11,
    LIBVROOM_ERROR_AMBIGUOUS_SEPARATOR = 12,
    LIBVROOM_ERROR_FILE_TOO_LARGE = 13,
    LIBVROOM_ERROR_IO = 14,
    LIBVROOM_ERROR_INTERNAL = 15,
    LIBVROOM_ERROR_NULL_POINTER = 100,
    LIBVROOM_ERROR_INVALID_ARGUMENT = 101,
    LIBVROOM_ERROR_OUT_OF_MEMORY = 102,
    LIBVROOM_ERROR_INVALID_HANDLE = 103
} libvroom_error_t;

const char* libvroom_error_string(libvroom_error_t error);

typedef enum libvroom_severity {
    LIBVROOM_SEVERITY_WARNING = 0,
    LIBVROOM_SEVERITY_ERROR = 1,
    LIBVROOM_SEVERITY_FATAL = 2
} libvroom_severity_t;

typedef enum libvroom_error_mode {
    LIBVROOM_MODE_STRICT = 0,
    LIBVROOM_MODE_PERMISSIVE = 1,
    LIBVROOM_MODE_BEST_EFFORT = 2
} libvroom_error_mode_t;

typedef struct libvroom_parser libvroom_parser_t;
typedef struct libvroom_index libvroom_index_t;
typedef struct libvroom_buffer libvroom_buffer_t;
typedef struct libvroom_dialect libvroom_dialect_t;
typedef struct libvroom_error_collector libvroom_error_collector_t;
typedef struct libvroom_detection_result libvroom_detection_result_t;

/**
 * Parse error information returned from error collector.
 *
 * @note The `message` and `context` pointers point to internal strings owned by the
 *       error collector. They are only valid as long as:
 *       1. The error collector has not been destroyed
 *       2. The error collector has not been cleared (libvroom_error_collector_clear)
 *       3. No new errors have been added to the collector
 *       If you need to persist error information, copy the strings before any of these events.
 */
typedef struct libvroom_parse_error {
    libvroom_error_t code;
    libvroom_severity_t severity;
    size_t line;
    size_t column;
    size_t byte_offset;
    const char* message;   /**< Error message - see struct documentation for lifetime */
    const char* context;   /**< Context around error - see struct documentation for lifetime */
} libvroom_parse_error_t;

/* Buffer Management */
libvroom_buffer_t* libvroom_buffer_load_file(const char* filename);
libvroom_buffer_t* libvroom_buffer_create(const uint8_t* data, size_t length);
const uint8_t* libvroom_buffer_data(const libvroom_buffer_t* buffer);
size_t libvroom_buffer_length(const libvroom_buffer_t* buffer);
void libvroom_buffer_destroy(libvroom_buffer_t* buffer);

/* Dialect Configuration */
libvroom_dialect_t* libvroom_dialect_create(char delimiter, char quote_char,
                                           char escape_char, bool double_quote);
char libvroom_dialect_delimiter(const libvroom_dialect_t* dialect);
char libvroom_dialect_quote_char(const libvroom_dialect_t* dialect);
char libvroom_dialect_escape_char(const libvroom_dialect_t* dialect);
bool libvroom_dialect_double_quote(const libvroom_dialect_t* dialect);
void libvroom_dialect_destroy(libvroom_dialect_t* dialect);

/* Error Collector */
libvroom_error_collector_t* libvroom_error_collector_create(libvroom_error_mode_t mode, size_t max_errors);
libvroom_error_mode_t libvroom_error_collector_mode(const libvroom_error_collector_t* collector);
bool libvroom_error_collector_has_errors(const libvroom_error_collector_t* collector);
bool libvroom_error_collector_has_fatal(const libvroom_error_collector_t* collector);
size_t libvroom_error_collector_count(const libvroom_error_collector_t* collector);
libvroom_error_t libvroom_error_collector_get(const libvroom_error_collector_t* collector, size_t index, libvroom_parse_error_t* error);
void libvroom_error_collector_clear(libvroom_error_collector_t* collector);
void libvroom_error_collector_destroy(libvroom_error_collector_t* collector);

/* Index Structure */
libvroom_index_t* libvroom_index_create(size_t buffer_length, size_t num_threads);
size_t libvroom_index_num_threads(const libvroom_index_t* index);
size_t libvroom_index_columns(const libvroom_index_t* index);
uint64_t libvroom_index_count(const libvroom_index_t* index, size_t thread_id);
uint64_t libvroom_index_total_count(const libvroom_index_t* index);
const uint64_t* libvroom_index_positions(const libvroom_index_t* index);
void libvroom_index_destroy(libvroom_index_t* index);

/* Parser */
libvroom_parser_t* libvroom_parser_create(void);
libvroom_error_t libvroom_parse(libvroom_parser_t* parser, const libvroom_buffer_t* buffer, libvroom_index_t* index, libvroom_error_collector_t* errors, const libvroom_dialect_t* dialect);
void libvroom_parser_destroy(libvroom_parser_t* parser);

/* Dialect Detection */
libvroom_detection_result_t* libvroom_detect_dialect(const libvroom_buffer_t* buffer);
bool libvroom_detection_result_success(const libvroom_detection_result_t* result);
double libvroom_detection_result_confidence(const libvroom_detection_result_t* result);
libvroom_dialect_t* libvroom_detection_result_dialect(const libvroom_detection_result_t* result);
size_t libvroom_detection_result_columns(const libvroom_detection_result_t* result);
size_t libvroom_detection_result_rows_analyzed(const libvroom_detection_result_t* result);
bool libvroom_detection_result_has_header(const libvroom_detection_result_t* result);
const char* libvroom_detection_result_warning(const libvroom_detection_result_t* result);
void libvroom_detection_result_destroy(libvroom_detection_result_t* result);
libvroom_error_t libvroom_parse_auto(libvroom_parser_t* parser, const libvroom_buffer_t* buffer, libvroom_index_t* index, libvroom_error_collector_t* errors, libvroom_detection_result_t** detected);

/* Utility Functions */
size_t libvroom_recommended_threads(void);
size_t libvroom_simd_padding(void);

#ifdef __cplusplus
}
#endif

#endif /* LIBVROOM_C_H */
