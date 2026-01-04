/**
 * @file simdcsv_c.h
 * @brief C API wrapper for the simdcsv high-performance CSV parser.
 */

#ifndef SIMDCSV_C_H
#define SIMDCSV_C_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

#define SIMDCSV_VERSION_MAJOR 0
#define SIMDCSV_VERSION_MINOR 1
#define SIMDCSV_VERSION_PATCH 0

const char* simdcsv_version(void);

typedef enum simdcsv_error {
    SIMDCSV_OK = 0,
    SIMDCSV_ERROR_UNCLOSED_QUOTE = 1,
    SIMDCSV_ERROR_INVALID_QUOTE_ESCAPE = 2,
    SIMDCSV_ERROR_QUOTE_IN_UNQUOTED = 3,
    SIMDCSV_ERROR_INCONSISTENT_FIELDS = 4,
    SIMDCSV_ERROR_FIELD_TOO_LARGE = 5,
    SIMDCSV_ERROR_MIXED_LINE_ENDINGS = 6,
    SIMDCSV_ERROR_INVALID_LINE_ENDING = 7,
    SIMDCSV_ERROR_INVALID_UTF8 = 8,
    SIMDCSV_ERROR_NULL_BYTE = 9,
    SIMDCSV_ERROR_EMPTY_HEADER = 10,
    SIMDCSV_ERROR_DUPLICATE_COLUMNS = 11,
    SIMDCSV_ERROR_AMBIGUOUS_SEPARATOR = 12,
    SIMDCSV_ERROR_FILE_TOO_LARGE = 13,
    SIMDCSV_ERROR_IO = 14,
    SIMDCSV_ERROR_INTERNAL = 15,
    SIMDCSV_ERROR_NULL_POINTER = 100,
    SIMDCSV_ERROR_INVALID_ARGUMENT = 101,
    SIMDCSV_ERROR_OUT_OF_MEMORY = 102,
    SIMDCSV_ERROR_INVALID_HANDLE = 103
} simdcsv_error_t;

const char* simdcsv_error_string(simdcsv_error_t error);

typedef enum simdcsv_severity {
    SIMDCSV_SEVERITY_WARNING = 0,
    SIMDCSV_SEVERITY_ERROR = 1,
    SIMDCSV_SEVERITY_FATAL = 2
} simdcsv_severity_t;

typedef enum simdcsv_error_mode {
    SIMDCSV_MODE_STRICT = 0,
    SIMDCSV_MODE_PERMISSIVE = 1,
    SIMDCSV_MODE_BEST_EFFORT = 2
} simdcsv_error_mode_t;

typedef struct simdcsv_parser simdcsv_parser_t;
typedef struct simdcsv_index simdcsv_index_t;
typedef struct simdcsv_buffer simdcsv_buffer_t;
typedef struct simdcsv_dialect simdcsv_dialect_t;
typedef struct simdcsv_error_collector simdcsv_error_collector_t;
typedef struct simdcsv_detection_result simdcsv_detection_result_t;

typedef struct simdcsv_parse_error {
    simdcsv_error_t code;
    simdcsv_severity_t severity;
    size_t line;
    size_t column;
    size_t byte_offset;
    const char* message;
    const char* context;
} simdcsv_parse_error_t;

/* Buffer Management */
simdcsv_buffer_t* simdcsv_buffer_load_file(const char* filename);
simdcsv_buffer_t* simdcsv_buffer_create(const uint8_t* data, size_t length);
const uint8_t* simdcsv_buffer_data(const simdcsv_buffer_t* buffer);
size_t simdcsv_buffer_length(const simdcsv_buffer_t* buffer);
void simdcsv_buffer_destroy(simdcsv_buffer_t* buffer);

/* Dialect Configuration */
simdcsv_dialect_t* simdcsv_dialect_csv(void);
simdcsv_dialect_t* simdcsv_dialect_tsv(void);
simdcsv_dialect_t* simdcsv_dialect_semicolon(void);
simdcsv_dialect_t* simdcsv_dialect_pipe(void);
simdcsv_dialect_t* simdcsv_dialect_create(char delimiter, char quote_char,
                                           char escape_char, bool double_quote);
char simdcsv_dialect_delimiter(const simdcsv_dialect_t* dialect);
char simdcsv_dialect_quote_char(const simdcsv_dialect_t* dialect);
char simdcsv_dialect_escape_char(const simdcsv_dialect_t* dialect);
bool simdcsv_dialect_double_quote(const simdcsv_dialect_t* dialect);
simdcsv_error_t simdcsv_dialect_validate(const simdcsv_dialect_t* dialect);
size_t simdcsv_dialect_to_string(const simdcsv_dialect_t* dialect, char* buffer, size_t buffer_size);
void simdcsv_dialect_destroy(simdcsv_dialect_t* dialect);

/* Error Collector */
simdcsv_error_collector_t* simdcsv_error_collector_create(simdcsv_error_mode_t mode, size_t max_errors);
simdcsv_error_mode_t simdcsv_error_collector_mode(const simdcsv_error_collector_t* collector);
void simdcsv_error_collector_set_mode(simdcsv_error_collector_t* collector, simdcsv_error_mode_t mode);
bool simdcsv_error_collector_has_errors(const simdcsv_error_collector_t* collector);
bool simdcsv_error_collector_has_fatal(const simdcsv_error_collector_t* collector);
size_t simdcsv_error_collector_count(const simdcsv_error_collector_t* collector);
simdcsv_error_t simdcsv_error_collector_get(const simdcsv_error_collector_t* collector, size_t index, simdcsv_parse_error_t* error);
void simdcsv_error_collector_clear(simdcsv_error_collector_t* collector);
void simdcsv_error_collector_destroy(simdcsv_error_collector_t* collector);

/* Index Structure */
simdcsv_index_t* simdcsv_index_create(size_t buffer_length, size_t num_threads);
size_t simdcsv_index_num_threads(const simdcsv_index_t* index);
size_t simdcsv_index_columns(const simdcsv_index_t* index);
uint64_t simdcsv_index_count(const simdcsv_index_t* index, size_t thread_id);
uint64_t simdcsv_index_total_count(const simdcsv_index_t* index);
const uint64_t* simdcsv_index_positions(const simdcsv_index_t* index);
void simdcsv_index_destroy(simdcsv_index_t* index);

/* Parser */
simdcsv_parser_t* simdcsv_parser_create(void);
simdcsv_error_t simdcsv_parse(simdcsv_parser_t* parser, const simdcsv_buffer_t* buffer, simdcsv_index_t* index, const simdcsv_dialect_t* dialect);
simdcsv_error_t simdcsv_parse_with_errors(simdcsv_parser_t* parser, const simdcsv_buffer_t* buffer, simdcsv_index_t* index, simdcsv_error_collector_t* errors, const simdcsv_dialect_t* dialect);
simdcsv_error_t simdcsv_parse_two_pass_with_errors(simdcsv_parser_t* parser, const simdcsv_buffer_t* buffer, simdcsv_index_t* index, simdcsv_error_collector_t* errors, const simdcsv_dialect_t* dialect);
void simdcsv_parser_destroy(simdcsv_parser_t* parser);

/* Dialect Detection */
simdcsv_detection_result_t* simdcsv_detect_dialect(const simdcsv_buffer_t* buffer);
bool simdcsv_detection_result_success(const simdcsv_detection_result_t* result);
double simdcsv_detection_result_confidence(const simdcsv_detection_result_t* result);
simdcsv_dialect_t* simdcsv_detection_result_dialect(const simdcsv_detection_result_t* result);
size_t simdcsv_detection_result_columns(const simdcsv_detection_result_t* result);
size_t simdcsv_detection_result_rows_analyzed(const simdcsv_detection_result_t* result);
bool simdcsv_detection_result_has_header(const simdcsv_detection_result_t* result);
const char* simdcsv_detection_result_warning(const simdcsv_detection_result_t* result);
void simdcsv_detection_result_destroy(simdcsv_detection_result_t* result);
simdcsv_error_t simdcsv_parse_auto(simdcsv_parser_t* parser, const simdcsv_buffer_t* buffer, simdcsv_index_t* index, simdcsv_error_collector_t* errors, simdcsv_detection_result_t** detected);

/* Utility Functions */
size_t simdcsv_recommended_threads(void);
size_t simdcsv_simd_padding(void);

#ifdef __cplusplus
}
#endif

#endif /* SIMDCSV_C_H */
