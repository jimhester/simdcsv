/**
 * @file simdcsv_c.h
 * @brief C API for the simdcsv high-performance CSV parser.
 *
 * This header provides a stable C ABI wrapper around the simdcsv C++ library,
 * enabling language bindings for Python, R, Julia, Rust, and other languages
 * through their respective FFI mechanisms.
 *
 * @section memory Memory Management
 *
 * The API uses explicit memory management with create/destroy function pairs:
 * - simdcsv_parser_create() / simdcsv_parser_destroy()
 * - simdcsv_index_create() / simdcsv_index_destroy()
 * - simdcsv_errors_create() / simdcsv_errors_destroy()
 * - simdcsv_alloc_buffer() / simdcsv_free_buffer()
 *
 * Strings returned by functions that allocate memory (e.g., simdcsv_errors_summary)
 * must be freed by the caller using simdcsv_free_string().
 *
 * Pointers in structs (e.g., message, context in simdcsv_parse_error_t) are borrowed
 * and remain valid only until the owning object is modified or destroyed.
 *
 * @section threading Thread Safety
 *
 * - Parser objects (simdcsv_parser_t) are stateless and can be shared across threads.
 * - Index objects (simdcsv_index_t) should not be accessed concurrently during parsing.
 * - Error collectors (simdcsv_errors_t) are NOT thread-safe; use one per thread.
 * - Buffer allocation functions are thread-safe.
 *
 * @example
 * @code
 * #include "simdcsv_c.h"
 * #include <stdio.h>
 *
 * int main(void) {
 *     // Load a CSV file
 *     uint8_t* buffer = NULL;
 *     size_t length = 0;
 *     if (simdcsv_load_file("data.csv", 64, &buffer, &length) != 0) {
 *         fprintf(stderr, "Failed to load file\n");
 *         return 1;
 *     }
 *
 *     // Create parser and index
 *     simdcsv_parser_t* parser = simdcsv_parser_create();
 *     simdcsv_index_t* index = simdcsv_index_create(parser, length, 1);
 *
 *     // Create error collector
 *     simdcsv_errors_t* errors = simdcsv_errors_create(SIMDCSV_ERROR_MODE_PERMISSIVE);
 *
 *     // Parse with default CSV dialect
 *     simdcsv_dialect_t dialect = simdcsv_dialect_csv();
 *     int result = simdcsv_parse_with_errors(parser, buffer, index, length, errors, &dialect);
 *
 *     if (result == 0) {
 *         printf("Parsed %llu columns\n", simdcsv_index_columns(index));
 *         printf("Found %zu field positions\n", (size_t)simdcsv_index_count(index, 0));
 *     }
 *
 *     // Check for errors
 *     if (simdcsv_errors_has_errors(errors)) {
 *         char* summary = simdcsv_errors_summary(errors);
 *         printf("Errors:\n%s\n", summary);
 *         simdcsv_free_string(summary);
 *     }
 *
 *     // Cleanup
 *     simdcsv_errors_destroy(errors);
 *     simdcsv_index_destroy(index);
 *     simdcsv_parser_destroy(parser);
 *     simdcsv_free_buffer(buffer);
 *
 *     return 0;
 * }
 * @endcode
 */

#ifndef SIMDCSV_C_H
#define SIMDCSV_C_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* ============================================================================
 * Version Information
 * ========================================================================== */

/**
 * @brief Returns the simdcsv library version string.
 * @return Static string containing version (e.g., "0.1.0"). Do not free.
 */
const char* simdcsv_version(void);

/* ============================================================================
 * Opaque Handle Types
 * ========================================================================== */

/** @brief Opaque handle to a CSV parser instance. */
typedef struct simdcsv_parser simdcsv_parser_t;

/** @brief Opaque handle to a parsed index (field positions). */
typedef struct simdcsv_index simdcsv_index_t;

/** @brief Opaque handle to an error collector. */
typedef struct simdcsv_errors simdcsv_errors_t;

/* ============================================================================
 * Enumerations
 * ========================================================================== */

/**
 * @brief Error codes representing different types of CSV parsing errors.
 *
 * These correspond to simdcsv::ErrorCode in the C++ API.
 */
typedef enum {
    SIMDCSV_ERROR_NONE = 0,              /**< No error */
    SIMDCSV_ERROR_UNCLOSED_QUOTE,        /**< Quoted field not closed before EOF */
    SIMDCSV_ERROR_INVALID_QUOTE_ESCAPE,  /**< Invalid quote escape sequence */
    SIMDCSV_ERROR_QUOTE_IN_UNQUOTED,     /**< Quote in middle of unquoted field */
    SIMDCSV_ERROR_INCONSISTENT_FIELDS,   /**< Row has different field count than header */
    SIMDCSV_ERROR_FIELD_TOO_LARGE,       /**< Field exceeds maximum size (reserved) */
    SIMDCSV_ERROR_MIXED_LINE_ENDINGS,    /**< Inconsistent line endings */
    SIMDCSV_ERROR_INVALID_LINE_ENDING,   /**< Invalid line ending sequence (reserved) */
    SIMDCSV_ERROR_INVALID_UTF8,          /**< Invalid UTF-8 sequence (reserved) */
    SIMDCSV_ERROR_NULL_BYTE,             /**< Unexpected null byte in data */
    SIMDCSV_ERROR_EMPTY_HEADER,          /**< Header row is empty */
    SIMDCSV_ERROR_DUPLICATE_COLUMNS,     /**< Duplicate column names in header */
    SIMDCSV_ERROR_AMBIGUOUS_SEPARATOR,   /**< Cannot determine separator (reserved) */
    SIMDCSV_ERROR_FILE_TOO_LARGE,        /**< File exceeds maximum size (reserved) */
    SIMDCSV_ERROR_IO_ERROR,              /**< File I/O error (reserved) */
    SIMDCSV_ERROR_INTERNAL               /**< Internal parser error */
} simdcsv_error_code_t;

/**
 * @brief Severity levels for parse errors.
 */
typedef enum {
    SIMDCSV_SEVERITY_WARNING,  /**< Non-fatal issue, parser continues */
    SIMDCSV_SEVERITY_ERROR,    /**< Recoverable error, can skip affected row */
    SIMDCSV_SEVERITY_FATAL     /**< Unrecoverable error, parsing must stop */
} simdcsv_error_severity_t;

/**
 * @brief Error handling modes that control parser behavior on errors.
 */
typedef enum {
    SIMDCSV_ERROR_MODE_STRICT,      /**< Stop on first error */
    SIMDCSV_ERROR_MODE_PERMISSIVE,  /**< Collect all errors, try to recover */
    SIMDCSV_ERROR_MODE_BEST_EFFORT  /**< Ignore errors, parse what's possible */
} simdcsv_error_mode_t;

/**
 * @brief Line ending styles detected in CSV files.
 */
typedef enum {
    SIMDCSV_LINE_ENDING_LF,      /**< Unix style (\\n) */
    SIMDCSV_LINE_ENDING_CRLF,    /**< Windows style (\\r\\n) */
    SIMDCSV_LINE_ENDING_CR,      /**< Old Mac style (\\r) */
    SIMDCSV_LINE_ENDING_MIXED,   /**< Mixed line endings */
    SIMDCSV_LINE_ENDING_UNKNOWN  /**< Not yet determined */
} simdcsv_line_ending_t;

/* ============================================================================
 * Data Structures
 * ========================================================================== */

/**
 * @brief CSV dialect configuration.
 *
 * Specifies the formatting parameters for a CSV file.
 */
typedef struct {
    char delimiter;                      /**< Field separator (default: ',') */
    char quote_char;                     /**< Quote character (default: '"') */
    char escape_char;                    /**< Escape character (default: '"') */
    int double_quote;                    /**< Use "" for escaping (1=true, 0=false) */
    simdcsv_line_ending_t line_ending;   /**< Detected line ending style */
} simdcsv_dialect_t;

/**
 * @brief Information about a single parse error.
 *
 * @note The message and context pointers are borrowed from the error collector.
 *       They remain valid only until the error collector is modified or destroyed.
 */
typedef struct {
    simdcsv_error_code_t code;       /**< Error type */
    simdcsv_error_severity_t severity; /**< Severity level */
    size_t line;                     /**< Line number (1-indexed) */
    size_t column;                   /**< Column number (1-indexed) */
    size_t byte_offset;              /**< Byte offset from start */
    const char* message;             /**< Error description (borrowed) */
    const char* context;             /**< Data context snippet (borrowed) */
} simdcsv_parse_error_t;

/**
 * @brief Result of dialect detection.
 *
 * @note The warning pointer is borrowed and valid until the next detection call.
 */
typedef struct {
    simdcsv_dialect_t dialect;       /**< Detected dialect configuration */
    double confidence;               /**< Detection confidence [0.0, 1.0] */
    int has_header;                  /**< Whether first row appears to be header */
    size_t detected_columns;         /**< Number of columns detected */
    size_t rows_analyzed;            /**< Number of rows analyzed */
    const char* warning;             /**< Detection warning (borrowed, may be NULL) */
} simdcsv_detection_result_t;

/* ============================================================================
 * Memory Management Functions
 * ========================================================================== */

/**
 * @brief Allocates a SIMD-aligned buffer with padding.
 *
 * The buffer is aligned to 64-byte cache line boundaries and includes
 * extra padding bytes for safe SIMD overreads.
 *
 * @param length The number of bytes of actual data to store.
 * @param padding Extra bytes to allocate for SIMD safety (typically 64).
 * @return Pointer to allocated buffer, or NULL on failure.
 *
 * @note Free with simdcsv_free_buffer(), not standard free().
 */
uint8_t* simdcsv_alloc_buffer(size_t length, size_t padding);

/**
 * @brief Frees a buffer allocated by simdcsv_alloc_buffer() or simdcsv_load_file().
 *
 * @param buffer Pointer to the buffer to free. NULL is safely ignored.
 */
void simdcsv_free_buffer(void* buffer);

/**
 * @brief Frees a string allocated by simdcsv functions.
 *
 * Use this to free strings returned by functions like simdcsv_errors_summary().
 *
 * @param str Pointer to the string to free. NULL is safely ignored.
 */
void simdcsv_free_string(char* str);

/**
 * @brief Loads an entire file into a SIMD-aligned buffer.
 *
 * @param filename Path to the file to load.
 * @param padding Extra bytes for SIMD safety (typically 64).
 * @param[out] out_buffer Pointer to receive the allocated buffer.
 * @param[out] out_length Pointer to receive the file size (excluding padding).
 * @return 0 on success, non-zero on error.
 *
 * @note Free the buffer with simdcsv_free_buffer() when done.
 */
int simdcsv_load_file(const char* filename, size_t padding,
                      uint8_t** out_buffer, size_t* out_length);

/* ============================================================================
 * Parser Lifecycle Functions
 * ========================================================================== */

/**
 * @brief Creates a new CSV parser instance.
 *
 * @return Pointer to the parser, or NULL on allocation failure.
 *
 * @note Free with simdcsv_parser_destroy() when done.
 */
simdcsv_parser_t* simdcsv_parser_create(void);

/**
 * @brief Destroys a parser instance and frees its resources.
 *
 * @param parser Parser to destroy. NULL is safely ignored.
 */
void simdcsv_parser_destroy(simdcsv_parser_t* parser);

/* ============================================================================
 * Index (Parsing Result) Lifecycle Functions
 * ========================================================================== */

/**
 * @brief Creates a new index for storing parsed field positions.
 *
 * @param parser The parser that will use this index.
 * @param buffer_length Length of the buffer to be parsed.
 * @param n_threads Number of threads for parallel parsing (1 for single-threaded).
 * @return Pointer to the index, or NULL on allocation failure.
 *
 * @note Free with simdcsv_index_destroy() when done.
 */
simdcsv_index_t* simdcsv_index_create(simdcsv_parser_t* parser,
                                      size_t buffer_length, uint8_t n_threads);

/**
 * @brief Destroys an index and frees its resources.
 *
 * @param index Index to destroy. NULL is safely ignored.
 */
void simdcsv_index_destroy(simdcsv_index_t* index);

/**
 * @brief Returns the number of columns detected in the CSV.
 *
 * @param index The parsed index.
 * @return Number of columns, or 0 if index is NULL or not parsed.
 */
uint64_t simdcsv_index_columns(const simdcsv_index_t* index);

/**
 * @brief Returns the number of threads used for parsing.
 *
 * @param index The parsed index.
 * @return Number of threads, or 0 if index is NULL.
 */
uint8_t simdcsv_index_n_threads(const simdcsv_index_t* index);

/**
 * @brief Returns the count of field positions found by a specific thread.
 *
 * @param index The parsed index.
 * @param thread_id Thread ID (0 to n_threads-1).
 * @return Count of positions for that thread, or 0 if invalid.
 */
uint64_t simdcsv_index_count(const simdcsv_index_t* index, uint8_t thread_id);

/**
 * @brief Returns a pointer to the raw field position array.
 *
 * For multi-threaded parsing, positions are interleaved by thread.
 * Position for thread T at logical index I is at: positions[I * n_threads + T]
 *
 * @param index The parsed index.
 * @return Pointer to the position array, or NULL if index is NULL.
 *
 * @note The returned pointer is valid only while the index exists.
 */
const uint64_t* simdcsv_index_positions(const simdcsv_index_t* index);

/**
 * @brief Returns the total count of field positions across all threads.
 *
 * @param index The parsed index.
 * @return Total count of positions, or 0 if index is NULL.
 */
uint64_t simdcsv_index_total_count(const simdcsv_index_t* index);

/**
 * @brief Serializes an index to a binary file.
 *
 * @param index The index to save.
 * @param filename Path to the output file.
 * @return 0 on success, non-zero on error.
 */
int simdcsv_index_write(const simdcsv_index_t* index, const char* filename);

/**
 * @brief Deserializes an index from a binary file.
 *
 * @param filename Path to the input file.
 * @return Pointer to the loaded index, or NULL on error.
 *
 * @note Free with simdcsv_index_destroy() when done.
 */
simdcsv_index_t* simdcsv_index_read(const char* filename);

/* ============================================================================
 * Error Collector Lifecycle Functions
 * ========================================================================== */

/**
 * @brief Creates a new error collector with the specified mode.
 *
 * @param mode Error handling mode (STRICT, PERMISSIVE, or BEST_EFFORT).
 * @return Pointer to the error collector, or NULL on allocation failure.
 *
 * @note Free with simdcsv_errors_destroy() when done.
 * @note Error collectors are NOT thread-safe. Use one per thread.
 */
simdcsv_errors_t* simdcsv_errors_create(simdcsv_error_mode_t mode);

/**
 * @brief Destroys an error collector and frees its resources.
 *
 * @param errors Error collector to destroy. NULL is safely ignored.
 */
void simdcsv_errors_destroy(simdcsv_errors_t* errors);

/**
 * @brief Clears all collected errors.
 *
 * @param errors Error collector to clear. NULL is safely ignored.
 */
void simdcsv_errors_clear(simdcsv_errors_t* errors);

/**
 * @brief Sets the error handling mode.
 *
 * @param errors Error collector to modify.
 * @param mode New error handling mode.
 */
void simdcsv_errors_set_mode(simdcsv_errors_t* errors, simdcsv_error_mode_t mode);

/**
 * @brief Gets the current error handling mode.
 *
 * @param errors Error collector to query.
 * @return Current mode, or SIMDCSV_ERROR_MODE_STRICT if errors is NULL.
 */
simdcsv_error_mode_t simdcsv_errors_get_mode(const simdcsv_errors_t* errors);

/**
 * @brief Checks if any errors have been collected.
 *
 * @param errors Error collector to check.
 * @return 1 if errors exist, 0 otherwise.
 */
int simdcsv_errors_has_errors(const simdcsv_errors_t* errors);

/**
 * @brief Checks if any fatal errors have been collected.
 *
 * @param errors Error collector to check.
 * @return 1 if fatal errors exist, 0 otherwise.
 */
int simdcsv_errors_has_fatal(const simdcsv_errors_t* errors);

/**
 * @brief Returns the number of collected errors.
 *
 * @param errors Error collector to query.
 * @return Number of errors, or 0 if errors is NULL.
 */
size_t simdcsv_errors_count(const simdcsv_errors_t* errors);

/**
 * @brief Retrieves a specific error by index.
 *
 * @param errors Error collector to query.
 * @param index Error index (0 to count-1).
 * @param[out] out_error Pointer to receive the error information.
 * @return 0 on success, non-zero if index is out of bounds.
 *
 * @note The message and context pointers in out_error are borrowed from the
 *       error collector and remain valid until it is modified or destroyed.
 */
int simdcsv_errors_get(const simdcsv_errors_t* errors, size_t index,
                       simdcsv_parse_error_t* out_error);

/**
 * @brief Generates a human-readable summary of all errors.
 *
 * @param errors Error collector to summarize.
 * @return Newly allocated string with summary. Caller must free with simdcsv_free_string().
 *         Returns NULL if errors is NULL or allocation fails.
 */
char* simdcsv_errors_summary(const simdcsv_errors_t* errors);

/* ============================================================================
 * Parsing Functions
 * ========================================================================== */

/**
 * @brief Parses a CSV buffer without error collection.
 *
 * This is the fastest parsing method but provides no error information.
 *
 * @param parser Parser instance.
 * @param buffer Data buffer (must be SIMD-aligned with padding).
 * @param index Index to populate with field positions.
 * @param length Buffer length (excluding padding).
 * @param dialect CSV dialect configuration (NULL for default CSV).
 * @return 0 on success, non-zero on failure.
 */
int simdcsv_parse(simdcsv_parser_t* parser, const uint8_t* buffer,
                  simdcsv_index_t* index, size_t length,
                  const simdcsv_dialect_t* dialect);

/**
 * @brief Parses a CSV buffer with error collection (single-threaded).
 *
 * @param parser Parser instance.
 * @param buffer Data buffer (must be SIMD-aligned with padding).
 * @param index Index to populate with field positions.
 * @param length Buffer length (excluding padding).
 * @param errors Error collector to receive parse errors.
 * @param dialect CSV dialect configuration (NULL for default CSV).
 * @return 0 on success (no fatal errors), non-zero on failure.
 */
int simdcsv_parse_with_errors(simdcsv_parser_t* parser, const uint8_t* buffer,
                              simdcsv_index_t* index, size_t length,
                              simdcsv_errors_t* errors,
                              const simdcsv_dialect_t* dialect);

/**
 * @brief Parses a CSV buffer with error collection (multi-threaded).
 *
 * Uses the two-pass algorithm with speculative multi-threaded parsing.
 *
 * @param parser Parser instance.
 * @param buffer Data buffer (must be SIMD-aligned with padding).
 * @param index Index to populate (must be created with n_threads > 1).
 * @param length Buffer length (excluding padding).
 * @param errors Error collector to receive parse errors.
 * @param dialect CSV dialect configuration (NULL for default CSV).
 * @return 0 on success (no fatal errors), non-zero on failure.
 */
int simdcsv_parse_mt(simdcsv_parser_t* parser, const uint8_t* buffer,
                     simdcsv_index_t* index, size_t length,
                     simdcsv_errors_t* errors,
                     const simdcsv_dialect_t* dialect);

/**
 * @brief Auto-detects dialect and parses a CSV buffer.
 *
 * First detects the dialect, then parses using the detected settings.
 *
 * @param parser Parser instance.
 * @param buffer Data buffer (must be SIMD-aligned with padding).
 * @param index Index to populate with field positions.
 * @param length Buffer length (excluding padding).
 * @param errors Error collector to receive parse errors.
 * @param[out] out_detected Pointer to receive detected dialect info (may be NULL).
 * @return 0 on success (no fatal errors), non-zero on failure.
 */
int simdcsv_parse_auto(simdcsv_parser_t* parser, const uint8_t* buffer,
                       simdcsv_index_t* index, size_t length,
                       simdcsv_errors_t* errors,
                       simdcsv_detection_result_t* out_detected);

/* ============================================================================
 * Dialect Detection Functions
 * ========================================================================== */

/**
 * @brief Detects the dialect of CSV data in a buffer.
 *
 * @param buffer Data buffer to analyze.
 * @param length Buffer length.
 * @param[out] out_result Pointer to receive detection results.
 * @return 0 on success, non-zero on failure.
 */
int simdcsv_detect_dialect(const uint8_t* buffer, size_t length,
                           simdcsv_detection_result_t* out_result);

/**
 * @brief Detects the dialect of a CSV file.
 *
 * @param filename Path to the CSV file.
 * @param[out] out_result Pointer to receive detection results.
 * @return 0 on success, non-zero on failure.
 */
int simdcsv_detect_dialect_file(const char* filename,
                                simdcsv_detection_result_t* out_result);

/* ============================================================================
 * Dialect Helper Functions
 * ========================================================================== */

/**
 * @brief Returns a standard CSV dialect (comma-separated, double-quoted).
 * @return Dialect configuration for standard CSV.
 */
simdcsv_dialect_t simdcsv_dialect_csv(void);

/**
 * @brief Returns a TSV dialect (tab-separated).
 * @return Dialect configuration for TSV.
 */
simdcsv_dialect_t simdcsv_dialect_tsv(void);

/**
 * @brief Returns a semicolon-separated dialect (European style).
 * @return Dialect configuration for semicolon-separated files.
 */
simdcsv_dialect_t simdcsv_dialect_semicolon(void);

/**
 * @brief Returns a pipe-separated dialect.
 * @return Dialect configuration for pipe-separated files.
 */
simdcsv_dialect_t simdcsv_dialect_pipe(void);

/**
 * @brief Validates a dialect configuration.
 *
 * @param dialect Dialect to validate.
 * @return 1 if valid, 0 if invalid.
 */
int simdcsv_dialect_is_valid(const simdcsv_dialect_t* dialect);

#ifdef __cplusplus
}
#endif

#endif /* SIMDCSV_C_H */
