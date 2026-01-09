/**
 * @file libvroom_c.h
 * @brief C API wrapper for the libvroom high-performance CSV parser.
 */

#ifndef LIBVROOM_C_H
#define LIBVROOM_C_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

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
  // Note: value 7 was previously INVALID_LINE_ENDING (removed in v0.2.0)
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
  const char* message; /**< Error message - see struct documentation for lifetime */
  const char* context; /**< Context around error - see struct documentation for lifetime */
} libvroom_parse_error_t;

/* Buffer Management */
libvroom_buffer_t* libvroom_buffer_load_file(const char* filename);
libvroom_buffer_t* libvroom_buffer_create(const uint8_t* data, size_t length);
const uint8_t* libvroom_buffer_data(const libvroom_buffer_t* buffer);
size_t libvroom_buffer_length(const libvroom_buffer_t* buffer);
void libvroom_buffer_destroy(libvroom_buffer_t* buffer);

/* Dialect Configuration */
libvroom_dialect_t* libvroom_dialect_create(char delimiter, char quote_char, char escape_char,
                                            bool double_quote);
char libvroom_dialect_delimiter(const libvroom_dialect_t* dialect);
char libvroom_dialect_quote_char(const libvroom_dialect_t* dialect);
char libvroom_dialect_escape_char(const libvroom_dialect_t* dialect);
bool libvroom_dialect_double_quote(const libvroom_dialect_t* dialect);
void libvroom_dialect_destroy(libvroom_dialect_t* dialect);

/* Error Collector */
libvroom_error_collector_t* libvroom_error_collector_create(libvroom_error_mode_t mode,
                                                            size_t max_errors);
libvroom_error_mode_t libvroom_error_collector_mode(const libvroom_error_collector_t* collector);
bool libvroom_error_collector_has_errors(const libvroom_error_collector_t* collector);
bool libvroom_error_collector_has_fatal(const libvroom_error_collector_t* collector);
size_t libvroom_error_collector_count(const libvroom_error_collector_t* collector);
libvroom_error_t libvroom_error_collector_get(const libvroom_error_collector_t* collector,
                                              size_t index, libvroom_parse_error_t* error);
void libvroom_error_collector_clear(libvroom_error_collector_t* collector);

/**
 * @brief Generate a human-readable summary of all collected parse errors.
 *
 * Creates a formatted string containing:
 * - Total error count with breakdown by severity (warnings, errors, fatal)
 * - Detailed listing of each error with location and message
 *
 * @param collector The error collector to summarize.
 * @return Newly allocated string containing the summary. The caller is responsible
 *         for freeing this string using free(). Returns NULL if collector is NULL
 *         or if memory allocation fails.
 *
 * @example
 * @code
 * libvroom_error_collector_t* errors = libvroom_error_collector_create(LIBVROOM_MODE_PERMISSIVE,
 * 100);
 * // ... parse with errors ...
 * char* summary = libvroom_error_collector_summary(errors);
 * if (summary) {
 *     printf("%s\n", summary);
 *     free(summary);
 * }
 * libvroom_error_collector_destroy(errors);
 * @endcode
 */
char* libvroom_error_collector_summary(const libvroom_error_collector_t* collector);

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
libvroom_error_t libvroom_parse(libvroom_parser_t* parser, const libvroom_buffer_t* buffer,
                                libvroom_index_t* index, libvroom_error_collector_t* errors,
                                const libvroom_dialect_t* dialect);
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
libvroom_error_t libvroom_parse_auto(libvroom_parser_t* parser, const libvroom_buffer_t* buffer,
                                     libvroom_index_t* index, libvroom_error_collector_t* errors,
                                     libvroom_detection_result_t** detected);

/* Utility Functions */
size_t libvroom_recommended_threads(void);
size_t libvroom_simd_padding(void);

/* Encoding Detection and Transcoding */

/**
 * @brief Character encodings supported by the parser.
 */
typedef enum libvroom_encoding {
  LIBVROOM_ENCODING_UTF8 = 0,     /**< UTF-8 (default) */
  LIBVROOM_ENCODING_UTF8_BOM = 1, /**< UTF-8 with BOM (EF BB BF) */
  LIBVROOM_ENCODING_UTF16_LE = 2, /**< UTF-16 Little Endian */
  LIBVROOM_ENCODING_UTF16_BE = 3, /**< UTF-16 Big Endian */
  LIBVROOM_ENCODING_UTF32_LE = 4, /**< UTF-32 Little Endian */
  LIBVROOM_ENCODING_UTF32_BE = 5, /**< UTF-32 Big Endian */
  LIBVROOM_ENCODING_LATIN1 = 6,   /**< Latin-1 (ISO-8859-1) */
  LIBVROOM_ENCODING_UNKNOWN = 7   /**< Unknown encoding */
} libvroom_encoding_t;

/**
 * @brief Result of encoding detection.
 */
typedef struct libvroom_encoding_result {
  libvroom_encoding_t encoding; /**< Detected encoding */
  size_t bom_length;            /**< Length of BOM in bytes (0 if no BOM) */
  double confidence;            /**< Detection confidence [0.0, 1.0] */
  bool needs_transcoding;       /**< True if transcoding to UTF-8 is needed */
} libvroom_encoding_result_t;

/**
 * @brief Opaque handle to a load result (buffer + encoding info).
 */
typedef struct libvroom_load_result libvroom_load_result_t;

/**
 * @brief Convert encoding enum to human-readable string.
 *
 * @param encoding The encoding to convert.
 * @return C-string name of the encoding (e.g., "UTF-16LE", "UTF-8").
 *         The returned string is statically allocated and should not be freed.
 */
const char* libvroom_encoding_string(libvroom_encoding_t encoding);

/**
 * @brief Detect the encoding of a byte buffer.
 *
 * Detection strategy:
 * 1. Check for BOM (Byte Order Mark) - most reliable
 * 2. If no BOM, use heuristics based on null byte patterns
 *
 * BOM patterns:
 * - UTF-8:    EF BB BF
 * - UTF-16 LE: FF FE (and not FF FE 00 00)
 * - UTF-16 BE: FE FF
 * - UTF-32 LE: FF FE 00 00
 * - UTF-32 BE: 00 00 FE FF
 *
 * @param data Pointer to the byte buffer.
 * @param length Length of the buffer in bytes.
 * @param result Pointer to store the detection result.
 * @return LIBVROOM_OK on success, error code on failure.
 */
libvroom_error_t libvroom_detect_encoding(const uint8_t* data, size_t length,
                                          libvroom_encoding_result_t* result);

/**
 * @brief Load a file with automatic encoding detection and transcoding.
 *
 * This function detects the encoding of a file (via BOM or heuristics),
 * and automatically transcodes UTF-16 and UTF-32 files to UTF-8. The
 * returned data is always UTF-8 (or ASCII-compatible) for parsing.
 *
 * @param filename Path to the file to load.
 * @return Handle to the load result, or NULL on failure.
 *         The caller must call libvroom_load_result_destroy() to free the result.
 */
libvroom_load_result_t* libvroom_load_file_with_encoding(const char* filename);

/**
 * @brief Get the buffer from a load result.
 *
 * @param result The load result handle.
 * @return Pointer to the loaded data (UTF-8 encoded), or NULL if invalid.
 *         The pointer is valid until libvroom_load_result_destroy() is called.
 */
const uint8_t* libvroom_load_result_data(const libvroom_load_result_t* result);

/**
 * @brief Get the length of the loaded data.
 *
 * @param result The load result handle.
 * @return Length of the data in bytes, or 0 if invalid.
 */
size_t libvroom_load_result_length(const libvroom_load_result_t* result);

/**
 * @brief Get the detected encoding from a load result.
 *
 * @param result The load result handle.
 * @return The detected encoding, or LIBVROOM_ENCODING_UNKNOWN if invalid.
 */
libvroom_encoding_t libvroom_load_result_encoding(const libvroom_load_result_t* result);

/**
 * @brief Get the BOM length from a load result.
 *
 * @param result The load result handle.
 * @return Length of the BOM in bytes (0 if no BOM), or 0 if invalid.
 */
size_t libvroom_load_result_bom_length(const libvroom_load_result_t* result);

/**
 * @brief Get the detection confidence from a load result.
 *
 * @param result The load result handle.
 * @return Detection confidence [0.0, 1.0], or 0.0 if invalid.
 */
double libvroom_load_result_confidence(const libvroom_load_result_t* result);

/**
 * @brief Check if the loaded data was modified from the original file.
 *
 * Returns true if any transformation was applied to the data, including:
 * - Transcoding from UTF-16 or UTF-32 to UTF-8
 * - Stripping a BOM (Byte Order Mark), including UTF-8 BOM
 *
 * @param result The load result handle.
 * @return true if the data was transformed, false if unchanged.
 */
bool libvroom_load_result_was_transcoded(const libvroom_load_result_t* result);

/**
 * @brief Create a buffer from the load result for parsing.
 *
 * Creates a new buffer suitable for use with libvroom_parse() and
 * related functions. The buffer includes the necessary SIMD padding.
 *
 * @param result The load result handle.
 * @return A new buffer handle, or NULL on failure.
 *         The caller must call libvroom_buffer_destroy() to free the buffer.
 *
 * @note This creates a copy of the data. For large files, consider using
 *       libvroom_load_result_data() directly if you need read-only access.
 */
libvroom_buffer_t* libvroom_load_result_to_buffer(const libvroom_load_result_t* result);

/**
 * @brief Destroy a load result and free its resources.
 *
 * @param result The load result handle to destroy.
 */
void libvroom_load_result_destroy(libvroom_load_result_t* result);

#ifdef __cplusplus
}
#endif

#endif /* LIBVROOM_C_H */
