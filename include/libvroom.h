/**
 * @file libvroom.h
 * @brief libvroom - High-performance CSV parser using portable SIMD instructions.
 * @version 0.1.0
 *
 * This is the main public header for the libvroom library. Include this single
 * header to access all public functionality.
 */

#ifndef LIBVROOM_H
#define LIBVROOM_H

#define LIBVROOM_VERSION_MAJOR 0
#define LIBVROOM_VERSION_MINOR 1
#define LIBVROOM_VERSION_PATCH 0
#define LIBVROOM_VERSION_STRING "0.1.0"

#include "error.h"
#include "dialect.h"
#include "two_pass.h"
#include "io_util.h"
#include "mem_util.h"

#include <optional>

namespace libvroom {

/**
 * @brief Algorithm selection for parsing.
 *
 * Allows choosing between different parsing implementations that offer
 * different performance characteristics.
 */
enum class ParseAlgorithm {
    /**
     * @brief Automatic algorithm selection (default).
     *
     * The parser chooses the best algorithm based on the data and options.
     * Currently uses the speculative multi-threaded algorithm.
     */
    AUTO,

    /**
     * @brief Speculative multi-threaded parsing.
     *
     * Uses speculative execution to find safe chunk boundaries for parallel
     * processing. Good general-purpose choice for large files.
     */
    SPECULATIVE,

    /**
     * @brief Two-pass algorithm with quote tracking.
     *
     * Traditional two-pass approach that tracks quote parity across chunks.
     * More predictable than speculative but may be slower for some files.
     */
    TWO_PASS,

    /**
     * @brief Branchless state machine implementation.
     *
     * Uses lookup tables to eliminate branch mispredictions in the parsing
     * hot path. Can provide significant speedups on data with many special
     * characters (quotes, delimiters) that cause branch mispredictions.
     *
     * Performance characteristics:
     * - Eliminates 90%+ of branches in parsing
     * - Single memory access per character for classification and transition
     * - Best for files with high quote/delimiter density
     */
    BRANCHLESS
};

/**
 * @brief Configuration options for parsing.
 *
 * ParseOptions provides a unified way to configure CSV parsing, combining
 * dialect selection, error handling mode, and algorithm selection into a single
 * structure. This enables a single parse() method to handle all use cases.
 *
 * ## Key Behaviors
 *
 * - **Dialect**: If dialect is nullopt (default), the dialect is auto-detected
 *   from the data. Set an explicit dialect (e.g., Dialect::csv()) to skip detection.
 * - **Error mode**: Controls how errors are handled during parsing:
 *   - STRICT (default): Stop on first error
 *   - PERMISSIVE: Collect all errors, stop only on fatal
 *   - BEST_EFFORT: Parse as much as possible, ignore errors
 * - **Algorithm**: Choose parsing algorithm for performance tuning. Default (AUTO)
 *   uses speculative multi-threaded parsing.
 * - **Detection options**: Only used when dialect is nullopt and auto-detection runs.
 *
 * ## Error Handling
 *
 * All errors are returned in the Result object, never thrown as exceptions.
 * The `error_mode` setting controls parser behavior when errors are encountered,
 * but errors are always collected and accessible via Result::errors().
 *
 * @example
 * @code
 * Parser parser;
 *
 * // Auto-detect dialect, STRICT mode (default - stops on first error)
 * auto result = parser.parse(buf, len);
 * if (!result.success()) {
 *     for (const auto& err : result.errors()) {
 *         std::cerr << err.to_string() << "\n";
 *     }
 * }
 *
 * // Collect all errors with PERMISSIVE mode
 * auto result = parser.parse(buf, len, {.error_mode = ErrorMode::PERMISSIVE});
 *
 * // Explicit CSV dialect with STRICT mode
 * auto result = parser.parse(buf, len, {.dialect = Dialect::csv()});
 *
 * // Explicit dialect with PERMISSIVE error collection
 * auto result = parser.parse(buf, len, {
 *     .dialect = Dialect::tsv(),
 *     .error_mode = ErrorMode::PERMISSIVE
 * });
 *
 * // Use branchless algorithm for maximum performance
 * auto result = parser.parse(buf, len, {
 *     .dialect = Dialect::csv(),
 *     .algorithm = ParseAlgorithm::BRANCHLESS
 * });
 * @endcode
 *
 * @see Parser::parse() for the unified parsing method
 * @see Dialect for dialect configuration options
 * @see ErrorMode for error handling mode options
 * @see ParseAlgorithm for algorithm selection
 */
struct ParseOptions {
    /**
     * @brief Dialect configuration for parsing.
     *
     * If nullopt (default), the dialect is auto-detected from the data using
     * the CleverCSV-inspired algorithm. Set to an explicit dialect to skip
     * detection and use the specified format.
     *
     * Common explicit dialects:
     * - Dialect::csv() - Standard comma-separated values
     * - Dialect::tsv() - Tab-separated values
     * - Dialect::semicolon() - Semicolon-separated (European style)
     * - Dialect::pipe() - Pipe-separated
     */
    std::optional<Dialect> dialect = std::nullopt;

    /**
     * @brief Error handling mode for parsing.
     *
     * Controls how the parser responds to errors:
     * - STRICT (default): Stop parsing on first error of any severity
     * - PERMISSIVE: Collect all errors, only stop on FATAL errors
     * - BEST_EFFORT: Parse as much as possible, ignoring all errors
     *
     * Regardless of mode, all encountered errors are stored in Result::errors().
     *
     * @note This replaces the previous `errors` pointer parameter. Errors are
     *       now always returned in the Result object.
     */
    ErrorMode error_mode = ErrorMode::STRICT;

    /**
     * @brief Options for dialect auto-detection.
     *
     * Only used when dialect is nullopt and auto-detection runs.
     * Allows customizing detection sample size, candidate delimiters, etc.
     */
    DetectionOptions detection_options = DetectionOptions();

    /**
     * @brief Algorithm to use for parsing.
     *
     * Allows selecting different parsing implementations for performance tuning.
     * Default is AUTO which currently uses the speculative multi-threaded algorithm.
     */
    ParseAlgorithm algorithm = ParseAlgorithm::AUTO;

    /**
     * @brief [DEPRECATED] External error collector pointer.
     *
     * @deprecated **Use `error_mode` instead and access errors via Result::errors().**
     *
     * This field exists for backward compatibility only. New code should:
     * 1. Set `error_mode` to the desired ErrorMode (STRICT, PERMISSIVE, BEST_EFFORT)
     * 2. Access errors via `Result::errors()`, `Result::has_errors()`, etc.
     *
     * Example of the new pattern:
     * ```cpp
     * auto result = parser.parse(buf, len, {.error_mode = ErrorMode::PERMISSIVE});
     * if (result.has_errors()) {
     *     for (const auto& err : result.errors()) { ... }
     * }
     * ```
     *
     * If this field is set, errors will be copied to the external collector
     * after parsing for backward compatibility.
     */
    ErrorCollector* errors = nullptr;

    /**
     * @brief Factory for default options (auto-detect dialect, STRICT mode).
     */
    static ParseOptions defaults() { return ParseOptions{}; }

    /**
     * @brief Factory for options with explicit dialect.
     */
    static ParseOptions with_dialect(const Dialect& d) {
        ParseOptions opts;
        opts.dialect = d;
        return opts;
    }

    /**
     * @brief Factory for options with specific error mode.
     */
    static ParseOptions with_error_mode(ErrorMode mode) {
        ParseOptions opts;
        opts.error_mode = mode;
        return opts;
    }

    /**
     * @brief Factory for options with both dialect and error mode.
     */
    static ParseOptions with_dialect_and_error_mode(const Dialect& d, ErrorMode mode) {
        ParseOptions opts;
        opts.dialect = d;
        opts.error_mode = mode;
        return opts;
    }

    /**
     * @brief Factory for options with specific algorithm.
     */
    static ParseOptions with_algorithm(ParseAlgorithm algo) {
        ParseOptions opts;
        opts.algorithm = algo;
        return opts;
    }

    /**
     * @brief Factory for branchless parsing (performance optimization).
     *
     * Convenience factory for using the branchless state machine algorithm
     * with an explicit dialect. This combination provides the best performance
     * for files with known format.
     */
    static ParseOptions branchless(const Dialect& d = Dialect::csv()) {
        ParseOptions opts;
        opts.dialect = d;
        opts.algorithm = ParseAlgorithm::BRANCHLESS;
        return opts;
    }

    // =========================================================================
    // Deprecated factory methods - for backward compatibility
    // =========================================================================

    /**
     * @brief [DEPRECATED] Factory for options with external error collection.
     * @deprecated Use with_error_mode(ErrorMode::PERMISSIVE) instead and access
     *             errors via Result::errors().
     */
    LIBVROOM_DEPRECATED("Use with_error_mode() instead and access errors via Result::errors()")
    static ParseOptions with_errors(ErrorCollector& e) {
        ParseOptions opts;
        opts.errors = &e;
        opts.error_mode = e.mode();
        return opts;
    }

    /**
     * @brief [DEPRECATED] Factory for options with dialect and external error collection.
     * @deprecated Use with_dialect_and_error_mode() instead and access errors via Result::errors().
     */
    LIBVROOM_DEPRECATED("Use with_dialect_and_error_mode() instead and access errors via Result::errors()")
    static ParseOptions with_dialect_and_errors(const Dialect& d, ErrorCollector& e) {
        ParseOptions opts;
        opts.dialect = d;
        opts.errors = &e;
        opts.error_mode = e.mode();
        return opts;
    }
};

/**
 * @brief RAII wrapper for SIMD-aligned file buffers.
 *
 * FileBuffer provides automatic memory management for buffers loaded with
 * load_file() or allocated with allocate_padded_buffer(). It ensures proper
 * cleanup using aligned_free() and supports move semantics for efficient
 * transfer of ownership.
 *
 * The buffer is cache-line aligned (64 bytes) with additional padding for
 * safe SIMD overreads. This allows SIMD operations to read beyond the actual
 * data length without bounds checking.
 *
 * @note FileBuffer is move-only. Copy operations are deleted to prevent
 *       accidental double-free or shallow copy issues.
 *
 * @example
 * @code
 * // Load a file using the convenience function
 * libvroom::FileBuffer buffer = libvroom::load_file("data.csv");
 *
 * if (buffer) {  // Check if valid using operator bool
 *     std::cout << "Loaded " << buffer.size() << " bytes\n";
 *
 *     // Access data
 *     const uint8_t* data = buffer.data();
 *
 *     // Parse with libvroom
 *     libvroom::Parser parser;
 *     auto result = parser.parse(data, buffer.size());
 * }
 * // Memory automatically freed when buffer goes out of scope
 * @endcode
 *
 * @see load_file() To create a FileBuffer from a file path.
 * @see allocate_padded_buffer() For manual buffer allocation.
 */
class FileBuffer {
public:
    /// Default constructor. Creates an empty, invalid buffer.
    FileBuffer() : data_(nullptr), size_(0) {}
    /**
     * @brief Construct a FileBuffer from raw data.
     * @param data Pointer to SIMD-aligned buffer (takes ownership).
     * @param size Size of the data in bytes.
     * @warning The data pointer must have been allocated with aligned_malloc()
     *          or allocate_padded_buffer(). The FileBuffer takes ownership.
     */
    FileBuffer(uint8_t* data, size_t size) : data_(data), size_(size) {}

    /**
     * @brief Move constructor.
     * @param other The FileBuffer to move from.
     */
    FileBuffer(FileBuffer&& other) noexcept : data_(other.data_), size_(other.size_) {
        other.data_ = nullptr;
        other.size_ = 0;
    }
    /**
     * @brief Move assignment operator.
     * @param other The FileBuffer to move from.
     * @return Reference to this buffer.
     */
    FileBuffer& operator=(FileBuffer&& other) noexcept {
        if (this != &other) {
            free();
            data_ = other.data_;
            size_ = other.size_;
            other.data_ = nullptr;
            other.size_ = 0;
        }
        return *this;
    }

    // Copy operations deleted to prevent double-free
    FileBuffer(const FileBuffer&) = delete;
    FileBuffer& operator=(const FileBuffer&) = delete;

    /// Destructor. Frees the buffer using aligned_free().
    ~FileBuffer() { free(); }

    /// @return Const pointer to the buffer data.
    const uint8_t* data() const { return data_; }

    /// @return Mutable pointer to the buffer data.
    uint8_t* data() { return data_; }

    /// @return Size of the data in bytes (not including padding).
    size_t size() const { return size_; }

    /// @return true if the buffer contains valid data.
    bool valid() const { return data_ != nullptr; }

    /// @return true if the buffer is empty (size == 0).
    bool empty() const { return size_ == 0; }

    /// @return true if the buffer is valid, enabling `if (buffer)` syntax.
    explicit operator bool() const { return valid(); }

    /**
     * Release ownership of the buffer and return the raw pointer.
     * After calling this method, the FileBuffer no longer owns the memory
     * and the caller is responsible for freeing it using aligned_free().
     * @return The raw pointer to the buffer data, or nullptr if the buffer was empty/invalid.
     */
    uint8_t* release() {
        uint8_t* ptr = data_;
        data_ = nullptr;
        size_ = 0;
        return ptr;
    }

private:
    void free() {
        if (data_) {
            aligned_free(data_);
            data_ = nullptr;
            size_ = 0;
        }
    }
    uint8_t* data_;
    size_t size_;
};

/**
 * @brief Loads a file into a FileBuffer with SIMD-aligned memory.
 *
 * This function loads the entire file contents into a newly allocated buffer
 * that is cache-line aligned with padding for safe SIMD operations. The
 * FileBuffer takes ownership of the allocated memory and will free it when
 * destroyed.
 *
 * @param filename Path to the file to load.
 * @param padding Extra bytes to allocate for SIMD overreads (default: 64).
 * @return FileBuffer containing the file data. Check valid() for success.
 * @throws std::runtime_error if file cannot be opened or read.
 *
 * @note Memory ownership is transferred to FileBuffer - do not manually free.
 */
inline FileBuffer load_file(const std::string& filename, size_t padding = 64) {
    auto corpus = get_corpus(filename, padding);
    // Note: get_corpus() allocates memory via allocate_padded_buffer() which
    // must be freed with aligned_free(). FileBuffer takes ownership and will
    // call aligned_free() in its destructor.
    return FileBuffer(const_cast<uint8_t*>(corpus.data()), corpus.size());
}

/**
 * @brief Free a buffer returned by get_corpus().
 *
 * This is a convenience wrapper around aligned_free() for buffers returned
 * by the legacy get_corpus() function. For new code, prefer using FileBuffer
 * with load_file() which provides automatic memory management.
 *
 * @param corpus Reference to the string_view to free. After calling,
 *               the string_view's data pointer will be invalidated.
 *
 * @deprecated Prefer using FileBuffer with load_file() for automatic cleanup.
 *
 * @see load_file() For automatic memory management.
 * @see FileBuffer For RAII buffer management.
 */
inline void free_buffer(std::basic_string_view<uint8_t>& corpus) {
    if (corpus.data()) {
        aligned_free(const_cast<uint8_t*>(corpus.data()));
    }
}

/**
 * @brief High-level CSV parser with automatic index management.
 *
 * Parser provides a simplified interface over the lower-level two_pass class.
 * It manages index allocation internally and returns a Result object containing
 * the parsed index, dialect information, success status, and any errors encountered.
 *
 * ## Unified Error Handling
 *
 * Parser uses a **unified Result-based error handling pattern**:
 *
 * - **All errors in Result**: Access via `Result::errors()`, `Result::has_errors()`, etc.
 * - **No exceptions for parse errors**: Malformed CSV never throws; errors are in Result
 * - **Three error modes**: STRICT (default), PERMISSIVE, BEST_EFFORT
 * - **Automatic multi-threading**: Thread-local errors are merged automatically
 *
 * Exceptions are reserved for truly exceptional conditions such as:
 * - Memory allocation failures (std::bad_alloc)
 * - Internal programming errors (std::logic_error)
 *
 * ## Usage Pattern
 *
 * ```cpp
 * Parser parser;
 *
 * // STRICT mode (default): stops on first error
 * auto result = parser.parse(buf, len);
 *
 * // PERMISSIVE mode: collects all errors
 * auto result = parser.parse(buf, len, {.error_mode = ErrorMode::PERMISSIVE});
 *
 * // Check for errors - always via Result
 * if (!result.success() || result.has_errors()) {
 *     for (const auto& err : result.errors()) {
 *         std::cerr << err.to_string() << "\n";
 *     }
 * }
 * ```
 *
 * The Parser supports:
 * - Single-threaded and multi-threaded parsing
 * - Explicit dialect specification or auto-detection
 * - Three error modes: STRICT, PERMISSIVE, and BEST_EFFORT
 *
 * @note For maximum performance with manual control, use two_pass directly.
 *       Parser is designed for convenience and typical use cases.
 *
 * @example Basic parsing with STRICT mode (default)
 * @code
 * #include "libvroom.h"
 *
 * // Load CSV file
 * libvroom::FileBuffer buffer = libvroom::load_file("data.csv");
 *
 * // Create parser with 4 threads
 * libvroom::Parser parser(4);
 *
 * // Parse with auto-detect dialect, STRICT mode (default)
 * // Errors are NEVER thrown - always returned in Result
 * auto result = parser.parse(buffer.data(), buffer.size());
 *
 * if (result.success()) {
 *     std::cout << "Parsed " << result.num_columns() << " columns\n";
 *     std::cout << "Total indexes: " << result.total_indexes() << "\n";
 * } else {
 *     // Access errors via Result::errors()
 *     for (const auto& err : result.errors()) {
 *         std::cerr << err.to_string() << "\n";
 *     }
 * }
 * @endcode
 *
 * @example Collecting all errors with PERMISSIVE mode
 * @code
 * libvroom::Parser parser(4);
 *
 * // PERMISSIVE mode: collect all errors in Result
 * auto result = parser.parse(buffer.data(), buffer.size(),
 *                            {.error_mode = libvroom::ErrorMode::PERMISSIVE});
 *
 * std::cout << "Detected dialect: " << result.dialect.to_string() << "\n";
 *
 * // Access errors via Result - unified interface
 * if (result.has_errors()) {
 *     std::cout << "Found " << result.error_count() << " issues:\n";
 *     std::cout << result.error_summary() << "\n";
 *     for (const auto& err : result.errors()) {
 *         std::cerr << err.to_string() << "\n";
 *     }
 * }
 * @endcode
 *
 * @see two_pass For lower-level parsing with full control.
 * @see FileBuffer For loading CSV files.
 * @see Dialect For dialect configuration options.
 * @see ErrorMode For error handling mode options.
 */
class Parser {
public:
    /**
     * @brief Result of a parsing operation.
     *
     * Contains the parsed index, dialect used (or detected), success status,
     * and any errors encountered during parsing. This structure provides a
     * unified interface for error handling - all error information is contained
     * within the Result object rather than thrown as exceptions or passed via
     * separate out-parameters.
     *
     * ## Error Handling
     *
     * The Result object owns an ErrorCollector that accumulates all errors
     * found during parsing. Check for errors using:
     * - `success()` - returns false if fatal errors occurred
     * - `has_errors()` - returns true if any errors (including warnings) were found
     * - `errors()` - access the list of ParseError objects
     * - `error_count()` - get the number of errors found
     * - `error_summary()` - get a human-readable summary
     *
     * This structure is move-only since the underlying index contains raw pointers.
     */
    struct Result {
        index idx;               ///< The parsed field index.
        bool successful{false};  ///< Whether parsing completed without fatal errors.
        Dialect dialect;         ///< The dialect used for parsing.
        DetectionResult detection;  ///< Detection result (populated when auto-detecting).

        Result() : error_collector_(ErrorMode::STRICT) {}
        Result(Result&&) = default;
        Result& operator=(Result&&) = default;

        // Prevent copying - index contains raw pointers
        Result(const Result&) = delete;
        Result& operator=(const Result&) = delete;

        /// @return true if parsing was successful (no fatal errors).
        bool success() const { return successful; }

        /// @return Number of columns detected in the CSV.
        size_t num_columns() const { return idx.columns; }

        /**
         * @brief Get total number of field separator positions found.
         * @return Sum of indexes across all parsing threads.
         */
        size_t total_indexes() const {
            if (!idx.n_indexes) return 0;
            size_t total = 0;
            for (uint8_t t = 0; t < idx.n_threads; ++t) {
                total += idx.n_indexes[t];
            }
            return total;
        }

        // =====================================================================
        // Error Information Access
        // =====================================================================

        /**
         * @brief Check if any errors were recorded during parsing.
         * @return true if at least one error (including warnings) was recorded.
         */
        bool has_errors() const { return error_collector_.has_errors(); }

        /**
         * @brief Check if any fatal errors were recorded.
         * @return true if at least one FATAL error was recorded.
         */
        bool has_fatal_errors() const { return error_collector_.has_fatal_errors(); }

        /**
         * @brief Get the number of errors recorded during parsing.
         * @return Number of errors in the collection.
         */
        size_t error_count() const { return error_collector_.error_count(); }

        /**
         * @brief Get read-only access to all recorded errors.
         * @return Const reference to the vector of ParseError objects.
         */
        const std::vector<ParseError>& errors() const { return error_collector_.errors(); }

        /**
         * @brief Get a summary string of all errors.
         * @return Human-readable summary of error counts by type.
         */
        std::string error_summary() const { return error_collector_.summary(); }

        /**
         * @brief Get the error mode used during parsing.
         * @return The ErrorMode that was used.
         */
        ErrorMode error_mode() const { return error_collector_.mode(); }

    private:
        friend class Parser;
        ErrorCollector error_collector_;  ///< Internal error collector.
    };

    /**
     * @brief Construct a Parser with the specified number of threads.
     * @param num_threads Number of threads to use for parsing (default: 1).
     *                    Use std::thread::hardware_concurrency() for CPU count.
     */
    explicit Parser(size_t num_threads = 1)
        : num_threads_(num_threads > 0 ? num_threads : 1) {}

    /**
     * @brief Unified parse method with configurable options.
     *
     * This is the primary parsing method that handles all use cases through
     * the ParseOptions structure. It provides a unified, Result-based error
     * handling interface - parse errors are NEVER thrown as exceptions.
     *
     * ## Error Handling
     *
     * All parse errors are returned in Result::errors(), never thrown.
     * The `options.error_mode` controls parser behavior:
     * - **STRICT** (default): Stop on first error
     * - **PERMISSIVE**: Collect all errors, stop only on fatal
     * - **BEST_EFFORT**: Parse as much as possible, ignore errors
     *
     * Check Result::success() to determine if parsing succeeded.
     * Check Result::has_errors() to see if any issues were found.
     *
     * ## Dialect Detection
     *
     * - **dialect = nullopt** (default): Auto-detect dialect from data
     * - **dialect = Dialect::xxx()**: Use the specified dialect
     *
     * @param buf Pointer to the CSV data buffer. Must remain valid during parsing.
     *            Should have at least 64 bytes of padding beyond len for SIMD safety.
     * @param len Length of the CSV data in bytes (excluding any padding).
     * @param options Configuration options for parsing (default: auto-detect, STRICT mode).
     *
     * @return Result containing:
     *         - Parsed index (Result::idx)
     *         - Dialect used (Result::dialect)
     *         - Detection info (Result::detection)
     *         - Success status (Result::success())
     *         - Any errors found (Result::errors())
     *
     * @note This method does NOT throw exceptions for parse errors. Only truly
     *       exceptional conditions (memory allocation failure, internal errors)
     *       may throw.
     *
     * @example
     * @code
     * Parser parser;
     *
     * // Auto-detect dialect, STRICT mode (stops on first error)
     * auto result = parser.parse(buf, len);
     * if (!result.success()) {
     *     std::cerr << "Parse failed: " << result.error_summary() << "\n";
     *     for (const auto& err : result.errors()) {
     *         std::cerr << "  " << err.to_string() << "\n";
     *     }
     *     return;
     * }
     *
     * // Collect ALL errors with PERMISSIVE mode
     * auto result = parser.parse(buf, len, {.error_mode = ErrorMode::PERMISSIVE});
     * std::cout << "Found " << result.error_count() << " issues\n";
     *
     * // Explicit CSV dialect
     * auto result = parser.parse(buf, len, {.dialect = Dialect::csv()});
     *
     * // Explicit TSV dialect with PERMISSIVE error collection
     * auto result = parser.parse(buf, len, {
     *     .dialect = Dialect::tsv(),
     *     .error_mode = ErrorMode::PERMISSIVE
     * });
     * @endcode
     *
     * @see ParseOptions for configuration details
     * @see Result for accessing errors and parsed data
     */
    Result parse(const uint8_t* buf, size_t len,
                 const ParseOptions& options = ParseOptions{}) {
        Result result;
        result.idx = parser_.init(len, num_threads_);

        // Set up the error collector with the requested mode
        result.error_collector_.set_mode(options.error_mode);

        // Determine dialect (explicit or auto-detect)
        if (options.dialect.has_value()) {
            result.dialect = options.dialect.value();
        } else {
            // Auto-detect dialect
            DialectDetector detector(options.detection_options);
            result.detection = detector.detect(buf, len);
            result.dialect = result.detection.success()
                ? result.detection.dialect : Dialect::csv();
        }

        // Suppress deprecation warnings for internal calls to two_pass methods
        // (Parser is the public API that wraps these deprecated methods)
        LIBVROOM_SUPPRESS_DEPRECATION_START

        // Always use error-collecting parsing paths for unified error handling.
        // The error_mode in the collector controls whether parsing stops early.
        if (!options.dialect.has_value()) {
            // Auto-detect path with errors
            result.successful = parser_.parse_auto(
                buf, result.idx, len, result.error_collector_, &result.detection,
                options.detection_options);
            result.dialect = result.detection.dialect;
        } else {
            // Explicit dialect - respect algorithm selection
            if (options.algorithm == ParseAlgorithm::BRANCHLESS) {
                // Use branchless implementation with error collection
                result.successful = parser_.parse_branchless_with_errors(
                    buf, result.idx, len, result.error_collector_, result.dialect);
            } else {
                // Default: use two-pass with error collection for all other algorithms
                // This ensures consistent error reporting across all code paths
                result.successful = parser_.parse_two_pass_with_errors(
                    buf, result.idx, len, result.error_collector_, result.dialect);
            }
        }

        LIBVROOM_SUPPRESS_DEPRECATION_END

        // For backward compatibility: copy errors to external collector if provided
        if (options.errors != nullptr) {
            options.errors->merge_from(result.error_collector_);
        }

        return result;
    }

    // =========================================================================
    // Legacy methods - Provided for backward compatibility.
    // These delegate to the unified parse() method above.
    // New code should use the unified parse() method with ParseOptions.
    // =========================================================================

    /**
     * @brief Parse with explicit dialect (legacy method).
     *
     * @deprecated Use parse(buf, len, {.dialect = dialect}) instead and access
     *             errors via Result::errors().
     *
     * This method is provided for backward compatibility. New code should use
     * the unified parse() method with ParseOptions.
     */
    LIBVROOM_DEPRECATED("Use parse(buf, len, {.dialect = dialect}) instead")
    Result parse(const uint8_t* buf, size_t len, const Dialect& dialect) {
        return parse(buf, len, ParseOptions::with_dialect(dialect));
    }

    /**
     * @brief Parse with error collection (legacy method).
     *
     * @deprecated Use parse(buf, len, {.dialect = dialect, .error_mode = mode})
     *             instead and access errors via Result::errors().
     *
     * This method is provided for backward compatibility. New code should use
     * the unified parse() method with ParseOptions.
     */
    LIBVROOM_DEPRECATED("Use parse(buf, len, {.dialect = ..., .error_mode = ...}) and Result::errors() instead")
    Result parse_with_errors(const uint8_t* buf, size_t len,
                             ErrorCollector& errors,
                             const Dialect& dialect = Dialect::csv()) {
        LIBVROOM_SUPPRESS_DEPRECATION_START
        return parse(buf, len, ParseOptions::with_dialect_and_errors(dialect, errors));
        LIBVROOM_SUPPRESS_DEPRECATION_END
    }

    /**
     * @brief Parse with auto-detection and error collection (legacy method).
     *
     * @deprecated Use parse(buf, len, {.error_mode = mode}) instead and access
     *             errors via Result::errors().
     *
     * This method is provided for backward compatibility. New code should use
     * the unified parse() method with ParseOptions.
     */
    LIBVROOM_DEPRECATED("Use parse(buf, len, {.error_mode = ...}) and Result::errors() instead")
    Result parse_auto(const uint8_t* buf, size_t len, ErrorCollector& errors) {
        LIBVROOM_SUPPRESS_DEPRECATION_START
        return parse(buf, len, ParseOptions::with_errors(errors));
        LIBVROOM_SUPPRESS_DEPRECATION_END
    }

    /**
     * @brief Set the number of threads for parsing.
     * @param num_threads Number of threads (minimum 1).
     */
    void set_num_threads(size_t num_threads) {
        num_threads_ = num_threads > 0 ? num_threads : 1;
    }

    /// @return Current number of threads configured for parsing.
    size_t num_threads() const { return num_threads_; }

private:
    two_pass parser_;
    size_t num_threads_;
};

/**
 * @brief Detect CSV dialect from a memory buffer.
 *
 * Convenience function that creates a DialectDetector and detects the
 * dialect from the provided data.
 *
 * @param buf Pointer to CSV data.
 * @param len Length of the data in bytes.
 * @param options Detection options (sample size, candidates, etc.).
 * @return DetectionResult with detected dialect and confidence.
 *
 * @example
 * @code
 * auto result = libvroom::detect_dialect(buffer.data(), buffer.size());
 * if (result.success()) {
 *     std::cout << "Delimiter: '" << result.dialect.delimiter << "'\n";
 *     std::cout << "Confidence: " << result.confidence << "\n";
 * }
 * @endcode
 *
 * @see DialectDetector For more control over detection.
 * @see detect_dialect_file() For detecting from a file path.
 */
inline DetectionResult detect_dialect(const uint8_t* buf, size_t len,
                                       const DetectionOptions& options = DetectionOptions()) {
    DialectDetector detector(options);
    return detector.detect(buf, len);
}

/**
 * @brief Detect CSV dialect from a file.
 *
 * Convenience function that loads a file and detects its dialect.
 * Only samples the beginning of the file for efficiency.
 *
 * @param filename Path to the CSV file.
 * @param options Detection options (sample size, candidates, etc.).
 * @return DetectionResult with detected dialect and confidence.
 *
 * @example
 * @code
 * auto result = libvroom::detect_dialect_file("data.csv");
 * if (result.success()) {
 *     std::cout << "Detected " << result.detected_columns << " columns\n";
 *     std::cout << "Format: " << result.dialect.to_string() << "\n";
 * }
 * @endcode
 *
 * @see DialectDetector For more control over detection.
 * @see detect_dialect() For detecting from an in-memory buffer.
 */
inline DetectionResult detect_dialect_file(const std::string& filename,
                                            const DetectionOptions& options = DetectionOptions()) {
    DialectDetector detector(options);
    return detector.detect_file(filename);
}

} // namespace libvroom

#endif // LIBVROOM_H
