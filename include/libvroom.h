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
 * dialect selection, error handling, and algorithm selection into a single
 * structure. This enables a single parse() method to handle all use cases.
 *
 * Key behaviors:
 * - **Dialect**: If dialect is nullopt (default), the dialect is auto-detected
 *   from the data. Set an explicit dialect (e.g., Dialect::csv()) to skip detection.
 * - **Error collection**: If errors is nullptr (default), parsing uses the fast
 *   path and throws on errors. Provide an ErrorCollector for error-tolerant parsing.
 * - **Algorithm**: Choose parsing algorithm for performance tuning. Default (AUTO)
 *   uses speculative multi-threaded parsing.
 * - **Detection options**: Only used when dialect is nullopt and auto-detection runs.
 *
 * @example
 * @code
 * Parser parser;
 *
 * // Auto-detect dialect, throw on errors (fast path)
 * auto result = parser.parse(buf, len);
 *
 * // Auto-detect dialect, collect errors
 * ErrorCollector errors(ErrorMode::PERMISSIVE);
 * auto result = parser.parse(buf, len, {.errors = &errors});
 *
 * // Explicit CSV dialect, throw on errors
 * auto result = parser.parse(buf, len, {.dialect = Dialect::csv()});
 *
 * // Explicit dialect with error collection
 * auto result = parser.parse(buf, len, {
 *     .dialect = Dialect::tsv(),
 *     .errors = &errors
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
 * @see ErrorCollector for error handling configuration
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
     * @brief Error collector for error-tolerant parsing.
     *
     * If nullptr (default), parsing uses the fast path that throws on errors.
     * Provide an ErrorCollector pointer to enable error collection mode, where
     * errors are accumulated and parsing continues based on the collector's mode.
     *
     * @note The ErrorCollector must remain valid for the duration of parsing.
     */
    ErrorCollector* errors = nullptr;

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
     *
     * Note: When errors is non-null, some algorithms may fall back to simpler
     * implementations to ensure accurate error position tracking.
     */
    ParseAlgorithm algorithm = ParseAlgorithm::AUTO;

    /**
     * @brief Factory for default options (auto-detect dialect, fast path).
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
     * @brief Factory for options with error collection.
     */
    static ParseOptions with_errors(ErrorCollector& e) {
        ParseOptions opts;
        opts.errors = &e;
        return opts;
    }

    /**
     * @brief Factory for options with both dialect and error collection.
     */
    static ParseOptions with_dialect_and_errors(const Dialect& d, ErrorCollector& e) {
        ParseOptions opts;
        opts.dialect = d;
        opts.errors = &e;
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
 * @brief Result of loading a file with RAII memory management.
 *
 * Combines an AlignedPtr (owning the buffer) with size information.
 * This provides RAII semantics while also tracking the data size
 * (since padding is allocated but not counted in the logical size).
 *
 * @example
 * @code
 * auto [buffer, size] = libvroom::load_file_to_ptr("data.csv");
 * if (buffer) {
 *     parser.parse(buffer.get(), size);
 * }
 * // Memory automatically freed when buffer goes out of scope
 * @endcode
 */
struct AlignedBuffer {
    AlignedPtr ptr;   ///< Smart pointer owning the buffer
    size_t size{0};   ///< Size of the data (not including padding)

    /// Default constructor creates an empty, invalid buffer.
    AlignedBuffer() = default;

    /// Construct from pointer and size.
    AlignedBuffer(AlignedPtr p, size_t s) : ptr(std::move(p)), size(s) {}

    /// Move constructor.
    AlignedBuffer(AlignedBuffer&&) = default;

    /// Move assignment.
    AlignedBuffer& operator=(AlignedBuffer&&) = default;

    // Non-copyable
    AlignedBuffer(const AlignedBuffer&) = delete;
    AlignedBuffer& operator=(const AlignedBuffer&) = delete;

    /// @return true if the buffer is valid.
    explicit operator bool() const { return ptr != nullptr; }

    /// @return Pointer to the buffer data.
    uint8_t* data() { return ptr.get(); }

    /// @return Const pointer to the buffer data.
    const uint8_t* data() const { return ptr.get(); }

    /// @return true if the buffer is empty.
    bool empty() const { return size == 0; }

    /// @return true if the buffer is valid.
    bool valid() const { return ptr != nullptr; }

    /// Release ownership and return the raw pointer.
    uint8_t* release() {
        size = 0;
        return ptr.release();
    }
};

/**
 * @brief Loads a file into an AlignedBuffer with RAII memory management.
 *
 * This function provides an alternative to load_file() that returns an
 * AlignedBuffer (using AlignedPtr internally) instead of FileBuffer.
 * Both approaches provide automatic memory management; AlignedBuffer
 * exposes the underlying smart pointer type for compatibility with
 * code that works with AlignedPtr directly.
 *
 * @param filename Path to the file to load.
 * @param padding Extra bytes to allocate for SIMD overreads (default: 64).
 * @return AlignedBuffer containing the file data. Check with if(buffer) or valid().
 * @throws std::runtime_error if file cannot be opened or read.
 *
 * @example
 * @code
 * auto buffer = libvroom::load_file_to_ptr("data.csv");
 * if (buffer) {
 *     libvroom::Parser parser;
 *     auto result = parser.parse(buffer.data(), buffer.size);
 * }
 * // Memory automatically freed when buffer goes out of scope
 * @endcode
 *
 * @see load_file() For FileBuffer-based loading.
 * @see load_stdin_to_ptr() For reading from stdin.
 */
inline AlignedBuffer load_file_to_ptr(const std::string& filename, size_t padding = 64) {
    auto corpus = get_corpus(filename, padding);
    AlignedPtr ptr(const_cast<uint8_t*>(corpus.data()));
    return AlignedBuffer(std::move(ptr), corpus.size());
}

/**
 * @brief Loads stdin into an AlignedBuffer with RAII memory management.
 *
 * Reads all data from standard input into an RAII-managed buffer.
 * Useful for piping data into CSV processing tools.
 *
 * @param padding Extra bytes to allocate for SIMD overreads (default: 64).
 * @return AlignedBuffer containing the stdin data. Check with if(buffer) or valid().
 * @throws std::runtime_error if reading fails or allocation fails.
 *
 * @example
 * @code
 * // cat data.csv | ./my_program
 * auto buffer = libvroom::load_stdin_to_ptr();
 * if (buffer) {
 *     libvroom::Parser parser;
 *     auto result = parser.parse(buffer.data(), buffer.size);
 * }
 * // Memory automatically freed when buffer goes out of scope
 * @endcode
 *
 * @see load_file_to_ptr() For loading from files.
 * @see get_corpus_stdin() For manual memory management.
 */
inline AlignedBuffer load_stdin_to_ptr(size_t padding = 64) {
    auto corpus = get_corpus_stdin(padding);
    AlignedPtr ptr(const_cast<uint8_t*>(corpus.data()));
    return AlignedBuffer(std::move(ptr), corpus.size());
}

/**
 * @brief Loads a file using the legacy get_corpus() API, wrapped with RAII.
 *
 * This function wraps the raw pointer from get_corpus() in an AlignedPtr
 * for automatic memory management. It's useful when you need to work with
 * existing code that uses get_corpus() but want RAII semantics.
 *
 * @param filename Path to the file to load.
 * @param padding Extra bytes to allocate for SIMD overreads.
 * @return A pair of (AlignedPtr, size). The AlignedPtr is empty on failure.
 * @throws std::runtime_error if file cannot be opened or read.
 *
 * @deprecated Prefer load_file_to_ptr() for new code.
 *
 * @example
 * @code
 * auto [ptr, size] = libvroom::wrap_corpus(get_corpus("data.csv", 64));
 * // Memory automatically freed when ptr goes out of scope
 * @endcode
 */
inline std::pair<AlignedPtr, size_t> wrap_corpus(std::basic_string_view<uint8_t> corpus) {
    return {AlignedPtr(const_cast<uint8_t*>(corpus.data())), corpus.size()};
}

/**
 * @brief High-level CSV parser with automatic index management.
 *
 * Parser provides a simplified interface over the lower-level two_pass class.
 * It manages index allocation internally and returns a Result object containing
 * the parsed index, dialect information, and success status.
 *
 * The Parser supports:
 * - Single-threaded and multi-threaded parsing
 * - Explicit dialect specification or auto-detection
 * - Error collection in permissive mode
 *
 * @note For maximum performance with manual control, use two_pass directly.
 *       Parser is designed for convenience and typical use cases.
 *
 * @example
 * @code
 * #include "libvroom.h"
 *
 * // Load CSV file
 * libvroom::FileBuffer buffer = libvroom::load_file("data.csv");
 *
 * // Create parser with 4 threads
 * libvroom::Parser parser(4);
 *
 * // Parse with default CSV dialect
 * auto result = parser.parse(buffer.data(), buffer.size());
 *
 * if (result.success()) {
 *     std::cout << "Parsed " << result.num_columns() << " columns\n";
 *     std::cout << "Total indexes: " << result.total_indexes() << "\n";
 * }
 * @endcode
 *
 * @example
 * @code
 * // Parse with auto-detection and error collection
 * libvroom::Parser parser(4);
 * libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);
 *
 * auto result = parser.parse_auto(buffer.data(), buffer.size(), errors);
 *
 * std::cout << "Detected dialect: " << result.dialect.to_string() << "\n";
 *
 * if (errors.has_errors()) {
 *     for (const auto& err : errors.errors()) {
 *         std::cerr << err.to_string() << "\n";
 *     }
 * }
 * @endcode
 *
 * @see two_pass For lower-level parsing with full control.
 * @see FileBuffer For loading CSV files.
 * @see Dialect For dialect configuration options.
 */
class Parser {
public:
    /**
     * @brief Result of a parsing operation.
     *
     * Contains the parsed index, dialect used (or detected), and success status.
     * This structure is move-only since the underlying index contains raw pointers.
     */
    struct Result {
        index idx;               ///< The parsed field index.
        bool successful{false};  ///< Whether parsing completed without fatal errors.
        Dialect dialect;         ///< The dialect used for parsing.
        DetectionResult detection;  ///< Detection result (populated by parse_auto).

        Result() = default;
        Result(Result&&) = default;
        Result& operator=(Result&&) = default;

        // Prevent copying - index contains raw pointers
        Result(const Result&) = delete;
        Result& operator=(const Result&) = delete;

        /// @return true if parsing was successful.
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
     * the ParseOptions structure. It unifies the previous parse(), parse_with_errors(),
     * and parse_auto() methods into a single entry point.
     *
     * Behavior based on options:
     * - **dialect = nullopt** (default): Auto-detect dialect from data
     * - **dialect = Dialect::xxx()**: Use the specified dialect
     * - **errors = nullptr** (default): Fast path, throws on errors
     * - **errors = &collector**: Collect errors, continue parsing based on mode
     *
     * @param buf Pointer to the CSV data buffer. Must remain valid during parsing.
     *            Should have at least 64 bytes of padding beyond len for SIMD safety.
     * @param len Length of the CSV data in bytes (excluding any padding).
     * @param options Configuration options for parsing (default: auto-detect, fast path).
     *
     * @return Result containing the parsed index, dialect used, and detection info.
     *
     * @throws std::runtime_error On parsing errors when options.errors is nullptr.
     *
     * @example
     * @code
     * Parser parser;
     *
     * // Auto-detect dialect, throw on errors (simplest usage)
     * auto result = parser.parse(buf, len);
     *
     * // Auto-detect with error collection
     * ErrorCollector errors(ErrorMode::PERMISSIVE);
     * auto result = parser.parse(buf, len, {.errors = &errors});
     *
     * // Explicit CSV dialect
     * auto result = parser.parse(buf, len, {.dialect = Dialect::csv()});
     *
     * // Explicit TSV dialect with error collection
     * auto result = parser.parse(buf, len, ParseOptions::with_dialect_and_errors(
     *     Dialect::tsv(), errors));
     * @endcode
     *
     * @see ParseOptions for configuration details
     */
    Result parse(const uint8_t* buf, size_t len,
                 const ParseOptions& options = ParseOptions{}) {
        Result result;
        result.idx = parser_.init(len, num_threads_);

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

        // Select parsing implementation based on algorithm and error collection
        if (options.errors != nullptr) {
            // Error collection mode - algorithm selection determines implementation
            if (!options.dialect.has_value()) {
                // Auto-detect path with errors
                result.successful = parser_.parse_auto(
                    buf, result.idx, len, *options.errors, &result.detection,
                    options.detection_options);
                result.dialect = result.detection.dialect;
            } else {
                // Explicit dialect with errors - respect algorithm selection
                if (options.algorithm == ParseAlgorithm::BRANCHLESS) {
                    // Use branchless implementation with error collection
                    result.successful = parser_.parse_branchless_with_errors(
                        buf, result.idx, len, *options.errors, result.dialect);
                } else {
                    // Default to switch-based implementation (faster for error collection)
                    result.successful = parser_.parse_with_errors(
                        buf, result.idx, len, *options.errors, result.dialect);
                }
            }
        } else {
            // Fast path (throws on error) - respects algorithm selection
            switch (options.algorithm) {
                case ParseAlgorithm::BRANCHLESS:
                    result.successful = parser_.parse_branchless(
                        buf, result.idx, len, result.dialect);
                    break;
                case ParseAlgorithm::TWO_PASS:
                    result.successful = parser_.parse_two_pass(
                        buf, result.idx, len, result.dialect);
                    break;
                case ParseAlgorithm::SPECULATIVE:
                    result.successful = parser_.parse_speculate(
                        buf, result.idx, len, result.dialect);
                    break;
                case ParseAlgorithm::AUTO:
                default:
                    // AUTO currently uses speculative (same as parse())
                    result.successful = parser_.parse(
                        buf, result.idx, len, result.dialect);
                    break;
            }
        }

        LIBVROOM_SUPPRESS_DEPRECATION_END

        return result;
    }

    // =========================================================================
    // Legacy methods - Provided for backward compatibility.
    // These delegate to the unified parse() method above.
    // =========================================================================

    /**
     * @brief Parse with explicit dialect (legacy method).
     *
     * @deprecated Use parse(buf, len, {.dialect = dialect}) instead.
     *
     * This method is provided for backward compatibility. New code should use
     * the unified parse() method with ParseOptions.
     */
    Result parse(const uint8_t* buf, size_t len, const Dialect& dialect) {
        return parse(buf, len, ParseOptions::with_dialect(dialect));
    }

    /**
     * @brief Parse with error collection (legacy method).
     *
     * @deprecated Use parse(buf, len, {.dialect = dialect, .errors = &errors}) instead.
     *
     * This method is provided for backward compatibility. New code should use
     * the unified parse() method with ParseOptions.
     */
    Result parse_with_errors(const uint8_t* buf, size_t len,
                             ErrorCollector& errors,
                             const Dialect& dialect = Dialect::csv()) {
        return parse(buf, len, ParseOptions::with_dialect_and_errors(dialect, errors));
    }

    /**
     * @brief Parse with auto-detection and error collection (legacy method).
     *
     * @deprecated Use parse(buf, len, {.errors = &errors}) instead.
     *
     * This method is provided for backward compatibility. New code should use
     * the unified parse() method with ParseOptions.
     */
    Result parse_auto(const uint8_t* buf, size_t len, ErrorCollector& errors) {
        return parse(buf, len, ParseOptions::with_errors(errors));
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
