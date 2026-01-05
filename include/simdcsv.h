/**
 * @file simdcsv.h
 * @brief simdcsv - High-performance CSV parser using portable SIMD instructions.
 * @version 0.1.0
 *
 * This is the main public header for the simdcsv library. Include this single
 * header to access all public functionality.
 */

#ifndef SIMDCSV_H
#define SIMDCSV_H

#define SIMDCSV_VERSION_MAJOR 0
#define SIMDCSV_VERSION_MINOR 1
#define SIMDCSV_VERSION_PATCH 0
#define SIMDCSV_VERSION_STRING "0.1.0"

#include "error.h"
#include "dialect.h"
#include "two_pass.h"
#include "io_util.h"
#include "mem_util.h"

#include <optional>

namespace simdcsv {

/**
 * @brief Configuration options for parsing.
 *
 * ParseOptions provides a unified way to configure CSV parsing, combining
 * dialect selection and error handling into a single structure. This enables
 * a single parse() method to handle all use cases.
 *
 * Key behaviors:
 * - **Dialect**: If dialect is nullopt (default), the dialect is auto-detected
 *   from the data. Set an explicit dialect (e.g., Dialect::csv()) to skip detection.
 * - **Error collection**: If errors is nullptr (default), parsing uses the fast
 *   path and throws on errors. Provide an ErrorCollector for error-tolerant parsing.
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
 * @endcode
 *
 * @see Parser::parse() for the unified parsing method
 * @see Dialect for dialect configuration options
 * @see ErrorCollector for error handling configuration
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
};

class FileBuffer {
public:
    FileBuffer() : data_(nullptr), size_(0) {}
    FileBuffer(uint8_t* data, size_t size) : data_(data), size_(size) {}
    FileBuffer(FileBuffer&& other) noexcept : data_(other.data_), size_(other.size_) {
        other.data_ = nullptr;
        other.size_ = 0;
    }
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
    FileBuffer(const FileBuffer&) = delete;
    FileBuffer& operator=(const FileBuffer&) = delete;
    ~FileBuffer() { free(); }

    const uint8_t* data() const { return data_; }
    uint8_t* data() { return data_; }
    size_t size() const { return size_; }
    bool valid() const { return data_ != nullptr; }
    bool empty() const { return size_ == 0; }
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

inline void free_buffer(std::basic_string_view<uint8_t>& corpus) {
    if (corpus.data()) {
        aligned_free(const_cast<uint8_t*>(corpus.data()));
    }
}

class Parser {
public:
    struct Result {
        index idx;
        bool successful{false};
        Dialect dialect;
        DetectionResult detection;

        Result() = default;
        Result(Result&&) = default;
        Result& operator=(Result&&) = default;
        // Prevent copying - index may contain raw pointers
        Result(const Result&) = delete;
        Result& operator=(const Result&) = delete;

        bool success() const { return successful; }
        size_t num_columns() const { return idx.columns; }
        size_t total_indexes() const {
            if (!idx.n_indexes) return 0;
            size_t total = 0;
            for (uint8_t t = 0; t < idx.n_threads; ++t) {
                total += idx.n_indexes[t];
            }
            return total;
        }
    };

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

        if (options.dialect.has_value()) {
            // Explicit dialect provided
            result.dialect = options.dialect.value();

            if (options.errors != nullptr) {
                // Explicit dialect + error collection
                result.successful = parser_.parse_with_errors(
                    buf, result.idx, len, *options.errors, result.dialect);
            } else {
                // Explicit dialect + fast path (throws on error)
                result.successful = parser_.parse(buf, result.idx, len, result.dialect);
            }
        } else {
            // Auto-detect dialect
            if (options.errors != nullptr) {
                // Auto-detect + error collection
                result.successful = parser_.parse_auto(
                    buf, result.idx, len, *options.errors, &result.detection);
                result.dialect = result.detection.dialect;
            } else {
                // Auto-detect + fast path: detect first, then parse
                DialectDetector detector(options.detection_options);
                result.detection = detector.detect(buf, len);
                result.dialect = result.detection.success()
                    ? result.detection.dialect : Dialect::csv();
                result.successful = parser_.parse(buf, result.idx, len, result.dialect);
            }
        }

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

    void set_num_threads(size_t num_threads) {
        num_threads_ = num_threads > 0 ? num_threads : 1;
    }
    size_t num_threads() const { return num_threads_; }

private:
    two_pass parser_;
    size_t num_threads_;
};

inline DetectionResult detect_dialect(const uint8_t* buf, size_t len,
                                       const DetectionOptions& options = DetectionOptions()) {
    DialectDetector detector(options);
    return detector.detect(buf, len);
}

inline DetectionResult detect_dialect_file(const std::string& filename,
                                            const DetectionOptions& options = DetectionOptions()) {
    DialectDetector detector(options);
    return detector.detect_file(filename);
}

} // namespace simdcsv

#endif // SIMDCSV_H
