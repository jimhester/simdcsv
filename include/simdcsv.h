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

namespace simdcsv {

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
 * simdcsv::FileBuffer buffer = simdcsv::load_file("data.csv");
 *
 * if (buffer) {  // Check if valid using operator bool
 *     std::cout << "Loaded " << buffer.size() << " bytes\n";
 *
 *     // Access data
 *     const uint8_t* data = buffer.data();
 *
 *     // Parse with simdcsv
 *     simdcsv::Parser parser;
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
 * #include "simdcsv.h"
 *
 * // Load CSV file
 * simdcsv::FileBuffer buffer = simdcsv::load_file("data.csv");
 *
 * // Create parser with 4 threads
 * simdcsv::Parser parser(4);
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
 * simdcsv::Parser parser(4);
 * simdcsv::ErrorCollector errors(simdcsv::ErrorMode::PERMISSIVE);
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
     * @brief Parse CSV data with a specified dialect.
     *
     * @param buf Pointer to CSV data. Must remain valid during parsing.
     *            Should have at least 32 bytes of padding for SIMD safety.
     * @param len Length of the CSV data in bytes.
     * @param dialect The dialect to use (default: standard CSV).
     * @return Result containing the parsed index and status.
     *
     * @throws std::runtime_error On parsing errors (e.g., malformed quotes).
     *
     * @example
     * @code
     * simdcsv::Parser parser(4);
     * auto result = parser.parse(buffer.data(), buffer.size());
     *
     * // Parse TSV file
     * auto result = parser.parse(buffer.data(), buffer.size(),
     *                            simdcsv::Dialect::tsv());
     * @endcode
     */
    Result parse(const uint8_t* buf, size_t len,
                 const Dialect& dialect = Dialect::csv()) {
        Result result;
        result.dialect = dialect;
        result.idx = parser_.init(len, num_threads_);
        result.successful = parser_.parse(buf, result.idx, len, dialect);
        return result;
    }

    /**
     * @brief Parse CSV data with error collection.
     *
     * Unlike parse(), this method collects errors instead of throwing and
     * allows parsing to continue past recoverable errors based on the
     * ErrorCollector's mode.
     *
     * @param buf Pointer to CSV data. Must remain valid during parsing.
     * @param len Length of the CSV data in bytes.
     * @param errors ErrorCollector to accumulate parsing errors.
     * @param dialect The dialect to use (default: standard CSV).
     * @return Result containing the parsed index and status.
     *
     * @see ErrorMode For different error handling strategies.
     */
    Result parse_with_errors(const uint8_t* buf, size_t len,
                             ErrorCollector& errors,
                             const Dialect& dialect = Dialect::csv()) {
        Result result;
        result.dialect = dialect;
        result.idx = parser_.init(len, num_threads_);
        result.successful = parser_.parse_with_errors(buf, result.idx, len, errors, dialect);
        return result;
    }

    /**
     * @brief Parse CSV data with automatic dialect detection.
     *
     * Detects the CSV dialect (delimiter, quote character, etc.) and then
     * parses using the detected settings. The detected dialect is stored
     * in the Result.
     *
     * @param buf Pointer to CSV data. Must remain valid during parsing.
     * @param len Length of the CSV data in bytes.
     * @param errors ErrorCollector to accumulate parsing errors.
     * @return Result containing the parsed index, detected dialect, and status.
     *
     * @example
     * @code
     * simdcsv::Parser parser(4);
     * simdcsv::ErrorCollector errors;
     *
     * auto result = parser.parse_auto(buffer.data(), buffer.size(), errors);
     *
     * if (result.success()) {
     *     std::cout << "Detected: " << result.dialect.to_string() << "\n";
     *     std::cout << "Confidence: " << result.detection.confidence << "\n";
     * }
     * @endcode
     *
     * @see DialectDetector For standalone dialect detection.
     */
    Result parse_auto(const uint8_t* buf, size_t len, ErrorCollector& errors) {
        Result result;
        result.idx = parser_.init(len, num_threads_);
        result.successful = parser_.parse_auto(buf, result.idx, len, errors, &result.detection);
        result.dialect = result.detection.dialect;
        return result;
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
 * auto result = simdcsv::detect_dialect(buffer.data(), buffer.size());
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
 * auto result = simdcsv::detect_dialect_file("data.csv");
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

} // namespace simdcsv

#endif // SIMDCSV_H
