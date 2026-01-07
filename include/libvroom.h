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

#include "dialect.h"
#include "error.h"
#include "io_util.h"
#include "mem_util.h"
#include "two_pass.h"
#include "value_extraction.h"

#include <optional>
#include <unordered_map>

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
 * @brief Size limits for secure CSV parsing.
 *
 * These limits prevent denial-of-service attacks through excessive memory
 * allocation. They can be configured based on the expected data and available
 * system resources.
 *
 * ## Security Considerations
 *
 * Without size limits, a malicious CSV file could cause:
 * - **Memory exhaustion**: The parser allocates index arrays proportional to
 *   file size. A 1GB file allocates ~8GB for indexes (one uint64_t per byte).
 * - **Integer overflow**: Unchecked size calculations could overflow, leading
 *   to undersized allocations and buffer overflows.
 *
 * ## Defaults
 *
 * Default limits are chosen to handle most legitimate use cases while
 * providing protection against malicious inputs:
 * - max_file_size: 10GB (handles very large datasets)
 * - max_field_size: 16MB (larger than most legitimate fields)
 *
 * @example
 * @code
 * // Use default limits
 * libvroom::Parser parser;
 * auto result = parser.parse(buf, len);
 *
 * // Use custom limits for large file processing
 * libvroom::SizeLimits limits;
 * limits.max_file_size = 50ULL * 1024 * 1024 * 1024;  // 50GB
 * auto result = parser.parse(buf, len, {.limits = limits});
 *
 * // Disable limits (NOT RECOMMENDED for untrusted input)
 * auto result = parser.parse(buf, len, {.limits = SizeLimits::unlimited()});
 * @endcode
 */
struct SizeLimits {
  /**
   * @brief Maximum file size in bytes (default: 10GB).
   *
   * Files larger than this limit will be rejected with FILE_TOO_LARGE error.
   * Set to 0 to disable the file size check (not recommended).
   */
  size_t max_file_size = 10ULL * 1024 * 1024 * 1024; // 10GB default

  /**
   * @brief Maximum field size in bytes (default: 16MB).
   *
   * Individual fields larger than this will trigger FIELD_TOO_LARGE error.
   * Set to 0 to disable field size checks.
   */
  size_t max_field_size = 16ULL * 1024 * 1024; // 16MB default

  /**
   * @brief Enable UTF-8 validation (default: false for performance).
   *
   * When true, the parser validates that all byte sequences are valid UTF-8.
   * Invalid sequences are reported as INVALID_UTF8 errors. This has a
   * performance cost, so only enable when parsing untrusted input that
   * claims to be UTF-8 encoded.
   */
  bool validate_utf8 = false;

  /**
   * @brief Factory for default limits (10GB file, 16MB field, no UTF-8 validation).
   */
  static SizeLimits defaults() { return SizeLimits{}; }

  /**
   * @brief Factory for unlimited parsing (disables all size checks).
   *
   * @warning Using unlimited limits with untrusted input is dangerous
   *          and may lead to denial-of-service through memory exhaustion.
   */
  static SizeLimits unlimited() {
    SizeLimits limits;
    limits.max_file_size = 0;
    limits.max_field_size = 0;
    return limits;
  }

  /**
   * @brief Factory for strict limits (suitable for web services).
   *
   * @param max_file Maximum file size in bytes (default: 100MB)
   * @param max_field Maximum field size in bytes (default: 1MB)
   */
  static SizeLimits strict(size_t max_file = 100ULL * 1024 * 1024,
                           size_t max_field = 1ULL * 1024 * 1024) {
    SizeLimits limits;
    limits.max_file_size = max_file;
    limits.max_field_size = max_field;
    limits.validate_utf8 = true;
    return limits;
  }
};

/**
 * @brief Check if a size multiplication would overflow.
 *
 * This function safely checks if multiplying two size_t values would overflow
 * before performing the multiplication. Used internally to prevent integer
 * overflow in memory allocation calculations.
 *
 * @param a First operand
 * @param b Second operand
 * @return true if multiplication would overflow, false if safe
 */
inline bool would_overflow_multiply(size_t a, size_t b) {
  if (a == 0 || b == 0)
    return false;
  return a > std::numeric_limits<size_t>::max() / b;
}

/**
 * @brief Check if a size addition would overflow.
 *
 * @param a First operand
 * @param b Second operand
 * @return true if addition would overflow, false if safe
 */
inline bool would_overflow_add(size_t a, size_t b) {
  return a > std::numeric_limits<size_t>::max() - b;
}

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
   * @deprecated Use result.errors() instead. The Result class now has an
   * internal ErrorCollector that is automatically populated during parsing.
   * This field is maintained for backward compatibility but will be removed
   * in a future version.
   *
   * If nullptr (default), errors are collected in Result's internal collector.
   * If a pointer is provided, errors go to both the external collector and
   * the Result's internal collector.
   *
   * @note The ErrorCollector must remain valid for the duration of parsing.
   *
   * Migration example:
   * @code
   * // Old pattern (deprecated):
   * ErrorCollector errors(ErrorMode::PERMISSIVE);
   * auto result = parser.parse(buf, len, {.errors = &errors});
   * if (errors.has_errors()) { ... }
   *
   * // New pattern (preferred):
   * auto result = parser.parse(buf, len);
   * if (result.has_errors()) {
   *     for (const auto& err : result.errors()) { ... }
   * }
   * @endcode
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
   * @brief Size limits for secure parsing.
   *
   * Controls maximum file and field sizes to prevent denial-of-service attacks.
   * Default limits are 10GB for files and 16MB for fields, which handles
   * most legitimate use cases while providing security protection.
   *
   * @see SizeLimits for configuration options
   */
  SizeLimits limits = SizeLimits::defaults();

  /**
   * @brief Factory for default options (auto-detect dialect, fast path).
   *
   * Equivalent to standard(). Both methods create identical options.
   */
  static ParseOptions defaults() { return ParseOptions{}; }

  /**
   * @brief Factory for standard options (auto-detect dialect, fast path).
   *
   * Creates default parsing options: auto-detect dialect, no error collection.
   * This is the recommended entry point for simple parsing use cases.
   *
   * Equivalent to defaults(). Both methods create identical options.
   */
  static ParseOptions standard() { return ParseOptions{}; }

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
  auto [buffer, size] = read_file(filename, padding);
  return FileBuffer(buffer.release(), size);
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
  AlignedPtr ptr; ///< Smart pointer owning the buffer
  size_t size{0}; ///< Size of the data (not including padding)

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
  auto [ptr, size] = read_file(filename, padding);
  return AlignedBuffer(std::move(ptr), size);
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
 */
inline AlignedBuffer load_stdin_to_ptr(size_t padding = 64) {
  auto [ptr, size] = read_stdin(padding);
  return AlignedBuffer(std::move(ptr), size);
}

/**
 * @brief Internal UTF-8 validation function.
 *
 * Validates UTF-8 encoding and reports any invalid byte sequences to the
 * error collector. This function implements the UTF-8 state machine to
 * detect encoding errors including:
 * - Invalid leading bytes
 * - Truncated multi-byte sequences
 * - Overlong encodings
 * - Surrogate code points (U+D800-U+DFFF)
 * - Code points exceeding U+10FFFF
 *
 * @param buf Pointer to data buffer
 * @param len Length of data in bytes
 * @param errors ErrorCollector to receive validation errors
 *
 * @note This is an internal function used by Parser when SizeLimits::validate_utf8 is true.
 */
inline void validate_utf8_internal(const uint8_t* buf, size_t len, ErrorCollector& errors) {
  size_t line = 1;
  size_t column = 1;
  size_t i = 0;

  while (i < len) {
    // Track line/column for error reporting
    if (buf[i] == '\n') {
      line++;
      column = 1;
      i++;
      continue;
    }
    if (buf[i] == '\r') {
      // Handle CRLF
      if (i + 1 < len && buf[i + 1] == '\n') {
        i++; // Skip \r, let \n be handled next iteration
      } else {
        line++;
        column = 1;
      }
      i++;
      continue;
    }

    // Check for valid UTF-8 sequences
    uint8_t byte = buf[i];

    if ((byte & 0x80) == 0) {
      // Single-byte ASCII (0xxxxxxx)
      column++;
      i++;
    } else if ((byte & 0xE0) == 0xC0) {
      // Two-byte sequence (110xxxxx 10xxxxxx)
      if (i + 1 >= len || (buf[i + 1] & 0xC0) != 0x80) {
        errors.add_error(ErrorCode::INVALID_UTF8, ErrorSeverity::ERROR, line, column, i,
                         "Invalid UTF-8 sequence: truncated 2-byte sequence");
        if (errors.should_stop())
          return;
        column++;
        i++;
        continue;
      }
      // Check for overlong encoding (code points < 0x80 encoded as 2 bytes)
      if ((byte & 0x1E) == 0) {
        errors.add_error(ErrorCode::INVALID_UTF8, ErrorSeverity::ERROR, line, column, i,
                         "Invalid UTF-8 sequence: overlong 2-byte encoding");
        if (errors.should_stop())
          return;
      }
      column++;
      i += 2;
    } else if ((byte & 0xF0) == 0xE0) {
      // Three-byte sequence (1110xxxx 10xxxxxx 10xxxxxx)
      if (i + 2 >= len || (buf[i + 1] & 0xC0) != 0x80 || (buf[i + 2] & 0xC0) != 0x80) {
        errors.add_error(ErrorCode::INVALID_UTF8, ErrorSeverity::ERROR, line, column, i,
                         "Invalid UTF-8 sequence: truncated 3-byte sequence");
        if (errors.should_stop())
          return;
        column++;
        i++;
        continue;
      }
      // Check for overlong encoding and surrogate code points
      uint32_t cp = ((byte & 0x0F) << 12) | ((buf[i + 1] & 0x3F) << 6) | (buf[i + 2] & 0x3F);
      if (cp < 0x800) {
        errors.add_error(ErrorCode::INVALID_UTF8, ErrorSeverity::ERROR, line, column, i,
                         "Invalid UTF-8 sequence: overlong 3-byte encoding");
        if (errors.should_stop())
          return;
      } else if (cp >= 0xD800 && cp <= 0xDFFF) {
        errors.add_error(ErrorCode::INVALID_UTF8, ErrorSeverity::ERROR, line, column, i,
                         "Invalid UTF-8 sequence: surrogate code point");
        if (errors.should_stop())
          return;
      }
      column++;
      i += 3;
    } else if ((byte & 0xF8) == 0xF0) {
      // Four-byte sequence (11110xxx 10xxxxxx 10xxxxxx 10xxxxxx)
      if (i + 3 >= len || (buf[i + 1] & 0xC0) != 0x80 || (buf[i + 2] & 0xC0) != 0x80 ||
          (buf[i + 3] & 0xC0) != 0x80) {
        errors.add_error(ErrorCode::INVALID_UTF8, ErrorSeverity::ERROR, line, column, i,
                         "Invalid UTF-8 sequence: truncated 4-byte sequence");
        if (errors.should_stop())
          return;
        column++;
        i++;
        continue;
      }
      // Check for overlong encoding and code points > U+10FFFF
      uint32_t cp = ((byte & 0x07) << 18) | ((buf[i + 1] & 0x3F) << 12) |
                    ((buf[i + 2] & 0x3F) << 6) | (buf[i + 3] & 0x3F);
      if (cp < 0x10000) {
        errors.add_error(ErrorCode::INVALID_UTF8, ErrorSeverity::ERROR, line, column, i,
                         "Invalid UTF-8 sequence: overlong 4-byte encoding");
        if (errors.should_stop())
          return;
      } else if (cp > 0x10FFFF) {
        errors.add_error(ErrorCode::INVALID_UTF8, ErrorSeverity::ERROR, line, column, i,
                         "Invalid UTF-8 sequence: code point exceeds U+10FFFF");
        if (errors.should_stop())
          return;
      }
      column++;
      i += 4;
    } else {
      // Invalid leading byte (10xxxxxx continuation byte without leading byte,
      // or invalid 5/6-byte sequence starts 111110xx/1111110x)
      errors.add_error(ErrorCode::INVALID_UTF8, ErrorSeverity::ERROR, line, column, i,
                       "Invalid UTF-8 sequence: invalid leading byte");
      if (errors.should_stop())
        return;
      column++;
      i++;
    }
  }
}

/**
 * @brief High-level CSV parser with automatic index management.
 *
 * Parser provides a simplified interface over the lower-level TwoPass class.
 * It manages index allocation internally and returns a Result object containing
 * the parsed index, dialect information, and success status.
 *
 * The Parser supports:
 * - Single-threaded and multi-threaded parsing
 * - Explicit dialect specification or auto-detection
 * - Error collection in permissive mode
 *
 * @note For maximum performance with manual control, use TwoPass directly.
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
 * @see TwoPass For lower-level parsing with full control.
 * @see FileBuffer For loading CSV files.
 * @see Dialect For dialect configuration options.
 */
class Parser {
public:
  /**
   * @brief A single row in a parsed CSV result.
   *
   * Row provides access to individual fields within a row by column index or name.
   * It supports type-safe value extraction with automatic type conversion.
   *
   * @note Row objects are lightweight views that do not own the underlying data.
   *       They remain valid only as long as the parent Result object exists.
   */
  class Row {
  public:
    Row(const ValueExtractor* extractor, size_t row_index,
        const std::unordered_map<std::string, size_t>* column_map)
        : extractor_(extractor), row_index_(row_index), column_map_(column_map) {}

    /**
     * @brief Get a field value by column index with type conversion.
     *
     * @tparam T The type to convert to (int32_t, int64_t, double, bool, std::string)
     * @param col Column index (0-based)
     * @return ExtractResult<T> containing the value or error/NA status
     *
     * @example
     * @code
     * auto age = row.get<int>(1);
     * if (age.ok()) {
     *     std::cout << "Age: " << age.get() << "\n";
     * }
     * @endcode
     */
    template <typename T> ExtractResult<T> get(size_t col) const {
      return extractor_->get<T>(row_index_, col);
    }

    /**
     * @brief Get a field value by column name with type conversion.
     *
     * @tparam T The type to convert to (int32_t, int64_t, double, bool, std::string)
     * @param name Column name (must match header exactly)
     * @return ExtractResult<T> containing the value or error/NA status
     * @throws std::out_of_range if column name is not found
     *
     * @example
     * @code
     * auto name = row.get<std::string>("name");
     * auto age = row.get<int>("age");
     * @endcode
     */
    template <typename T> ExtractResult<T> get(const std::string& name) const {
      auto it = column_map_->find(name);
      if (it == column_map_->end()) {
        throw std::out_of_range("Column not found: " + name);
      }
      return extractor_->get<T>(row_index_, it->second);
    }

    /**
     * @brief Get a string view of a field by column index.
     *
     * This is the most efficient way to access string data as it avoids copying.
     * The returned view is valid only as long as the parent Result exists.
     *
     * @param col Column index (0-based)
     * @return std::string_view of the field contents (quotes stripped)
     */
    std::string_view get_string_view(size_t col) const {
      return extractor_->get_string_view(row_index_, col);
    }

    /**
     * @brief Get a string view of a field by column name.
     * @param name Column name
     * @return std::string_view of the field contents
     * @throws std::out_of_range if column name is not found
     */
    std::string_view get_string_view(const std::string& name) const {
      auto it = column_map_->find(name);
      if (it == column_map_->end()) {
        throw std::out_of_range("Column not found: " + name);
      }
      return extractor_->get_string_view(row_index_, it->second);
    }

    /**
     * @brief Get a copy of a field as a string by column index.
     *
     * This handles unescaping of quoted fields (converting "" to ").
     *
     * @param col Column index (0-based)
     * @return std::string with the field value
     */
    std::string get_string(size_t col) const { return extractor_->get_string(row_index_, col); }

    /**
     * @brief Get a copy of a field as a string by column name.
     * @param name Column name
     * @return std::string with the field value
     * @throws std::out_of_range if column name is not found
     */
    std::string get_string(const std::string& name) const {
      auto it = column_map_->find(name);
      if (it == column_map_->end()) {
        throw std::out_of_range("Column not found: " + name);
      }
      return extractor_->get_string(row_index_, it->second);
    }

    /// @return The number of columns in this row.
    size_t num_columns() const { return extractor_->num_columns(); }

    /// @return The 0-based row index.
    size_t row_index() const { return row_index_; }

  private:
    const ValueExtractor* extractor_;
    size_t row_index_;
    const std::unordered_map<std::string, size_t>* column_map_;
  };

  /**
   * @brief Iterator for iterating over rows in a parsed CSV result.
   *
   * RowIterator is a forward iterator that yields Row objects for each data row.
   * It skips the header row automatically when has_header is true.
   */
  class ResultRowIterator {
  public:
    using iterator_category = std::forward_iterator_tag;
    using value_type = Row;
    using difference_type = std::ptrdiff_t;
    using pointer = Row*;
    using reference = Row;

    ResultRowIterator(const ValueExtractor* extractor, size_t row,
                      const std::unordered_map<std::string, size_t>* column_map)
        : extractor_(extractor), row_(row), column_map_(column_map) {}

    Row operator*() const { return Row(extractor_, row_, column_map_); }

    ResultRowIterator& operator++() {
      ++row_;
      return *this;
    }
    ResultRowIterator operator++(int) {
      auto tmp = *this;
      ++row_;
      return tmp;
    }

    bool operator==(const ResultRowIterator& other) const { return row_ == other.row_; }
    bool operator!=(const ResultRowIterator& other) const { return row_ != other.row_; }

  private:
    const ValueExtractor* extractor_;
    size_t row_;
    const std::unordered_map<std::string, size_t>* column_map_;
  };

  /**
   * @brief Iterable view over rows in a parsed CSV result.
   *
   * RowView provides begin() and end() iterators for use in range-based for loops.
   */
  class RowView {
  public:
    RowView(const ValueExtractor* extractor,
            const std::unordered_map<std::string, size_t>* column_map)
        : extractor_(extractor), column_map_(column_map) {}

    ResultRowIterator begin() const { return ResultRowIterator(extractor_, 0, column_map_); }

    ResultRowIterator end() const {
      return ResultRowIterator(extractor_, extractor_->num_rows(), column_map_);
    }

    /// @return The number of rows in this view.
    size_t size() const { return extractor_->num_rows(); }

    /// @return true if there are no data rows.
    bool empty() const { return extractor_->num_rows() == 0; }

  private:
    const ValueExtractor* extractor_;
    const std::unordered_map<std::string, size_t>* column_map_;
  };

  /**
   * @brief Result of a parsing operation.
   *
   * Contains the parsed index, dialect used (or detected), and success status.
   * This structure is move-only since the underlying index contains raw pointers.
   *
   * Result provides a convenient API for iterating over rows and accessing columns,
   * as well as integrated error handling through the built-in ErrorCollector.
   *
   * @example Row iteration
   * @code
   * auto result = parser.parse(buffer.data(), buffer.size());
   * for (auto row : result.rows()) {
   *     auto name = row.get<std::string>("name");
   *     auto age = row.get<int>("age");
   *     if (name.ok() && age.ok()) {
   *         std::cout << name.get() << " is " << age.get() << " years old\n";
   *     }
   * }
   * @endcode
   *
   * @example Column extraction
   * @code
   * auto names = result.column<std::string>("name");
   * auto ages = result.column<int64_t>("age");
   * @endcode
   *
   * @example Error handling (unified API)
   * @code
   * auto result = parser.parse(buffer.data(), buffer.size());
   * if (result.has_errors()) {
   *     std::cerr << result.error_summary() << std::endl;
   *     for (const auto& err : result.errors()) {
   *         std::cerr << err.to_string() << std::endl;
   *     }
   * }
   * @endcode
   */
  struct Result {
    ParseIndex idx;            ///< The parsed field index.
    bool successful{false};    ///< Whether parsing completed without fatal errors.
    Dialect dialect;           ///< The dialect used for parsing.
    DetectionResult detection; ///< Detection result (populated by parse_auto).

  private:
    const uint8_t* buf_{nullptr};                                ///< Pointer to the parsed buffer.
    size_t len_{0};                                              ///< Length of the parsed buffer.
    mutable std::unique_ptr<ValueExtractor> extractor_;          ///< Lazy-initialized extractor.
    mutable std::unordered_map<std::string, size_t> column_map_; ///< Column name to index map.
    mutable bool column_map_initialized_{false};
    /// Internal error collector for unified error handling.
    /// Uses PERMISSIVE mode by default to collect all errors without stopping.
    ErrorCollector error_collector_{ErrorMode::PERMISSIVE};

    void ensure_extractor() const {
      if (!extractor_ && buf_ && len_ > 0) {
        extractor_ = std::make_unique<ValueExtractor>(buf_, len_, idx, dialect);
      }
    }

    void ensure_column_map() const {
      if (!column_map_initialized_) {
        ensure_extractor();
        if (extractor_ && extractor_->has_header()) {
          auto headers = extractor_->get_header();
          for (size_t i = 0; i < headers.size(); ++i) {
            column_map_[headers[i]] = i;
          }
        }
        column_map_initialized_ = true;
      }
    }

  public:
    Result() = default;
    Result(Result&&) = default;
    Result& operator=(Result&&) = default;

    // Prevent copying - index contains raw pointers
    Result(const Result&) = delete;
    Result& operator=(const Result&) = delete;

    /**
     * @brief Store buffer reference for later iteration.
     *
     * This is called internally by Parser::parse() to enable row iteration.
     * Users should not call this directly.
     *
     * @param buf Pointer to the CSV data buffer.
     * @param len Length of the buffer.
     */
    void set_buffer(const uint8_t* buf, size_t len) {
      buf_ = buf;
      len_ = len;
      // Reset extractor and column map since buffer changed
      extractor_.reset();
      column_map_.clear();
      column_map_initialized_ = false;
    }

    /// @return true if parsing was successful.
    bool success() const { return successful; }

    /// @return Number of columns detected in the CSV.
    size_t num_columns() const {
      ensure_extractor();
      return extractor_ ? extractor_->num_columns() : idx.columns;
    }

    /**
     * @brief Get total number of field separator positions found.
     * @return Sum of indexes across all parsing threads.
     */
    size_t total_indexes() const {
      if (!idx.n_indexes)
        return 0;
      size_t total = 0;
      for (uint8_t t = 0; t < idx.n_threads; ++t) {
        total += idx.n_indexes[t];
      }
      return total;
    }

    // =====================================================================
    // Row/Column Iteration API
    // =====================================================================

    /**
     * @brief Get the number of data rows (excluding header).
     * @return Number of data rows.
     */
    size_t num_rows() const {
      ensure_extractor();
      return extractor_ ? extractor_->num_rows() : 0;
    }

    /**
     * @brief Get an iterable view over all data rows.
     *
     * This enables range-based for loop iteration over the parsed CSV.
     *
     * @return RowView for iteration.
     *
     * @example
     * @code
     * for (auto row : result.rows()) {
     *     std::cout << row.get_string(0) << "\n";
     * }
     * @endcode
     */
    RowView rows() const {
      ensure_extractor();
      ensure_column_map();
      return RowView(extractor_.get(), &column_map_);
    }

    /**
     * @brief Get a specific row by index.
     *
     * @param row_index 0-based row index (excluding header).
     * @return Row object for accessing fields.
     * @throws std::out_of_range if row_index >= num_rows().
     */
    Row row(size_t row_index) const {
      ensure_extractor();
      ensure_column_map();
      if (row_index >= num_rows()) {
        throw std::out_of_range("Row index out of range");
      }
      return Row(extractor_.get(), row_index, &column_map_);
    }

    /**
     * @brief Extract an entire column as a vector of optional values.
     *
     * @tparam T The type to convert values to (int32_t, int64_t, double, bool).
     * @param col Column index (0-based).
     * @return Vector of optional values (nullopt for NA/missing values).
     *
     * @example
     * @code
     * auto ages = result.column<int64_t>(1);
     * for (const auto& age : ages) {
     *     if (age) {
     *         std::cout << *age << "\n";
     *     }
     * }
     * @endcode
     */
    template <typename T> std::vector<std::optional<T>> column(size_t col) const {
      ensure_extractor();
      return extractor_ ? extractor_->extract_column<T>(col) : std::vector<std::optional<T>>{};
    }

    /**
     * @brief Extract an entire column by name as a vector of optional values.
     *
     * @tparam T The type to convert values to.
     * @param name Column name (must match header exactly).
     * @return Vector of optional values.
     * @throws std::out_of_range if column name is not found.
     *
     * @example
     * @code
     * auto names = result.column<std::string>("name");
     * auto ages = result.column<int64_t>("age");
     * @endcode
     */
    template <typename T> std::vector<std::optional<T>> column(const std::string& name) const {
      ensure_column_map();
      auto it = column_map_.find(name);
      if (it == column_map_.end()) {
        throw std::out_of_range("Column not found: " + name);
      }
      return column<T>(it->second);
    }

    /**
     * @brief Extract a column with a default value for NA/missing entries.
     *
     * @tparam T The type to convert values to.
     * @param col Column index (0-based).
     * @param default_value Value to use for NA/missing entries.
     * @return Vector of values with default substituted for NA.
     *
     * @example
     * @code
     * auto ages = result.column_or<int64_t>(1, -1);  // -1 for missing
     * @endcode
     */
    template <typename T> std::vector<T> column_or(size_t col, T default_value) const {
      ensure_extractor();
      return extractor_ ? extractor_->extract_column_or<T>(col, default_value) : std::vector<T>{};
    }

    /**
     * @brief Extract a column by name with a default value for NA/missing entries.
     *
     * @tparam T The type to convert values to.
     * @param name Column name.
     * @param default_value Value to use for NA/missing entries.
     * @return Vector of values with default substituted for NA.
     * @throws std::out_of_range if column name is not found.
     */
    template <typename T> std::vector<T> column_or(const std::string& name, T default_value) const {
      ensure_column_map();
      auto it = column_map_.find(name);
      if (it == column_map_.end()) {
        throw std::out_of_range("Column not found: " + name);
      }
      return column_or<T>(it->second, default_value);
    }

    /**
     * @brief Extract a string column as string_views (zero-copy).
     *
     * @param col Column index (0-based).
     * @return Vector of string_views into the original buffer.
     * @note Views are valid only as long as the original buffer exists.
     */
    std::vector<std::string_view> column_string_view(size_t col) const {
      ensure_extractor();
      return extractor_ ? extractor_->extract_column_string_view(col)
                        : std::vector<std::string_view>{};
    }

    /**
     * @brief Extract a string column by name as string_views (zero-copy).
     *
     * @param name Column name.
     * @return Vector of string_views into the original buffer.
     * @throws std::out_of_range if column name is not found.
     */
    std::vector<std::string_view> column_string_view(const std::string& name) const {
      ensure_column_map();
      auto it = column_map_.find(name);
      if (it == column_map_.end()) {
        throw std::out_of_range("Column not found: " + name);
      }
      return column_string_view(it->second);
    }

    /**
     * @brief Extract a string column as strings (with proper unescaping).
     *
     * @param col Column index (0-based).
     * @return Vector of strings with quotes and escapes processed.
     */
    std::vector<std::string> column_string(size_t col) const {
      ensure_extractor();
      return extractor_ ? extractor_->extract_column_string(col) : std::vector<std::string>{};
    }

    /**
     * @brief Extract a string column by name as strings.
     *
     * @param name Column name.
     * @return Vector of strings with quotes and escapes processed.
     * @throws std::out_of_range if column name is not found.
     */
    std::vector<std::string> column_string(const std::string& name) const {
      ensure_column_map();
      auto it = column_map_.find(name);
      if (it == column_map_.end()) {
        throw std::out_of_range("Column not found: " + name);
      }
      return column_string(it->second);
    }

    /**
     * @brief Get the column headers.
     *
     * @return Vector of column names from the header row.
     * @throws std::runtime_error if the CSV has no header row.
     */
    std::vector<std::string> header() const {
      ensure_extractor();
      return extractor_ ? extractor_->get_header() : std::vector<std::string>{};
    }

    /**
     * @brief Check if the CSV has a header row.
     * @return true if a header row is present.
     */
    bool has_header() const {
      ensure_extractor();
      return extractor_ ? extractor_->has_header() : true;
    }

    /**
     * @brief Set whether the CSV has a header row.
     *
     * @param has_header true if first row should be treated as header.
     */
    void set_has_header(bool has_header) {
      ensure_extractor();
      if (extractor_) {
        extractor_->set_has_header(has_header);
        // Reset column map since header status changed
        column_map_.clear();
        column_map_initialized_ = false;
      }
    }

    /**
     * @brief Get the column index for a column name.
     *
     * @param name Column name.
     * @return Column index, or std::nullopt if not found.
     */
    std::optional<size_t> column_index(const std::string& name) const {
      ensure_column_map();
      auto it = column_map_.find(name);
      if (it == column_map_.end()) {
        return std::nullopt;
      }
      return it->second;
    }

    // =====================================================================
    // Error Handling API (Unified)
    // =====================================================================

    /**
     * @brief Check if any errors were recorded during parsing.
     *
     * This method provides a unified way to check for errors without
     * needing to pass an external ErrorCollector.
     *
     * @return true if at least one error was recorded.
     *
     * @example
     * @code
     * auto result = parser.parse(buffer.data(), buffer.size());
     * if (result.has_errors()) {
     *     std::cerr << "Parsing encountered errors\n";
     * }
     * @endcode
     */
    bool has_errors() const { return error_collector_.has_errors(); }

    /**
     * @brief Check if any fatal errors were recorded during parsing.
     *
     * Fatal errors indicate unrecoverable parsing failures, such as
     * unclosed quotes at end of file.
     *
     * @return true if at least one FATAL error was recorded.
     */
    bool has_fatal_errors() const { return error_collector_.has_fatal_errors(); }

    /**
     * @brief Get the number of errors recorded during parsing.
     *
     * @return Number of errors in the internal error collector.
     */
    size_t error_count() const { return error_collector_.error_count(); }

    /**
     * @brief Get read-only access to all recorded errors.
     *
     * @return Const reference to the vector of ParseError objects.
     *
     * @example
     * @code
     * auto result = parser.parse(buffer.data(), buffer.size());
     * for (const auto& err : result.errors()) {
     *     std::cerr << err.to_string() << std::endl;
     * }
     * @endcode
     */
    const std::vector<ParseError>& errors() const { return error_collector_.errors(); }

    /**
     * @brief Get a summary string of all errors.
     *
     * @return Human-readable summary of error counts by type.
     *
     * @example
     * @code
     * auto result = parser.parse(buffer.data(), buffer.size());
     * if (result.has_errors()) {
     *     std::cerr << result.error_summary() << std::endl;
     * }
     * @endcode
     */
    std::string error_summary() const { return error_collector_.summary(); }

    /**
     * @brief Get the error handling mode used during parsing.
     *
     * @return The ErrorMode of the internal error collector.
     */
    ErrorMode error_mode() const { return error_collector_.mode(); }

    /**
     * @brief Get mutable access to the internal error collector.
     *
     * This method is primarily for internal use by Parser::parse() to
     * populate errors during parsing. Users should prefer the convenience
     * methods has_errors(), errors(), etc.
     *
     * @return Reference to the internal ErrorCollector.
     */
    ErrorCollector& error_collector() { return error_collector_; }

    /**
     * @brief Get read-only access to the internal error collector.
     *
     * @return Const reference to the internal ErrorCollector.
     */
    const ErrorCollector& error_collector() const { return error_collector_; }
  };

  /**
   * @brief Construct a Parser with the specified number of threads.
   * @param num_threads Number of threads to use for parsing (default: 1).
   *                    Use std::thread::hardware_concurrency() for CPU count.
   */
  explicit Parser(size_t num_threads = 1) : num_threads_(num_threads > 0 ? num_threads : 1) {}

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
  Result parse(const uint8_t* buf, size_t len, const ParseOptions& options = ParseOptions{}) {
    Result result;

    // SECURITY: Validate file size limits before any allocation
    if (options.limits.max_file_size > 0 && len > options.limits.max_file_size) {
      if (options.errors != nullptr) {
        options.errors->add_error(ErrorCode::FILE_TOO_LARGE, ErrorSeverity::FATAL, 1, 1, 0,
                                  "File size " + std::to_string(len) + " bytes exceeds maximum " +
                                      std::to_string(options.limits.max_file_size) + " bytes");
        result.error_collector().merge_from(*options.errors);
        result.successful = false;
        return result;
      } else {
        throw std::runtime_error("File size " + std::to_string(len) + " bytes exceeds maximum " +
                                 std::to_string(options.limits.max_file_size) + " bytes");
      }
    }

    // Initialize index with size limits (will validate overflow internally)
    result.idx = parser_.init_safe(len, num_threads_, options.errors);
    if (result.idx.indexes == nullptr) {
      // Allocation failed or would overflow
      if (options.errors != nullptr) {
        result.error_collector().merge_from(*options.errors);
      }
      result.successful = false;
      return result;
    }

    // UTF-8 validation (optional, enabled via SizeLimits::validate_utf8)
    if (options.limits.validate_utf8 && options.errors != nullptr) {
      validate_utf8_internal(buf, len, *options.errors);
      if (options.errors->should_stop()) {
        result.error_collector().merge_from(*options.errors);
        result.successful = false;
        return result;
      }
    }

    // Determine dialect (explicit or auto-detect)
    if (options.dialect.has_value()) {
      result.dialect = options.dialect.value();
    } else {
      // Auto-detect dialect
      DialectDetector detector(options.detection_options);
      result.detection = detector.detect(buf, len);
      result.dialect = result.detection.success() ? result.detection.dialect : Dialect::csv();
    }

    // Suppress deprecation warnings for internal calls to TwoPass methods
    // (Parser is the public API that wraps these deprecated methods)
    LIBVROOM_SUPPRESS_DEPRECATION_START

    // Select parsing implementation based on algorithm and error collection
    if (options.errors != nullptr) {
      // Error collection mode - algorithm selection determines implementation
      if (!options.dialect.has_value()) {
        // Auto-detect path with errors
        result.successful = parser_.parse_auto(buf, result.idx, len, *options.errors,
                                               &result.detection, options.detection_options);
        result.dialect = result.detection.dialect;
      } else {
        // Explicit dialect with errors - respect algorithm selection
        if (options.algorithm == ParseAlgorithm::BRANCHLESS) {
          // Use branchless implementation with error collection
          result.successful = parser_.parse_branchless_with_errors(buf, result.idx, len,
                                                                   *options.errors, result.dialect);
        } else {
          // Default to switch-based implementation (faster for error collection)
          result.successful =
              parser_.parse_with_errors(buf, result.idx, len, *options.errors, result.dialect);
        }
      }
      // Copy errors from external collector to internal collector
      result.error_collector().merge_from(*options.errors);
    } else {
      // Fast path (throws on error) - respects algorithm selection
      switch (options.algorithm) {
      case ParseAlgorithm::BRANCHLESS:
        result.successful = parser_.parse_branchless(buf, result.idx, len, result.dialect);
        break;
      case ParseAlgorithm::TWO_PASS:
        result.successful = parser_.parse_two_pass(buf, result.idx, len, result.dialect);
        break;
      case ParseAlgorithm::SPECULATIVE:
        result.successful = parser_.parse_speculate(buf, result.idx, len, result.dialect);
        break;
      case ParseAlgorithm::AUTO:
      default:
        // AUTO currently uses speculative (same as parse())
        result.successful = parser_.parse(buf, result.idx, len, result.dialect);
        break;
      }
    }

    LIBVROOM_SUPPRESS_DEPRECATION_END

    // Store buffer reference to enable row/column iteration
    result.set_buffer(buf, len);

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
  Result parse_with_errors(const uint8_t* buf, size_t len, ErrorCollector& errors,
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
  void set_num_threads(size_t num_threads) { num_threads_ = num_threads > 0 ? num_threads : 1; }

  /// @return Current number of threads configured for parsing.
  size_t num_threads() const { return num_threads_; }

private:
  TwoPass parser_;
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
