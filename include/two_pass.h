#ifndef TWO_PASS_H
#define TWO_PASS_H

/**
 * @file two_pass.h
 * @brief Internal implementation of the high-performance CSV parser.
 *
 * @warning **Do not include this header directly.** Use `#include "libvroom.h"`
 *          and the `Parser` class instead. This header contains internal
 *          implementation details that may change without notice.
 *
 * This header provides the core parsing functionality of the libvroom library.
 * The parser uses a speculative multi-threaded two-pass algorithm based on
 * research by Chang et al. (SIGMOD 2019) combined with SIMD techniques from
 * Langdale & Lemire (simdjson).
 *
 * ## Recommended Usage
 *
 * ```cpp
 * #include "libvroom.h"
 *
 * libvroom::Parser parser(num_threads);
 *
 * // Auto-detect dialect, throw on errors (simplest)
 * auto result = parser.parse(buf, len);
 *
 * // With error collection
 * libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);
 * auto result = parser.parse(buf, len, {.errors = &errors});
 *
 * // Explicit dialect
 * auto result = parser.parse(buf, len, {.dialect = libvroom::Dialect::tsv()});
 * ```
 *
 * ## Algorithm Overview
 *
 * 1. **First Pass**: Scans for line boundaries while tracking quote parity.
 *    Finds safe split points where the file can be divided for parallel
 * processing.
 *
 * 2. **Speculative Chunking**: The file is divided into chunks based on quote
 *    parity analysis. Multiple threads can speculatively parse chunks.
 *
 * 3. **Second Pass**: SIMD-based field indexing using a state machine.
 * Processes 64 bytes at a time using Google Highway portable SIMD intrinsics.
 *
 * @see libvroom::Parser for the unified public API
 * @see libvroom::ParseOptions for configuration options
 * @see libvroom::index for the result structure containing field positions
 */

#include "branchless_state_machine.h"
#include "dialect.h"
#include "error.h"
#include "simd_highway.h"
#include <cassert>
#include <cstdint>
#include <cstring> // for memcpy
#include <future>
#include <limits>
#include <sstream>
#include <unistd.h> // for getopt
#include <unordered_set>
#include <vector>

// Deprecation macro for cross-compiler support
#if defined(__GNUC__) || defined(__clang__)
#define LIBVROOM_DEPRECATED(msg) __attribute__((deprecated(msg)))
#elif defined(_MSC_VER)
#define LIBVROOM_DEPRECATED(msg) __declspec(deprecated(msg))
#else
#define LIBVROOM_DEPRECATED(msg)
#endif

// Macro to suppress deprecation warnings for internal use
// (Parser class needs to call deprecated methods)
#ifdef __GNUC__
#define LIBVROOM_SUPPRESS_DEPRECATION_START                                    \
  _Pragma("GCC diagnostic push")                                               \
      _Pragma("GCC diagnostic ignored \"-Wdeprecated-declarations\"")
#define LIBVROOM_SUPPRESS_DEPRECATION_END _Pragma("GCC diagnostic pop")
#elif defined(_MSC_VER)
#define LIBVROOM_SUPPRESS_DEPRECATION_START                                    \
  __pragma(warning(push)) __pragma(warning(disable : 4996))
#define LIBVROOM_SUPPRESS_DEPRECATION_END __pragma(warning(pop))
#else
#define LIBVROOM_SUPPRESS_DEPRECATION_START
#define LIBVROOM_SUPPRESS_DEPRECATION_END
#endif

namespace libvroom {

/// Sentinel value indicating an invalid or unset position.
constexpr static uint64_t null_pos = std::numeric_limits<uint64_t>::max();

/**
 * @brief Result structure containing parsed CSV field positions.
 *
 * The ParseIndex class stores the byte offsets of field separators (commas and
 * newlines) found during CSV parsing. These positions enable efficient random
 * access to individual fields without re-parsing the entire file.
 *
 * When using multi-threaded parsing, field positions are interleaved across
 * threads. For example, with 4 threads: thread 0 stores positions at indices 0,
 * 4, 8, ...; thread 1 stores at indices 1, 5, 9, ...; and so on.
 *
 * @note This class is move-only. Copy operations are deleted to prevent
 * accidental expensive copies of large index arrays.
 *
 * @note Memory management uses std::unique_ptr for automatic RAII cleanup. The
 *       raw pointer accessors (n_indexes, indexes) are provided for
 * compatibility with existing parsing code but ownership remains with the
 * unique_ptr members.
 *
 * @warning The caller must ensure the index remains valid while accessing the
 *          underlying buffer data. The index stores byte offsets, not the data
 * itself.
 *
 * @example
 * @code
 * // Create parser and initialize index
 * libvroom::Parser parser(num_threads);
 * auto result = parser.parse(buffer, length);
 *
 * // Access field positions from result.idx
 * // For single-threaded: positions are at result.idx.indexes[0],
 * result.idx.indexes[1], ...
 * // For multi-threaded: use stride of result.idx.n_threads
 * @endcode
 */
class ParseIndex {
public:
  /// Number of columns detected in the CSV (set after parsing header).
  uint64_t columns{0};

  /// Number of threads used for parsing. Determines the interleave stride.
  /// Changed from uint8_t to uint16_t to support systems with >255 cores.
  uint16_t n_threads{0};

  /// Array of size n_threads containing the count of indexes found by each
  /// thread.
  /// @note This is a raw pointer accessor for compatibility; memory is managed
  /// by n_indexes_ptr_.
  uint64_t *n_indexes{nullptr};

  /// Array of field separator positions (byte offsets). Interleaved by thread.
  /// @note This is a raw pointer accessor for compatibility; memory is managed
  /// by indexes_ptr_.
  uint64_t *indexes{nullptr};

  /// Default constructor. Creates an empty, uninitialized index.
  ParseIndex() = default;

  /**
   * @brief Move constructor.
   *
   * Transfers ownership of index arrays from another ParseIndex object.
   *
   * @param other The ParseIndex to move from. Will be left in a valid but empty
   * state.
   */
  ParseIndex(ParseIndex &&other) noexcept
      : columns(other.columns), n_threads(other.n_threads),
        n_indexes(other.n_indexes), indexes(other.indexes),
        n_indexes_ptr_(std::move(other.n_indexes_ptr_)),
        indexes_ptr_(std::move(other.indexes_ptr_)) {
    other.n_indexes = nullptr;
    other.indexes = nullptr;
  }

  /**
   * @brief Move assignment operator.
   *
   * Releases current resources and takes ownership from another ParseIndex.
   *
   * @param other The ParseIndex to move from. Will be left in a valid but empty
   * state.
   * @return Reference to this ParseIndex.
   */
  ParseIndex &operator=(ParseIndex &&other) noexcept {
    if (this != &other) {
      columns = other.columns;
      n_threads = other.n_threads;
      n_indexes = other.n_indexes;
      indexes = other.indexes;
      n_indexes_ptr_ = std::move(other.n_indexes_ptr_);
      indexes_ptr_ = std::move(other.indexes_ptr_);
      other.n_indexes = nullptr;
      other.indexes = nullptr;
    }
    return *this;
  }

  // Delete copy operations to prevent accidental copies
  ParseIndex(const ParseIndex &) = delete;
  ParseIndex &operator=(const ParseIndex &) = delete;

  /**
   * @brief Serialize the index to a binary file.
   *
   * Writes the index structure to disk for later retrieval, avoiding the need
   * to re-parse large CSV files.
   *
   * @param filename Path to the output file.
   * @throws std::runtime_error If writing fails.
   */
  void write(const std::string &filename);

  /**
   * @brief Deserialize the index from a binary file.
   *
   * Reads a previously saved index structure from disk.
   *
   * @param filename Path to the input file.
   * @throws std::runtime_error If reading fails.
   *
   * @warning The n_indexes and indexes arrays must be pre-allocated before
   * calling.
   */
  void read(const std::string &filename);

  /**
   * @brief Destructor. Releases allocated index arrays via RAII.
   *
   * Memory is automatically freed when the unique_ptr members are destroyed.
   */
  ~ParseIndex() = default;

  void fill_double_array(ParseIndex *idx, uint64_t column, double *out) {}

private:
  /// RAII owner for n_indexes array. Memory freed automatically on destruction.
  std::unique_ptr<uint64_t[]> n_indexes_ptr_;

  /// RAII owner for indexes array. Memory freed automatically on destruction.
  std::unique_ptr<uint64_t[]> indexes_ptr_;

  // Allow TwoPass::init() to set the private members
  friend class TwoPass;
};

/// @brief Backward-compatible alias for ParseIndex.
/// @deprecated Use ParseIndex instead. This alias will be removed in a future
/// version.
using index [[deprecated("Use ParseIndex instead")]] = ParseIndex;

/**
 * @brief High-performance CSV parser using a speculative two-pass algorithm.
 *
 * The two_pass class implements a multi-threaded CSV parsing algorithm that
 * achieves high performance through SIMD operations and speculative parallel
 * processing. The algorithm is based on research by Chang et al. (SIGMOD 2019)
 * combined with SIMD techniques from Langdale & Lemire (simdjson).
 *
 * The parsing algorithm works in two phases:
 *
 * 1. **First Pass**: Scans the file to find safe split points where the file
 *    can be divided for parallel processing. Tracks quote parity to ensure
 *    chunks don't split in the middle of quoted fields.
 *
 * 2. **Second Pass**: Each thread parses its assigned chunk using a state
 *    machine to identify field boundaries. Results are stored in an interleaved
 *    format in the index structure.
 *
 * @note Thread Safety: The parser itself is stateless and thread-safe.
 *       However, each index object should only be accessed by one thread
 *       during parsing. Multiple parsers can run concurrently with separate
 *       index objects.
 *
 * @example
 * @code
 * #include "two_pass.h"
 * #include "io_util.h"
 *
 * // Load CSV file with SIMD-aligned padding
 * auto [buffer, length] = libvroom::load_file("data.csv");
 *
 * // Create parser and initialize index
 * libvroom::two_pass parser;
 * libvroom::index idx = parser.init(length, 4);  // 4 threads
 *
 * // Parse without error collection (throws on error)
 * parser.parse(buffer, idx, length);
 *
 * // Or parse with error collection
 * libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);
 * bool success = parser.parse_with_errors(buffer, idx, length, errors);
 *
 * if (!success || errors.error_count() > 0) {
 *     for (const auto& err : errors.get_errors()) {
 *         std::cerr << "Line " << err.line << ": " << err.message << "\n";
 *     }
 * }
 * @endcode
 *
 * @see index For the result structure containing field positions
 * @see ErrorCollector For error handling during parsing
 */
class TwoPass {
public:
  /**
   * @brief Statistics from the first pass of parsing.
   *
   * The Stats structure contains information gathered during the first pass
   * that is used to determine safe chunk boundaries for multi-threaded parsing.
   *
   * @note These statistics are primarily for internal use by the parser's
   *       multi-threading logic.
   */
  struct Stats {
    /// Total number of quote characters found in the chunk.
    uint64_t n_quotes{0};

    /// Position of first newline at even quote count (safe split point if
    /// unquoted). Set to null_pos if no such newline exists.
    uint64_t first_even_nl{null_pos};

    /// Position of first newline at odd quote count (safe split point if
    /// quoted). Set to null_pos if no such newline exists.
    uint64_t first_odd_nl{null_pos};
  };
  /**
   * @brief First pass SIMD scan with dialect-aware quote character.
   */
  static Stats first_pass_simd(const uint8_t *buf, size_t start, size_t end,
                               char quote_char = '"') {
    Stats out;
    assert(end >= start && "Invalid range: end must be >= start");
    size_t len = end - start;
    size_t idx = 0;
    bool needs_even = out.first_even_nl == null_pos;
    bool needs_odd = out.first_odd_nl == null_pos;
    buf += start;
    for (; idx < len; idx += 64) {
      __builtin_prefetch(buf + idx + 128);

      simd_input in = fill_input(buf + idx);
      uint64_t mask = ~0ULL;

      /* TODO: look into removing branches if possible */
      if (len - idx < 64) {
        mask = blsmsk_u64(1ULL << (len - idx));
      }

      uint64_t quotes =
          cmp_mask_against_input(in, static_cast<uint8_t>(quote_char)) & mask;

      if (needs_even || needs_odd) {
        // Support LF, CRLF, and CR-only line endings
        uint64_t nl = compute_line_ending_mask_simple(in, mask);
        if (nl == 0) {
          continue;
        }
        if (needs_even) {
          uint64_t quote_mask2 = find_quote_mask(quotes, ~0ULL) & mask;
          uint64_t even_nl = quote_mask2 & nl;
          if (even_nl > 0) {
            out.first_even_nl = start + idx + trailing_zeroes(even_nl);
          }
          needs_even = false;
        }
        if (needs_odd) {
          uint64_t quote_mask = find_quote_mask(quotes, 0ULL) & mask;
          uint64_t odd_nl = quote_mask & nl & mask;
          if (odd_nl > 0) {
            out.first_odd_nl = start + idx + trailing_zeroes(odd_nl);
          }
          needs_odd = false;
        }
      }

      out.n_quotes += count_ones(quotes);
    }
    return out;
  }

  /**
   * @brief First pass scalar scan with dialect-aware quote character.
   */
  static Stats first_pass_chunk(const uint8_t *buf, size_t start, size_t end,
                                char quote_char = '"');

  static Stats first_pass_naive(const uint8_t *buf, size_t start, size_t end);

  /**
   * @brief Check if character is not a delimiter, newline (LF or CR), or quote.
   */
  static bool is_other(uint8_t c, char delimiter = ',', char quote_char = '"') {
    return c != static_cast<uint8_t>(delimiter) && c != '\n' && c != '\r' &&
           c != static_cast<uint8_t>(quote_char);
  }

  enum quote_state { AMBIGUOUS, QUOTED, UNQUOTED };

  /**
   * @brief Determine quote state at a position using backward scanning.
   */
  static quote_state get_quotation_state(const uint8_t *buf, size_t start,
                                         char delimiter = ',',
                                         char quote_char = '"');

  /**
   * @brief Speculative first pass with dialect-aware quote character.
   */
  static Stats first_pass_speculate(const uint8_t *buf, size_t start,
                                    size_t end, char delimiter = ',',
                                    char quote_char = '"');

  /**
   * @brief Second pass SIMD scan with dialect-aware delimiter and quote
   * character.
   */
  static uint64_t second_pass_simd(const uint8_t *buf, size_t start, size_t end,
                                   ParseIndex *out, size_t thread_id,
                                   char delimiter = ',',
                                   char quote_char = '"') {
    bool is_quoted = false;
    assert(end >= start && "Invalid range: end must be >= start");
    size_t len = end - start;
    uint64_t idx = 0;
    size_t n_indexes = 0;
    size_t i = thread_id;
    uint64_t prev_iter_inside_quote = 0ULL; // either all zeros or all ones
    uint64_t base = 0;
    buf += start;

    for (; idx < len; idx += 64) {
      __builtin_prefetch(buf + idx + 128);
      simd_input in = fill_input(buf + idx);

      uint64_t mask = ~0ULL;

      if (len - idx < 64) {
        mask = blsmsk_u64(1ULL << (len - idx));
      }

      uint64_t quotes =
          cmp_mask_against_input(in, static_cast<uint8_t>(quote_char)) & mask;

      uint64_t quote_mask = find_quote_mask2(quotes, prev_iter_inside_quote);
      uint64_t sep =
          cmp_mask_against_input(in, static_cast<uint8_t>(delimiter)) & mask;
      // Support LF, CRLF, and CR-only line endings
      uint64_t end_mask = compute_line_ending_mask_simple(in, mask);
      uint64_t field_sep = (end_mask | sep) & ~quote_mask;
      n_indexes += write(out->indexes + thread_id, base, start + idx,
                         out->n_threads, field_sep);
    }
    return n_indexes;
  }

  /**
   * @brief Branchless SIMD second pass using lookup table state machine.
   *
   * This method uses the branchless state machine implementation which
   * eliminates branch mispredictions by using precomputed lookup tables for
   * character classification and state transitions.
   *
   * Performance characteristics:
   * - Eliminates 90%+ of branches in the parsing hot path
   * - Uses SIMD for parallel character classification
   * - Single memory access per character for classification
   * - Single memory access per character for state transition
   *
   * @param sm Pre-initialized branchless state machine
   * @param buf Input buffer
   * @param start Start position in buffer
   * @param end End position in buffer
   * @param out Index structure to store results
   * @param thread_id Thread ID for interleaved storage
   * @return Number of field separators found
   */
  static uint64_t second_pass_simd_branchless(const BranchlessStateMachine &sm,
                                              const uint8_t *buf, size_t start,
                                              size_t end, ParseIndex *out,
                                              size_t thread_id) {
    return libvroom::second_pass_simd_branchless(
        sm, buf, start, end, out->indexes, thread_id, out->n_threads);
  }

  /**
   * @brief Parser state machine states for CSV field parsing.
   *
   * The CSV parser uses a finite state machine to track its position within
   * the CSV structure. Each character transition updates the state based on
   * whether it's a quote, comma, newline, or other character.
   *
   * State transitions:
   * - RECORD_START + '"' -> QUOTED_FIELD
   * - RECORD_START + ',' -> FIELD_START
   * - RECORD_START + '\n' -> RECORD_START
   * - RECORD_START + other -> UNQUOTED_FIELD
   * - QUOTED_FIELD + '"' -> QUOTED_END (potential close or escape)
   * - QUOTED_END + '"' -> QUOTED_FIELD (escaped quote)
   * - QUOTED_END + ',' -> FIELD_START (field ended)
   * - QUOTED_END + '\n' -> RECORD_START (record ended)
   */
  enum csv_state {
    RECORD_START,   ///< At the beginning of a new record (row).
    FIELD_START,    ///< At the beginning of a new field (after comma).
    UNQUOTED_FIELD, ///< Inside an unquoted field.
    QUOTED_FIELD,   ///< Inside a quoted field.
    QUOTED_END ///< Just saw a quote inside a quoted field (might be closing or
               ///< escape).
  };

  // Error result from state transitions
  struct StateResult {
    csv_state state;
    ErrorCode error;
  };

  really_inline static StateResult quoted_state(csv_state in) {
    // LCOV_EXCL_BR_START - State machine branches are covered by integration
    // tests
    switch (in) {
    case RECORD_START:
      return {QUOTED_FIELD, ErrorCode::NONE};
    case FIELD_START:
      return {QUOTED_FIELD, ErrorCode::NONE};
    case UNQUOTED_FIELD:
      // Quote in middle of unquoted field
      return {UNQUOTED_FIELD, ErrorCode::QUOTE_IN_UNQUOTED_FIELD};
    case QUOTED_FIELD:
      return {QUOTED_END, ErrorCode::NONE};
    case QUOTED_END:
      return {QUOTED_FIELD, ErrorCode::NONE};
    }
    // LCOV_EXCL_BR_STOP
    return {in, ErrorCode::INTERNAL_ERROR}; // LCOV_EXCL_LINE - unreachable
  }

  really_inline static StateResult comma_state(csv_state in) {
    // LCOV_EXCL_BR_START - State machine branches are covered by integration
    // tests
    switch (in) {
    case RECORD_START:
      return {FIELD_START, ErrorCode::NONE};
    case FIELD_START:
      return {FIELD_START, ErrorCode::NONE};
    case UNQUOTED_FIELD:
      return {FIELD_START, ErrorCode::NONE};
    case QUOTED_FIELD:
      return {QUOTED_FIELD, ErrorCode::NONE};
    case QUOTED_END:
      return {FIELD_START, ErrorCode::NONE};
    }
    // LCOV_EXCL_BR_STOP
    return {in, ErrorCode::INTERNAL_ERROR}; // LCOV_EXCL_LINE - unreachable
  }

  really_inline static StateResult newline_state(csv_state in) {
    // LCOV_EXCL_BR_START - State machine branches are covered by integration
    // tests
    switch (in) {
    case RECORD_START:
      return {RECORD_START, ErrorCode::NONE};
    case FIELD_START:
      return {RECORD_START, ErrorCode::NONE};
    case UNQUOTED_FIELD:
      return {RECORD_START, ErrorCode::NONE};
    case QUOTED_FIELD:
      return {QUOTED_FIELD, ErrorCode::NONE};
    case QUOTED_END:
      return {RECORD_START, ErrorCode::NONE};
    }
    // LCOV_EXCL_BR_STOP
    return {in, ErrorCode::INTERNAL_ERROR}; // LCOV_EXCL_LINE - unreachable
  }

  really_inline static StateResult other_state(csv_state in) {
    // LCOV_EXCL_BR_START - State machine branches are covered by integration
    // tests
    switch (in) {
    case RECORD_START:
      return {UNQUOTED_FIELD, ErrorCode::NONE};
    case FIELD_START:
      return {UNQUOTED_FIELD, ErrorCode::NONE};
    case UNQUOTED_FIELD:
      return {UNQUOTED_FIELD, ErrorCode::NONE};
    case QUOTED_FIELD:
      return {QUOTED_FIELD, ErrorCode::NONE};
    case QUOTED_END:
      // Invalid character after closing quote
      return {UNQUOTED_FIELD, ErrorCode::INVALID_QUOTE_ESCAPE};
    }
    // LCOV_EXCL_BR_STOP
    return {in, ErrorCode::INTERNAL_ERROR}; // LCOV_EXCL_LINE - unreachable
  }

  really_inline static size_t add_position(ParseIndex *out, size_t i,
                                           size_t pos) {
    out->indexes[i] = pos;
    return i + out->n_threads;
  }

  // Default context size for error messages (characters before/after error
  // position)
  static constexpr size_t DEFAULT_ERROR_CONTEXT_SIZE = 20;

  // Helper to get context around an error position
  // Returns a string representation of the buffer content near the given
  // position
  static std::string
  get_context(const uint8_t *buf, size_t len, size_t pos,
              size_t context_size = DEFAULT_ERROR_CONTEXT_SIZE);

  // Helper to calculate line and column from byte offset
  // SECURITY: buf_len parameter ensures we never read past buffer bounds
  static void get_line_column(const uint8_t *buf, size_t buf_len, size_t offset,
                              size_t &line, size_t &column);

  /**
   * @brief Second pass with error collection and dialect support.
   */
  static uint64_t second_pass_chunk(const uint8_t *buf, size_t start,
                                    size_t end, ParseIndex *out,
                                    size_t thread_id,
                                    ErrorCollector *errors = nullptr,
                                    size_t total_len = 0, char delimiter = ',',
                                    char quote_char = '"');

  /**
   * @brief Second pass that throws on error (backward compatible), with dialect
   * support.
   */
  static uint64_t second_pass_chunk_throwing(const uint8_t *buf, size_t start,
                                             size_t end, ParseIndex *out,
                                             size_t thread_id,
                                             char delimiter = ',',
                                             char quote_char = '"');

  /**
   * @brief Parse using speculative multi-threading with dialect support.
   *
   * @deprecated Use Parser::parse() with ParseOptions{.algorithm =
   * ParseAlgorithm::SPECULATIVE} instead. This method will be made private in a
   * future version.
   */
  LIBVROOM_DEPRECATED(
      "Use Parser::parse() with ParseAlgorithm::SPECULATIVE instead")
  bool parse_speculate(const uint8_t *buf, ParseIndex &out, size_t len,
                       const Dialect &dialect = Dialect::csv());

  /**
   * @brief Parse using two-pass algorithm with dialect support.
   *
   * @deprecated Use Parser::parse() with ParseOptions{.algorithm =
   * ParseAlgorithm::TWO_PASS} instead. This method will be made private in a
   * future version.
   */
  LIBVROOM_DEPRECATED(
      "Use Parser::parse() with ParseAlgorithm::TWO_PASS instead")
  bool parse_two_pass(const uint8_t *buf, ParseIndex &out, size_t len,
                      const Dialect &dialect = Dialect::csv());

  /**
   * @brief Parse a CSV buffer and build the field index.
   *
   * @deprecated Use Parser::parse() from libvroom.h instead. The Parser class
   *             provides a simpler, unified API with automatic index
   * management.
   */
  LIBVROOM_DEPRECATED("Use Parser::parse() from libvroom.h instead")
  bool parse(const uint8_t *buf, ParseIndex &out, size_t len,
             const Dialect &dialect = Dialect::csv());

  // Result from multi-threaded branchless parsing with error collection
  struct branchless_chunk_result {
    uint64_t n_indexes;
    ErrorCollector errors;

    branchless_chunk_result() : n_indexes(0), errors(ErrorMode::PERMISSIVE) {}
  };

  /**
   * @brief Static wrapper for thread-safe branchless parsing with error
   * collection.
   */
  static branchless_chunk_result second_pass_branchless_chunk_with_errors(
      const BranchlessStateMachine &sm, const uint8_t *buf, size_t start,
      size_t end, ParseIndex *out, size_t thread_id, size_t total_len,
      ErrorMode mode);

  /**
   * @brief Parse a CSV buffer using branchless state machine with error
   * collection.
   */
  bool parse_branchless_with_errors(const uint8_t *buf, ParseIndex &out,
                                    size_t len, ErrorCollector &errors,
                                    const Dialect &dialect = Dialect::csv());

  /**
   * @brief Parse a CSV buffer using branchless state machine (optimized).
   *
   * @deprecated Use Parser::parse() with ParseOptions::branchless() instead.
   */
  LIBVROOM_DEPRECATED(
      "Use Parser::parse() with ParseOptions::branchless() instead")
  bool parse_branchless(const uint8_t *buf, ParseIndex &out, size_t len,
                        const Dialect &dialect = Dialect::csv());

  /**
   * @brief Parse a CSV buffer with automatic dialect detection.
   *
   * @deprecated Use Parser::parse() with default ParseOptions (auto-detects
   * dialect).
   */
  LIBVROOM_DEPRECATED("Use Parser::parse() with {.errors = &errors} instead "
                      "(auto-detects dialect)")
  bool
  parse_auto(const uint8_t *buf, ParseIndex &out, size_t len,
             ErrorCollector &errors, DetectionResult *detected = nullptr,
             const DetectionOptions &detection_options = DetectionOptions());

  /**
   * @brief Detect the dialect of a CSV buffer without parsing.
   */
  static DetectionResult
  detect_dialect(const uint8_t *buf, size_t len,
                 const DetectionOptions &options = DetectionOptions());

  // Result from multi-threaded parsing with error collection
  struct chunk_result {
    uint64_t n_indexes;
    ErrorCollector errors;

    chunk_result() : n_indexes(0), errors(ErrorMode::PERMISSIVE) {}
  };

  /**
   * @brief Static wrapper for thread-safe parsing with error collection and
   * dialect.
   */
  static chunk_result
  second_pass_chunk_with_errors(const uint8_t *buf, size_t start, size_t end,
                                ParseIndex *out, size_t thread_id,
                                size_t total_len, ErrorMode mode,
                                char delimiter = ',', char quote_char = '"');

  /**
   * @brief Parse a CSV buffer with error collection using multi-threading.
   *
   * @deprecated Use Parser::parse() with {.dialect = ..., .errors = &errors}
   * instead.
   */
  LIBVROOM_DEPRECATED(
      "Use Parser::parse() with {.dialect = ..., .errors = &errors} instead")
  bool parse_two_pass_with_errors(const uint8_t *buf, ParseIndex &out,
                                  size_t len, ErrorCollector &errors,
                                  const Dialect &dialect = Dialect::csv());

  /**
   * @brief Parse a CSV buffer with detailed error collection (single-threaded).
   *
   * @deprecated Use Parser::parse() with {.dialect = ..., .errors = &errors}
   * instead.
   */
  LIBVROOM_DEPRECATED(
      "Use Parser::parse() with {.dialect = ..., .errors = &errors} instead")
  bool parse_with_errors(const uint8_t *buf, ParseIndex &out, size_t len,
                         ErrorCollector &errors,
                         const Dialect &dialect = Dialect::csv());

  // Check for empty header
  static bool check_empty_header(const uint8_t *buf, size_t len,
                                 ErrorCollector &errors);

  /**
   * @brief Check for duplicate column names in header with dialect support.
   */
  static void check_duplicate_columns(const uint8_t *buf, size_t len,
                                      ErrorCollector &errors,
                                      char delimiter = ',',
                                      char quote_char = '"');

  /**
   * @brief Check for inconsistent field counts with dialect support.
   */
  static void check_field_counts(const uint8_t *buf, size_t len,
                                 ErrorCollector &errors, char delimiter = ',',
                                 char quote_char = '"');

  // Check for mixed line endings
  static void check_line_endings(const uint8_t *buf, size_t len,
                                 ErrorCollector &errors);

  /**
   * @brief Perform full CSV validation with comprehensive error checking.
   *
   * @deprecated Use Parser::parse() with {.dialect = ..., .errors = &errors}
   * instead.
   */
  LIBVROOM_DEPRECATED(
      "Use Parser::parse() with {.dialect = ..., .errors = &errors} instead")
  bool parse_validate(const uint8_t *buf, ParseIndex &out, size_t len,
                      ErrorCollector &errors,
                      const Dialect &dialect = Dialect::csv());

  /**
   * @brief Initialize an index structure for parsing.
   */
  ParseIndex init(size_t len, size_t n_threads);

  /**
   * @brief Initialize an index structure with overflow validation.
   */
  ParseIndex init_safe(size_t len, size_t n_threads,
                       ErrorCollector *errors = nullptr);
};

/// @brief Backward-compatible alias for TwoPass.
/// @deprecated Use TwoPass instead. This alias will be removed in a future
/// version.
using two_pass [[deprecated("Use TwoPass instead")]] = TwoPass;

} // namespace libvroom

#endif // TWO_PASS_H
