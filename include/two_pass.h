/**
 * @file two_pass.h
 * @brief High-performance CSV parser using a speculative two-pass algorithm.
 *
 * This header provides the core parsing functionality of the simdcsv library.
 * The parser uses a speculative multi-threaded two-pass algorithm based on
 * research by Chang et al. (SIGMOD 2019) combined with SIMD techniques from
 * Langdale & Lemire (simdjson).
 *
 * The algorithm works as follows:
 * 1. **First Pass**: Scans for line boundaries while tracking quote parity.
 *    Finds safe split points where the file can be divided for parallel processing.
 *
 * 2. **Speculative Chunking**: The file is divided into chunks based on quote
 *    parity analysis. Multiple threads can speculatively parse chunks.
 *
 * 3. **Second Pass**: SIMD-based field indexing using a state machine. Processes
 *    64 bytes at a time using Google Highway portable SIMD intrinsics.
 *
 * @see index for the result structure containing field positions
 * @see two_pass for the main parser class
 * @see ErrorCollector for error handling during parsing
 */

#include <unistd.h>  // for getopt
#include <cstdint>
#include <future>
#include <limits>
#include <vector>
#include <cstring>  // for memcpy
#include <unordered_set>
#include <sstream>
#include "inttypes.h"
#include "simd_highway.h"
#include "error.h"
#include "dialect.h"
#include "branchless_state_machine.h"

namespace simdcsv {

/// Sentinel value indicating an invalid or unset position.
constexpr static uint64_t null_pos = std::numeric_limits<uint64_t>::max();

/**
 * @brief Result structure containing parsed CSV field positions.
 *
 * The index class stores the byte offsets of field separators (commas and newlines)
 * found during CSV parsing. These positions enable efficient random access to
 * individual fields without re-parsing the entire file.
 *
 * When using multi-threaded parsing, field positions are interleaved across threads.
 * For example, with 4 threads: thread 0 stores positions at indices 0, 4, 8, ...;
 * thread 1 stores at indices 1, 5, 9, ...; and so on.
 *
 * @note This class is move-only. Copy operations are deleted to prevent accidental
 *       expensive copies of large index arrays.
 *
 * @warning The caller must ensure the index remains valid while accessing the
 *          underlying buffer data. The index stores byte offsets, not the data itself.
 *
 * @example
 * @code
 * // Create parser and initialize index
 * simdcsv::two_pass parser;
 * simdcsv::index idx = parser.init(buffer_length, num_threads);
 *
 * // Parse the CSV data
 * parser.parse(buffer, idx, buffer_length);
 *
 * // Access field positions
 * // For single-threaded: positions are at idx.indexes[0], idx.indexes[1], ...
 * // For multi-threaded: use stride of idx.n_threads
 * @endcode
 */
class index {
 public:
  /// Number of columns detected in the CSV (set after parsing header).
  uint64_t columns{0};

  /// Number of threads used for parsing. Determines the interleave stride.
  uint8_t n_threads{0};

  /// Array of size n_threads containing the count of indexes found by each thread.
  uint64_t* n_indexes{nullptr};

  /// Array of field separator positions (byte offsets). Interleaved by thread.
  uint64_t* indexes{nullptr};

  /// Default constructor. Creates an empty, uninitialized index.
  index() = default;

  /**
   * @brief Move constructor.
   *
   * Transfers ownership of index arrays from another index object.
   *
   * @param other The index to move from. Will be left in a valid but empty state.
   */
  index(index&& other) noexcept
      : columns(other.columns),
        n_threads(other.n_threads),
        n_indexes(other.n_indexes),
        indexes(other.indexes) {
    other.n_indexes = nullptr;
    other.indexes = nullptr;
  }

  /**
   * @brief Move assignment operator.
   *
   * Releases current resources and takes ownership from another index.
   *
   * @param other The index to move from. Will be left in a valid but empty state.
   * @return Reference to this index.
   */
  index& operator=(index&& other) noexcept {
    if (this != &other) {
      delete[] indexes;
      delete[] n_indexes;
      columns = other.columns;
      n_threads = other.n_threads;
      n_indexes = other.n_indexes;
      indexes = other.indexes;
      other.n_indexes = nullptr;
      other.indexes = nullptr;
    }
    return *this;
  }

  // Delete copy operations to prevent accidental copies
  index(const index&) = delete;
  index& operator=(const index&) = delete;

  /**
   * @brief Serialize the index to a binary file.
   *
   * Writes the index structure to disk for later retrieval, avoiding the need
   * to re-parse large CSV files.
   *
   * @param filename Path to the output file.
   * @throws std::runtime_error If writing fails.
   */
  void write(const std::string& filename) {
    std::FILE* fp = std::fopen(filename.c_str(), "wb");
    if (!((std::fwrite(&columns, sizeof(uint64_t), 1, fp) == 1) &&
          (std::fwrite(&n_threads, sizeof(uint8_t), 1, fp) == 1) &&
          (std::fwrite(n_indexes, sizeof(uint64_t), n_threads, fp) == n_threads))) {
      throw std::runtime_error("error writing index");
    }
    size_t total_size = 0;
    for (int i = 0; i < n_threads; ++i) {
      total_size += n_indexes[i];
    }
    if (std::fwrite(indexes, sizeof(uint64_t), total_size, fp) != total_size) {
      throw std::runtime_error("error writing index2");
    }

    std::fclose(fp);
  }

  /**
   * @brief Deserialize the index from a binary file.
   *
   * Reads a previously saved index structure from disk.
   *
   * @param filename Path to the input file.
   * @throws std::runtime_error If reading fails.
   *
   * @warning The n_indexes and indexes arrays must be pre-allocated before calling.
   */
  void read(const std::string& filename) {
    std::FILE* fp = std::fopen(filename.c_str(), "rb");
    if (!((std::fread(&columns, sizeof(uint64_t), 1, fp) == 1) &&
          (std::fread(&n_threads, sizeof(uint8_t), 1, fp) == 1) &&
          (std::fread(n_indexes, sizeof(uint64_t), n_threads, fp) == n_threads))) {
      throw std::runtime_error("error reading index");
    }
    size_t total_size = 0;
    for (int i = 0; i < n_threads; ++i) {
      total_size += n_indexes[i];
    }
    if (std::fread(indexes, sizeof(uint64_t), total_size, fp) != total_size) {
      throw std::runtime_error("error reading index2");
    }

    std::fclose(fp);
  }

  /**
   * @brief Destructor. Releases allocated index arrays.
   */
  ~index() {
    if (indexes) {
      delete[] indexes;
    }
    if (n_indexes) {
      delete[] n_indexes;
    }
  }

  void fill_double_array(index* idx, uint64_t column, double* out) {}
};

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
 * auto [buffer, length] = simdcsv::load_file("data.csv");
 *
 * // Create parser and initialize index
 * simdcsv::two_pass parser;
 * simdcsv::index idx = parser.init(length, 4);  // 4 threads
 *
 * // Parse without error collection (throws on error)
 * parser.parse(buffer, idx, length);
 *
 * // Or parse with error collection
 * simdcsv::ErrorCollector errors(simdcsv::ErrorMode::PERMISSIVE);
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
class two_pass {
 public:
  /**
   * @brief Statistics from the first pass of parsing.
   *
   * The stats structure contains information gathered during the first pass
   * that is used to determine safe chunk boundaries for multi-threaded parsing.
   *
   * @note These statistics are primarily for internal use by the parser's
   *       multi-threading logic.
   */
  struct stats {
    /// Total number of quote characters found in the chunk.
    uint64_t n_quotes{0};

    /// Position of first newline at even quote count (safe split point if unquoted).
    /// Set to null_pos if no such newline exists.
    uint64_t first_even_nl{null_pos};

    /// Position of first newline at odd quote count (safe split point if quoted).
    /// Set to null_pos if no such newline exists.
    uint64_t first_odd_nl{null_pos};
  };
  /**
   * @brief First pass SIMD scan with dialect-aware quote character.
   */
  static stats first_pass_simd(const uint8_t* buf, size_t start, size_t end,
                               char quote_char = '"') {
    stats out;
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

      uint64_t quotes = cmp_mask_against_input(in, static_cast<uint8_t>(quote_char)) & mask;

      if (needs_even || needs_odd) {
        uint64_t nl = cmp_mask_against_input(in, '\n') & mask;
        if (nl == 0) {
          continue;
        }
        if (needs_even) {
          uint64_t quote_mask2 = find_quote_mask(in, quotes, ~0ULL) & mask;
          uint64_t even_nl = quote_mask2 & nl;
          if (even_nl > 0) {
            out.first_even_nl = start + idx + trailing_zeroes(even_nl);
          }
          needs_even = false;
        }
        if (needs_odd) {
          uint64_t quote_mask = find_quote_mask(in, quotes, 0ULL) & mask;
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
  static stats first_pass_chunk(const uint8_t* buf, size_t start, size_t end,
                                char quote_char = '"') {
    stats out;
    uint64_t i = start;
    bool needs_even = out.first_even_nl == null_pos;
    bool needs_odd = out.first_odd_nl == null_pos;
    while (i < end) {
      if (buf[i] == '\n') {
        bool is_even = (out.n_quotes % 2) == 0;
        if (needs_even && is_even) {
          out.first_even_nl = i;
          needs_even = false;
        } else if (needs_odd && !is_even) {
          out.first_odd_nl = i;
          needs_odd = false;
        }
      } else if (buf[i] == static_cast<uint8_t>(quote_char)) {
        ++out.n_quotes;
      }
      ++i;
    }
    return out;
  }

  static stats first_pass_naive(const uint8_t* buf, size_t start, size_t end) {
    stats out;
    uint64_t i = start;
    while (i < end) {
      if (buf[i] == '\n') {
        out.first_even_nl = i;
        return out;
      }
      ++i;
    }
    return out;
  }

  /**
   * @brief Check if character is not a delimiter, newline, or quote.
   */
  static bool is_other(uint8_t c, char delimiter = ',', char quote_char = '"') {
    return c != static_cast<uint8_t>(delimiter) && c != '\n' && c != static_cast<uint8_t>(quote_char);
  }

  enum quote_state { AMBIGUOUS, QUOTED, UNQUOTED };

  /**
   * @brief Determine quote state at a position using backward scanning.
   */
  static quote_state get_quotation_state(const uint8_t* buf, size_t start,
                                         char delimiter = ',', char quote_char = '"') {
    // 64kb
    constexpr int SPECULATION_SIZE = 1 << 16;

    if (start == 0) {
      return UNQUOTED;
    }

    size_t end = start > SPECULATION_SIZE ? start - SPECULATION_SIZE : 0;
    size_t i = start;
    size_t num_quotes = 0;

    // FIXED: Use i > end to avoid unsigned underflow when i reaches 0
    while (i > end) {
      if (buf[i] == static_cast<uint8_t>(quote_char)) {
        // q-o case
        if (i + 1 < start && is_other(buf[i + 1], delimiter, quote_char)) {
          return num_quotes % 2 == 0 ? QUOTED : UNQUOTED;
        }

        // o-q case
        else if (i > end && is_other(buf[i - 1], delimiter, quote_char)) {
          return num_quotes % 2 == 0 ? UNQUOTED : QUOTED;
        }
        ++num_quotes;
      }
      --i;
    }
    // Check the last position (i == end)
    if (buf[end] == static_cast<uint8_t>(quote_char)) {
      ++num_quotes;
    }
    return AMBIGUOUS;
  }

  /**
   * @brief Speculative first pass with dialect-aware quote character.
   */
  static stats first_pass_speculate(const uint8_t* buf, size_t start, size_t end,
                                    char delimiter = ',', char quote_char = '"') {
    auto is_quoted = get_quotation_state(buf, start, delimiter, quote_char);
#ifndef SIMDCSV_BENCHMARK_MODE
    printf("start: %lu\tis_ambigious: %s\tstate: %s\n", start,
           is_quoted == AMBIGUOUS ? "true" : "false",
           is_quoted == QUOTED ? "quoted" : "unquoted");
#endif

    for (size_t i = start; i < end; ++i) {
      if (buf[i] == '\n') {
        if (is_quoted == UNQUOTED || is_quoted == AMBIGUOUS) {
          return {0, i, null_pos};
        } else {
          return {1, null_pos, i};
        }
      } else if (buf[i] == static_cast<uint8_t>(quote_char)) {
        is_quoted = is_quoted == UNQUOTED ? QUOTED : UNQUOTED;
      }
    }
    return {0, null_pos, null_pos};
  }

  /**
   * @brief Second pass SIMD scan with dialect-aware delimiter and quote character.
   */
  static uint64_t second_pass_simd(const uint8_t* buf, size_t start, size_t end,
                                   index* out, size_t thread_id,
                                   char delimiter = ',', char quote_char = '"') {
    bool is_quoted = false;
    size_t len = end - start;
    uint64_t idx = 0;
    size_t n_indexes = 0;
    size_t i = thread_id;
    uint64_t prev_iter_inside_quote = 0ULL;  // either all zeros or all ones
    uint64_t base = 0;
    buf += start;

    for (; idx < len; idx += 64) {
      __builtin_prefetch(buf + idx + 128);
      simd_input in = fill_input(buf + idx);

      uint64_t mask = ~0ULL;

      if (len - idx < 64) {
        mask = blsmsk_u64(1ULL << (len - idx));
      }

      uint64_t quotes = cmp_mask_against_input(in, static_cast<uint8_t>(quote_char)) & mask;

      uint64_t quote_mask = find_quote_mask2(in, quotes, prev_iter_inside_quote);
      uint64_t sep = cmp_mask_against_input(in, static_cast<uint8_t>(delimiter));
      uint64_t end_mask = cmp_mask_against_input(in, '\n');
      uint64_t field_sep = (end_mask | sep) & ~quote_mask;
      n_indexes +=
          write(out->indexes + thread_id, base, start + idx, out->n_threads, field_sep);
    }
    return n_indexes;
  }

  /**
   * @brief Branchless SIMD second pass using lookup table state machine.
   *
   * This method uses the branchless state machine implementation which eliminates
   * branch mispredictions by using precomputed lookup tables for character
   * classification and state transitions.
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
  static uint64_t second_pass_simd_branchless(const BranchlessStateMachine& sm,
                                               const uint8_t* buf, size_t start, size_t end,
                                               index* out, size_t thread_id) {
    return simdcsv::second_pass_simd_branchless(
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
    RECORD_START,    ///< At the beginning of a new record (row).
    FIELD_START,     ///< At the beginning of a new field (after comma).
    UNQUOTED_FIELD,  ///< Inside an unquoted field.
    QUOTED_FIELD,    ///< Inside a quoted field.
    QUOTED_END       ///< Just saw a quote inside a quoted field (might be closing or escape).
  };

  // Error result from state transitions
  struct state_result {
    csv_state state;
    ErrorCode error;
  };

  really_inline static state_result quoted_state(csv_state in) {
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
    return {in, ErrorCode::INTERNAL_ERROR};
  }

  really_inline static state_result comma_state(csv_state in) {
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
    return {in, ErrorCode::INTERNAL_ERROR};
  }

  really_inline static state_result newline_state(csv_state in) {
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
    return {in, ErrorCode::INTERNAL_ERROR};
  }

  really_inline static state_result other_state(csv_state in) {
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
    return {in, ErrorCode::INTERNAL_ERROR};
  }

  really_inline static size_t add_position(index* out, size_t i, size_t pos) {
    out->indexes[i] = pos;
    return i + out->n_threads;
  }

  // Default context size for error messages (characters before/after error position)
  static constexpr size_t DEFAULT_ERROR_CONTEXT_SIZE = 20;

  // Helper to get context around an error position
  // Returns a string representation of the buffer content near the given position
  static std::string get_context(const uint8_t* buf, size_t len, size_t pos,
                                 size_t context_size = DEFAULT_ERROR_CONTEXT_SIZE) {
    // Handle empty buffer case
    if (len == 0 || buf == nullptr) return "";

    // Bounds check
    size_t safe_pos = pos < len ? pos : len - 1;
    size_t ctx_start = safe_pos > context_size ? safe_pos - context_size : 0;
    size_t ctx_end = std::min(safe_pos + context_size, len);

    std::string ctx;
    // Reserve space to avoid reallocations (worst case: every char becomes 2 chars like \n)
    ctx.reserve((ctx_end - ctx_start) * 2);

    for (size_t i = ctx_start; i < ctx_end; ++i) {
      char c = static_cast<char>(buf[i]);
      if (c == '\n') ctx += "\\n";
      else if (c == '\r') ctx += "\\r";
      else if (c == '\0') ctx += "\\0";
      else if (c >= 32 && c < 127) ctx += c;
      else ctx += "?";
    }
    return ctx;
  }

  // Helper to calculate line and column from byte offset
  // SECURITY: buf_len parameter ensures we never read past buffer bounds
  static void get_line_column(const uint8_t* buf, size_t buf_len, size_t offset, size_t& line, size_t& column) {
    line = 1;
    column = 1;
    // Ensure we don't read past buffer bounds
    size_t safe_offset = offset < buf_len ? offset : buf_len;
    for (size_t i = 0; i < safe_offset; ++i) {
      if (buf[i] == '\n') {
        ++line;
        column = 1;
      } else if (buf[i] != '\r') {
        ++column;
      }
    }
  }

  /**
   * @brief Second pass with error collection and dialect support.
   */
  static uint64_t second_pass_chunk(const uint8_t* buf, size_t start, size_t end,
                                    index* out, size_t thread_id,
                                    ErrorCollector* errors = nullptr,
                                    size_t total_len = 0,
                                    char delimiter = ',', char quote_char = '"') {
    uint64_t pos = start;
    size_t n_indexes = 0;
    size_t i = thread_id;
    csv_state s = RECORD_START;

    while (pos < end) {
      uint8_t value = buf[pos];

      // Use effective buffer length for bounds checking
      size_t buf_len = total_len > 0 ? total_len : end;

      // Check for null bytes
      if (value == '\0' && errors) {
        size_t line, col;
        get_line_column(buf, buf_len, pos, line, col);
        errors->add_error(ErrorCode::NULL_BYTE, ErrorSeverity::ERROR,
                          line, col, pos, "Null byte in data",
                          get_context(buf, buf_len, pos));
        if (errors->should_stop()) return n_indexes;
        ++pos;
        continue;
      }

      state_result result;
      if (value == static_cast<uint8_t>(quote_char)) {
        result = quoted_state(s);
        if (result.error != ErrorCode::NONE && errors) {
          size_t line, col;
          get_line_column(buf, buf_len, pos, line, col);
          std::string msg = "Quote character '";
          msg += quote_char;
          msg += "' in unquoted field";
          errors->add_error(result.error, ErrorSeverity::ERROR,
                            line, col, pos, msg,
                            get_context(buf, buf_len, pos));
          if (errors->should_stop()) return n_indexes;
        }
        s = result.state;
      } else if (value == static_cast<uint8_t>(delimiter)) {
        if (s != QUOTED_FIELD) {
          i = add_position(out, i, pos);
          ++n_indexes;
        }
        result = comma_state(s);
        s = result.state;
      } else if (value == '\n') {
        if (s != QUOTED_FIELD) {
          i = add_position(out, i, pos);
          ++n_indexes;
        }
        result = newline_state(s);
        s = result.state;
      } else {
        result = other_state(s);
        if (result.error != ErrorCode::NONE && errors) {
          size_t line, col;
          get_line_column(buf, buf_len, pos, line, col);
          std::string msg = "Invalid character after closing quote '";
          msg += quote_char;
          msg += "'";
          errors->add_error(result.error, ErrorSeverity::ERROR,
                            line, col, pos, msg,
                            get_context(buf, buf_len, pos));
          if (errors->should_stop()) return n_indexes;
        }
        s = result.state;
      }
      ++pos;
    }

    // Use effective buffer length for bounds checking
    size_t buf_len = total_len > 0 ? total_len : end;

    // Check for unclosed quote at end of chunk
    if (s == QUOTED_FIELD && errors && end == buf_len) {
      size_t line, col;
      get_line_column(buf, buf_len, pos > 0 ? pos - 1 : 0, line, col);
      std::string msg = "Unclosed quote '";
      msg += quote_char;
      msg += "' at end of file";
      errors->add_error(ErrorCode::UNCLOSED_QUOTE, ErrorSeverity::FATAL,
                        line, col, pos, msg,
                        get_context(buf, buf_len, pos > 20 ? pos - 20 : 0));
    }

    return n_indexes;
  }

  /**
   * @brief Second pass that throws on error (backward compatible), with dialect support.
   */
  static uint64_t second_pass_chunk_throwing(const uint8_t* buf, size_t start, size_t end,
                                             index* out, size_t thread_id,
                                             char delimiter = ',', char quote_char = '"') {
    uint64_t pos = start;
    size_t n_indexes = 0;
    size_t i = thread_id;
    csv_state s = RECORD_START;

    while (pos < end) {
      uint8_t value = buf[pos];
      state_result result;
      if (value == static_cast<uint8_t>(quote_char)) {
        result = quoted_state(s);
        if (result.error != ErrorCode::NONE) {
          std::string msg = "Quote character '";
          msg += quote_char;
          msg += "' in unquoted field";
          throw std::runtime_error(msg);
        }
        s = result.state;
      } else if (value == static_cast<uint8_t>(delimiter)) {
        if (s != QUOTED_FIELD) {
          i = add_position(out, i, pos);
          ++n_indexes;
        }
        s = comma_state(s).state;
      } else if (value == '\n') {
        if (s != QUOTED_FIELD) {
          i = add_position(out, i, pos);
          ++n_indexes;
        }
        s = newline_state(s).state;
      } else {
        result = other_state(s);
        if (result.error != ErrorCode::NONE) {
          std::string msg = "Invalid character after closing quote '";
          msg += quote_char;
          msg += "'";
          throw std::runtime_error(msg);
        }
        s = result.state;
      }
      ++pos;
    }
    return n_indexes;
  }

  /**
   * @brief Parse using speculative multi-threading with dialect support.
   */
  bool parse_speculate(const uint8_t* buf, index& out, size_t len,
                       const Dialect& dialect = Dialect::csv()) {
    char delim = dialect.delimiter;
    char quote = dialect.quote_char;
    uint8_t n_threads = out.n_threads;
    // Validate n_threads: treat 0 as single-threaded to avoid division by zero
    if (n_threads == 0) n_threads = 1;
    if (n_threads == 1) {
      out.n_indexes[0] = second_pass_simd(buf, 0, len, &out, 0, delim, quote);
      return true;
    }
    size_t chunk_size = len / n_threads;
    // If chunk size is too small, small chunks may not contain any newlines,
    // causing first_pass_speculate to return null_pos. Fall back to single-threaded.
    if (chunk_size < 64) {
      // CRITICAL: Must update n_threads to 1 for correct stride in write()
      out.n_threads = 1;
      out.n_indexes[0] = second_pass_simd(buf, 0, len, &out, 0, delim, quote);
      return true;
    }
    std::vector<uint64_t> chunk_pos(n_threads + 1);
    std::vector<std::future<stats>> first_pass_fut(n_threads);
    std::vector<std::future<uint64_t>> second_pass_fut(n_threads);

    for (int i = 0; i < n_threads; ++i) {
      first_pass_fut[i] = std::async(std::launch::async,
          [buf, chunk_size, i, delim, quote]() {
            return first_pass_speculate(buf, chunk_size * i, chunk_size * (i + 1), delim, quote);
          });
    }

    auto st = first_pass_fut[0].get();
#ifndef SIMDCSV_BENCHMARK_MODE
    printf("i: %i\teven: %" PRIu64 "\todd: %" PRIu64 "\tquotes: %" PRIu64 "\n", 0,
           st.first_even_nl, st.first_odd_nl, st.n_quotes);
#endif
    chunk_pos[0] = 0;
    for (int i = 1; i < n_threads; ++i) {
      auto st = first_pass_fut[i].get();
#ifndef SIMDCSV_BENCHMARK_MODE
      printf("i: %i\teven: %" PRIu64 "\todd: %" PRIu64 "\tquotes: %" PRIu64 "\n", i,
             st.first_even_nl, st.first_odd_nl, st.n_quotes);
#endif
      chunk_pos[i] = st.n_quotes == 0 ? st.first_even_nl : st.first_odd_nl;
    }
    chunk_pos[n_threads] = len;

    // Safety check: if any chunk_pos is null_pos, fall back to single-threaded
    for (int i = 1; i < n_threads; ++i) {
      if (chunk_pos[i] == null_pos) {
        // CRITICAL: Must update n_threads to 1 for correct stride in write()
        out.n_threads = 1;
        out.n_indexes[0] = second_pass_simd(buf, 0, len, &out, 0, delim, quote);
        return true;
      }
    }

    for (int i = 0; i < n_threads; ++i) {
      second_pass_fut[i] = std::async(std::launch::async,
          [buf, &out, &chunk_pos, i, delim, quote]() {
            return second_pass_simd(buf, chunk_pos[i], chunk_pos[i + 1], &out, i, delim, quote);
          });
    }

    for (int i = 0; i < n_threads; ++i) {
      out.n_indexes[i] = second_pass_fut[i].get();
    }

    return true;
  }

  /**
   * @brief Parse using two-pass algorithm with dialect support.
   */
  bool parse_two_pass(const uint8_t* buf, index& out, size_t len,
                      const Dialect& dialect = Dialect::csv()) {
    char delim = dialect.delimiter;
    char quote = dialect.quote_char;
    uint8_t n_threads = out.n_threads;
    // Validate n_threads: treat 0 as single-threaded to avoid division by zero
    if (n_threads == 0) n_threads = 1;
    if (n_threads == 1) {
      out.n_indexes[0] = second_pass_simd(buf, 0, len, &out, 0, delim, quote);
      return true;
    }
    size_t chunk_size = len / n_threads;
    // If chunk size is too small, small chunks may not contain any newlines,
    // causing first_pass_chunk to return null_pos. Fall back to single-threaded.
    if (chunk_size < 64) {
      // CRITICAL: Must update n_threads to 1 for correct stride in write()
      out.n_threads = 1;
      out.n_indexes[0] = second_pass_simd(buf, 0, len, &out, 0, delim, quote);
      return true;
    }
    std::vector<uint64_t> chunk_pos(n_threads + 1);
    std::vector<std::future<stats>> first_pass_fut(n_threads);
    std::vector<std::future<uint64_t>> second_pass_fut(n_threads);

    for (int i = 0; i < n_threads; ++i) {
      first_pass_fut[i] = std::async(std::launch::async,
          [buf, chunk_size, i, quote]() {
            return first_pass_chunk(buf, chunk_size * i, chunk_size * (i + 1), quote);
          });
    }

    auto st = first_pass_fut[0].get();
    size_t n_quotes = st.n_quotes;
#ifndef SIMDCSV_BENCHMARK_MODE
    printf("i: %i\teven: %" PRIu64 "\todd: %" PRIu64 "\tquotes: %" PRIu64 "\n", 0,
           st.first_even_nl, st.first_odd_nl, st.n_quotes);
#endif
    chunk_pos[0] = 0;
    for (int i = 1; i < n_threads; ++i) {
      auto st = first_pass_fut[i].get();
#ifndef SIMDCSV_BENCHMARK_MODE
      printf("i: %i\teven: %" PRIu64 "\todd: %" PRIu64 "\tquotes: %" PRIu64 "\n", i,
             st.first_even_nl, st.first_odd_nl, st.n_quotes);
#endif
      chunk_pos[i] = (n_quotes % 2) == 0 ? st.first_even_nl : st.first_odd_nl;
      n_quotes += st.n_quotes;
    }
    chunk_pos[n_threads] = len;

    // Safety check: if any chunk_pos is null_pos, fall back to single-threaded
    for (int i = 1; i < n_threads; ++i) {
      if (chunk_pos[i] == null_pos) {
        // CRITICAL: Must update n_threads to 1 for correct stride in write()
        out.n_threads = 1;
        out.n_indexes[0] = second_pass_simd(buf, 0, len, &out, 0, delim, quote);
        return true;
      }
    }

    for (int i = 0; i < n_threads; ++i) {
      second_pass_fut[i] = std::async(std::launch::async,
          [buf, &out, &chunk_pos, i, delim, quote]() {
            return second_pass_chunk_throwing(buf, chunk_pos[i], chunk_pos[i + 1], &out, i, delim, quote);
          });
    }

    for (int i = 0; i < n_threads; ++i) {
      out.n_indexes[i] = second_pass_fut[i].get();
    }

    return true;
  }

  /**
   * @brief Parse a CSV buffer and build the field index.
   *
   * This is the primary parsing method for fast CSV parsing without detailed
   * error collection. It uses the speculative multi-threaded algorithm for
   * optimal performance on large files.
   *
   * The method populates the index structure with byte offsets of all field
   * separators (commas and newlines) found in the CSV data.
   *
   * @param buf Pointer to the CSV data buffer. Must remain valid during parsing.
   *            Should have at least 32 bytes of padding beyond len for SIMD safety.
   * @param out The index structure to populate. Must be initialized via init().
   * @param len Length of the CSV data in bytes (excluding any padding).
   * @param dialect The dialect to use for parsing (default: CSV with comma and double-quote).
   *
   * @return true if parsing completed successfully, false otherwise.
   *
   * @throws std::runtime_error On parsing errors (e.g., malformed quotes).
   *
   * @note For error-tolerant parsing with detailed error information, use
   *       parse_with_errors() or parse_two_pass_with_errors() instead.
   *
   * @note The buffer should be loaded using io_util.h functions which ensure
   *       proper SIMD-aligned padding.
   *
   * @example
   * @code
   * simdcsv::two_pass parser;
   * simdcsv::index idx = parser.init(length, 4);
   *
   * // Parse standard CSV
   * parser.parse(buffer, idx, length);
   *
   * // Parse tab-separated values
   * parser.parse(buffer, idx, length, simdcsv::Dialect::tsv());
   *
   * // Parse semicolon-separated (European style)
   * parser.parse(buffer, idx, length, simdcsv::Dialect::semicolon());
   * @endcode
   *
   * @see parse_with_errors() For single-threaded parsing with error collection.
   * @see parse_two_pass_with_errors() For multi-threaded parsing with error collection.
   */
  bool parse(const uint8_t* buf, index& out, size_t len,
             const Dialect& dialect = Dialect::csv()) {
    return parse_speculate(buf, out, len, dialect);
  }

  /**
   * @brief Parse a CSV buffer using branchless state machine (optimized).
   *
   * This method uses the branchless state machine implementation for improved
   * performance by eliminating branch mispredictions. It's recommended for
   * large files where the branch misprediction overhead is significant.
   *
   * The branchless implementation uses:
   * - Lookup table character classification (O(1) per character)
   * - Lookup table state transitions (O(1) per character)
   * - SIMD-accelerated character detection
   *
   * @param buf Pointer to the CSV data buffer. Must remain valid during parsing.
   *            Should have at least 32 bytes of padding beyond len for SIMD safety.
   * @param out The index structure to populate. Must be initialized via init().
   * @param len Length of the CSV data in bytes (excluding any padding).
   * @param dialect The dialect to use for parsing (default: CSV with comma and double-quote).
   *
   * @return true if parsing completed successfully, false otherwise.
   *
   * @note This method is optimized for performance over error reporting.
   *       It does NOT collect errors like unclosed quotes, null bytes, or
   *       invalid escape sequences. For detailed error information, use
   *       parse_with_errors() instead.
   *
   * @warning When parsing untrusted input, consider using parse_with_errors()
   *          to detect malformed CSV that this method may silently accept.
   *
   * @see parse() For the standard parsing method
   * @see parse_with_errors() For parsing with error collection
   * @see BranchlessStateMachine For implementation details
   */
  bool parse_branchless(const uint8_t* buf, index& out, size_t len,
                        const Dialect& dialect = Dialect::csv()) {
    BranchlessStateMachine sm(dialect.delimiter, dialect.quote_char);
    uint8_t n_threads = out.n_threads;

    // Validate n_threads: treat 0 as single-threaded to avoid division by zero
    if (n_threads == 0) n_threads = 1;

    if (n_threads == 1) {
      out.n_indexes[0] = second_pass_simd_branchless(sm, buf, 0, len, &out, 0);
      return true;
    }

    // Multi-threaded parsing with branchless second pass
    size_t chunk_size = len / n_threads;

    // If chunk size is too small, fall back to single-threaded
    if (chunk_size < 64) {
      out.n_threads = 1;
      out.n_indexes[0] = second_pass_simd_branchless(sm, buf, 0, len, &out, 0);
      return true;
    }

    std::vector<uint64_t> chunk_pos(n_threads + 1);
    std::vector<std::future<stats>> first_pass_fut(n_threads);
    std::vector<std::future<uint64_t>> second_pass_fut(n_threads);

    char delim = dialect.delimiter;
    char quote = dialect.quote_char;

    // First pass: find chunk boundaries (reuse existing implementation)
    for (int i = 0; i < n_threads; ++i) {
      first_pass_fut[i] = std::async(std::launch::async,
          [buf, chunk_size, i, delim, quote]() {
            return first_pass_speculate(buf, chunk_size * i, chunk_size * (i + 1), delim, quote);
          });
    }

    auto st = first_pass_fut[0].get();
    chunk_pos[0] = 0;
    for (int i = 1; i < n_threads; ++i) {
      auto st = first_pass_fut[i].get();
      chunk_pos[i] = st.n_quotes == 0 ? st.first_even_nl : st.first_odd_nl;
    }
    chunk_pos[n_threads] = len;

    // Safety check: if any chunk_pos is null_pos, fall back to single-threaded
    for (int i = 1; i < n_threads; ++i) {
      if (chunk_pos[i] == null_pos) {
        out.n_threads = 1;
        out.n_indexes[0] = second_pass_simd_branchless(sm, buf, 0, len, &out, 0);
        return true;
      }
    }

    // Second pass: branchless parsing of each chunk
    // Capture sm by value since it's small (~300 bytes) and we need thread safety
    for (int i = 0; i < n_threads; ++i) {
      second_pass_fut[i] = std::async(std::launch::async,
          [sm, buf, &out, &chunk_pos, i]() {
            return second_pass_simd_branchless(sm, buf, chunk_pos[i], chunk_pos[i + 1], &out, i);
          });
    }

    for (int i = 0; i < n_threads; ++i) {
      out.n_indexes[i] = second_pass_fut[i].get();
    }

    return true;
  }

  /**
   * @brief Parse a CSV buffer with automatic dialect detection.
   *
   * This method first detects the CSV dialect (delimiter, quote character, etc.)
   * and then parses the file using the detected dialect. It combines dialect
   * detection with parsing in a single convenient call.
   *
   * The detection uses a CleverCSV-inspired algorithm that analyzes pattern
   * consistency and cell type inference to identify the most likely dialect.
   *
   * @param buf Pointer to the CSV data buffer. Must remain valid during parsing.
   *            Should have at least 32 bytes of padding beyond len for SIMD safety.
   * @param out The index structure to populate. Must be initialized via init().
   * @param len Length of the CSV data in bytes (excluding any padding).
   * @param errors ErrorCollector to accumulate parsing errors.
   * @param detected Optional pointer to receive the detected dialect result.
   *                 If nullptr, detection result is not returned.
   *
   * @return true if parsing completed successfully, false otherwise.
   *
   * @example
   * @code
   * simdcsv::two_pass parser;
   * simdcsv::index idx = parser.init(length, 4);
   * simdcsv::ErrorCollector errors(simdcsv::ErrorMode::PERMISSIVE);
   * simdcsv::DetectionResult detected;
   *
   * bool success = parser.parse_auto(buffer, idx, length, errors, &detected);
   *
   * if (detected.success()) {
   *     std::cout << "Detected: " << detected.dialect.to_string() << "\n";
   *     std::cout << "Columns: " << detected.detected_columns << "\n";
   * }
   * @endcode
   *
   * @see Dialect For dialect configuration options.
   * @see DialectDetector For standalone dialect detection.
   * @see parse() For parsing without auto-detection.
   */
  bool parse_auto(const uint8_t* buf, index& out, size_t len,
                  ErrorCollector& errors, DetectionResult* detected = nullptr) {
    // Perform dialect detection
    DialectDetector detector;
    DetectionResult result = detector.detect(buf, len);

    // Store detection result if requested
    if (detected != nullptr) {
      *detected = result;
    }

    // Use detected dialect if successful, otherwise fall back to standard CSV
    Dialect dialect = result.success() ? result.dialect : Dialect::csv();

    // Add info message about detected dialect
    if (result.success()) {
      Dialect csv = Dialect::csv();
      if (result.dialect.delimiter != csv.delimiter ||
          result.dialect.quote_char != csv.quote_char) {
        std::string msg = "Auto-detected dialect: " + result.dialect.to_string();
        errors.add_error(ErrorCode::NONE, ErrorSeverity::WARNING,
                         1, 1, 0, msg, "");
      }
    }

    // Parse with detected dialect
    return parse_two_pass_with_errors(buf, out, len, errors, dialect);
  }

  /**
   * @brief Detect the dialect of a CSV buffer without parsing.
   *
   * This is a convenience method that performs only dialect detection without
   * full parsing. Use this when you need to determine the file format before
   * deciding how to process it.
   *
   * @param buf Pointer to the CSV data buffer.
   * @param len Length of the data in bytes.
   * @param options Optional detection options (sample size, candidate delimiters, etc.).
   *
   * @return DetectionResult containing the detected dialect and confidence.
   *
   * @example
   * @code
   * auto result = simdcsv::two_pass::detect_dialect(buffer, length);
   * if (result.success()) {
   *     std::cout << "Delimiter: " << result.dialect.delimiter << "\n";
   *     std::cout << "Confidence: " << result.confidence << "\n";
   * }
   * @endcode
   */
  static DetectionResult detect_dialect(const uint8_t* buf, size_t len,
                                         const DetectionOptions& options = DetectionOptions()) {
    DialectDetector detector(options);
    return detector.detect(buf, len);
  }

  // Result from multi-threaded parsing with error collection
  struct chunk_result {
    uint64_t n_indexes;
    ErrorCollector errors;

    chunk_result() : n_indexes(0), errors(ErrorMode::PERMISSIVE) {}
  };

  /**
   * @brief Static wrapper for thread-safe parsing with error collection and dialect.
   */
  static chunk_result second_pass_chunk_with_errors(
      const uint8_t* buf, size_t start, size_t end,
      index* out, size_t thread_id, size_t total_len, ErrorMode mode,
      char delimiter = ',', char quote_char = '"') {
    chunk_result result;
    result.errors.set_mode(mode);
    result.n_indexes = second_pass_chunk(buf, start, end, out, thread_id,
                                         &result.errors, total_len, delimiter, quote_char);
    return result;
  }

  /**
   * @brief Parse a CSV buffer with error collection using multi-threading.
   *
   * This method combines the performance of multi-threaded parsing with
   * comprehensive error collection. Each thread maintains its own local
   * error collector, and errors are merged and sorted by byte offset after
   * parsing completes.
   *
   * The method performs the same checks as parse_with_errors():
   * - Empty header detection
   * - Duplicate column name detection
   * - Mixed line ending warnings
   * - Quote errors (unclosed quotes, quotes in unquoted fields)
   * - Inconsistent field counts across rows
   * - Null byte detection
   *
   * @param buf Pointer to the CSV data buffer. Must remain valid during parsing.
   * @param out The index structure to populate. Must be initialized via init()
   *            with the desired number of threads.
   * @param len Length of the CSV data in bytes.
   * @param errors ErrorCollector to accumulate parsing errors. Thread-local
   *               errors are merged into this collector after parsing.
   * @param dialect The dialect to use for parsing (default: CSV with comma and double-quote).
   *
   * @return true if parsing completed without fatal errors, false if fatal
   *         errors occurred.
   *
   * @note Thread Safety: The provided ErrorCollector must not be accessed by
   *       other threads during parsing. After this method returns, all errors
   *       have been merged and sorted by byte offset.
   *
   * @note This method automatically falls back to single-threaded parsing for
   *       small files or when chunk boundaries cannot be safely determined.
   *
   * @warning Error ordering: Due to parallel execution, errors may not be
   *          discovered in file order during parsing. However, the final
   *          error list is sorted by byte offset for consistent output.
   *
   * @example
   * @code
   * simdcsv::two_pass parser;
   * size_t num_threads = std::thread::hardware_concurrency();
   * simdcsv::index idx = parser.init(length, num_threads);
   * simdcsv::ErrorCollector errors(simdcsv::ErrorMode::PERMISSIVE);
   *
   * // Parse with default CSV dialect
   * bool success = parser.parse_two_pass_with_errors(buffer, idx, length, errors);
   *
   * // Parse with tab-separated values
   * bool success = parser.parse_two_pass_with_errors(buffer, idx, length, errors,
   *                                                   simdcsv::Dialect::tsv());
   * @endcode
   *
   * @see parse_with_errors() For single-threaded parsing with precise ordering.
   * @see ErrorCollector::merge_sorted() For error merging details.
   */
  bool parse_two_pass_with_errors(const uint8_t* buf, index& out, size_t len,
                                  ErrorCollector& errors,
                                  const Dialect& dialect = Dialect::csv()) {
    char delim = dialect.delimiter;
    char quote = dialect.quote_char;

    // Handle empty input
    if (len == 0) return true;

    // Check structural issues first (single-threaded, fast)
    check_empty_header(buf, len, errors);
    if (errors.should_stop()) return false;

    check_duplicate_columns(buf, len, errors, delim, quote);
    if (errors.should_stop()) return false;

    check_line_endings(buf, len, errors);
    if (errors.should_stop()) return false;

    uint8_t n_threads = out.n_threads;

    // Validate n_threads: treat 0 as single-threaded to avoid division by zero
    if (n_threads == 0) n_threads = 1;

    // For single-threaded, use the simpler path
    if (n_threads == 1) {
      out.n_indexes[0] = second_pass_chunk(buf, 0, len, &out, 0, &errors, len, delim, quote);
      check_field_counts(buf, len, errors, delim, quote);
      return !errors.has_fatal_errors();
    }

    size_t chunk_size = len / n_threads;
    std::vector<uint64_t> chunk_pos(n_threads + 1);
    std::vector<std::future<stats>> first_pass_fut(n_threads);
    std::vector<std::future<chunk_result>> second_pass_fut(n_threads);

    // First pass: find chunk boundaries
    for (int i = 0; i < n_threads; ++i) {
      first_pass_fut[i] = std::async(std::launch::async,
          [buf, chunk_size, i, quote]() {
            return first_pass_chunk(buf, chunk_size * i, chunk_size * (i + 1), quote);
          });
    }

    auto st = first_pass_fut[0].get();
    size_t n_quotes = st.n_quotes;
#ifndef SIMDCSV_BENCHMARK_MODE
    printf("i: %i\teven: %" PRIu64 "\todd: %" PRIu64 "\tquotes: %" PRIu64 "\n", 0,
           st.first_even_nl, st.first_odd_nl, st.n_quotes);
#endif
    chunk_pos[0] = 0;
    for (int i = 1; i < n_threads; ++i) {
      auto st = first_pass_fut[i].get();
#ifndef SIMDCSV_BENCHMARK_MODE
      printf("i: %i\teven: %" PRIu64 "\todd: %" PRIu64 "\tquotes: %" PRIu64 "\n", i,
             st.first_even_nl, st.first_odd_nl, st.n_quotes);
#endif
      chunk_pos[i] = (n_quotes % 2) == 0 ? st.first_even_nl : st.first_odd_nl;
      n_quotes += st.n_quotes;
    }
    chunk_pos[n_threads] = len;

    // Safety check: if any chunk_pos is null_pos, fall back to single-threaded
    for (int i = 1; i < n_threads; ++i) {
      if (chunk_pos[i] == null_pos) {
        out.n_threads = 1;
        out.n_indexes[0] = second_pass_chunk(buf, 0, len, &out, 0, &errors, len, delim, quote);
        check_field_counts(buf, len, errors, delim, quote);
        return !errors.has_fatal_errors();
      }
    }

    // Second pass: parse with thread-local error collectors
    ErrorMode mode = errors.mode();
    for (int i = 0; i < n_threads; ++i) {
      second_pass_fut[i] = std::async(std::launch::async,
          [buf, &out, &chunk_pos, i, len, mode, delim, quote]() {
            return second_pass_chunk_with_errors(buf, chunk_pos[i], chunk_pos[i + 1],
                                                  &out, i, len, mode, delim, quote);
          });
    }

    // Collect results and merge errors
    std::vector<ErrorCollector> thread_errors;
    thread_errors.reserve(n_threads);

    for (int i = 0; i < n_threads; ++i) {
      auto result = second_pass_fut[i].get();
      out.n_indexes[i] = result.n_indexes;
      thread_errors.push_back(std::move(result.errors));
    }

    // Merge all thread-local errors, sorted by byte offset
    errors.merge_sorted(thread_errors);

    // Check field counts after parsing (single-threaded, scans file linearly)
    check_field_counts(buf, len, errors, delim, quote);

    return !errors.has_fatal_errors();
  }

  /**
   * @brief Parse a CSV buffer with detailed error collection (single-threaded).
   *
   * This method provides comprehensive error detection and collection while
   * parsing. It runs single-threaded to ensure errors are reported in exact
   * file order, making it ideal for validation and debugging.
   *
   * The method performs the following checks:
   * - Empty header detection
   * - Duplicate column name detection
   * - Mixed line ending warnings
   * - Quote errors (unclosed quotes, quotes in unquoted fields)
   * - Inconsistent field counts across rows
   * - Null byte detection
   *
   * @param buf Pointer to the CSV data buffer. Must remain valid during parsing.
   * @param out The index structure to populate. Must be initialized via init().
   * @param len Length of the CSV data in bytes.
   * @param errors ErrorCollector to accumulate parsing errors. The collector's
   *               mode (STRICT, PERMISSIVE, BEST_EFFORT) controls behavior.
   *
   * @return true if parsing completed without fatal errors, false if fatal
   *         errors occurred or parsing was stopped early (STRICT mode).
   *
   * @note This method is single-threaded for precise error position tracking.
   *       For large files where performance is critical, consider
   *       parse_two_pass_with_errors() which uses multi-threading.
   *
   * @example
   * @code
   * simdcsv::two_pass parser;
   * simdcsv::index idx = parser.init(length, 1);
   * simdcsv::ErrorCollector errors(simdcsv::ErrorMode::PERMISSIVE);
   *
   * bool success = parser.parse_with_errors(buffer, idx, length, errors);
   *
   * // Check for errors
   * if (errors.error_count() > 0) {
   *     std::cout << "Found " << errors.error_count() << " errors:\n";
   *     for (const auto& err : errors.get_errors()) {
   *         std::cout << "Line " << err.line << ", Col " << err.column
   *                   << ": " << err.message << "\n";
   *     }
   * }
   *
   * if (!success) {
   *     std::cerr << "Parsing failed due to fatal errors\n";
   * }
   * @endcode
   *
   * @see parse_two_pass_with_errors() For multi-threaded parsing with errors.
   * @see ErrorCollector For error handling configuration and access.
   * @see ErrorMode For different error handling strategies.
   */
  /**
   * @brief Parse a CSV buffer with detailed error collection (single-threaded).
   */
  bool parse_with_errors(const uint8_t* buf, index& out, size_t len, ErrorCollector& errors,
                         const Dialect& dialect = Dialect::csv()) {
    char delim = dialect.delimiter;
    char quote = dialect.quote_char;

    // Check structural issues first
    check_empty_header(buf, len, errors);
    if (errors.should_stop()) return false;

    check_duplicate_columns(buf, len, errors, delim, quote);
    if (errors.should_stop()) return false;

    check_line_endings(buf, len, errors);
    if (errors.should_stop()) return false;

    // Single-threaded parsing for accurate error position tracking
    out.n_indexes[0] = second_pass_chunk(buf, 0, len, &out, 0, &errors, len, delim, quote);

    // Check field counts after parsing
    check_field_counts(buf, len, errors, delim, quote);

    return !errors.has_fatal_errors();
  }

  // Check for empty header
  static bool check_empty_header(const uint8_t* buf, size_t len, ErrorCollector& errors) {
    if (len == 0) return true;
    if (buf[0] == '\n' || buf[0] == '\r') {
      errors.add_error(ErrorCode::EMPTY_HEADER, ErrorSeverity::ERROR,
                       1, 1, 0, "Header row is empty", "");
      return false;
    }
    return true;
  }

  /**
   * @brief Check for duplicate column names in header with dialect support.
   */
  static void check_duplicate_columns(const uint8_t* buf, size_t len, ErrorCollector& errors,
                                      char delimiter = ',', char quote_char = '"') {
    if (len == 0) return;

    // Find end of first line
    size_t header_end = 0;
    bool in_quote = false;
    while (header_end < len) {
      if (buf[header_end] == static_cast<uint8_t>(quote_char)) in_quote = !in_quote;
      else if (!in_quote && (buf[header_end] == '\n' || buf[header_end] == '\r')) break;
      ++header_end;
    }

    // Parse header fields
    std::vector<std::string> fields;
    std::string current;
    in_quote = false;
    for (size_t i = 0; i < header_end; ++i) {
      if (buf[i] == static_cast<uint8_t>(quote_char)) {
        in_quote = !in_quote;
      } else if (!in_quote && buf[i] == static_cast<uint8_t>(delimiter)) {
        fields.push_back(current);
        current.clear();
      } else if (buf[i] != '\r') {
        current += static_cast<char>(buf[i]);
      }
    }
    fields.push_back(current);

    // Check for duplicates
    std::unordered_set<std::string> seen;
    for (size_t i = 0; i < fields.size(); ++i) {
      if (seen.count(fields[i]) > 0) {
        errors.add_error(ErrorCode::DUPLICATE_COLUMN_NAMES, ErrorSeverity::WARNING,
                         1, i + 1, 0, "Duplicate column name: '" + fields[i] + "'", fields[i]);
      }
      seen.insert(fields[i]);
    }
  }

  /**
   * @brief Check for inconsistent field counts with dialect support.
   */
  static void check_field_counts(const uint8_t* buf, size_t len, ErrorCollector& errors,
                                 char delimiter = ',', char quote_char = '"') {
    if (len == 0) return;

    size_t expected_fields = 0;
    size_t current_fields = 1;
    size_t current_line = 1;
    size_t line_start = 0;
    bool in_quote = false;
    bool header_done = false;

    for (size_t i = 0; i < len; ++i) {
      if (buf[i] == static_cast<uint8_t>(quote_char)) {
        in_quote = !in_quote;
      } else if (!in_quote) {
        if (buf[i] == static_cast<uint8_t>(delimiter)) {
          ++current_fields;
        } else if (buf[i] == '\n') {
          if (!header_done) {
            expected_fields = current_fields;
            header_done = true;
          } else if (current_fields != expected_fields) {
            std::ostringstream msg;
            msg << "Expected " << expected_fields << " fields but found " << current_fields;
            errors.add_error(ErrorCode::INCONSISTENT_FIELD_COUNT, ErrorSeverity::ERROR,
                             current_line, 1, line_start, msg.str(),
                             get_context(buf, len, line_start, 40));
            if (errors.should_stop()) return;
          }
          current_fields = 1;
          ++current_line;
          line_start = i + 1;
        }
      }
    }

    // Check last line if no trailing newline
    if (header_done && current_fields != expected_fields && line_start < len) {
      std::ostringstream msg;
      msg << "Expected " << expected_fields << " fields but found " << current_fields;
      errors.add_error(ErrorCode::INCONSISTENT_FIELD_COUNT, ErrorSeverity::ERROR,
                       current_line, 1, line_start, msg.str(),
                       get_context(buf, len, line_start, 40));
    }
  }

  // Check for mixed line endings
  static void check_line_endings(const uint8_t* buf, size_t len, ErrorCollector& errors) {
    bool has_crlf = false;
    bool has_lf = false;
    bool has_cr = false;

    for (size_t i = 0; i < len; ++i) {
      if (buf[i] == '\r') {
        if (i + 1 < len && buf[i + 1] == '\n') {
          has_crlf = true;
          ++i;
        } else {
          has_cr = true;
        }
      } else if (buf[i] == '\n') {
        has_lf = true;
      }
    }

    int types = (has_crlf ? 1 : 0) + (has_lf ? 1 : 0) + (has_cr ? 1 : 0);
    if (types > 1) {
      errors.add_error(ErrorCode::MIXED_LINE_ENDINGS, ErrorSeverity::WARNING,
                       1, 1, 0, "Mixed line endings detected", "");
    }
  }

  /**
   * @brief Perform full CSV validation with comprehensive error checking.
   *
   * This method is functionally equivalent to parse_with_errors() but named
   * to emphasize its validation purpose. Use this when your primary goal is
   * to check a CSV file for errors rather than extract data.
   *
   * Validation checks performed:
   * - Empty header row detection
   * - Duplicate column name detection (warns on duplicates)
   * - Mixed line endings detection (CRLF, LF, CR combinations)
   * - Quote handling errors (unclosed quotes, quotes in unquoted fields)
   * - Invalid characters after closing quotes
   * - Inconsistent field counts across rows
   * - Null byte detection
   *
   * @param buf Pointer to the CSV data buffer.
   * @param out The index structure to populate (from init()).
   * @param len Length of the CSV data in bytes.
   * @param errors ErrorCollector to accumulate validation errors.
   * @param dialect The dialect to use for validation (default: CSV with comma and double-quote).
   *
   * @return true if validation passed without fatal errors, false otherwise.
   *
   * @see parse_with_errors() Equivalent functionality
   * @see ErrorCollector For accessing validation results
   */
  bool parse_validate(const uint8_t* buf, index& out, size_t len, ErrorCollector& errors,
                      const Dialect& dialect = Dialect::csv()) {
    char delim = dialect.delimiter;
    char quote = dialect.quote_char;

    // Check structural issues first
    check_empty_header(buf, len, errors);
    if (errors.should_stop()) return false;

    check_duplicate_columns(buf, len, errors, delim, quote);
    if (errors.should_stop()) return false;

    check_line_endings(buf, len, errors);
    if (errors.should_stop()) return false;

    // Parse with error collection
    out.n_indexes[0] = second_pass_chunk(buf, 0, len, &out, 0, &errors, len, delim, quote);

    // Check field counts after parsing
    check_field_counts(buf, len, errors, delim, quote);

    return !errors.has_fatal_errors();
  }

  /**
   * @brief Initialize an index structure for parsing.
   *
   * Allocates memory for storing field separator positions. The index must be
   * initialized before calling any parse method. The allocated size is based on
   * the maximum possible number of fields (one per byte in worst case).
   *
   * @param len Length of the CSV buffer in bytes. Determines maximum index capacity.
   * @param n_threads Number of threads to use for parsing. Use 1 for single-threaded
   *                  parsing, or a higher value for multi-threaded parsing.
   *                  Recommended: std::thread::hardware_concurrency() for large files.
   *
   * @return An initialized index structure ready for parsing.
   *
   * @note The returned index owns its memory and will free it on destruction.
   *       Use move semantics to transfer ownership.
   *
   * @warning For very large files, this allocates len * sizeof(uint64_t) bytes
   *          for the indexes array. Consider the memory implications.
   *
   * @example
   * @code
   * simdcsv::two_pass parser;
   *
   * // Single-threaded parsing
   * simdcsv::index idx = parser.init(buffer_length, 1);
   *
   * // Multi-threaded parsing with 4 threads
   * simdcsv::index idx = parser.init(buffer_length, 4);
   *
   * // Use hardware concurrency
   * simdcsv::index idx = parser.init(buffer_length,
   *                                   std::thread::hardware_concurrency());
   * @endcode
   */
  index init(size_t len, size_t n_threads) {
    index out;
    out.n_threads = n_threads;
    out.n_indexes = new uint64_t[n_threads];

    out.indexes = new uint64_t[len];
    return out;
  }
};

class parser {
 public:
  parser() noexcept {};
  void parse(const uint8_t* buf, size_t len) {}

 private:
};
}  // namespace simdcsv
