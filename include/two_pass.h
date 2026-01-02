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

namespace simdcsv {

constexpr static uint64_t null_pos = std::numeric_limits<uint64_t>::max();
class index {
 public:
  uint64_t columns{0};
  uint8_t n_threads{0};
  uint64_t* n_indexes{nullptr};
  uint64_t* indexes{nullptr};

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

class two_pass {
 public:
  struct stats {
    uint64_t n_quotes{0};
    uint64_t first_even_nl{null_pos};
    uint64_t first_odd_nl{null_pos};
  };
  static stats first_pass_simd(const uint8_t* buf, size_t start, size_t end) {
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

      uint64_t quotes = cmp_mask_against_input(in, '"') & mask;

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

  static stats first_pass_chunk(const uint8_t* buf, size_t start, size_t end) {
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
      } else if (buf[i] == '"') {
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
    }
    return out;
  }

  static bool is_other(uint8_t c) { return c != ',' && c != '\n' && c != '"'; }

  enum quote_state { AMBIGUOUS, QUOTED, UNQUOTED };

  static quote_state get_quotation_state(const uint8_t* buf, size_t start) {
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
      if (buf[i] == '"') {
        // q-o case
        if (i + 1 < start && is_other(buf[i + 1])) {
          return num_quotes % 2 == 0 ? QUOTED : UNQUOTED;
        }

        // o-q case
        else if (i > end && is_other(buf[i - 1])) {
          return num_quotes % 2 == 0 ? UNQUOTED : QUOTED;
        }
        ++num_quotes;
      }
      --i;
    }
    // Check the last position (i == end)
    if (buf[end] == '"') {
      ++num_quotes;
    }
    return AMBIGUOUS;
  }

  static stats first_pass_speculate(const uint8_t* buf, size_t start, size_t end) {
    auto is_quoted = get_quotation_state(buf, start);
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
      } else if (buf[i] == '"') {
        is_quoted = is_quoted == UNQUOTED ? QUOTED : UNQUOTED;
      }
    }
    return {0, null_pos, null_pos};
  }

  static uint64_t second_pass_simd(const uint8_t* buf, size_t start, size_t end,
                                   index* out, size_t thread_id) {
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

      uint64_t quotes = cmp_mask_against_input(in, '"') & mask;

      uint64_t quote_mask = find_quote_mask2(in, quotes, prev_iter_inside_quote);
      uint64_t sep = cmp_mask_against_input(in, ',');
      uint64_t end = cmp_mask_against_input(in, '\n');
      uint64_t field_sep = (end | sep) & ~quote_mask;
      n_indexes +=
          write(out->indexes + thread_id, base, start + idx, out->n_threads, field_sep);
    }
    return n_indexes;
  }

  enum csv_state { RECORD_START, FIELD_START, UNQUOTED_FIELD, QUOTED_FIELD, QUOTED_END };

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

  // Helper to get context around an error position
  // Returns a string representation of the buffer content near the given position
  static std::string get_context(const uint8_t* buf, size_t len, size_t pos, size_t context_size = 20) {
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

  // Second pass with error collection
  static uint64_t second_pass_chunk(const uint8_t* buf, size_t start, size_t end,
                                    index* out, size_t thread_id,
                                    ErrorCollector* errors = nullptr,
                                    size_t total_len = 0) {
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
      switch (value) {
        case '"':
          result = quoted_state(s);
          if (result.error != ErrorCode::NONE && errors) {
            size_t line, col;
            get_line_column(buf, buf_len, pos, line, col);
            errors->add_error(result.error, ErrorSeverity::ERROR,
                              line, col, pos, "Quote character in unquoted field",
                              get_context(buf, buf_len, pos));
            if (errors->should_stop()) return n_indexes;
          }
          s = result.state;
          break;
        case ',':
          if (s != QUOTED_FIELD) {
            i = add_position(out, i, pos);
            ++n_indexes;
          }
          result = comma_state(s);
          s = result.state;
          break;
        case '\n':
          if (s != QUOTED_FIELD) {
            i = add_position(out, i, pos);
            ++n_indexes;
          }
          result = newline_state(s);
          s = result.state;
          break;
        default:
          result = other_state(s);
          if (result.error != ErrorCode::NONE && errors) {
            size_t line, col;
            get_line_column(buf, buf_len, pos, line, col);
            errors->add_error(result.error, ErrorSeverity::ERROR,
                              line, col, pos, "Invalid character after closing quote",
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
      errors->add_error(ErrorCode::UNCLOSED_QUOTE, ErrorSeverity::FATAL,
                        line, col, pos, "Unclosed quote at end of file",
                        get_context(buf, buf_len, pos > 20 ? pos - 20 : 0));
    }

    return n_indexes;
  }

  // Original version for backward compatibility (throws on error)
  static uint64_t second_pass_chunk_throwing(const uint8_t* buf, size_t start, size_t end,
                                             index* out, size_t thread_id) {
    uint64_t pos = start;
    size_t n_indexes = 0;
    size_t i = thread_id;
    csv_state s = RECORD_START;

    while (pos < end) {
      uint8_t value = buf[pos];
      state_result result;
      switch (value) {
        case '"':
          result = quoted_state(s);
          if (result.error != ErrorCode::NONE) {
            throw std::runtime_error("Quote in unquoted field");
          }
          s = result.state;
          break;
        case ',':
          if (s != QUOTED_FIELD) {
            i = add_position(out, i, pos);
            ++n_indexes;
          }
          s = comma_state(s).state;
          break;
        case '\n':
          if (s != QUOTED_FIELD) {
            i = add_position(out, i, pos);
            ++n_indexes;
          }
          s = newline_state(s).state;
          break;
        default:
          result = other_state(s);
          if (result.error != ErrorCode::NONE) {
            throw std::runtime_error("Invalid character after closing quote");
          }
          s = result.state;
      }
      ++pos;
    }
    return n_indexes;
  }

  bool parse_speculate(const uint8_t* buf, index& out, size_t len) {
    uint8_t n_threads = out.n_threads;
    // Validate n_threads: treat 0 as single-threaded to avoid division by zero
    if (n_threads == 0) n_threads = 1;
    if (n_threads == 1) {
      out.n_indexes[0] = second_pass_simd(buf, 0, len, &out, 0);
      return true;
    }
    size_t chunk_size = len / n_threads;
    // If chunk size is too small, small chunks may not contain any newlines,
    // causing first_pass_speculate to return null_pos. Fall back to single-threaded.
    if (chunk_size < 64) {
      // CRITICAL: Must update n_threads to 1 for correct stride in write()
      out.n_threads = 1;
      out.n_indexes[0] = second_pass_simd(buf, 0, len, &out, 0);
      return true;
    }
    std::vector<uint64_t> chunk_pos(n_threads + 1);
    std::vector<std::future<stats>> first_pass_fut(n_threads);
    std::vector<std::future<uint64_t>> second_pass_fut(n_threads);

    for (int i = 0; i < n_threads; ++i) {
      first_pass_fut[i] = std::async(std::launch::async, first_pass_speculate, buf,
                                     chunk_size * i, chunk_size * (i + 1));
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
        out.n_indexes[0] = second_pass_simd(buf, 0, len, &out, 0);
        return true;
      }
    }

    for (int i = 0; i < n_threads; ++i) {
      second_pass_fut[i] = std::async(std::launch::async, second_pass_simd, buf,
                                      chunk_pos[i], chunk_pos[i + 1], &out, i);
    }

    for (int i = 0; i < n_threads; ++i) {
      out.n_indexes[i] = second_pass_fut[i].get();
    }

    return true;
  }

  bool parse_two_pass(const uint8_t* buf, index& out, size_t len) {
    uint8_t n_threads = out.n_threads;
    // Validate n_threads: treat 0 as single-threaded to avoid division by zero
    if (n_threads == 0) n_threads = 1;
    if (n_threads == 1) {
      out.n_indexes[0] = second_pass_simd(buf, 0, len, &out, 0);
      return true;
    }
    size_t chunk_size = len / n_threads;
    // If chunk size is too small, small chunks may not contain any newlines,
    // causing first_pass_chunk to return null_pos. Fall back to single-threaded.
    if (chunk_size < 64) {
      // CRITICAL: Must update n_threads to 1 for correct stride in write()
      out.n_threads = 1;
      out.n_indexes[0] = second_pass_simd(buf, 0, len, &out, 0);
      return true;
    }
    std::vector<uint64_t> chunk_pos(n_threads + 1);
    std::vector<std::future<stats>> first_pass_fut(n_threads);
    std::vector<std::future<uint64_t>> second_pass_fut(n_threads);

    for (int i = 0; i < n_threads; ++i) {
      first_pass_fut[i] = std::async(std::launch::async, first_pass_chunk, buf,
                                     chunk_size * i, chunk_size * (i + 1));
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
        out.n_indexes[0] = second_pass_simd(buf, 0, len, &out, 0);
        return true;
      }
    }

    for (int i = 0; i < n_threads; ++i) {
      second_pass_fut[i] = std::async(std::launch::async, second_pass_chunk_throwing, buf,
                                      chunk_pos[i], chunk_pos[i + 1], &out, i);
    }

    for (int i = 0; i < n_threads; ++i) {
      out.n_indexes[i] = second_pass_fut[i].get();
    }

    return true;
  }

  bool parse(const uint8_t* buf, index& out, size_t len) {
    return parse_speculate(buf, out, len);
    // auto index = parse_two_pass(buf, out, len);

    // return index;
  }

  // Result from multi-threaded parsing with error collection
  struct chunk_result {
    uint64_t n_indexes;
    ErrorCollector errors;

    chunk_result() : n_indexes(0), errors(ErrorMode::PERMISSIVE) {}
  };

  // Static wrapper for thread-safe parsing with error collection
  static chunk_result second_pass_chunk_with_errors(
      const uint8_t* buf, size_t start, size_t end,
      index* out, size_t thread_id, size_t total_len, ErrorMode mode) {
    chunk_result result;
    result.errors.set_mode(mode);
    result.n_indexes = second_pass_chunk(buf, start, end, out, thread_id,
                                         &result.errors, total_len);
    return result;
  }

  // Multi-threaded parsing with error collection using thread-local collectors
  // Each thread collects errors locally, then they are merged and sorted by offset
  //
  // THREAD SAFETY: ErrorCollector must not be accessed by other threads during parsing.
  // After parsing completes, errors are merged from thread-local collectors and sorted
  // by byte offset for consistent ordering.
  bool parse_two_pass_with_errors(const uint8_t* buf, index& out, size_t len,
                                  ErrorCollector& errors) {
    // Handle empty input
    if (len == 0) return true;

    // Check structural issues first (single-threaded, fast)
    check_empty_header(buf, len, errors);
    if (errors.should_stop()) return false;

    check_duplicate_columns(buf, len, errors);
    if (errors.should_stop()) return false;

    check_line_endings(buf, len, errors);
    if (errors.should_stop()) return false;

    uint8_t n_threads = out.n_threads;

    // Validate n_threads: treat 0 as single-threaded to avoid division by zero
    if (n_threads == 0) n_threads = 1;

    // For single-threaded, use the simpler path
    if (n_threads == 1) {
      out.n_indexes[0] = second_pass_chunk(buf, 0, len, &out, 0, &errors, len);
      check_field_counts(buf, len, errors);
      return !errors.has_fatal_errors();
    }

    size_t chunk_size = len / n_threads;
    std::vector<uint64_t> chunk_pos(n_threads + 1);
    std::vector<std::future<stats>> first_pass_fut(n_threads);
    std::vector<std::future<chunk_result>> second_pass_fut(n_threads);

    // First pass: find chunk boundaries
    for (int i = 0; i < n_threads; ++i) {
      first_pass_fut[i] = std::async(std::launch::async, first_pass_chunk, buf,
                                     chunk_size * i, chunk_size * (i + 1));
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
        out.n_indexes[0] = second_pass_chunk(buf, 0, len, &out, 0, &errors, len);
        check_field_counts(buf, len, errors);
        return !errors.has_fatal_errors();
      }
    }

    // Second pass: parse with thread-local error collectors
    ErrorMode mode = errors.mode();
    for (int i = 0; i < n_threads; ++i) {
      second_pass_fut[i] = std::async(std::launch::async,
          second_pass_chunk_with_errors, buf,
          chunk_pos[i], chunk_pos[i + 1], &out, i, len, mode);
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
    check_field_counts(buf, len, errors);

    return !errors.has_fatal_errors();
  }

  // Parse with error collection - single-threaded for reliable error reporting
  // Use this when you need precise error ordering or for small files
  bool parse_with_errors(const uint8_t* buf, index& out, size_t len, ErrorCollector& errors) {
    // Check structural issues first
    check_empty_header(buf, len, errors);
    if (errors.should_stop()) return false;

    check_duplicate_columns(buf, len, errors);
    if (errors.should_stop()) return false;

    check_line_endings(buf, len, errors);
    if (errors.should_stop()) return false;

    // Single-threaded parsing for accurate error position tracking
    out.n_indexes[0] = second_pass_chunk(buf, 0, len, &out, 0, &errors, len);

    // Check field counts after parsing
    check_field_counts(buf, len, errors);

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

  // Check for duplicate column names in header
  static void check_duplicate_columns(const uint8_t* buf, size_t len, ErrorCollector& errors) {
    if (len == 0) return;

    // Find end of first line
    size_t header_end = 0;
    bool in_quote = false;
    while (header_end < len) {
      if (buf[header_end] == '"') in_quote = !in_quote;
      else if (!in_quote && (buf[header_end] == '\n' || buf[header_end] == '\r')) break;
      ++header_end;
    }

    // Parse header fields
    std::vector<std::string> fields;
    std::string current;
    in_quote = false;
    for (size_t i = 0; i < header_end; ++i) {
      if (buf[i] == '"') {
        in_quote = !in_quote;
      } else if (!in_quote && buf[i] == ',') {
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

  // Check for inconsistent field counts
  static void check_field_counts(const uint8_t* buf, size_t len, ErrorCollector& errors) {
    if (len == 0) return;

    size_t expected_fields = 0;
    size_t current_fields = 1;
    size_t current_line = 1;
    size_t line_start = 0;
    bool in_quote = false;
    bool header_done = false;

    for (size_t i = 0; i < len; ++i) {
      if (buf[i] == '"') {
        in_quote = !in_quote;
      } else if (!in_quote) {
        if (buf[i] == ',') {
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

  // Full validation parse - checks all error types
  bool parse_validate(const uint8_t* buf, index& out, size_t len, ErrorCollector& errors) {
    // Check structural issues first
    check_empty_header(buf, len, errors);
    if (errors.should_stop()) return false;

    check_duplicate_columns(buf, len, errors);
    if (errors.should_stop()) return false;

    check_line_endings(buf, len, errors);
    if (errors.should_stop()) return false;

    // Parse with error collection
    out.n_indexes[0] = second_pass_chunk(buf, 0, len, &out, 0, &errors, len);

    // Check field counts after parsing
    check_field_counts(buf, len, errors);

    return !errors.has_fatal_errors();
  }

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
