#include <unistd.h>  // for getopt
#include <cstdint>
#include <future>
#include <limits>
#include <vector>
#include <cstring>  // for memcpy
#include "inttypes.h"
#include "simd_highway.h"

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

  really_inline static csv_state quoted_state(csv_state in) {
    switch (in) {
      case RECORD_START:
        return QUOTED_FIELD;
      case FIELD_START:
        return QUOTED_FIELD;
      case UNQUOTED_FIELD:
        throw std::runtime_error("invalid 1");
      case QUOTED_FIELD:
        return QUOTED_END;
      case QUOTED_END:
        return QUOTED_FIELD;
    }
    throw std::runtime_error("should never happen");
  }

  really_inline static csv_state comma_state(csv_state in) {
    switch (in) {
      case RECORD_START:
        return FIELD_START;
      case FIELD_START:
        return FIELD_START;
      case UNQUOTED_FIELD:
        return FIELD_START;
        throw std::runtime_error("invalid 2");
      case QUOTED_FIELD:
        return QUOTED_FIELD;
      case QUOTED_END:
        return FIELD_START;
    }
    throw std::runtime_error("should never happen");
  }

  really_inline static csv_state newline_state(csv_state in) {
    switch (in) {
      case RECORD_START:
        return RECORD_START;
      case FIELD_START:
        return RECORD_START;
      case UNQUOTED_FIELD:
        return RECORD_START;
        throw std::runtime_error("invalid 3");
      case QUOTED_FIELD:
        return QUOTED_FIELD;
      case QUOTED_END:
        return RECORD_START;
    }
    throw std::runtime_error("should never happen");
  }

  really_inline static csv_state other_state(csv_state in) {
    switch (in) {
      case RECORD_START:
        return UNQUOTED_FIELD;
      case FIELD_START:
        return UNQUOTED_FIELD;
      case UNQUOTED_FIELD:
        return UNQUOTED_FIELD;
        throw std::runtime_error("invalid 4");
      case QUOTED_FIELD:
        return QUOTED_FIELD;
      case QUOTED_END:
        throw std::runtime_error("invalid 5");
    }
    throw std::runtime_error("should never happen");
  }

  really_inline static size_t add_position(index* out, size_t i, size_t pos) {
    out->indexes[i] = pos;
    return i + out->n_threads;
  }

  static uint64_t second_pass_chunk(const uint8_t* buf, size_t start, size_t end,
                                    index* out, size_t thread_id) {
    bool is_quoted = false;
    uint64_t pos = start;
    size_t n_indexes = 0;
    size_t i = thread_id;
    csv_state s = RECORD_START;

    while (pos < end) {
      uint8_t value = buf[pos];
      switch (value) {
        case '"':
          s = quoted_state(s);
          break;
        case ',':
          if (s != QUOTED_FIELD) {
            i = add_position(out, i, pos);
            ++n_indexes;
          }
          s = comma_state(s);
          break;
        case '\n':
          if (s != QUOTED_FIELD) {
            i = add_position(out, i, pos);
            ++n_indexes;
          }
          s = newline_state(s);
          break;
        default:
          s = other_state(s);
      }
      ++pos;
    }
    return n_indexes;
  }

  bool parse_speculate(const uint8_t* buf, index& out, size_t len) {
    uint8_t n_threads = out.n_threads;
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
      second_pass_fut[i] = std::async(std::launch::async, second_pass_chunk, buf,
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
