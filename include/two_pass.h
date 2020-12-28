#include <unistd.h>  // for getopt
#include <cstdint>
#include <future>
#include <limits>
#include <vector>
#include "simd.h"

namespace simdcsv {

constexpr static uint64_t null_pos = std::numeric_limits<uint64_t>::max();
struct index {
  uint8_t n_threads{0};
  uint64_t* n_indexes;
  uint64_t* indexes;
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
        mask = _blsmsk_u64(1ULL << (len - idx));
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
        mask = _blsmsk_u64(1ULL << (len - idx));
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

  static uint64_t second_pass_chunk(const uint8_t* buf, size_t start, size_t end,
                                    index* out, size_t thread_id) {
    bool is_quoted = false;
    uint64_t pos = start;
    size_t n_indexes = 0;
    size_t i = thread_id;
    while (pos < end) {
      uint8_t value = buf[pos];
      if (!is_quoted && (value == ',' || value == '\n')) {
        out->indexes[i] = pos;
        i += out->n_threads;
        ++n_indexes;
      } else if (value == '"') {
        is_quoted = !is_quoted;
      }
      ++pos;
    }
    return n_indexes;
  }

  bool parse(const uint8_t* buf, index& out, size_t len) {
    uint8_t n_threads = out.n_threads;
    if (n_threads == 1) {
      out.n_indexes[0] = second_pass_simd(buf, 0, len, &out, 0);
      return true;
    }
    size_t chunk_size = len / n_threads;
    std::vector<uint64_t> chunk_pos(n_threads + 1);
    std::vector<std::future<stats>> first_pass_fut(n_threads);
    std::vector<std::future<uint64_t>> second_pass_fut(n_threads);

    for (int i = 0; i < n_threads; ++i) {
      first_pass_fut[i] = std::async(std::launch::async, first_pass_simd, buf,
                                     chunk_size * i, chunk_size * (i + 1));
    }
    for (int i = 0; i < n_threads; ++i) {
    }

    auto st = first_pass_fut[0].get();
    size_t n_quotes = st.n_quotes;
    printf("i: %i\teven: %llu\todd: %llu\tquotes: %llu\n", 0, st.first_even_nl,
           st.first_odd_nl, st.n_quotes);
    chunk_pos[0] = 0;
    for (int i = 1; i < n_threads; ++i) {
      auto st = first_pass_fut[i].get();
      printf("i: %i\teven: %llu\todd: %llu\tquotes: %llu\n", i, st.first_even_nl,
             st.first_odd_nl, st.n_quotes);
      chunk_pos[i] = (n_quotes % 2) == 0 ? st.first_even_nl : st.first_odd_nl;
      n_quotes += st.n_quotes;
    }
    chunk_pos[n_threads] = len;

    for (int i = 0; i < n_threads; ++i) {
      second_pass_fut[i] = std::async(std::launch::async, second_pass_simd, buf,
                                      chunk_pos[i], chunk_pos[i + 1], &out, i);
    }

    for (int i = 0; i < n_threads; ++i) {
      out.n_indexes[i] = second_pass_fut[i].get();
    }

    return true;
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
