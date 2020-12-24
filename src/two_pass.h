#include <unistd.h>  // for getopt
#include <cstdint>
#include <future>
#include <limits>
#include <vector>

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
  static stats first_pass_chunk(const uint8_t* buf, size_t start, size_t end) {
    stats out;
    uint64_t i = start;
    /* TODO: use SIMD to make search faster */
    while (i < end) {
      if (buf[i] == '\n') {
        bool needs_even = out.first_even_nl == null_pos;
        bool needs_odd = out.first_odd_nl == null_pos;
        bool is_even = (out.n_quotes % 2) == 0;
        if (needs_even && is_even) {
          out.first_even_nl = i;
        } else if (needs_odd && !is_even) {
          out.first_odd_nl = i;
        }
      } else if (buf[i] == '"') {
        ++out.n_quotes;
      }
      ++i;
    }
    return out;
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

  index parse(const uint8_t* buf, size_t len, size_t n_threads) {
    size_t chunk_size = len / n_threads;
    std::vector<uint64_t> chunk_pos(n_threads + 1);
    std::vector<std::future<stats>> first_pass_fut(n_threads);
    std::vector<std::future<uint64_t>> second_pass_fut(n_threads);

    for (int i = 0; i < n_threads; ++i) {
      first_pass_fut[i] = std::async(std::launch::async, first_pass_chunk, buf,
                                     chunk_size * i, chunk_size * (i + 1));
    }
    for (int i = 0; i < n_threads; ++i) {
    }

    size_t n_quotes = first_pass_fut[0].get().n_quotes;
    chunk_pos[0] = 0;
    for (int i = 1; i < n_threads; ++i) {
      auto st = first_pass_fut[i].get();
      chunk_pos[i] = (n_quotes % 2) == 0 ? st.first_even_nl : st.first_odd_nl;
      n_quotes += st.n_quotes;
    }
    chunk_pos[n_threads] = len;

    index out;
    out.n_threads = n_threads;
    out.n_indexes = new uint64_t[n_threads];

    out.indexes = new uint64_t[len];
    for (int i = 0; i < n_threads; ++i) {
      second_pass_fut[i] = std::async(std::launch::async, second_pass_chunk, buf,
                                      chunk_pos[i], chunk_pos[i + 1], &out, i);
    }

    for (int i = 0; i < n_threads; ++i) {
      out.n_indexes[i] = second_pass_fut[i].get();
    }
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
