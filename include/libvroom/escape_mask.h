#pragma once

#include <cstdint>

namespace libvroom {

struct EscapeMaskResult {
  uint64_t escaped;
  uint64_t escape;
};

// Compute escaped character bitmask using simdjson's subtraction technique.
// bs_bits: backslash positions in a 64-byte block
// prev_escaped: cross-block state (0 or 1), updated on return
inline EscapeMaskResult compute_escaped_mask(uint64_t bs_bits, uint64_t& prev_escaped) {
  static constexpr uint64_t ODD_BITS = 0xAAAAAAAAAAAAAAAAULL;

  if (bs_bits == 0 && prev_escaped == 0) {
    return {0, 0};
  }

  uint64_t potential_escape = bs_bits & ~prev_escaped;
  uint64_t maybe_escaped = potential_escape << 1;
  uint64_t maybe_escaped_and_odd = maybe_escaped | ODD_BITS;
  uint64_t even_series_and_odd = maybe_escaped_and_odd - potential_escape;
  uint64_t escape_and_terminal_code = even_series_and_odd ^ ODD_BITS;

  uint64_t escaped = escape_and_terminal_code ^ (bs_bits | prev_escaped);
  uint64_t escape = escape_and_terminal_code & bs_bits;
  prev_escaped = escape >> 63;

  return {escaped, escape};
}

} // namespace libvroom
