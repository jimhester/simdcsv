#pragma once

// SIMD-accelerated SplitFields iterator using Google Highway
//
// The key optimization is boundary caching: when we do a 64-byte SIMD scan,
// we find ALL field boundaries in that block and cache them. Subsequent
// next() calls extract from the cache without re-scanning.

#include "libvroom/escape_mask.h"
#include "libvroom/quote_parity.h"

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string_view>

// Force inline macro for hot path functions
#if defined(__GNUC__) || defined(__clang__)
#define VROOM_FORCE_INLINE __attribute__((always_inline)) inline
#elif defined(_MSC_VER)
#define VROOM_FORCE_INLINE __forceinline
#else
#define VROOM_FORCE_INLINE inline
#endif

// Portable count trailing zeros
#if defined(__GNUC__) || defined(__clang__)
#define VROOM_CTZ64(x) __builtin_ctzll(x)
#elif defined(_MSC_VER)
#include <intrin.h>
VROOM_FORCE_INLINE unsigned long vroom_ctz64_msvc(uint64_t x) {
  unsigned long index;
  _BitScanForward64(&index, x);
  return index;
}
#define VROOM_CTZ64(x) vroom_ctz64_msvc(x)
#else
VROOM_FORCE_INLINE unsigned vroom_ctz64_portable(uint64_t x) {
  unsigned count = 0;
  while ((x & 1) == 0 && count < 64) {
    x >>= 1;
    ++count;
  }
  return count;
}
#define VROOM_CTZ64(x) vroom_ctz64_portable(x)
#endif

namespace libvroom {

// Forward declarations for Highway-based SIMD functions
// (defined in src/parser/split_fields_iter.cpp)
uint64_t scan_for_char_simd(const char* data, size_t len, char c);
uint64_t scan_for_two_chars_simd(const char* data, size_t len, char c1, char c2);

namespace detail {

constexpr size_t SIMD_SIZE = 64;

// Scan for character using Highway SIMD
VROOM_FORCE_INLINE uint64_t scan_for_char(const char* data, size_t len, char c) {
  if (len >= SIMD_SIZE) {
    return scan_for_char_simd(data, len, c);
  }
  // Scalar fallback for short data
  uint64_t mask = 0;
  for (size_t i = 0; i < len && i < 64; ++i) {
    if (data[i] == c) {
      mask |= (1ULL << i);
    }
  }
  return mask;
}

// Scan for two characters using Highway SIMD
VROOM_FORCE_INLINE uint64_t scan_for_two_chars(const char* data, size_t len, char c1, char c2) {
  if (len >= SIMD_SIZE) {
    return scan_for_two_chars_simd(data, len, c1, c2);
  }
  // Scalar fallback
  uint64_t mask = 0;
  for (size_t i = 0; i < len && i < 64; ++i) {
    if (data[i] == c1 || data[i] == c2) {
      mask |= (1ULL << i);
    }
  }
  return mask;
}

} // namespace detail

// Direct port of Polars' SplitFields iterator
class SplitFields {
public:
  VROOM_FORCE_INLINE SplitFields(const char* slice, size_t size, char separator, char quote_char,
                                 char eol_char, bool escape_backslash = false)
      : v_(slice), remaining_(size), separator_(separator), finished_(false),
        finished_inside_quote_(false), quote_char_(quote_char), quoting_(quote_char != 0),
        eol_char_(eol_char), escape_backslash_(escape_backslash), previous_valid_ends_(0),
        prev_escaped_(0) {}

  VROOM_FORCE_INLINE SplitFields(const char* slice, size_t size, std::string_view separator,
                                 char quote_char, char eol_char, bool escape_backslash = false)
      : v_(slice), remaining_(size), separator_(separator.size() == 1 ? separator[0] : '\0'),
        finished_(false), finished_inside_quote_(false), quote_char_(quote_char),
        quoting_(quote_char != 0), eol_char_(eol_char), escape_backslash_(escape_backslash),
        previous_valid_ends_(0), prev_escaped_(0), multi_byte_(separator.size() > 1) {
    if (multi_byte_) {
      multi_sep_ = separator;
    }
  }

  VROOM_FORCE_INLINE bool next(const char*& field_data, size_t& field_len, bool& needs_escaping) {
    if (finished_) {
      return false;
    }

    if (multi_byte_) {
      return next_multi_byte(field_data, field_len, needs_escaping);
    }

    // HOT PATH: Check cache first
    if (previous_valid_ends_ != 0) {
      size_t pos = VROOM_CTZ64(previous_valid_ends_);
      previous_valid_ends_ >>= (pos + 1);

      needs_escaping = (quoting_ && remaining_ > 0 && v_[0] == quote_char_);

      if (v_[pos] == eol_char_) {
        return finish_eol(field_data, field_len, needs_escaping, pos);
      }

      field_data = v_;
      field_len = pos;
      v_ += pos + 1;
      remaining_ -= pos + 1;
      return true;
    }

    if (remaining_ == 0) {
      return finish(field_data, field_len, false);
    }

    needs_escaping = false;

    size_t pos;
    if (quoting_ && v_[0] == quote_char_) {
      needs_escaping = true;
      bool not_in_field_previous_iter = true;
      pos = scan_quoted_field(not_in_field_previous_iter);
    } else {
      pos = scan_unquoted_field();
    }

    if (pos >= remaining_) {
      return finish(field_data, field_len, needs_escaping);
    }

    char c = v_[pos];
    if (c == eol_char_) {
      return finish_eol(field_data, field_len, needs_escaping, pos);
    }

    field_data = v_;
    field_len = pos;
    v_ += pos + 1;
    remaining_ -= pos + 1;
    return true;
  }

  VROOM_FORCE_INLINE const char* v() const { return v_; }
  VROOM_FORCE_INLINE size_t remaining() const { return remaining_; }
  VROOM_FORCE_INLINE bool finished() const { return finished_; }

  // Returns true if the last field consumed was a quoted field that never
  // had its closing quote found (i.e., the data ended inside a quote).
  VROOM_FORCE_INLINE bool finished_inside_quote() const { return finished_inside_quote_; }

private:
  const char* v_;
  size_t remaining_;
  char separator_;
  bool finished_;
  bool finished_inside_quote_;
  char quote_char_;
  bool quoting_;
  char eol_char_;
  bool escape_backslash_;
  uint64_t previous_valid_ends_;
  uint64_t prev_escaped_; // Cross-block state for escape mask computation
  std::string_view multi_sep_;
  bool multi_byte_ = false;

  VROOM_FORCE_INLINE bool eof_eol(char c) const { return c == separator_ || c == eol_char_; }

  VROOM_FORCE_INLINE bool finish_eol(const char*& field_data, size_t& field_len,
                                     bool needs_escaping, size_t pos) {
    (void)needs_escaping;
    finished_ = true;
    field_data = v_;
    field_len = pos;
    v_ += pos + 1;
    remaining_ -= pos + 1;
    return true;
  }

  VROOM_FORCE_INLINE bool finish(const char*& field_data, size_t& field_len, bool needs_escaping) {
    finished_ = true;
    // If we consumed all data while in a quoted field, the quote may be unclosed.
    // Check: if the field starts and ends with quote, it's properly closed.
    // Otherwise it's genuinely unclosed (e.g., "unclosed with no closing quote).
    if (needs_escaping &&
        !(remaining_ >= 2 && v_[0] == quote_char_ && v_[remaining_ - 1] == quote_char_)) {
      finished_inside_quote_ = true;
    }
    field_data = v_;
    field_len = remaining_;
    v_ += remaining_;
    remaining_ = 0;
    return true;
  }

  VROOM_FORCE_INLINE bool next_multi_byte(const char*& field_data, size_t& field_len,
                                          bool& needs_escaping) {
    if (remaining_ == 0) {
      return finish(field_data, field_len, false);
    }

    needs_escaping = false;
    bool in_quote = false;
    const size_t sep_len = multi_sep_.size();

    if (quoting_ && remaining_ > 0 && v_[0] == quote_char_) {
      needs_escaping = true;
    }

    for (size_t i = 0; i < remaining_; ++i) {
      char c = v_[i];
      if (c == quote_char_ && quoting_) {
        if (in_quote && i + 1 < remaining_ && v_[i + 1] == quote_char_) {
          ++i; // escaped quote
          continue;
        }
        in_quote = !in_quote;
      }
      if (!in_quote) {
        if (c == eol_char_) {
          return finish_eol(field_data, field_len, needs_escaping, i);
        }
        if (i + sep_len <= remaining_ && std::memcmp(v_ + i, multi_sep_.data(), sep_len) == 0) {
          field_data = v_;
          field_len = i;
          v_ += i + sep_len;
          remaining_ -= i + sep_len;
          return true;
        }
      }
    }
    return finish(field_data, field_len, needs_escaping);
  }

  VROOM_FORCE_INLINE size_t scan_quoted_field(bool& not_in_field_previous_iter) {
    size_t total_idx = 0;
    prev_escaped_ = 0; // Reset escape state for new field scan

    while (remaining_ - total_idx > detail::SIMD_SIZE) {
      const char* bytes = v_ + total_idx;

      uint64_t sep_mask = detail::scan_for_char(bytes, detail::SIMD_SIZE, separator_);
      uint64_t eol_mask = detail::scan_for_char(bytes, detail::SIMD_SIZE, eol_char_);
      uint64_t quote_mask = detail::scan_for_char(bytes, detail::SIMD_SIZE, quote_char_);

      uint64_t escaped = 0;
      if (escape_backslash_) {
        uint64_t bs_mask = detail::scan_for_char(bytes, detail::SIMD_SIZE, '\\');
        auto [esc, escape] = compute_escaped_mask(bs_mask, prev_escaped_);
        escaped = esc;
        // Remove escaped quotes from quote mask - they don't toggle quote state
        quote_mask &= ~escaped;
      }

      uint64_t end_mask = (sep_mask | eol_mask) & ~escaped;

      uint64_t not_in_quote_field = prefix_xorsum_inclusive(quote_mask);

      if (not_in_field_previous_iter) {
        not_in_quote_field = ~not_in_quote_field;
      }
      not_in_field_previous_iter = (not_in_quote_field & (1ULL << (detail::SIMD_SIZE - 1))) != 0;
      end_mask &= not_in_quote_field;

      if (end_mask != 0) {
        size_t pos = VROOM_CTZ64(end_mask);
        total_idx += pos;

        if (pos == detail::SIMD_SIZE - 1) {
          previous_valid_ends_ = 0;
        } else {
          previous_valid_ends_ = end_mask >> (pos + 1);
        }

        return total_idx;
      } else {
        total_idx += detail::SIMD_SIZE;
      }
    }

    // Scalar fallback
    bool in_field = !not_in_field_previous_iter;
    const char* bytes = v_ + total_idx;
    size_t len = remaining_ - total_idx;

    if (escape_backslash_) {
      size_t i = 0;
      // If previous SIMD block ended with a backslash, first byte is escaped
      if (prev_escaped_ != 0 && len > 0) {
        i = 1; // Skip the escaped character
      }
      for (; i < len; ++i) {
        char c = bytes[i];
        if (c == '\\' && i + 1 < len) {
          ++i; // Skip escaped character
          continue;
        }
        if (c == quote_char_) {
          in_field = !in_field;
        }
        if (!in_field && eof_eol(c)) {
          return total_idx + i;
        }
      }
    } else {
      for (size_t i = 0; i < len; ++i) {
        char c = bytes[i];
        if (c == quote_char_) {
          in_field = !in_field;
        }
        if (!in_field && eof_eol(c)) {
          return total_idx + i;
        }
      }
    }

    return remaining_;
  }

  VROOM_FORCE_INLINE size_t scan_unquoted_field() {
    size_t total_idx = 0;
    uint64_t prev_esc = 0;

    while (remaining_ - total_idx > detail::SIMD_SIZE) {
      const char* bytes = v_ + total_idx;

      uint64_t end_mask =
          detail::scan_for_two_chars(bytes, detail::SIMD_SIZE, separator_, eol_char_);

      if (escape_backslash_) {
        uint64_t bs_mask = detail::scan_for_char(bytes, detail::SIMD_SIZE, '\\');
        auto [escaped, escape] = compute_escaped_mask(bs_mask, prev_esc);
        // prev_esc is already updated by compute_escaped_mask via reference
        end_mask &= ~escaped;
      }

      if (end_mask != 0) {
        size_t pos = VROOM_CTZ64(end_mask);
        total_idx += pos;

        if (pos == detail::SIMD_SIZE - 1) {
          previous_valid_ends_ = 0;
        } else {
          previous_valid_ends_ = end_mask >> (pos + 1);
        }

        return total_idx;
      } else {
        total_idx += detail::SIMD_SIZE;
      }
    }

    // Scalar fallback
    const char* bytes = v_ + total_idx;
    size_t len = remaining_ - total_idx;

    if (escape_backslash_) {
      size_t i = 0;
      // If previous SIMD block ended with a backslash, first byte is escaped
      if (prev_esc != 0 && len > 0) {
        i = 1; // Skip the escaped character
      }
      for (; i < len; ++i) {
        if (bytes[i] == '\\' && i + 1 < len) {
          ++i; // Skip escaped character
          continue;
        }
        if (eof_eol(bytes[i])) {
          return total_idx + i;
        }
      }
    } else {
      for (size_t i = 0; i < len; ++i) {
        if (eof_eol(bytes[i])) {
          return total_idx + i;
        }
      }
    }

    return remaining_;
  }
};

} // namespace libvroom
