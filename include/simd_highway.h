#ifndef SIMD_HIGHWAY_H
#define SIMD_HIGHWAY_H

// Portable SIMD implementation using Google Highway
// Replaces x86-specific AVX2 intrinsics with cross-platform Highway API

#include <cstdint>
#include <cstring>
#include "common_defs.h"

// Include Highway for portable SIMD
#undef HWY_TARGET_INCLUDE
#include "hwy/highway.h"

namespace simdcsv {

// Namespace alias for Highway operations
namespace hn = hwy::HWY_NAMESPACE;

// SIMD input structure - holds 64 bytes of data
struct simd_input {
  alignas(64) uint8_t data[64];

  static simd_input load(const uint8_t* ptr) {
    simd_input in;
    std::memcpy(in.data, ptr, 64);
    return in;
  }
};

// Bit manipulation functions (portable, no SIMD needed)
really_inline uint64_t clear_lowest_bit(uint64_t input_num) {
  return input_num & (input_num - 1);
}

really_inline uint64_t blsmsk_u64(uint64_t input_num) {
  return input_num ^ (input_num - 1);
}

really_inline int trailing_zeroes(uint64_t input_num) {
  if (input_num == 0) return 64;
  return __builtin_ctzll(input_num);
}

really_inline long long int count_ones(uint64_t input_num) {
  return __builtin_popcountll(input_num);
}

// Fill SIMD input from memory
really_inline simd_input fill_input(const uint8_t* ptr) {
  return simd_input::load(ptr);
}

// Compare mask against input using Highway SIMD
HWY_ATTR really_inline uint64_t cmp_mask_against_input(simd_input in, uint8_t m) {
  const hn::ScalableTag<uint8_t> d;
  const size_t N = hn::Lanes(d);

  // Set comparison value in all lanes
  const auto match_value = hn::Set(d, m);

  uint64_t result = 0;
  size_t processed = 0;

  // Process data in Highway vector-sized chunks
  while (processed < 64) {
    const size_t remaining = 64 - processed;
    const size_t chunk_size = (remaining < N) ? remaining : N;

    if (chunk_size == N) {
      // Full vector load and compare
      auto vec = hn::LoadU(d, in.data + processed);
      auto mask = hn::Eq(vec, match_value);
      uint64_t bits = hn::BitsFromMask(d, mask);
      result |= (bits << processed);
    } else {
      // Handle remaining bytes with scalar fallback
      for (size_t i = 0; i < chunk_size; ++i) {
        if (in.data[processed + i] == m) {
          result |= (1ULL << (processed + i));
        }
      }
    }

    processed += N;
  }

  return result;
}

// Find quote mask using XOR prefix computation
really_inline uint64_t find_quote_mask(simd_input in, uint64_t quote_bits,
                                       uint64_t prev_iter_inside_quote) {
  uint64_t quote_mask = 0;
  uint64_t state = prev_iter_inside_quote;

  for (int i = 0; i < 64; i++) {
    if (quote_bits & (1ULL << i)) {
      state ^= 1;
    }
    quote_mask |= (state << i);
  }

  return quote_mask;
}

really_inline uint64_t find_quote_mask2(simd_input in, uint64_t quote_bits,
                                        uint64_t& prev_iter_inside_quote) {
  uint64_t quote_mask = 0;
  uint64_t state = prev_iter_inside_quote;

  for (int i = 0; i < 64; i++) {
    if (quote_bits & (1ULL << i)) {
      state ^= 1;
    }
    quote_mask |= (state << i);
  }

  prev_iter_inside_quote = state;
  return quote_mask;
}

// Write indexes to output array
really_inline int write(uint64_t* base_ptr, uint64_t& base, uint64_t idx, int stride,
                        uint64_t bits) {
  if (bits == 0) return 0;
  int cnt = static_cast<int>(count_ones(bits));

  for (int i = 0; i < 8; i++) {
    base_ptr[(base + i) * stride] = idx + trailing_zeroes(bits);
    bits = clear_lowest_bit(bits);
  }

  if (unlikely(cnt > 8)) {
    for (int i = 8; i < 16; i++) {
      base_ptr[(base + i) * stride] = idx + trailing_zeroes(bits);
      bits = clear_lowest_bit(bits);
    }

    if (unlikely(cnt > 16)) {
      int i = 16;
      do {
        base_ptr[(base + i) * stride] = idx + trailing_zeroes(bits);
        bits = clear_lowest_bit(bits);
        i++;
      } while (i < cnt);
    }
  }

  base += cnt;
  return cnt;
}

}  // namespace simdcsv

#endif  // SIMD_HIGHWAY_H
