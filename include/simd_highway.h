#ifndef SIMD_HIGHWAY_H
#define SIMD_HIGHWAY_H

// Portable SIMD implementation using Google Highway
// Replaces x86-specific AVX2 intrinsics with cross-platform Highway API

#include <cstdint>
#include <cstring>
#include <cstdio>  // for fprintf debugging
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
// Pass by const reference to avoid ABI issues with 64-byte alignment on ARM
HWY_ATTR really_inline uint64_t cmp_mask_against_input(const simd_input& in, uint8_t m) {
  // ===== HIGHWAY_DEBUG_START =====
  fprintf(stderr, "[HIGHWAY_DEBUG] cmp_mask_against_input ENTRY: m=%u\n", m);
  // ===== HIGHWAY_DEBUG_END =====

  const hn::ScalableTag<uint8_t> d;
  const size_t N = hn::Lanes(d);

  // ===== HIGHWAY_DEBUG_START =====
  fprintf(stderr, "[HIGHWAY_DEBUG] ScalableTag created: N=%zu lanes\n", N);
  // ===== HIGHWAY_DEBUG_END =====

  // Set comparison value in all lanes
  const auto match_value = hn::Set(d, m);

  // ===== HIGHWAY_DEBUG_START =====
  fprintf(stderr, "[HIGHWAY_DEBUG] Set() completed\n");
  // ===== HIGHWAY_DEBUG_END =====

  uint64_t result = 0;

  // Process data in Highway vector-sized chunks
  size_t i = 0;
  size_t iteration = 0;
  for (; i + N <= 64; i += N) {
    // ===== HIGHWAY_DEBUG_START =====
    fprintf(stderr, "[HIGHWAY_DEBUG] SIMD iteration %zu: i=%zu, i+N=%zu\n", iteration, i, i+N);
    // ===== HIGHWAY_DEBUG_END =====

    auto vec = hn::LoadU(d, in.data + i);

    // ===== HIGHWAY_DEBUG_START =====
    fprintf(stderr, "[HIGHWAY_DEBUG] LoadU completed at i=%zu\n", i);
    // ===== HIGHWAY_DEBUG_END =====

    auto mask = hn::Eq(vec, match_value);

    // ===== HIGHWAY_DEBUG_START =====
    fprintf(stderr, "[HIGHWAY_DEBUG] Eq completed at i=%zu\n", i);
    // ===== HIGHWAY_DEBUG_END =====

    uint64_t bits = hn::BitsFromMask(d, mask);

    // ===== HIGHWAY_DEBUG_START =====
    fprintf(stderr, "[HIGHWAY_DEBUG] BitsFromMask completed: bits=0x%llx\n", (unsigned long long)bits);
    // ===== HIGHWAY_DEBUG_END =====

    result |= (bits << i);

    // ===== HIGHWAY_DEBUG_START =====
    fprintf(stderr, "[HIGHWAY_DEBUG] Bit shift completed: result=0x%llx\n", (unsigned long long)result);
    iteration++;
    // ===== HIGHWAY_DEBUG_END =====
  }

  // ===== HIGHWAY_DEBUG_START =====
  fprintf(stderr, "[HIGHWAY_DEBUG] SIMD loop done, i=%zu, starting scalar loop\n", i);
  // ===== HIGHWAY_DEBUG_END =====

  // Handle remaining bytes with scalar code
  for (; i < 64; ++i) {
    if (in.data[i] == m) {
      result |= (1ULL << i);
    }
  }

  // ===== HIGHWAY_DEBUG_START =====
  fprintf(stderr, "[HIGHWAY_DEBUG] cmp_mask_against_input EXIT: result=0x%llx\n", (unsigned long long)result);
  // ===== HIGHWAY_DEBUG_END =====

  return result;
}

// Find quote mask using XOR prefix computation
// Pass by const reference to avoid ABI issues with 64-byte alignment on ARM
really_inline uint64_t find_quote_mask(const simd_input& in, uint64_t quote_bits,
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

really_inline uint64_t find_quote_mask2(const simd_input& in, uint64_t quote_bits,
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
