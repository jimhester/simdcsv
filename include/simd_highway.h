#ifndef SIMD_HIGHWAY_H
#define SIMD_HIGHWAY_H

// Portable SIMD implementation using Google Highway
// Replaces x86-specific AVX2 intrinsics with cross-platform Highway API

#undef HWY_TARGET_INCLUDE
#define HWY_TARGET_INCLUDE "simd_highway.h"
#include <hwy/foreach_target.h>
#include <hwy/highway.h>
#include "common_defs.h"

HWY_BEFORE_NAMESPACE();
namespace simdcsv {
namespace HWY_NAMESPACE {

namespace hn = hwy::HWY_NAMESPACE;

// SIMD input structure - holds 64 bytes of data (equivalent to 2x __m256i)
struct simd_input {
  alignas(64) uint8_t data[64];

  // Load from pointer
  static simd_input load(const uint8_t* ptr) {
    simd_input in;
    std::memcpy(in.data, ptr, 64);
    return in;
  }
};

// Clear lowest bit (equivalent to _blsr_u64)
really_inline uint64_t clear_lowest_bit(uint64_t input_num) {
  return input_num & (input_num - 1);
}

// Get mask from lowest set bit (equivalent to _blsmsk_u64)
// Sets all bits from LSB up to and including the lowest set bit
really_inline uint64_t blsmsk_u64(uint64_t input_num) {
  return input_num ^ (input_num - 1);
}

// Count trailing zeros (equivalent to __builtin_ctzll)
really_inline int trailing_zeroes(uint64_t input_num) {
  if (input_num == 0) return 64;
  return __builtin_ctzll(input_num);
}

// Fill SIMD input from memory (equivalent to loading 2x __m256i)
really_inline simd_input fill_input(const uint8_t* ptr) {
  return simd_input::load(ptr);
}

// Population count (equivalent to _popcnt64)
really_inline long long int count_ones(uint64_t input_num) {
  return __builtin_popcountll(input_num);
}

// Compare mask against input - finds all bytes matching 'm' in 64-byte block
// Returns 64-bit mask with bits set where bytes match
really_inline uint64_t cmp_mask_against_input(simd_input in, uint8_t m) {
  const hn::ScalableTag<uint8_t> d;
  const auto target = hn::Set(d, m);

  uint64_t result = 0;
  constexpr size_t kLanes = 64;  // Always process 64 bytes

  // Process in Highway vector-sized chunks
  const size_t N = hn::Lanes(d);
  for (size_t i = 0; i < kLanes; i += N) {
    const auto chunk = hn::Load(d, in.data + i);
    const auto mask = hn::Eq(chunk, target);
    const uint64_t chunk_bits = hn::GetLane(hn::BitCast(hn::RebindToUnsigned<decltype(d)>(), hn::VecFromMask(d, mask)));

    // Build result mask bit by bit
    for (size_t j = 0; j < N && (i + j) < kLanes; ++j) {
      if ((in.data[i + j] == m)) {
        result |= (1ULL << (i + j));
      }
    }
  }

  return result;
}

// Find quote mask using carry-less multiplication for quote state tracking
// This is the tricky part - PCLMULQDQ not directly available in Highway
// We implement a portable alternative using XOR prefix computation
really_inline uint64_t find_quote_mask(simd_input in, uint64_t quote_bits,
                                       uint64_t prev_iter_inside_quote) {
  // Compute prefix XOR to determine quote state at each position
  // This is equivalent to the carry-less multiplication approach
  uint64_t quote_mask = 0;
  uint64_t state = prev_iter_inside_quote;

  for (int i = 0; i < 64; i++) {
    if (quote_bits & (1ULL << i)) {
      state ^= 1;  // Toggle quote state
    }
    quote_mask |= (state << i);
  }

  return quote_mask;
}

// Alternative find_quote_mask that updates prev_iter_inside_quote
really_inline uint64_t find_quote_mask2(simd_input in, uint64_t quote_bits,
                                        uint64_t& prev_iter_inside_quote) {
  uint64_t quote_mask = 0;
  uint64_t state = prev_iter_inside_quote;

  for (int i = 0; i < 64; i++) {
    if (quote_bits & (1ULL << i)) {
      state ^= 1;  // Toggle quote state
    }
    quote_mask |= (state << i);
  }

  // Update state for next iteration (state at position 63)
  prev_iter_inside_quote = state;

  return quote_mask;
}

// Write indexes to output array
really_inline int write(uint64_t* base_ptr, uint64_t& base, uint64_t idx, int stride,
                        uint64_t bits) {
  if (bits == 0) return 0;
  int cnt = static_cast<int>(count_ones(bits));

  // Do the first 8 all together
  for (int i = 0; i < 8; i++) {
    base_ptr[(base + i) * stride] = idx + trailing_zeroes(bits);
    bits = clear_lowest_bit(bits);
  }

  // Do the next 8 all together
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

}  // namespace HWY_NAMESPACE
}  // namespace simdcsv
HWY_AFTER_NAMESPACE();

#if HWY_ONCE
namespace simdcsv {

// Export types and functions for use in other compilation units
using simd_input = HWY_NAMESPACE::simd_input;
using HWY_NAMESPACE::clear_lowest_bit;
using HWY_NAMESPACE::blsmsk_u64;
using HWY_NAMESPACE::trailing_zeroes;
using HWY_NAMESPACE::fill_input;
using HWY_NAMESPACE::count_ones;
using HWY_NAMESPACE::cmp_mask_against_input;
using HWY_NAMESPACE::find_quote_mask;
using HWY_NAMESPACE::find_quote_mask2;
using HWY_NAMESPACE::write;

}  // namespace simdcsv
#endif  // HWY_ONCE

#endif  // SIMD_HIGHWAY_H
