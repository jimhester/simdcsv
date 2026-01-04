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
// Pass by const reference to avoid ABI issues with 64-byte alignment on ARM
HWY_ATTR really_inline uint64_t cmp_mask_against_input(const simd_input& in, uint8_t m) {
  const hn::ScalableTag<uint8_t> d;
  const size_t N = hn::Lanes(d);

  // Set comparison value in all lanes
  const auto match_value = hn::Set(d, m);

  uint64_t result = 0;

  // Process data in Highway vector-sized chunks
  size_t i = 0;
  for (; i + N <= 64; i += N) {
    auto vec = hn::LoadU(d, in.data + i);
    auto mask = hn::Eq(vec, match_value);
    uint64_t bits = hn::BitsFromMask(d, mask);
    result |= (bits << i);
  }

  // Handle remaining bytes with scalar code
  for (; i < 64; ++i) {
    if (in.data[i] == m) {
      result |= (1ULL << i);
    }
  }

  return result;
}

// Find quote mask using carryless multiplication (PCLMULQDQ on x86, PMULL on ARM).
// This computes a parallel prefix XOR over quote bit positions in constant time,
// replacing a O(64) scalar loop with a single SIMD instruction (~28x speedup).
// Pass by const reference to avoid ABI issues with 64-byte alignment on ARM.
HWY_ATTR really_inline uint64_t find_quote_mask(const simd_input& in, uint64_t quote_bits,
                                                uint64_t prev_iter_inside_quote) {
  // Use Highway's portable CLMul which maps to:
  // - x86: PCLMULQDQ instruction
  // - ARM: PMULL instruction
  // - Other: Software fallback
  const hn::FixedTag<uint64_t, 2> d;  // 128-bit vector of uint64_t

  // Load quote_bits into the lower 64 bits of a 128-bit vector
  // Multiplying by 0xFF...FF computes the prefix XOR (each bit becomes the XOR
  // of all bits at lower positions)
  auto quote_vec = hn::Set(d, quote_bits);
  auto all_ones = hn::Set(d, ~0ULL);

  // CLMulLower multiplies the lower 64 bits of each 128-bit block
  // The result's lower 64 bits contain the prefix XOR we need
  auto result = hn::CLMulLower(quote_vec, all_ones);

  // Extract the lower 64-bit result
  uint64_t quote_mask = hn::GetLane(result);

  // XOR with previous iteration state to handle quotes spanning chunks
  quote_mask ^= prev_iter_inside_quote;

  return quote_mask;
}

// Find quote mask with state tracking using carryless multiplication
// This version updates prev_iter_inside_quote for the next iteration
HWY_ATTR really_inline uint64_t find_quote_mask2(const simd_input& in, uint64_t quote_bits,
                                                 uint64_t& prev_iter_inside_quote) {
  // Use Highway's portable CLMul which maps to:
  // - x86: PCLMULQDQ instruction
  // - ARM: PMULL instruction
  // - Other: Software fallback
  const hn::FixedTag<uint64_t, 2> d;  // 128-bit vector of uint64_t

  // Load quote_bits into the lower 64 bits of a 128-bit vector
  // Multiplying by 0xFF...FF computes the prefix XOR
  auto quote_vec = hn::Set(d, quote_bits);
  auto all_ones = hn::Set(d, ~0ULL);

  // CLMulLower multiplies the lower 64 bits of each 128-bit block
  auto result = hn::CLMulLower(quote_vec, all_ones);

  // Extract the lower 64-bit result
  uint64_t quote_mask = hn::GetLane(result);

  // XOR with previous iteration state to handle quotes spanning chunks
  quote_mask ^= prev_iter_inside_quote;

  // Update state for next iteration: if MSB is set, we're inside a quote
  // Arithmetic right shift propagates the sign bit to all positions
  prev_iter_inside_quote = static_cast<uint64_t>(static_cast<int64_t>(quote_mask) >> 63);

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
