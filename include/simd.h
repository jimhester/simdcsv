#include <x86intrin.h>
#include "common_defs.h"

struct simd_input {
  __m256i lo;
  __m256i hi;
};

/* result might be undefined when input_num is zero */
really_inline uint64_t clear_lowest_bit(uint64_t input_num) {
  return _blsr_u64(input_num);
}

/* result might be undefined when input_num is zero */
static inline int trailing_zeroes(uint64_t input_num) {
  return __builtin_ctzll(input_num);
}

really_inline simd_input fill_input(const uint8_t* ptr) {
  struct simd_input in;
  in.lo = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(ptr + 0));
  in.hi = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(ptr + 32));
  return in;
}

really_inline long long int count_ones(uint64_t input_num) {
  return _popcnt64(input_num);
}

// a straightforward comparison of a mask against input. 5 uops; would be
// cheaper in AVX512.
really_inline uint64_t cmp_mask_against_input(simd_input in, uint8_t m) {
  const __m256i mask = _mm256_set1_epi8(m);
  __m256i cmp_res_0 = _mm256_cmpeq_epi8(in.lo, mask);
  uint64_t res_0 = static_cast<uint32_t>(_mm256_movemask_epi8(cmp_res_0));
  __m256i cmp_res_1 = _mm256_cmpeq_epi8(in.hi, mask);
  uint64_t res_1 = _mm256_movemask_epi8(cmp_res_1);
  return res_0 | (res_1 << 32);
}

// flatten out values in 'bits' assuming that they are are to have values of idx
// plus their position in the bitvector, and store these indexes at
// base_ptr[base] incrementing base as we go
// will potentially store extra values beyond end of valid bits, so base_ptr
// needs to be large enough to handle this
really_inline void write(uint64_t* base_ptr, uint64_t idx, uint64_t bits) {
  // In some instances, the next branch is expensive because it is mispredicted.
  // Unfortunately, in other cases,
  // it helps tremendously.
  if (bits == 0) return;
  int cnt = static_cast<int>(count_ones(bits));

  // Do the first 8 all together
  for (int i = 0; i < 8; i++) {
    base_ptr[i] = idx + trailing_zeroes(bits);
    bits = clear_lowest_bit(bits);
  }

  // Do the next 8 all together (we hope in most cases it won't happen at all
  // and the branch is easily predicted).
  if (unlikely(cnt > 8)) {
    for (int i = 8; i < 16; i++) {
      base_ptr[i] = idx + trailing_zeroes(bits);
      bits = clear_lowest_bit(bits);
    }

    // Most files don't have 16+ structurals per block, so we take several basically
    // guaranteed branch mispredictions here. 16+ structurals per block means either
    // punctuation ({} [] , :) or the start of a value ("abc" true 123) every four
    // characters.
    if (unlikely(cnt > 16)) {
      int i = 16;
      do {
        base_ptr[i] = idx + trailing_zeroes(bits);
        bits = clear_lowest_bit(bits);
        i++;
      } while (i < cnt);
    }
  }

  base_ptr += cnt;
}
