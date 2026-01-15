/**
 * @file fuzz_dialect_detection.cpp
 * @brief LibFuzzer target for fuzz testing dialect detection.
 */

#include "dialect.h"
#include "mem_util.h"

#include <cstddef>
#include <cstdint>
#include <cstring>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
  if (size == 0)
    return 0;
  // 16KB limit: Dialect detection only examines the first portion of data,
  // so a smaller limit allows faster fuzzing without losing coverage
  constexpr size_t MAX_INPUT_SIZE = 16 * 1024;
  if (size > MAX_INPUT_SIZE)
    size = MAX_INPUT_SIZE;

  // Use immediate RAII to prevent leaks if an exception occurs
  AlignedPtr guard = make_aligned_ptr(size, 64);
  if (!guard)
    return 0;
  uint8_t* buf = guard.get();
  std::memcpy(buf, data, size);
  std::memset(buf + size, 0, 64);

  libvroom::DialectDetector detector;
  libvroom::DetectionResult result = detector.detect(buf, size);
  (void)result.success();

  return 0;
}
