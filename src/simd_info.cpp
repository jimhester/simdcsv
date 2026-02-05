#include "libvroom/simd_info.h"

#include "hwy/targets.h"

namespace libvroom {

std::string simd_best_target() {
  int64_t targets = hwy::SupportedTargets();
  // Highway uses lower bit positions for better targets
  // (AVX3=bit8, AVX2=bit9, ..., SCALAR=bit62), so best = lowest set bit.
  int64_t best = targets & -targets; // isolate lowest set bit
  return hwy::TargetName(best);
}

std::vector<std::string> simd_supported_targets() {
  std::vector<std::string> result;
  int64_t targets = hwy::SupportedTargets();
  // Iterate from lowest set bit (best) to highest (worst)
  while (targets != 0) {
    int64_t lowest = targets & -targets; // isolate lowest set bit
    result.push_back(hwy::TargetName(lowest));
    targets &= targets - 1; // clear lowest set bit
  }
  return result;
}

} // namespace libvroom
